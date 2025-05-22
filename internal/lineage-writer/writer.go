package writer

import (
	"database/sql"
	"pg_lineage/internal/service"
	"pg_lineage/pkg/config"
	"pg_lineage/pkg/depgraph"
	"pg_lineage/pkg/log"
	"strings"

	"sync"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type WriterContext struct {
	Neo4jDriver neo4j.Driver // 可选
	PgDriver    *sql.DB      // 可选：标准 Go SQL DB 接口
}

func InitWriterManager(ctx *WriterContext) *WriterManager {
	defaultWriterManager := &WriterManager{}

	defaultWriterManager.mu.Lock()
	defer defaultWriterManager.mu.Unlock()

	// 根据上下文注册需要的 writer
	if ctx.Neo4jDriver != nil {
		w := &Neo4jLineageWriter{}
		w.Init(ctx)

		defaultWriterManager.writers = append(defaultWriterManager.writers, w)
	}
	if ctx.PgDriver != nil {
		w := &PGLineageWriter{}
		w.Init(ctx)
		defaultWriterManager.writers = append(defaultWriterManager.writers, w)
	}

	return defaultWriterManager
}

type LineageWriter interface {
	Init(ctx *WriterContext) error
	WriteDashboardNode(d *service.DashboardFullWithMeta, s config.GrafanaService) error
	WritePanelNode(p *service.Panel, d *service.DashboardFullWithMeta, s config.GrafanaService) error
	WriteDash2PanelEdge(p *service.Panel, d *service.DashboardFullWithMeta, s config.GrafanaService) error
	WriteTable2PanelEdge(p *service.Panel, d *service.DashboardFullWithMeta, s config.GrafanaService, t []*service.Table, ds config.PostgresService) error
	WriteTableNode(t *service.Table, s config.PostgresService) error
	WriteFuncEdge(t *service.Udf, s config.PostgresService) error
	CompleteTableNode(t *service.Table, s config.PostgresService) error
	ResetGraph() error
}

type WriterManager struct {
	writers []LineageWriter
	mu      sync.RWMutex
}

type LineageWriterFunc func(LineageWriter) error

func (w *WriterManager) apply(fn LineageWriterFunc) error {
	for _, writer := range w.writers {
		if err := fn(writer); err != nil {
			log.Error(err)
		}
	}
	return nil
}

func (w *WriterManager) writeDashboardNode(d *service.DashboardFullWithMeta, s config.GrafanaService) error {
	return w.apply(func(writer LineageWriter) error {
		return writer.WriteDashboardNode(d, s)
	})
}

func (w *WriterManager) writePanelNode(p *service.Panel, d *service.DashboardFullWithMeta, s config.GrafanaService) error {
	return w.apply(func(writer LineageWriter) error {
		return writer.WritePanelNode(p, d, s)
	})
}

func (w *WriterManager) writeDash2PanelEdge(p *service.Panel, d *service.DashboardFullWithMeta, s config.GrafanaService) error {
	return w.apply(func(writer LineageWriter) error {
		return writer.WriteDash2PanelEdge(p, d, s)
	})
}

func (w *WriterManager) writeTable2PanelEdge(p *service.Panel, d *service.DashboardFullWithMeta, s config.GrafanaService, t []*service.Table, ds config.PostgresService) error {
	return w.apply(func(writer LineageWriter) error {
		return writer.WriteTable2PanelEdge(p, d, s, t, ds)
	})
}

func (w *WriterManager) writeTableNode(t *service.Table, s config.PostgresService) error {
	return w.apply(func(writer LineageWriter) error {
		return writer.WriteTableNode(t, s)
	})
}

func (w *WriterManager) writeFuncEdge(t *service.Udf, s config.PostgresService) error {
	return w.apply(func(writer LineageWriter) error {
		return writer.WriteFuncEdge(t, s)
	})
}

func (w *WriterManager) CompleteTableNode(t *service.Table, s config.PostgresService) error {
	return w.apply(func(writer LineageWriter) error {
		return writer.CompleteTableNode(t, s)
	})
}

func (w *WriterManager) ResetGraph() error {
	return w.apply(func(writer LineageWriter) error {
		return writer.ResetGraph()
	})
}

func (w *WriterManager) CreateGraphGrafana(p *service.Panel, d *service.DashboardFullWithMeta, s config.GrafanaService, dependencies []*service.Table, ds config.PostgresService) error {

	if err := w.writeDashboardNode(d, s); err != nil {
		return err
	}

	if err := w.writePanelNode(p, d, s); err != nil {
		return err
	}
	if err := w.writeDash2PanelEdge(p, d, s); err != nil {
		return err
	}

	// fix: filter dependencies with schemaname
	// dependencies := filterEmptySchema(dependencies)

	if err := w.writeTable2PanelEdge(p, d, s, dependencies, ds); err != nil {
		return err
	}

	return nil
}

func (w *WriterManager) CreateGraphPostgres(graph *depgraph.Graph, udf *service.Udf, s config.PostgresService) error {

	log.Infof("ShrinkGraph: %+v", graph)
	for i, layer := range graph.TopoSortedLayers() {
		log.Infof("ShrinkGraph %d: %s", i, strings.Join(layer, ", "))
	}

	// 创建点
	for _, v := range graph.GetNodes() {
		r, _ := v.(*service.Table)

		// TODO: Graph 中仍然存在临时节点, Why?
		if r.SchemaName == "" {
			log.Warnf("Invalid r.SchemaName: %+v", r)
			continue
		}

		r.Database = graph.GetNamespace()
		r.Calls = udf.Calls

		w.writeTableNode(r, s)
	}
	// 创建线
	for k, v := range graph.GetRelationships() {
		for kk := range v {

			udf.SrcID = k // 不含 namespace
			udf.DestID = kk
			udf.Database = graph.GetNamespace()

			w.writeFuncEdge(udf, s)
		}
	}

	return nil
}

// func filterEmptySchema(dependencies []*service.Table) []*service.Table {
// 	var result []*service.Table
// 	for _, t := range dependencies {
// 		if t.SchemaName != "" {
// 			result = append(result, t)
// 		}
// 	}
// 	return result
// }
