package writer

import (
	"errors"
	"fmt"
	"pg_lineage/internal/service"
	"pg_lineage/pkg/config"
	"pg_lineage/pkg/log"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type Neo4jLineageWriter struct {
	session neo4j.Session // 已在初始化时创建
}

func InitNeo4jDriver(c *config.Neo4jService) (neo4j.Driver, error) {
	if c == nil {
		return nil, fmt.Errorf("postgres config is nil")
	}

	return neo4j.NewDriver(c.URL, neo4j.BasicAuth(c.User, c.Password, ""))
}

func (w *Neo4jLineageWriter) Init(ctx *WriterContext) error {
	if ctx.Neo4jDriver == nil {
		return errors.New("Neo4j driver not provided")
	}

	w.session = ctx.Neo4jDriver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})

	return nil
}

func (w *Neo4jLineageWriter) ResetGraph() error {

	_, err := w.session.WriteTransaction(func(tx neo4j.Transaction) (any, error) {
		return tx.Run("MATCH (n:lineage) DETACH DELETE n", nil)
	})

	return err
}

func (w *Neo4jLineageWriter) WriteDashboardNode(d *service.DashboardFullWithMeta, s config.GrafanaService) error {
	_, err := w.session.WriteTransaction(func(tx neo4j.Transaction) (any, error) {
		return tx.Run(`
				MERGE (d:lineage:grafana:`+escapeLabel(s.Host)+`:`+escapeLabel(d.Meta.FolderTitle)+`:dashboard {id: $id})
				ON CREATE SET d.title = $title, d.uid = $uid, d.created = $created, d.created_by = $created_by
				ON MATCH SET d.updated = $updated, d.updated_by = $updated_by
				RETURN d.id
			`, map[string]any{
			"id":         fmt.Sprintf("%s>%d", s.Host, d.Dashboard.ID),
			"uid":        d.Dashboard.UID,
			"title":      d.Dashboard.Title,
			"created":    d.Meta.Created.String(),
			"created_by": d.Meta.CreatedBy,
			"updated":    d.Meta.Updated.String(),
			"updated_by": d.Meta.UpdatedBy,
		})
	})
	return err
}

func (w *Neo4jLineageWriter) WritePanelNode(p *service.Panel, d *service.DashboardFullWithMeta, s config.GrafanaService, dependencies []*service.SqlTableDependency, ds config.PostgresService) error {
	_, err := w.session.WriteTransaction(func(tx neo4j.Transaction) (any, error) {

		// 需要将 ID 作为唯一主键
		// CREATE CONSTRAINT ON (cc:lineage:grafana) ASSERT cc.id IS UNIQUE
		return tx.Run(`
				MERGE (n:lineage:grafana:`+escapeLabel(s.Host)+`:`+escapeLabel(d.Meta.FolderTitle)+`:panel {id: $id}) 
				ON CREATE SET n.dashboard_title = $dashboard_title, n.dashboard_uid = $dashboard_uid,
							n.panel_type = $panel_type, n.panel_title = $panel_title, n.panel_description = $panel_description,
							n.created = $created, n.created_by = $created_by, n.updated = $updated, n.updated_by = $updated_by,
							n.rawsql = $rawsql, n.udt = timestamp()
				ON MATCH SET n.udt = timestamp()
				RETURN n.id
			`,
			map[string]any{
				"id":                fmt.Sprintf("%s>%d>%d", s.Host, d.Dashboard.ID, p.ID),
				"dashboard_title":   d.Dashboard.Title,
				"dashboard_uid":     d.Dashboard.UID,
				"panel_type":        p.Type,
				"panel_title":       p.Title,
				"panel_description": p.Description,
				"rawsql":            "",
				"created":           d.Meta.Created.String(),
				"created_by":        d.Meta.CreatedBy,
				"updated":           d.Meta.Updated.String(),
				"updated_by":        d.Meta.UpdatedBy,
			})

	})
	return err
}

func (w *Neo4jLineageWriter) WriteTable2PanelEdge(p *service.Panel, d *service.DashboardFullWithMeta, s config.GrafanaService, dependencies []*service.SqlTableDependency, ds config.PostgresService) error {
	_, err := w.session.WriteTransaction(func(tx neo4j.Transaction) (any, error) {
		// 实际写入逻辑
		for _, dep := range dependencies {
			for _, t := range dep.Tables {

				_, err := tx.Run(`
					MATCH (pnode:lineage:`+ds.Type+` {id: $pid}), (cnode:lineage:grafana {id: $cid})
					CREATE (pnode)-[e:downstream {udt: timestamp()}]->(cnode)
					RETURN e
				`, map[string]any{
					"pid": fmt.Sprintf("%s.%s.%s", t.Database, t.SchemaName, t.RelName),
					"cid": fmt.Sprintf("%s>%d>%d", s.Host, d.Dashboard.ID, p.ID),
				})
				if err != nil {
					log.Error(err)
				}
			}
		}
		return nil, nil
	})
	return err
}

func (w *Neo4jLineageWriter) WriteDash2PanelEdge(p *service.Panel, d *service.DashboardFullWithMeta, s config.GrafanaService) error {
	_, err := w.session.WriteTransaction(func(tx neo4j.Transaction) (any, error) {
		// 需要将 ID 作为唯一主键
		return tx.Run(`
				MATCH (pnode:lineage:grafana:dashboard {id: $pid}), (cnode:lineage:grafana:panel {id: $cid})
				CREATE (pnode)-[e:contain {udt: timestamp()}]->(cnode)
				RETURN e
			`,

			map[string]any{
				"pid": fmt.Sprintf("%s>%d", s.Host, d.Dashboard.ID),
				"cid": fmt.Sprintf("%s>%d>%d", s.Host, d.Dashboard.ID, p.ID),
			})
	})
	return err
}

func escapeLabel(label string) string {
	return "`" + label + "`"
}

// 针对 Neo4j 建模，暂定的建模方案：
// 1. 点的 Label 最好是业务分类，但这得在元数据管理系统完备之后，前期先按照 Schema 做拆解
// 2. 线之前定义为了 UDF / Flink Job，但有一定的局限性，考虑将 UDF 转化为点保存
// 3. 对现有服务的改造，就时重写下面两个方法

// 创建图中节点
func (w *Neo4jLineageWriter) WriteTableNode(r *service.Table, s config.PostgresService) error {
	_, err := w.session.WriteTransaction(func(tx neo4j.Transaction) (any, error) {
		// 需要将 ID 作为唯一主键
		// CREATE CONSTRAINT ON (cc:lineage:postgresql) ASSERT cc.id IS UNIQUE
		return tx.Run(`
				MERGE (n:lineage:`+s.Type+`:`+escapeLabel(r.Database)+`:`+r.SchemaName+` {id: $id})
				ON CREATE SET n.database = $database, n.schemaname = $schemaname, n.relname = $relname, n.udt = timestamp(),
							n.relpersistence = $relpersistence, n.calls = $calls
				ON MATCH SET n.udt = timestamp(), n.relpersistence = $relpersistence, n.calls = n.calls + $calls
				RETURN n.id
			`,
			map[string]any{
				"id":             r.Database + "." + r.GetID(),
				"database":       r.Database,
				"schemaname":     r.SchemaName,
				"relname":        r.RelName,
				"relpersistence": r.RelPersistence,
				"calls":          r.Calls,
			})
	})
	return err
}

// 创建图中边
func (w *Neo4jLineageWriter) WriteFuncEdge(r *service.Udf, s config.PostgresService) error {
	_, err := w.session.WriteTransaction(func(tx neo4j.Transaction) (any, error) {
		return tx.Run(`
		MATCH (pnode {id: $pid}), (cnode {id: $cid})
		CREATE (pnode)-[e:downstream {id: $id, database: $database, schemaname: $schemaname, procname: $procname, calls: $calls, udt: timestamp()}]->(cnode)
		RETURN e
	`, map[string]any{
			"pid":        r.Database + "." + r.SrcID,
			"cid":        r.Database + "." + r.DestID,
			"id":         r.Database + "." + r.GetID(),
			"database":   r.Database,
			"schemaname": r.SchemaName,
			"procname":   r.ProcName,
			"calls":      r.Calls,
		})
	})

	return err
}

func (w *Neo4jLineageWriter) CompleteTableNode(r *service.Table, s config.PostgresService) error {
	// Create or update Neo4j node with PostgreSQL data
	cypher := `
		MERGE (n:lineage:` + s.Type + `:` + escapeLabel(r.Database) + `:` + r.SchemaName + ` {id: $id})
		ON CREATE SET n.database = $database, n.schemaname = $schemaname, n.relname = $relname,
					n.udt = timestamp(), n.description = $description,
					n.seq_scan = $seq_scan, n.seq_tup_read = $seq_tup_read,
					n.idx_scan = $idx_scan, n.idx_tup_fetch = $idx_tup_fetch
		ON MATCH SET n.udt = timestamp(), n.description = $description,
					n.seq_scan = $seq_scan, n.seq_tup_read = $seq_tup_read,
					n.idx_scan = $idx_scan, n.idx_tup_fetch = $idx_tup_fetch
	`
	_, err := w.session.WriteTransaction(func(transaction neo4j.Transaction) (any, error) {
		result, err := transaction.Run(cypher, map[string]any{
			"id":            r.Database + "." + r.GetID(),
			"database":      r.Database,
			"schemaname":    r.SchemaName,
			"relname":       r.RelName,
			"seq_scan":      r.SeqScan,
			"seq_tup_read":  r.SeqTupRead,
			"idx_scan":      r.IdxScan,
			"idx_tup_fetch": r.IdxTupFetch,
			"description":   r.Comment,
		})
		if err != nil {
			return nil, err
		}
		return result.Consume()
	})

	return err
}
