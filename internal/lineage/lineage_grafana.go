package lineage

import (
	"fmt"
	"pg_lineage/pkg/log"
	"strconv"

	"github.com/grafana/grafana-openapi-client-go/models"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type Panel struct {
	Datasource  any    `json:"datasource"`
	Description string `json:"description,omitempty"`
	ID          int    `json:"id"`
	Targets     []struct {
		Query    string `json:"query"`
		RawQuery bool   `json:"rawQuery"`
		RawSQL   string `json:"rawSql"`
	} `json:"targets,omitempty"`
	Title string `json:"title"`
	Type  string `json:"type"`
}

type Dashboard struct {
	ID         int      `json:"id"`
	Panels     []*Panel `json:"panels"`
	Tags       []string `json:"tags"`
	Templating struct {
		List []struct {
			Datasource any    `json:"datasource,omitempty"`
			Label      string `json:"label"`
			Query      any    `json:"query,omitempty"`
			Type       string `json:"type"`
		} `json:"list"`
	} `json:"templating"`
	Time struct {
		From string `json:"from"`
		To   string `json:"to"`
	} `json:"time"`
	Timezone string `json:"timezone"`
	Title    string `json:"title"`
	UID      string `json:"uid"`
	Version  int    `json:"version"`
}

type DashboardFullWithMeta struct {
	// dashboard
	Dashboard Dashboard `json:"dashboard,omitempty"`

	// meta
	Meta *models.DashboardMeta `json:"meta,omitempty"`
}

func (p *Panel) GetID() string {
	return strconv.Itoa(p.ID)
}

func (p *Panel) IsTemp() bool {
	return false
}

func CreatePanelGraph(session neo4j.Session, p *Panel, d *DashboardFullWithMeta, dependencies []*Table) error {

	// 开始事务
	tx, err := session.BeginTransaction()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Close()

	if err := CreatePanelNode(tx, p, d); err != nil {
		return fmt.Errorf("failed to insert Panel node: %w", err)
	}

	// 插入表节点并创建边
	for _, r := range dependencies {
		// fix: ShrinkGraph 中仍然存在临时节点
		if r.SchemaName == "" {
			log.Warnf("Invalid r.SchemaName: %+v", r)
			continue
		}

		if err := CreatePanelEdge(tx, p, d, r); err != nil {
			// return fmt.Errorf("failed to create relationship: %w", err)
			log.Errorf("failed to create relationship: %w", err)
			continue
		}
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// 创建图中节点
func CreatePanelNode(tx neo4j.Transaction, p *Panel, d *DashboardFullWithMeta) error {
	// 需要将 ID 作为唯一主键
	// CREATE CONSTRAINT ON (cc:Lineage:Grafana) ASSERT cc.id IS UNIQUE
	_, err := tx.Run(`
		MERGE (n:Lineage:Grafana:`+d.Meta.FolderTitle+` {id: $id}) 
		ON CREATE SET n.dashboard_title = $dashboard_title, n.dashboard_uid = $dashboard_uid,
					  n.panel_type = $panel_type, n.panel_title = $panel_title, n.panel_description = $panel_description,
					  n.created = $created, n.created_by = $created_by, n.updated = $updated, n.updated_by = $updated_by,
					  n.rawsql = $rawsql, n.udt = timestamp()
		ON MATCH SET n.udt = timestamp()
		RETURN n.id
	`,
		map[string]interface{}{
			"id":                fmt.Sprintf("%d:%d", d.Dashboard.ID, p.ID),
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

	return err
}

// 创建图中边
func CreatePanelEdge(tx neo4j.Transaction, p *Panel, d *DashboardFullWithMeta, t *Table) error {
	_, err := tx.Run(`
		MATCH (pnode:Lineage:PG:`+t.SchemaName+` {id: $pid}), (cnode:Lineage:Grafana {id: $cid})
		CREATE (pnode)-[e:DownStream {udt: timestamp()}]->(cnode)
		RETURN e
	`, map[string]interface{}{
		"pid": fmt.Sprintf("%s.%s.%s", t.Database, t.SchemaName, t.RelName),
		"cid": fmt.Sprintf("%d:%d", d.Dashboard.ID, p.ID),
	})

	return err
}
