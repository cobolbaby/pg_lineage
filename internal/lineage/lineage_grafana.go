package lineage

import (
	"fmt"
	"strconv"

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
	Panels     []Panel  `json:"panels"`
	Tags       []string `json:"tags"`
	Templating struct {
		List []struct {
			Datasource any    `json:"datasource"`
			Label      any    `json:"label"`
			Query      string `json:"query"`
			Type       string `json:"type"`
		} `json:"list"`
	} `json:"templating"`
	Time struct {
		From string `json:"from"`
		To   string `json:"to"`
	} `json:"time"`
	Timezone    string `json:"timezone"`
	Title       string `json:"title"`
	UID         string `json:"uid"`
	Version     int    `json:"version"`
	FolderTitle string `json:"folderTitle,omitempty"`
}

func (p *Panel) GetID() string {
	return strconv.Itoa(p.ID)
}

func (p *Panel) IsTemp() bool {
	return false
}

// 创建图中节点
func CreatePanelNode(tx neo4j.Transaction, p *Panel, d *Dashboard) (*Panel, error) {
	// 需要将 ID 作为唯一主键
	// CREATE CONSTRAINT ON (cc:Lineage:Grafana) ASSERT cc.id IS UNIQUE
	records, err := tx.Run(`
		MERGE (n:Lineage:Grafana:`+d.FolderTitle+` {id: $id}) 
		ON CREATE SET n.dashboard = $dashboard, n.panel = $panel, n.rawsql = $rawsql, n.udt = timestamp()
		ON MATCH SET n.udt = timestamp()
		RETURN n.id
	`,
		map[string]interface{}{
			"id":        fmt.Sprintf("%d:%d", d.ID, p.ID),
			"dashboard": d.Title,
			"panel":     p.Title,
			"rawsql":    "",
		})
	// In face of driver native errors, make sure to return them directly.
	// Depending on the error, the driver may try to execute the function again.
	if err != nil {
		return nil, err
	}
	record, err := records.Single()
	if err != nil {
		return nil, err
	}
	// You can also retrieve values by name, with e.g. `id, found := record.Get("n.id")`
	p.ID = record.Values[0].(int)
	return p, nil
}

// 创建图中边
func CreatePanelEdge(tx neo4j.Transaction, p *Panel, d *Dashboard, t *Table) error {
	_, err := tx.Run(`
		MATCH (pnode:Lineage:PG:$schmea {id: $pid}), (cnode:Lineage:Grafana {id: $cid})
		CREATE (pnode)-[e:DownStream {udt: timestamp()}]->(cnode)
		RETURN e
	`, map[string]interface{}{
		"schema": t.SchemaName,
		"pid":    fmt.Sprintf("%s.%s.%s", t.Database, t.SchemaName, t.RelName),
		"cid":    fmt.Sprintf("%d:%d", d.ID, p.ID),
	})

	return err
}
