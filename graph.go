package main

import (
	"log"
	"strings"

	"github.com/cobolbaby/lineage/depgraph"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

func CreateGraph(driver neo4j.Driver, graph *depgraph.Graph, extends *Op) error {
	// Sessions are short-lived, cheap to create and NOT thread safe. Typically create one or more sessions
	// per request in your web application. Make sure to call Close on the session when done.
	// For multi-database support, set sessionConfig.DatabaseName to requested database
	// Session config will default to write mode, if only reads are to be used configure session for
	// read mode.
	session := driver.NewSession(neo4j.SessionConfig{})
	defer session.Close()

	log.Printf("sqlTree.ShrinkGraph: %+v", graph)
	for i, layer := range graph.TopoSortedLayers() {
		log.Printf("ShrinkGraph %d: %s\n", i, strings.Join(layer, ", "))
	}

	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		// 创建点
		for _, v := range graph.GetNodes() {
			r, _ := v.(*Record)
			r.Database = graph.GetNamespace()
			if _, err := CreateNode(tx, r); err != nil {
				return nil, err
			}
		}
		// 创建线
		for k, v := range graph.GetRelationships() {
			for kk := range v {
				extends.SrcID = k // 不含 namespace
				extends.DestID = kk
				extends.Database = graph.GetNamespace()
				if _, err := CreateEdge(tx, extends); err != nil {
					return nil, err
				}
			}
		}

		return nil, nil
	})

	return err
}

// 创建图中节点
func CreateNode(tx neo4j.Transaction, r *Record) (*Record, error) {
	// 需要将 ID 作为唯一主键
	// CREATE CONSTRAINT ON (cc:CreditCard) ASSERT cc.number IS UNIQUE
	// MERGE (n:Table { id:  }) ON CREATE SET n.created = timestamp() ON MATCH SET n.lastAccessed = timestamp() RETURN n.name, n.created, n.lastAccessed
	records, err := tx.Run(`
		MERGE (n:Table {id: $id}) 
		ON CREATE SET n.database = $database, n.schemaname = $schemaname, n.relname = $relname, n.udt = timestamp(), n.size = $size, n.visited = $visited
		ON MATCH SET n.udt = timestamp(), n.size = $size, n.visited = $visited
		RETURN n.id
	`,
		map[string]interface{}{
			"id":         r.Database + "." + r.GetID(),
			"schemaname": r.SchemaName,
			"relname":    r.RelName,
			"database":   r.Database,
			"size":       r.Size,
			"visited":    r.Visited,
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
	r.ID = record.Values[0].(string)
	return r, nil
}

// 创建图中边
func CreateEdge(tx neo4j.Transaction, r *Op) (*Op, error) {
	_, err := tx.Run(`
		MATCH (pnode:Table {id: $pid}), (cnode:Table {id: $cid})
		CREATE (pnode)-[e:DOWNSTREAM {id: $id, database: $database, schemaname: $schemaname, procname: $procname}]->(cnode)
		RETURN e
	`, map[string]interface{}{
		"id":         r.Database + "." + r.GetID(),
		"database":   r.Database,
		"schemaname": r.SchemaName,
		"procname":   r.ProcName,
		"pid":        r.Database + "." + r.SrcID,
		"cid":        r.Database + "." + r.DestID,
	})

	return nil, err
}
