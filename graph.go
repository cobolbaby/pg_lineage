package main

import (
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

func CreateGraph(driver neo4j.Driver, graph *SqlTree) error {
	// Sessions are short-lived, cheap to create and NOT thread safe. Typically create one or more sessions
	// per request in your web application. Make sure to call Close on the session when done.
	// For multi-database support, set sessionConfig.DatabaseName to requested database
	// Session config will default to write mode, if only reads are to be used configure session for
	// read mode.
	session := driver.NewSession(neo4j.SessionConfig{})
	defer session.Close()

	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {

		// write one graph struc to Neo4j
		// 顺序遍历节点及其依赖

		// 创建点
		for _, v := range deduplicateNodes(append(graph.Source, graph.Target...)) {
			if _, err := CreateNode(tx, v); err != nil {
				return nil, err
			}
		}
		// 创建线
		for _, v := range graph.Edge {
			if _, err := CreateEdge(tx, v); err != nil {
				return nil, err
			}
		}

		return nil, nil
	})

	return err
}

func deduplicateNodes(nodes []*Record) []*Record {
	var unique []*Record
	uniqueMap := make(map[string]int)

	for _, v := range nodes {
		if v.SchemaName == "" {
			// TODO:需要定义为 proc 的名称
			v.SchemaName = ""
		}
		v.ID = v.SchemaName + "." + v.RelName

		if _, ok := uniqueMap[v.ID]; !ok {
			uniqueMap[v.ID] = 1
			unique = append(unique, v)
		} else {
			uniqueMap[v.ID]++
		}
	}

	return unique
}

// 创建图中节点
func CreateNode(tx neo4j.Transaction, r *Record) (*Record, error) {
	// 需要将 ID 作为唯一主键
	// CREATE CONSTRAINT ON (cc:CreditCard) ASSERT cc.number IS UNIQUE
	// MERGE (n:Table { id:  }) ON CREATE SET n.created = timestamp() ON MATCH SET n.lastAccessed = timestamp() RETURN n.name, n.created, n.lastAccessed
	// records, err := tx.Run("CREATE (n:Table { id: $id, schema_name: $schema_name, rel_name: $rel_name, type: $type }) RETURN n.id",
	records, err := tx.Run(`
		MERGE (n:Table { id: $id }) 
		ON CREATE SET n.schema_name = $schema_name, n.rel_name = $rel_name, n.type = $type, n.udt = timestamp()
		ON MATCH SET n.udt = timestamp()
		RETURN n.id
	`,
		map[string]interface{}{
			"id":          r.ID,
			"schema_name": r.SchemaName,
			"rel_name":    r.RelName,
			"type":        r.Type,
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
	return nil, nil
}
