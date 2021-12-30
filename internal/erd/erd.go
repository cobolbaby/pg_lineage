package erd

import (
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

func ResetGraph(driver neo4j.Driver) error {
	// Sessions are short-lived, cheap to create and NOT thread safe. Typically create one or more sessions
	// per request in your web application. Make sure to call Close on the session when done.
	// For multi-database support, set sessionConfig.DatabaseName to requested database
	// Session config will default to write mode, if only reads are to be used configure session for
	// read mode.
	session := driver.NewSession(neo4j.SessionConfig{})
	defer session.Close()

	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		return tx.Run("MATCH (n:ERD) DETACH DELETE n", nil)
	})

	return err
}

func CreateGraph(driver neo4j.Driver, relationShips map[string]*RelationShip) error {
	// Sessions are short-lived, cheap to create and NOT thread safe. Typically create one or more sessions
	// per request in your web application. Make sure to call Close on the session when done.
	// For multi-database support, set sessionConfig.DatabaseName to requested database
	// Session config will default to write mode, if only reads are to be used configure session for
	// read mode.
	session := driver.NewSession(neo4j.SessionConfig{})
	defer session.Close()

	// 点还是relation，边id用key，属性就是RelationShip
	for k, v := range relationShips {
		_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {

			// 创建点，起点，终点
			if err := CreateNode(tx, v.SColumn); err != nil {
				return nil, err
			}
			if err := CreateNode(tx, v.TColumn); err != nil {
				return nil, err
			}
			// 创建边
			if err := CreateEdge(tx, k, v); err != nil {
				return nil, err
			}

			return nil, nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func CreateNode(tx neo4j.Transaction, r *Column) error {
	// CREATE CONSTRAINT ON (cc:ERD) ASSERT cc.id IS UNIQUE
	records, err := tx.Run(`
		MERGE (n:ERD:`+r.Schema+` {id: $id}) 
		ON CREATE SET n.schemaname = $schemaname, n.relname = $relname, n.udt = timestamp()
		ON MATCH SET n.udt = timestamp()
		RETURN n.id
	`,
		map[string]interface{}{
			"id":         r.Schema + "." + r.RelName,
			"schemaname": r.Schema,
			"relname":    r.RelName,
		})
	// In face of driver native errors, make sure to return them directly.
	// Depending on the error, the driver may try to execute the function again.
	if err != nil {
		return err
	}
	if _, err := records.Single(); err != nil {
		return err
	}
	return nil
}

func CreateEdge(tx neo4j.Transaction, id string, r *RelationShip) error {
	_, err := tx.Run(`
		MATCH (snode:ERD {id: $sid}), (tnode:ERD {id: $tid})
		CREATE (snode)-[e:`+r.Type+` {id: $id, larg: $larg, rarg: $rarg, type: $type}]->(tnode)
		RETURN e
	`, map[string]interface{}{
		"id":   id,
		"sid":  r.SColumn.Schema + "." + r.SColumn.RelName,
		"tid":  r.TColumn.Schema + "." + r.TColumn.RelName,
		"larg": r.SColumn.Schema + "." + r.SColumn.RelName + "." + r.SColumn.Field,
		"rarg": r.TColumn.Schema + "." + r.TColumn.RelName + "." + r.TColumn.Field,
		"type": r.Type,
	})
	return err
}
