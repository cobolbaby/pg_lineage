package erd

import (
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

func ResetGraph(session neo4j.Session) error {

	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		return tx.Run("MATCH (n:ERD) DETACH DELETE n", nil)
	})

	return err
}

func CreateGraph(session neo4j.Session, relationShips map[string]*RelationShip) error {

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
	// CREATE CONSTRAINT ON (cc:ERD:PG) ASSERT cc.id IS UNIQUE
	_, err := tx.Run(`
		MERGE (n:ERD:PG:`+r.Schema+` {id: $id}) 
		ON CREATE SET n.schemaname = $schemaname, n.relname = $relname, n.udt = timestamp()
		ON MATCH SET n.udt = timestamp()
		RETURN n.id
	`,
		map[string]interface{}{
			"id":         r.Schema + "." + r.RelName,
			"schemaname": r.Schema,
			"relname":    r.RelName,
		})

	return err
}

func CreateEdge(tx neo4j.Transaction, id string, r *RelationShip) error {
	var cypher string

	if r.Type == "JOIN_INNER" {
		// Neo4jError: Neo.ClientError.Statement.SyntaxError (Only directed relationships are supported)
		cypher = `
			MATCH (snode:ERD:PG {id: $sid}), (tnode:ERD:PG {id: $tid})
			CREATE (snode)-[e:` + r.Type + ` {id: $id, larg: $larg, rarg: $rarg, type: $type}]->(tnode)
			RETURN e
		`
	} else if r.Type == "JOIN_LEFT" {
		cypher = `
			MATCH (snode:ERD:PG {id: $sid}), (tnode:ERD:PG {id: $tid})
			CREATE (snode)-[e:` + r.Type + ` {id: $id, larg: $larg, rarg: $rarg, type: $type}]->(tnode)
			RETURN e
		`
	} else {
		fmt.Printf("Unknown pg_query.JoinType: %s\n", r.Type)
		return nil
	}

	_, err := tx.Run(cypher, map[string]interface{}{
		"id":   id,
		"sid":  r.SColumn.Schema + "." + r.SColumn.RelName,
		"tid":  r.TColumn.Schema + "." + r.TColumn.RelName,
		"larg": r.SColumn.Schema + "." + r.SColumn.RelName + "." + r.SColumn.Field,
		"rarg": r.TColumn.Schema + "." + r.TColumn.RelName + "." + r.TColumn.Field,
		"type": r.Type,
	})

	return err
}
