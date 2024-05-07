package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

func main() {
	// Connect to PostgreSQL
	pgConnStr := "postgres://postgres:password@localhost:5432/postgres?sslmode=disable"
	pgConn, err := sql.Open("postgres", pgConnStr)
	if err != nil {
		log.Fatalf("Unable to connect to PostgreSQL: %v\n", err)
	}
	defer pgConn.Close()

	// Connect to Neo4j
	neo4jConn, err := neo4j.NewDriver("bolt://localhost:7687", neo4j.BasicAuth("neo4j", "neo4j123", ""))
	if err != nil {
		log.Fatalf("Unable to connect to Neo4j: %v\n", err)
	}
	defer neo4jConn.Close()

	// Read data from PostgreSQL
	rows, err := pgConn.QueryContext(context.Background(), "SELECT relname, schemaname, seq_scan FROM pg_stat_user_tables")
	if err != nil {
		log.Fatalf("Unable to execute query: %v\n", err)
	}
	defer rows.Close()

	// Update data in Neo4j
	session := neo4jConn.NewSession(neo4j.SessionConfig{})
	defer session.Close()

	for rows.Next() {
		var relname, schemaName string
		var seqScan int
		err := rows.Scan(&relname, &schemaName, &seqScan)
		if err != nil {
			log.Fatalf("Error scanning row: %v\n", err)
		}

		// Create or update Neo4j node with PostgreSQL data
		cypher := `
			MERGE (n:Lineage:` + schemaName + ` {id: $id})
			ON CREATE SET n.database = $database, n.schemaname = $schemaname, n.relname = $relname, n.udt = timestamp(), n.seq_scan = $seq_scan
			ON MATCH SET n.udt = timestamp(), n.seq_scan = $seq_scan
		`
		_, err = session.WriteTransaction(func(transaction neo4j.Transaction) (interface{}, error) {
			result, err := transaction.Run(cypher, map[string]interface{}{
				"id":         "postgres" + "." + schemaName + "." + relname,
				"database":   "postgres",
				"schemaname": schemaName,
				"relname":    relname,
				"seq_scan":   seqScan, // Set your value for seq_scan
			})
			if err != nil {
				return nil, err
			}
			return result.Consume()
		})
		if err != nil {
			log.Fatalf("Error creating/updating Neo4j node: %v\n", err)
		}
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("Error iterating rows: %v\n", err)
	}

	fmt.Println("Data updated successfully in Neo4j.")
}
