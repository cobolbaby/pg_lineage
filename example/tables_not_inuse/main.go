package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

func main() {
	// Neo4j connection details
	uri := "neo4j://localhost:7687" // Update with your Neo4j URI
	username := "neo4j"             // Replace with your username
	password := "neo4j123"          // Replace with your password

	// Create a Neo4j driver
	driver, err := neo4j.NewDriver(uri, neo4j.BasicAuth(username, password, ""))
	if err != nil {
		log.Fatal("Failed to create driver: ", err)
	}
	defer driver.Close()

	// Open a new session
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close()

	// Define the Cypher query
	query := `
		MATCH (n:Lineage)
		WHERE NOT (n)-[]-() AND n.calls IS NULL AND n.seq_scan = 0 AND n.idx_scan = 0
		RETURN n
	`

	// Execute the query
	result, err := session.Run(query, nil)
	if err != nil {
		log.Fatal("Failed to execute query: ", err)
	}

	// A map to store all unique property names
	uniqueKeys := make(map[string]bool)

	// A slice to hold all node properties
	var nodes []map[string]interface{}

	// Iterate over the result set
	for result.Next() {
		record := result.Record()
		nodeInterface, ok := record.Get("n")
		if !ok {
			log.Println("No node found")
			continue
		}

		// Assert that the interface is a neo4j.Node type
		node, ok := nodeInterface.(neo4j.Node)
		if !ok {
			log.Println("Failed to assert node type")
			continue
		}

		// Get the node's properties
		properties := node.Props

		// Track all unique property keys
		for key := range properties {
			uniqueKeys[key] = true
		}

		// Store the node properties
		nodes = append(nodes, properties)
	}

	// Convert the unique keys map to a sorted slice for the CSV header
	headers := []string{}
	for key := range uniqueKeys {
		headers = append(headers, key)
	}
	// sort.Strings(headers) // Sort the headers for consistency

	// Create a CSV file to write the data
	file, err := os.Create("tables_not_inuse.csv")
	if err != nil {
		log.Fatal("Could not create CSV file: ", err)
	}
	defer file.Close()

	// Create a CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write the CSV header
	writer.Write(headers)

	// Write each node's properties, ensuring correct order and empty values where necessary
	for _, properties := range nodes {
		row := make([]string, len(headers))
		for i, header := range headers {
			if value, ok := properties[header]; ok {
				row[i] = fmt.Sprintf("%v", value)
			} else {
				row[i] = "" // Leave empty if the property doesn't exist for this node
			}
		}
		writer.Write(row)
	}

	log.Println("CSV file created successfully!")
}
