package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"

	_ "github.com/lib/pq"

	"pg_lineage/internal/lineage"
	writer "pg_lineage/internal/lineage-writer"
	"pg_lineage/internal/service"
	C "pg_lineage/pkg/config"
	"pg_lineage/pkg/depgraph"
	"pg_lineage/pkg/log"
)

type QueryStore struct {
	Query     string
	Calls     int64
	TotalTime float64
	MinTime   float64
	MaxTime   float64
	MeanTime  float64
}

var config C.Config

func init() {
	configFile := flag.String("c", "./config/config.yaml", "path to config.yaml")
	flag.Parse()

	var err error
	if config, err = C.InitConfig(*configFile); err != nil {
		fmt.Println("InitConfig error:", err)
		os.Exit(1)
	}
	if err := log.InitLogger(&config.Log); err != nil {
		fmt.Println("InitLogger error:", err)
		os.Exit(1)
	}
}

func main() {
	log.Infof("Log level: %s, log file: %s", config.Log.Level, config.Log.Path)

	neo4jDriver, err := writer.InitNeo4jDriver(&config.Storage.Neo4j)
	if err != nil {
		log.Fatalf("InitNeo4jDriver error: %v", err)
	}
	defer safeClose("neo4j", neo4jDriver)

	pgWriterDriver, err := writer.InitPGClient(&config.Storage.Postgres)
	if err != nil {
		log.Fatalf("InitPostgresWriter error: %v", err)
	}
	defer safeClose("postgres writer", pgWriterDriver)

	writerManager := writer.InitWriterManager(&writer.WriterContext{
		Neo4jDriver: neo4jDriver,
		PgDriver:    pgWriterDriver,
	})

	if err := writerManager.ResetGraph(); err != nil {
		log.Fatalf("ResetGraph error: %v", err)
	}

	for _, dsConf := range config.Service.Postgres {
		processDataSource(dsConf, writerManager)
	}
}

func processDataSource(conf C.PostgresService, wm *writer.WriterManager) {
	log.Infof("Processing data source: %s", conf.Label)

	db, err := writer.InitPGClient(&conf)
	if err != nil {
		log.Errorf("Failed to connect to data source %s: %v", conf.Label, err)
		return
	}
	defer safeClose(conf.Label, db)

	queries, err := fetchQueryStats(db, conf.DBName)
	if err != nil {
		log.Errorf("Error fetching query stats for %s: %v", conf.Label, err)
		return
	}
	defer queries.Close()

	for queries.Next() {
		var qs QueryStore
		if err := queries.Scan(&qs.Query, &qs.Calls, &qs.TotalTime, &qs.MinTime, &qs.MaxTime, &qs.MeanTime); err != nil {
			log.Warnf("Query row scan error: %v", err)
			continue
		}
		handleQueryLineage(&qs, conf, db, wm)
	}

	if err := completeLineageGraph(conf, db, wm); err != nil {
		log.Errorf("Complete graph update error for %s: %v", conf.Label, err)
		return
	}
}

func fetchQueryStats(db *sql.DB, dbName string) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT 
			s.query, s.calls, s.total_time, s.min_time, s.max_time, s.mean_time
		FROM 
			pg_stat_statements s
		JOIN
			pg_database d ON d.oid = s.dbid
		WHERE
			d.datname = '%s' AND calls > 10
		ORDER BY s.mean_time DESC
		LIMIT 1000;`, dbName)
	return db.Query(query)
}

func handleQueryLineage(qs *QueryStore, conf C.PostgresService, db *sql.DB, wm *writer.WriterManager) {
	var graph *depgraph.Graph
	udf, err := lineage.IdentifyFuncCall(qs.Query)
	if err == nil {
		graph, err = lineage.HandleUDF4Lineage(db, udf)
	} else {
		graph, err = lineage.Parse(qs.Query)
	}

	if err != nil {
		log.Debugf("Skip invalid query: %s, err: %v", trimQuery(qs.Query), err)
		return
	}

	udf.Calls = qs.Calls
	graph.SetNamespace(conf.Label)

	log.Debugf("Lineage Graph for query: %s", trimQuery(qs.Query))
	for i, layer := range graph.TopoSortedLayers() {
		log.Debugf("Layer %d: %s", i, strings.Join(layer, ", "))
	}

	if err := wm.CreateGraphPostgres(graph.ShrinkGraph(), udf, conf); err != nil {
		log.Errorf("Failed to write lineage graph: %v", err)
	}
}

func completeLineageGraph(conf C.PostgresService, db *sql.DB, wm *writer.WriterManager) error {
	rows, err := db.Query(`
		SELECT 
			COALESCE(p.relname, st.relname) AS relname,
			COALESCE(n.nspname, st.schemaname) AS schemaname,
			SUM(st.seq_scan), SUM(st.seq_tup_read),
			SUM(COALESCE(st.idx_scan, 0)), SUM(COALESCE(st.idx_tup_fetch, 0)),
			STRING_AGG(DISTINCT COALESCE(obj_description(st.relid), ''), ' | ')
		FROM pg_stat_user_tables st
		LEFT JOIN pg_inherits i ON st.relid = i.inhrelid
		LEFT JOIN pg_class p ON i.inhparent = p.oid
		LEFT JOIN pg_namespace n ON p.relnamespace = n.oid
		WHERE st.schemaname !~ '^pg_temp_'
		  AND st.schemaname !~ '_del$'
		  AND st.schemaname NOT IN ('sync', 'sync_his', 'partman', 'debug')
		GROUP BY relname, schemaname
		ORDER BY schemaname, relname;
	`)
	if err != nil {
		return fmt.Errorf("failed to query table stats: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var t service.Table
		if err := rows.Scan(
			&t.RelName, &t.SchemaName,
			&t.SeqScan, &t.SeqTupRead,
			&t.IdxScan, &t.IdxTupFetch,
			&t.Comment,
		); err != nil {
			return fmt.Errorf("scan error: %w", err)
		}
		t.Database = conf.Label

		if err := wm.CompleteTableNode(&t, conf); err != nil {
			return fmt.Errorf("failed to complete node: %w", err)
		}
	}

	log.Infof("Lineage node metadata updated for: %s", conf.Label)
	return nil
}

func trimQuery(query string) string {
	if len(query) > 80 {
		return query[:80] + "..."
	}
	return query
}

func safeClose(name string, closer interface{ Close() error }) {
	if closer != nil {
		if err := closer.Close(); err != nil {
			log.Warnf("Error closing %s: %v", name, err)
		}
	}
}
