package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"

	"pg_lineage/internal/lineage"
	writer "pg_lineage/internal/lineage-writer"
	"pg_lineage/internal/service"
	C "pg_lineage/pkg/config"
	"pg_lineage/pkg/depgraph"
	"pg_lineage/pkg/log"

	_ "github.com/lib/pq"
)

type DataSource struct {
	Label  string
	DBName string
	DB     *sql.DB
}

type QueryStore struct {
	Query     string
	Calls     int64
	TotalTime float64
	MinTime   float64
	MaxTime   float64
	MeanTime  float64
}

var PG_QUERY_STORE = `
	SELECT 
		s.query, s.calls, s.total_time, s.min_time, s.max_time, s.mean_time
	FROM 
		pg_stat_statements s
	JOIN
		pg_database d ON d.oid = s.dbid
	WHERE
		d.datname = '%s'
		AND calls > 10
	ORDER BY
		s.mean_time DESC
	Limit 1000;
`

var config C.Config

func init() {
	configFile := flag.String("c", "./config/config.yaml", "path to config.yaml")
	flag.Parse()

	var err error
	if config, err = C.InitConfig(*configFile); err != nil {
		fmt.Println("InitConfig err: ", err)
		os.Exit(1)
	}
	if err = log.InitLogger(&config.Log); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func main() {
	log.Infof("log level: %s, log file: %s", config.Log.Level, config.Log.Path)

	neo4jDriver, err := writer.InitNeo4jDriver(&config.Storage.Neo4j)
	if err != nil {
		log.Error(err)
	}
	if neo4jDriver != nil {
		defer neo4jDriver.Close()
	}

	pgDriver, err := writer.InitPGClient(&config.Storage.Postgres)
	if err != nil {
		log.Error(err)
	}
	if pgDriver != nil {
		defer pgDriver.Close()
	}

	wm := writer.InitWriterManager(&writer.WriterContext{
		Neo4jDriver: neo4jDriver, // 或 nil
		PgDriver:    pgDriver,
	})

	// 每次都是重建整张图，避免重复写入，后期可以考虑优化
	if err := wm.ResetGraph(); err != nil {
		log.Fatal("ResetGraph err: ", err)
	}

	// Sessions are short-lived, cheap to create and NOT thread safe. Typically create one or more sessions
	// per request in your web application. Make sure to call Close on the session when done.
	// For multi-database support, set sessionConfig.DatabaseName to requested database
	// Session config will default to write mode, if only reads are to be used configure session for
	// read mode.
	// session := driver.NewSession(neo4j.SessionConfig{})
	// defer session.Close()

	// if err := erd.ResetGraph(session); err != nil {
	// 	log.Fatal("ResetGraph err: ", err)
	// }

	db, err := writer.InitPGClient(&config.Service.Postgres)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ds := &DataSource{
		Label:  config.Service.Postgres.Label,
		DBName: config.Service.Postgres.DBName,
		DB:     db,
	}

	// 支持获取pg_stat_statements中的sql语句
	querys, err := ds.DB.Query(fmt.Sprintf(PG_QUERY_STORE, ds.DBName))
	if err != nil {
		log.Fatal("db.Query err: ", err)
	}
	defer querys.Close()

	// m := make(map[string]*erd.RelationShip)

	for querys.Next() {

		var qs QueryStore
		_ = querys.Scan(&qs.Query, &qs.Calls, &qs.TotalTime, &qs.MinTime, &qs.MaxTime, &qs.MeanTime)

		// 生成血缘图
		// 一个 udf 会生成一颗 sqlTree，且不能将多个 udf 的 sqlTree 做合并，所以需要循环写入所有的 sqlTree
		generateTableLineage(&qs, ds, wm)

		// 为了避免重复插入，写入前依赖 MAP 特性做一次去重，并且最后一次性入库
		// r := generateTableJoinRelation(&qs, ds, session)
		// maps.Copy(m, r)

		// 扩展别的图.
	}

	// 一次性入库...
	// if err := erd.CreateGraph(session, m); err != nil {
	// 	log.Errorf("ERD err: %s ", err)
	// }

	// 查询所有表的使用信息，更新图数据库中的节点信息
	completeLineageGraph(ds, wm)

}

// // 生成一张 JOIN 图
// // 可以推导出关联关系的有 IN / JOIN
// func generateTableJoinRelation(qs *QueryStore, ds *DataSource, session neo4j.Session) map[string]*erd.RelationShip {
// 	log.Debugf("generateTableJoinRelation sql: %s", qs.Query)

// 	var m map[string]*erd.RelationShip

// 	if udf, err := lineage.IdentifyFuncCall(qs.Query); err == nil {
// 		m, _ = erd.HandleUDF4ERD(ds.DB, udf)
// 	} else {
// 		m, _ = erd.Parse(qs.Query)
// 	}

// 	n := make(map[string]*erd.RelationShip)
// 	for kk, vv := range m {
// 		// 过滤掉临时表
// 		if vv.SColumn == nil || vv.TColumn == nil || vv.SColumn.Schema == "" || vv.TColumn.Schema == "" {
// 			continue
// 		}
// 		n[kk] = vv
// 	}
// 	fmt.Printf("GetRelationShip: #%d\n", len(n))
// 	for _, v := range n {
// 		fmt.Printf("%s\n", v.ToString())
// 	}

// 	return n
// }

// 生成表血缘关系图
func generateTableLineage(qs *QueryStore, ds *DataSource, m *writer.WriterManager) {

	// 一个 udf 会生成一颗 Tree
	var sqlTree *depgraph.Graph

	udf, err := lineage.IdentifyFuncCall(qs.Query)
	if err == nil {
		sqlTree, err = lineage.HandleUDF4Lineage(ds.DB, udf)
	} else {
		sqlTree, err = lineage.Parse(qs.Query)
	}
	if err != nil {
		log.Errorf("Parse err: %s", err)
		return
	}

	udf.Calls = qs.Calls

	log.Debugf("UDF Graph: %+v", sqlTree)
	for i, layer := range sqlTree.TopoSortedLayers() {
		log.Debugf("UDF Graph %d: %s\n", i, strings.Join(layer, ", "))
	}

	// 设置所属命名空间，避免节点冲突
	sqlTree.SetNamespace(ds.Label)

	if err := m.CreateGraphPostgres(sqlTree.ShrinkGraph(), udf, config.Service.Postgres); err != nil {
		log.Errorf("UDF CreateGraph err: %s ", err)
	}
}

func completeLineageGraph(ds *DataSource, m *writer.WriterManager) {

	rows, err := ds.DB.Query(`
		SELECT 
			COALESCE(p.relname, st.relname) AS relname,
			COALESCE(n.nspname, st.schemaname) AS schemaname,
			SUM(st.seq_scan) AS seq_scan,
			SUM(st.seq_tup_read) AS seq_tup_read,
			SUM(COALESCE(st.idx_scan, 0)) AS idx_scan,
			SUM(COALESCE(st.idx_tup_fetch, 0)) AS idx_tup_fetch,
			STRING_AGG(DISTINCT COALESCE(obj_description(st.relid), ''), ' | ') AS comment
		FROM pg_stat_user_tables st
		LEFT JOIN pg_inherits i ON st.relid = i.inhrelid
		LEFT JOIN pg_class p ON i.inhparent = p.oid
		LEFT JOIN pg_namespace n ON p.relnamespace = n.oid
		WHERE st.schemaname !~ '^pg_temp_'
		AND st.schemaname !~ '_del$'
		AND st.schemaname NOT IN ('sync', 'sync_his', 'partman', 'debug')
		GROUP BY COALESCE(p.relname, st.relname),
				COALESCE(n.nspname, st.schemaname)
		ORDER BY schemaname, relname;
	`)
	if err != nil {
		log.Fatalf("Unable to execute query: %v\n", err)
	}
	defer rows.Close()

	for rows.Next() {
		var relname, schemaName, comment string
		var seqScan, seqTupRead, idxScan, idxTupFetch int64
		err := rows.Scan(&relname, &schemaName, &seqScan, &seqTupRead, &idxScan, &idxTupFetch, &comment)
		if err != nil {
			log.Fatalf("Error scanning row: %v\n", err)
		}

		err = m.CompleteTableNode(&service.Table{
			Database:    ds.Label,
			SchemaName:  schemaName,
			RelName:     relname,
			SeqScan:     seqScan,
			SeqTupRead:  seqTupRead,
			IdxScan:     idxScan,
			IdxTupFetch: idxTupFetch,
			Comment:     comment,
		}, config.Service.Postgres)
		if err != nil {
			log.Fatalf("Error updating Neo4j: %v\n", err)
		}

	}

	log.Info("Data updated successfully in Neo4j.")
}
