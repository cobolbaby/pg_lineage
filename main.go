package main

import (
	"database/sql"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/cobolbaby/lineage/pkg/depgraph"
	"github.com/cobolbaby/lineage/pkg/log"
	_ "github.com/lib/pq"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

var (
	PLPGSQL_UNHANLED_COMMANDS   = regexp.MustCompile(`(?i)set\s+(time zone|enable_)(.*?);`)
	PLPGSQL_GET_FUNC_DEFINITION = `
		SELECT nspname, proname, pg_get_functiondef(p.oid) as definition
		FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE nspname = '%s' and proname = '%s'
		LIMIT 1;
	`
	PG_QUERY_STORE = `
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
		Limit 500;
	`
)

type QueryStore struct {
	Query     string
	Calls     uint32
	TotalTime float64
	MinTime   float64
	MaxTime   float64
	MeanTime  float64
}

func init() {
	if err := log.InitLogger(&log.LoggerConfig{
		Level: "info",
		Path:  "./logs/lineage.log",
	}); err != nil {
		fmt.Println(err)
	}
}

func main() {
	// db, err := sql.Open("postgres", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
	db, err := sql.Open("postgres", DB_DSN)
	if err != nil {
		log.Fatal("sql.Open err: ", err)
	}
	defer db.Close()

	uri, _ := url.Parse(DB_DSN)
	dbName := strings.TrimPrefix(uri.Path, "/")
	dbAlias := DB_ALIAS

	driver, err := neo4j.NewDriver(NEO4J_URL, neo4j.BasicAuth(NEO4J_USER, NEO4J_PASSWORD, ""))
	if err != nil {
		log.Fatal("neo4j.NewDriver err: ", err)
	}
	// Handle driver lifetime based on your application lifetime requirements  driver's lifetime is usually
	// bound by the application lifetime, which usually implies one driver instance per application
	defer driver.Close()

	// 每次都是重建整张图，避免重复写入，后期可以考虑优化
	if err := ResetGraph(driver); err != nil {
		log.Fatal("ResetGraph err: ", err)
	}

	// 支持获取pg_stat_statements中的sql语句
	querys, err := db.Query(fmt.Sprintf(PG_QUERY_STORE, dbName))
	if err != nil {
		log.Fatal("db.Query err: ", err)
	}
	defer querys.Close()

	for querys.Next() {

		// 一个 UDF 一张图
		sqlTree := depgraph.New(dbAlias)

		var qs QueryStore
		_ = querys.Scan(&qs.Query, &qs.Calls, &qs.TotalTime, &qs.MinTime, &qs.MaxTime, &qs.MeanTime)
		udf, err := IdentifyFuncCall(qs.Query)
		if err != nil {
			continue
		}
		// udf = &Op{
		// 	Type:       "plpgsql",
		// 	ProcName:   "func_insert_fact_sn_info_f6",
		// 	SchemaName: "dw",
		// }
		if err := HandleUDF(sqlTree, db, udf); err != nil {
			log.Error("HandleUDF err: ", err)
		}

		// log.Debugf("UDF Graph: %+v", sqlTree)
		// for i, layer := range sqlTree.TopoSortedLayers() {
		// 	log.Debugf("UDF Graph %d: %s\n", i, strings.Join(layer, ", "))
		// }

		// TODO:完善辅助信息

		if err := CreateGraph(driver, sqlTree.ShrinkGraph(), udf); err != nil {
			log.Fatal("UDF CreateGraph err: ", err)
		}
	}

}

// 解析函数调用
func HandleUDF(sqlTree *depgraph.Graph, db *sql.DB, udf *Op) error {

	definition, err := GetUDFDefinition(db, udf)
	if err != nil {
		log.Errorf("GetUDFDefinition err: %s", err)
		return err
	}

	// 字符串过滤，后期 pg_query 支持 set 了，可以去掉
	// https://github.com/pganalyze/libpg_query/issues/125
	plpgsql := filterUnhandledCommands(definition)
	log.Info("plpgsql: ", plpgsql)

	if err := ParseUDF(sqlTree, plpgsql); err != nil {
		log.Errorf("ParseUDF err: %s", err)
		return err
	}

	return nil
}

// 过滤部分关键词
func filterUnhandledCommands(content string) string {
	return PLPGSQL_UNHANLED_COMMANDS.ReplaceAllString(content, "")
}

// 获取相关定义
func GetUDFDefinition(db *sql.DB, udf *Op) (string, error) {

	rows, err := db.Query(fmt.Sprintf(PLPGSQL_GET_FUNC_DEFINITION, udf.SchemaName, udf.ProcName))
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var nspname string
	var proname string
	var definition string

	for rows.Next() {
		err := rows.Scan(&nspname, &proname, &definition)
		switch err {
		case sql.ErrNoRows:
			log.Warn("No rows were returned")
		case nil:
			log.Infof("Query Data = (%s, %s)\n", nspname, proname)
		default:
			log.Fatalf("rows.Scan err: %s", err)
		}
	}

	return definition, nil
}
