package main

import (
	"database/sql"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/cobolbaby/lineage/internal/lineage"
	"github.com/cobolbaby/lineage/pkg/depgraph"
	"github.com/cobolbaby/lineage/pkg/log"
	sqlparser "github.com/cobolbaby/lineage/pkg/sqlparser4join"
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

type DataSource struct {
	Alias string
	Name  string
	DB    *sql.DB
}

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
	ds := &DataSource{
		Alias: DB_ALIAS,
		Name:  strings.TrimPrefix(uri.Path, "/"),
		DB:    db,
	}

	driver, err := neo4j.NewDriver(NEO4J_URL, neo4j.BasicAuth(NEO4J_USER, NEO4J_PASSWORD, ""))
	if err != nil {
		log.Fatal("neo4j.NewDriver err: ", err)
	}
	// Handle driver lifetime based on your application lifetime requirements  driver's lifetime is usually
	// bound by the application lifetime, which usually implies one driver instance per application
	defer driver.Close()

	// 每次都是重建整张图，避免重复写入，后期可以考虑优化
	if err := lineage.ResetGraph(driver); err != nil {
		log.Fatal("ResetGraph err: ", err)
	}

	// 支持获取pg_stat_statements中的sql语句
	querys, err := ds.DB.Query(fmt.Sprintf(PG_QUERY_STORE, ds.Name))
	if err != nil {
		log.Fatal("db.Query err: ", err)
	}
	defer querys.Close()

	for querys.Next() {

		var qs QueryStore
		_ = querys.Scan(&qs.Query, &qs.Calls, &qs.TotalTime, &qs.MinTime, &qs.MaxTime, &qs.MeanTime)

		// generateTableLineage(&qs, ds, driver)
		generateTableJoinRelation(&qs, ds, driver)

		// 扩展别的图.
	}

}

// 生成一张 JOIN 图
// 可以推导出关联关系的有 IN / JOIN
func generateTableJoinRelation(qs *QueryStore, ds *DataSource, driver neo4j.Driver) {

	// 跳过 udf
	if _, err := lineage.IdentifyFuncCall(qs.Query); err == nil {
		return
	}
	log.Infof("generateTableJoinRelation sql: %s", qs.Query)

	m, _ := sqlparser.Parse(qs.Query)

	counter := 0
	for _, vv := range m {
		// 过滤掉临时表
		if vv.SColumn == nil || vv.TColumn == nil || vv.SColumn.Schema == "" || vv.TColumn.Schema == "" {
			continue
		}
		counter += 1
		fmt.Printf("[%d] %s\n", counter, vv.ToString())
	}

}

// 生成表血缘关系图
func generateTableLineage(qs *QueryStore, ds *DataSource, driver neo4j.Driver) {

	// 一个 UDF 一张图
	sqlTree := depgraph.New(ds.Alias)

	udf, err := lineage.IdentifyFuncCall(qs.Query)
	if err != nil {
		return
	}
	// udf = &Op{
	// 	Type:       "plpgsql",
	// 	ProcName:   "func_insert_fact_sn_info_f6",
	// 	SchemaName: "dw",
	// }
	if err := HandleUDF(sqlTree, ds.DB, udf); err != nil {
		log.Errorf("HandleUDF %+v, err: %s", udf, err)
		return
	}

	log.Debugf("UDF Graph: %+v", sqlTree)
	for i, layer := range sqlTree.TopoSortedLayers() {
		log.Debugf("UDF Graph %d: %s\n", i, strings.Join(layer, ", "))
	}

	// TODO:完善辅助信息

	if err := lineage.CreateGraph(driver, sqlTree.ShrinkGraph(), udf); err != nil {
		log.Errorf("UDF CreateGraph err: %s ", err)
	}
}

// 解析函数调用
func HandleUDF(sqlTree *depgraph.Graph, db *sql.DB, udf *lineage.Op) error {
	log.Infof("HandleUDF: %s.%s", udf.SchemaName, udf.ProcName)

	// 排除系统函数的干扰 e.g. select now()
	if udf.SchemaName == "" || udf.SchemaName == "pg_catalog" {
		return fmt.Errorf("UDF %s is system function", udf.ProcName)
	}

	definition, err := GetUDFDefinition(db, udf)
	if err != nil {
		log.Errorf("GetUDFDefinition err: %s", err)
		return err
	}

	// 字符串过滤，后期 pg_query 支持 set 了，可以去掉
	// https://github.com/pganalyze/libpg_query/issues/125
	plpgsql := filterUnhandledCommands(definition)
	// log.Debug("plpgsql: ", plpgsql)

	if err := lineage.ParseUDF(sqlTree, plpgsql); err != nil {
		log.Errorf("ParseUDF %+v, err: %s", udf, err)
		return err
	}

	return nil
}

// 过滤部分关键词
func filterUnhandledCommands(content string) string {
	return PLPGSQL_UNHANLED_COMMANDS.ReplaceAllString(content, "")
}

// 获取相关定义
func GetUDFDefinition(db *sql.DB, udf *lineage.Op) (string, error) {

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
