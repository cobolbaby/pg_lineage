package main

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/cobolbaby/lineage/pkg/depgraph"
	"github.com/cobolbaby/lineage/pkg/log"
	_ "github.com/lib/pq"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	pg_query "github.com/pganalyze/pg_query_go/v2"
	"github.com/tidwall/gjson"
)

var (
	PLPGSQL_UNHANLED_COMMANDS = regexp.MustCompile(`set\s+(time zone|enable)(.*?);`)
	PLPGSQL_BLACKLIST_STMTS   = map[string]bool{
		"PLpgSQL_stmt_assign":     true,
		"PLpgSQL_stmt_raise":      true,
		"PLpgSQL_stmt_execsql":    false,
		"PLpgSQL_stmt_if":         true,
		"PLpgSQL_stmt_dynexecute": true, // 比较复杂，不太好支持
		"PLpgSQL_stmt_perform":    true, // 暂不支持
	}
	PLPGSQL_GET_FUNC_DEFINITION = `
		SELECT nspname, proname, pg_get_functiondef(p.oid) as definition
		FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE nspname = '%s' and proname = '%s'
		LIMIT 1;
	`
)

type Owner struct {
	Username string
	Nickname string
	ID       string
}

type Record struct {
	SchemaName string
	RelName    string
	Type       string
	Columns    []string
	Comment    string
	Visited    string
	Size       int64
	Layer      string
	Database   string
	Owner      *Owner
	CreateTime time.Time
	Labels     []string
	ID         string
}

func (r *Record) GetID() string {
	if r.ID != "" {
		return r.ID
	}

	if r.SchemaName != "" {
		return r.SchemaName + "." + r.RelName
	} else {
		switch r.RelName {
		case "pg_namespace", "pg_class", "pg_attribute", "pg_type":
			r.SchemaName = "pg_catalog"
			return r.SchemaName + "." + r.RelName
		default:
			return r.RelName
		}
	}
}

func (r *Record) IsTemp() bool {
	return r.SchemaName == "" ||
		strings.HasPrefix(r.RelName, "temp_") ||
		strings.HasPrefix(r.RelName, "tmp_")
}

type Op struct {
	Type       string
	ProcName   string
	SchemaName string
	Database   string
	Comment    string
	Owner      *Owner
	SrcID      string
	DestID     string
	ID         string
}

func (o *Op) GetID() string {
	if o.ID != "" {
		return o.ID
	}

	if o.SchemaName == "" {
		o.SchemaName = "public"
	}
	return o.SchemaName + "." + o.ProcName
}

type QueryStore struct {
	Query     string
	Calls     uint32
	TotalTime float64
	MinTime   float64
	MaxTime   float64
	MeanTime  float64
}

// 过滤部分关键词
func filterUnhandledCommands(content string) string {
	return PLPGSQL_UNHANLED_COMMANDS.ReplaceAllString(content, "")
}

func init() {
	if err := log.InitLogger(&log.LoggerConfig{
		Level: "debug",
		Path:  "./logs/lineage.log",
	}); err != nil {
		fmt.Println(err)
	}
}

func main() {

	// TODO:支持控制台输入

	// 创建 PG 数据库连接，并执行SQL语句
	db, err := sql.Open("postgres", DB_DSN)
	if err != nil {
		log.Fatal("sql.Open err: ", err)
	}
	defer db.Close()

	driver, err := neo4j.NewDriver(NEO4J_URL, neo4j.BasicAuth(NEO4J_USER, NEO4J_PASSWORD, ""))
	if err != nil {
		log.Fatal("neo4j.NewDriver err: ", err)
	}
	// Handle driver lifetime based on your application lifetime requirements  driver's lifetime is usually
	// bound by the application lifetime, which usually implies one driver instance per application
	defer driver.Close()

	// 一上来先重置，避免重复写入
	if err := ResetGraph(driver); err != nil {
		log.Fatal("ResetGraph err: ", err)
	}

	// 支持获取pg_stat_statements中的sql语句
	querys, err := db.Query(`
		SELECT 
			s.query, s.calls, s.total_time, s.min_time, s.max_time, s.mean_time
		FROM 
			pg_stat_statements s
		JOIN
			pg_database d ON d.oid = s.dbid
		WHERE
			d.datname = 'bdc'
		ORDER BY
			s.calls DESC
		Limit 100;
	`)
	if err != nil {
		log.Fatal("db.Query err: ", err)
	}
	defer querys.Close()

	var qs QueryStore
	for querys.Next() {
		_ = querys.Scan(&qs.Query, &qs.Calls, &qs.TotalTime, &qs.MinTime, &qs.MaxTime, &qs.MeanTime)

		queryRaw, _ := pg_query.ParseToJSON(qs.Query)
		log.Debug(queryRaw)

		// 检查是否存在 UDF
		if strings.Contains(queryRaw, "RangeFunction") {
			break
		}
	}

	// queryRaw, _ := pg_query.ParseToJSON(qs.Query)
	// v := gjson.Parse(queryRaw)
	// log.Debug(v)

	// 获取相关定义
	op := &Op{
		Type:       "plpgsql",
		ProcName:   "func_insert_fact_sn_info_f6",
		SchemaName: "dw",
	}
	rows, err := db.Query(fmt.Sprintf(PLPGSQL_GET_FUNC_DEFINITION, op.SchemaName, op.ProcName))
	if err != nil {
		log.Fatal("db.Query err: ", err)
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
			log.Fatal("rows.Scan err: ", err)
		}
	}

	// 字符串过滤
	plpgsql := filterUnhandledCommands(definition)
	// log.Printf(plpgsql)

	tree, err := pg_query.ParsePlPgSqlToJSON(plpgsql)
	if err != nil {
		log.Fatal("pg_query.ParsePlPgSqlToJSON err: ", err)
	}

	for _, v := range gjson.Parse(tree).Array() {
		// 重新生成
		sqlTree := depgraph.New(DB_ALIAS)

		for _, action := range v.Get("PLpgSQL_function.action.PLpgSQL_stmt_block.body").Array() {
			// 遍历属性
			action.ForEach(func(key, value gjson.Result) bool {
				// 没有配置，或者屏蔽掉的
				if enable, ok := PLPGSQL_BLACKLIST_STMTS[key.String()]; ok && enable {
					return false
				}

				// 递归调用 Parse
				if err := SQLParser(sqlTree, key.String(), value.String()); err != nil {
					log.Errorf("pg_query.ParseToJSON err: %s, sql: %s", err, value.String())
					return false
				}

				return true
			})
		}

		// TODO:完善点的信息

		log.Debugf("Graph: %+v", sqlTree)
		for i, layer := range sqlTree.TopoSortedLayers() {
			log.Infof("Graph %d: %s\n", i, strings.Join(layer, ", "))
		}

		if err := CreateGraph(driver, sqlTree.ShrinkGraph(), op); err != nil {
			log.Error("CreateGraph err: ", err)
		}
	}

}
