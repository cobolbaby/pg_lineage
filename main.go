package main

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	pg_query "github.com/pganalyze/pg_query_go/v2"
	"github.com/tidwall/gjson"

	_ "github.com/lib/pq"
)

var (
	REGEX_UNHANLED_COMMANDS = regexp.MustCompile(`set\s+(time zone|enbale_)(.*?);`)
	PLPGSQL_BLACKLIST_STMTS = map[string]bool{
		"PLpgSQL_stmt_raise":   true,
		"PLpgSQL_stmt_execsql": false,
		"PLpgSQL_stmt_assign":  true,
		"PLpgSQL_stmt_if":      true,
	}
	PLPGSQL_GET_FUNC_DEFINITION = `
		SELECT nspname, proname, pg_get_functiondef(p.oid) as definition
		FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE nspname || '.' || proname = '%s'
		LIMIT 1;
	`
)

const (
	DB_HOST        = "xxxx"
	DB_PORT        = 5494
	DB_USER        = "xxxx"
	DB_PASSWORD    = "xxxxxx"
	DB_NAME        = "bdc"
	NEO4J_URL      = "neo4j://localhost:7687"
	NEO4J_USER     = "neo4j"
	NEO4J_PASSWORD = "neo4j123"
)

type Record struct {
	SchemaName string
	RelName    string
	Type       string
	Columns    []string
	Comment    string
	Visited    string
	Layer      string
	DB         string
}

type SqlTree struct {
	Source []*Record `json:"source"`
	Target []*Record `json:"target"`
	Op     string    `json:"op"`
}

func SQLParser(operator, plan string) (*SqlTree, error) {
	// log.Printf("%s: %s\n", operator, plan)

	sqlTree := &SqlTree{}

	switch operator {

	case "PLpgSQL_stmt_execsql":
		// TODO:如果执行的是 select into
		subQuery := gjson.Get(plan, "sqlstmt.PLpgSQL_expr.query").String()

		subTree, err := pg_query.ParseToJSON(subQuery)
		if err != nil {
			return sqlTree, err
		}

		stmts := gjson.Get(subTree, "stmts").Array()
		for _, v := range stmts {

			fromClause := v.Get("stmt.SelectStmt.fromClause").Array()
			for _, vv := range fromClause {

				sqlTree.Source = append(sqlTree.Source, &Record{
					RelName:    vv.Get("RangeVar.relname").String(),
					SchemaName: vv.Get("RangeVar.schemaname").String(),
					Type:       "table",
				})
			}

		}

	}

	return sqlTree, nil
}

// 过滤部分关键词
func filterUnhandledCommands(content string) string {
	return REGEX_UNHANLED_COMMANDS.ReplaceAllString(content, "")
}

func main() {

	udf := "dwictf6.func_fact_failpart"

	// 创建 PG 数据库连接，并执行SQL语句
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatalln("sql.Open err: ", err)
	}
	defer db.Close()

	driver, err := neo4j.NewDriver(NEO4J_URL, neo4j.BasicAuth(NEO4J_USER, NEO4J_PASSWORD, ""))
	if err != nil {
		log.Fatalf("neo4j.NewDriver err: %s", err)
	}
	// Handle driver lifetime based on your application lifetime requirements  driver's lifetime is usually
	// bound by the application lifetime, which usually implies one driver instance per application
	defer driver.Close()

	rows, err := db.Query(fmt.Sprintf(PLPGSQL_GET_FUNC_DEFINITION, udf))
	if err != nil {
		log.Fatalln("db.Query err: ", err)
	}
	defer rows.Close()

	var nspname string
	var proname string
	var definition string

	for rows.Next() {
		err := rows.Scan(&nspname, &proname, &definition)
		switch err {
		case sql.ErrNoRows:
			fmt.Println("No rows were returned")
		case nil:
			fmt.Printf("Data row = (%s, %s)\n", nspname, proname)
		default:
			log.Fatalln("rows.Scan err: ", err)
		}
	}

	// 字符串过滤
	plpgsql := filterUnhandledCommands(definition)

	tree, err := pg_query.ParsePlPgSqlToJSON(plpgsql)
	if err != nil {
		log.Fatalln("pg_query.ParsePlPgSqlToJSON err: ", err)
	}

	for _, v := range gjson.Parse(tree).Array() {

		for _, action := range v.Get("PLpgSQL_function.action.PLpgSQL_stmt_block.body").Array() {
			// 遍历属性
			action.ForEach(func(key, value gjson.Result) bool {
				// 没有配置，或者屏蔽掉的
				if enable, ok := PLPGSQL_BLACKLIST_STMTS[key.String()]; ok && enable {
					return false
				}

				// 递归调用 Parse
				sqlTree, err := SQLParser(key.String(), value.String())
				if err != nil {
					log.Fatalf("pg_query.ParseToJSON err: %s, sql: %s", err, value.String())
				}

				log.Printf("%s Parser: %#v\n", key, *sqlTree)
				// 写入 Neo4j
				// neo4j.WriteToNeo4j(sqlTree)

				item, err := insertItem(driver)
				if err != nil {
					log.Fatalf("insertItem err: %s", err)
				}
				log.Printf("insertItem %v\n", item)

				return true
			})

		}
	}

	// 判断是否可以直接生成图
	// 如果可以直接出图，则直接构造图
	// 再过滤，生成图
	// 识别哪些是临时表，哪些是实体表
	// 写入数据库
	// 入库以后怎么查出来
}

func insertItem(driver neo4j.Driver) (*Item, error) {
	// Sessions are short-lived, cheap to create and NOT thread safe. Typically create one or more sessions
	// per request in your web application. Make sure to call Close on the session when done.
	// For multi-database support, set sessionConfig.DatabaseName to requested database
	// Session config will default to write mode, if only reads are to be used configure session for
	// read mode.
	session := driver.NewSession(neo4j.SessionConfig{})
	defer session.Close()
	result, err := session.WriteTransaction(createItemFn)
	if err != nil {
		return nil, err
	}
	return result.(*Item), nil
}

func createItemFn(tx neo4j.Transaction) (interface{}, error) {
	records, err := tx.Run("CREATE (n:Item { id: $id, name: $name }) RETURN n.id, n.name", map[string]interface{}{
		"id":   1,
		"name": "Item 1",
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
	return &Item{
		Id:   record.Values[0].(int64),
		Name: record.Values[1].(string),
	}, nil
}

type Item struct {
	Id   int64
	Name string
}
