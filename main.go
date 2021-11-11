package main

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"

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
	}
	PLPGSQL_GET_FUNC_DEFINITION = `
		SELECT nspname, proname, pg_get_functiondef(p.oid) as definition
		FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE nspname || '.' || proname = '%s'
		LIMIT 1;
	`
)

const (
	DB_HOST     = ""
	DB_PORT     = 5432
	DB_USER     = ""
	DB_PASSWORD = ""
	DB_NAME     = ""
)

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

	res := gjson.Parse(tree).Array()

	for _, v := range res {
		actions := v.Get("PLpgSQL_function.action.PLpgSQL_stmt_block.body").Array()

		for _, action := range actions {
			action.ForEach(func(key, value gjson.Result) bool {
				// 没有配置，或者屏蔽掉的
				if enable, ok := PLPGSQL_BLACKLIST_STMTS[key.String()]; ok && enable {
					return false
				}

				log.Printf("%s: %s\n", key, value)

				// 递归调用 Parse
				tree, err := pg_query.ParseToJSON(value.Get("sqlstmt.PLpgSQL_expr.query").String())
				if err != nil {
					log.Fatalf("pg_query.ParseToJSON err: %s, sql: %s", err, value.String())
				}

				log.Printf("%s: %s\n", key, tree)
				// 每个都解析完了，怎么办？

				return true
			})

		}
	}

	/*a := make(map[string]interface{})
	a["name"] = "dwictf6_func_fact_failpart"
	a["schema"] = "public"
	a["type"] = "table"
	a["columns"] = make([]interface{}, 0)
	a["constraints"] = make([]interface{}, 0)
	a["indexes"] = make([]interface{}, 0)
	a["triggers"] = make([]interface{}, 0)
	a["rules"] = make([]interface{}, 0)
	a["comments"] = make([]interface{}, 0)
	a["data"] = make([]interface{}, 0)
	a["children"] = make([]interface{}, 0)*/

	// 判断是否可以直接生成图
	// 如果可以直接出图，则直接构造图
	// 再过滤，生成图
	// 识别哪些是临时表，哪些是实体表
	// 写入数据库
	// 入库以后怎么查出来
}
