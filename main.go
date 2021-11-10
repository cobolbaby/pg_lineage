package main

import (
	"io/ioutil"
	"log"
	"regexp"

	pg_query "github.com/pganalyze/pg_query_go/v2"
	"github.com/tidwall/gjson"
)

var (
	REGEX_UNHANLED_COMMANDS = regexp.MustCompile(`set\s+(time zone|enbale_)(.*?);`)
	SHOULD_HANDLED_STMTS    = map[string]bool{
		"PLpgSQL_stmt_raise":   false,
		"PLpgSQL_stmt_execsql": true,
		"PLpgSQL_stmt_assign":  false,
	}
)

// 过滤部分关键词
func filterUnhandledCommands(content string) string {
	return REGEX_UNHANLED_COMMANDS.ReplaceAllString(content, "")
}

func main() {

	// 解析SQL文件
	content, err := ioutil.ReadFile("./sql/dwictf6_func_fact_failpart.sql")
	if err != nil {
		log.Fatalln("ioutil.ReadFile err: ", err)
	}

	// 字符串过滤
	plpgsql := filterUnhandledCommands(string(content))

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
				if enable, ok := SHOULD_HANDLED_STMTS[key.String()]; !ok || !enable {
					return false
				}

				log.Printf("%s: %s\n", key, value)

				// 递归调用 Parse
				_, err := pg_query.ParseToJSON(value.Get("sqlstmt.PLpgSQL_expr.query").String())
				if err != nil {
					log.Fatalf("pg_query.ParseToJSON err: %s, sql: %s", err, value.String())
				}

				// log.Printf("%s: %s\n", key, tree)
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
