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
			// 遍历对象属性
			for k, v := range action.Value().(map[string]interface{}) {
				if _, ok := SHOULD_HANDLED_STMTS[k]; ok {
					continue
				}

				log.Printf("%s: %v\n", k, v)
			}
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
