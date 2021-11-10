package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"regexp"

	pg_query "github.com/pganalyze/pg_query_go/v2"
)

var REGEX_UNHANLED_COMMANDS = regexp.MustCompile(`set\s+(time zone|enbale_)(.*?);`)

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
	// fmt.Printf("%s\n", tree)

	schema := new([]map[string]interface{})

	err = json.Unmarshal([]byte(tree), &schema)
	if err != nil {
		log.Fatalln("json.Unmarshal err: ", err)
	}

	blackStmts := map[string]bool{
		"PLpgSQL_stmt_raise": true,
	}

	// 数组遍历
	for _, v := range *schema {
		actions := v["PLpgSQL_function"].(map[string]interface{})["action"].(map[string]interface{})["PLpgSQL_stmt_block"].(map[string]interface{})["body"].([]interface{})
		for _, action := range actions {
			// 遍历对象属性
			for k, v := range action.(map[string]interface{}) {
				if _, ok := blackStmts[k]; ok {
					continue
				}

				log.Printf("%s: %v\n", k, v)
				// if k == "PLpgSQL_stmt_dynfunc_call" {
				// 	// fmt.Printf("%s: %v\n", k, v)
				// 	fmt.Printf("%s\n", v.(map[string]interface{})["funcname"].(map[string]interface{})["str"].(string))
				// }
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
