package main

import (
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v2"
)

func main() {

	sql := `
		SELECT * FROM demo.tbl1 a JOIN demo.tbl2 b ON a.id = b.tid;
	`

	result, err := pg_query.Parse(sql)
	if err != nil {
		panic(err)
	}

	// This will output "42"
	for _, v := range result.Stmts[0].Stmt.GetSelectStmt().GetFromClause() {

		// 优先遍历内层JOIN
		if v.GetJoinExpr().GetLarg().GetJoinExpr() != nil {
			// 递归调用
		}
		if v.GetJoinExpr().GetRarg().GetJoinExpr() != nil {
			// 递归调用
		}
		// 然后遍历外层JOIN
		if v.GetJoinExpr().GetLarg().GetRangeVar() != nil {
			// 解析表名，列名，JOIN条件，记录别名与实体表名的对应关系
			// 等待外层遍历完之后，先做表名替换，然后筛选出两个表都不是临时表的JOIN条件
		}
		if v.GetJoinExpr().GetRarg().GetRangeVar() != nil {
			// 直接返回
		}
		// 将关联条件输出，可以是单个字段关联，也可以是多字段关联

		fmt.Println(v.GetJoinExpr().GetLarg().GetRangeVar())

		// 获取别名定义
		tblAliasMap := make(map[string]string)

		lrAlias := v.GetJoinExpr().GetLarg().GetRangeVar().GetAlias().GetAliasname()
		lrReal := v.GetJoinExpr().GetLarg().GetRangeVar().GetSchemaname() + "." + v.GetJoinExpr().GetLarg().GetRangeVar().GetRelname()

		tblAliasMap[lrAlias] = lrReal

		rrAlias := v.GetJoinExpr().GetRarg().GetRangeVar().GetAlias().GetAliasname()
		rrReal := v.GetJoinExpr().GetRarg().GetRangeVar().GetSchemaname() + "." + v.GetJoinExpr().GetLarg().GetRangeVar().GetRelname()

		tblAliasMap[rrAlias] = rrReal

		lfields := v.GetJoinExpr().GetQuals().GetAExpr().GetLexpr().GetColumnRef().GetFields()
		fmt.Println(tblAliasMap[lfields[0].GetString_().GetStr()] + "." + lfields[1].GetString_().GetStr())

		rfields := v.GetJoinExpr().GetQuals().GetAExpr().GetRexpr().GetColumnRef().GetFields()
		fmt.Println(tblAliasMap[rfields[0].GetString_().GetStr()] + "." + rfields[1].GetString_().GetStr())

	}

}
