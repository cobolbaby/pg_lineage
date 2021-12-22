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

	/*
		[join_expr:{jointype:JOIN_INNER larg:{range_var:{relname:"tbl1" inh:true relpersistence:"p" location:17}} rarg:{range_var:{relname:"tbl2" inh:true relpersistence:"p" location:27}} quals:{a_expr:{kind:AEXPR_OP name:{string:{str:"="}} lexpr:{column_ref:{fields:{string:{str:"tbl1"}} fields:{string:{str:"id"}} location:35}} rexpr:{column_ref:{fields:{string:{str:"tbl2"}} fields:{string:{str:"tid"}} location:45}} location:43}}}]

		jointype:JOIN_INNER  larg:{range_var:{relname:"tbl1"  inh:true  relpersistence:"p"  location:17}}  rarg:{range_var:{relname:"tbl2"  inh:true  relpersistence:"p"  location:27}}  quals:{a_expr:{kind:AEXPR_OP  name:{string:{str:"="}}  lexpr:{column_ref:{fields:{string:{str:"tbl1"}}  fields:{string:{str:"id"}}  location:35}}  rexpr:{column_ref:{fields:{string:{str:"tbl2"}}  fields:{string:{str:"tid"}}  location:45}}  location:43}}
	*/

}
