package main

import (
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v2"
)

func main() {

	sql := `
		SELECT * FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.tid;
	`

	result, err := pg_query.Parse(sql)
	if err != nil {
		panic(err)
	}

	// This will output "42"
	for _, v := range result.Stmts[0].Stmt.GetSelectStmt().GetFromClause() {
		fmt.Println(v.GetJoinExpr().GetLarg().GetRangeVar())

		lfields := v.GetJoinExpr().GetQuals().GetAExpr().GetLexpr().GetColumnRef().GetFields()
		fmt.Println(lfields[0].GetString_().GetStr() + "." + lfields[1].GetString_().GetStr())

		rfields := v.GetJoinExpr().GetQuals().GetAExpr().GetRexpr().GetColumnRef().GetFields()
		fmt.Println(rfields[0].GetString_().GetStr() + "." + rfields[1].GetString_().GetStr())

		fmt.Println(v.GetJoinExpr().GetQuals().GetAExpr())
	}

	/*
		[join_expr:{jointype:JOIN_INNER larg:{range_var:{relname:"tbl1" inh:true relpersistence:"p" location:17}} rarg:{range_var:{relname:"tbl2" inh:true relpersistence:"p" location:27}} quals:{a_expr:{kind:AEXPR_OP name:{string:{str:"="}} lexpr:{column_ref:{fields:{string:{str:"tbl1"}} fields:{string:{str:"id"}} location:35}} rexpr:{column_ref:{fields:{string:{str:"tbl2"}} fields:{string:{str:"tid"}} location:45}} location:43}}}]

		jointype:JOIN_INNER  larg:{range_var:{relname:"tbl1"  inh:true  relpersistence:"p"  location:17}}  rarg:{range_var:{relname:"tbl2"  inh:true  relpersistence:"p"  location:27}}  quals:{a_expr:{kind:AEXPR_OP  name:{string:{str:"="}}  lexpr:{column_ref:{fields:{string:{str:"tbl1"}}  fields:{string:{str:"id"}}  location:35}}  rexpr:{column_ref:{fields:{string:{str:"tbl2"}}  fields:{string:{str:"tid"}}  location:45}}  location:43}}
	*/

}
