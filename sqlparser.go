package main

import (
	pg_query "github.com/pganalyze/pg_query_go/v2"
	"github.com/tidwall/gjson"
)

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
