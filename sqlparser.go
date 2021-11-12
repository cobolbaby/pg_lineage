package main

import (
	"log"

	pg_query "github.com/pganalyze/pg_query_go/v2"
	"github.com/tidwall/gjson"
)

func SQLParser(sqlTree *SqlTree, operator, plan string) error {
	log.Printf("%s: %s\n", operator, plan)

	switch operator {

	case "PLpgSQL_stmt_execsql":

		// 支持 select max(testtime) into _upper_testtime FROM ictf6.ictlogsn;

		// 支持 delete from dwictf6.fact_failpart dest

		// 支持 execute dynamic sql

		// 支持 drop table if exists fix2wcline; create temp table fix2wcline

		// 支持 insert into dwictf6.fact_failpart select * from xxxx

		// 支持 update manager.t_point_dw

		// 支持 PERFORM manager.insertlog(

		// TODO:如果执行的是 select into

		subQuery := gjson.Get(plan, "sqlstmt.PLpgSQL_expr.query").String()

		subTree, err := pg_query.ParseToJSON(subQuery)
		if err != nil {
			return err
		}
		log.Printf("%s: %s\n", subQuery, subTree)

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

	return nil
}
