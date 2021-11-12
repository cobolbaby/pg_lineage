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

			// 如果该 SQL 为 select 操作，则获取 from
			if v.Get("stmt.SelectStmt").Exists() {
				fromClause := v.Get("stmt.SelectStmt.fromClause").Array()
				for _, vv := range fromClause {

					sqlTree.Source = append(sqlTree.Source, &Record{
						RelName:    vv.Get("RangeVar.relname").String(),
						SchemaName: vv.Get("RangeVar.schemaname").String(),
						Type:       vv.Get("RangeVar.relpersistence").String(),
					})
				}

			}

			// 单独创建 table
			if v.Get("stmt.CreateStmt").Exists() {
				if v.Get("stmt.CreateStmt.relation").Exists() {
					relation := v.Get("stmt.CreateStmt.relation")
					sqlTree.Target = append(sqlTree.Target, &Record{
						RelName:    relation.Get("relname").String(),
						SchemaName: relation.Get("schemaname").String(),
						Type:       relation.Get("relpersistence").String(),
					})

				}
			}

			// 如果该 SQL 为 delete 操作，则填充目标节点
			if v.Get("stmt.DeleteStmt").Exists() {

				sqlTree.Target = append(sqlTree.Target, &Record{
					RelName:    v.Get("stmt.DeleteStmt.relation.relname").String(),
					SchemaName: v.Get("stmt.DeleteStmt.relation.schemaname").String(),
					Type:       v.Get("stmt.DeleteStmt.relation.relpersistence").String(),
				})
			}

			// 如果该 SQL 为 drop 操作，先跳过
			if v.Get("stmt.DropStmt").Exists() {
				break
			}

			// 如果该 SQL 为 create table as 操作，先跳过
			if v.Get("stmt.CreateTableAsStmt").Exists() {
				if v.Get("stmt.CreateTableAsStmt.query.SelectStmt.withClause").Exists() {
					ctes := v.Get("stmt.CreateTableAsStmt.query.SelectStmt.withClause.ctes").Array()
					for _, vv := range ctes {

						sqlTree.Target = append(sqlTree.Target, &Record{
							RelName:    vv.Get("CommonTableExpr.ctename").String(),
							SchemaName: "",
							Type:       "t",
						})

						fromClause := vv.Get("CommonTableExpr.ctequery.SelectStmt.fromClause").Array()
						for _, vvv := range fromClause {
							if vvv.Get("JoinExpr").Exists() {

								sqlTree.Source = append(sqlTree.Source,
									&Record{
										RelName:    vvv.Get("JoinExpr.larg.RangeVar.relname").String(),
										SchemaName: vvv.Get("JoinExpr.larg.RangeVar.schemaname").String(),
										Type:       vvv.Get("JoinExpr.larg.RangeVar.relpersistence").String(),
									},
									&Record{
										RelName:    vvv.Get("JoinExpr.rarg.RangeVar.relname").String(),
										SchemaName: vvv.Get("JoinExpr.rarg.RangeVar.schemaname").String(),
										Type:       vvv.Get("JoinExpr.rarg.RangeVar.relpersistence").String(),
									})
							}

							if vvv.Get("RangeVar").Exists() {
								sqlTree.Source = append(sqlTree.Source, &Record{
									RelName:    vvv.Get("RangeVar.relname").String(),
									SchemaName: vvv.Get("RangeVar.schemaname").String(),
									Type:       vvv.Get("RangeVar.relpersistence").String(),
								})
							}
						}
					}
				}
				if v.Get("stmt.CreateTableAsStmt.query.SelectStmt.fromClause").Exists() {
					fromClause := v.Get("stmt.CreateTableAsStmt.query.SelectStmt.fromClause").Array()
					for _, vv := range fromClause {

						if vv.Get("JoinExpr").Exists() {

							if vv.Get("JoinExpr.larg.RangeVar").Exists() {
								sqlTree.Source = append(sqlTree.Source, &Record{
									RelName:    vv.Get("JoinExpr.larg.RangeVar.relname").String(),
									SchemaName: vv.Get("JoinExpr.larg.RangeVar.schemaname").String(),
									Type:       vv.Get("JoinExpr.larg.RangeVar.relpersistence").String(),
								})
							}
							if vv.Get("JoinExpr.rarg.RangeVar").Exists() {
								sqlTree.Source = append(sqlTree.Source, &Record{
									RelName:    vv.Get("JoinExpr.rarg.RangeVar.relname").String(),
									SchemaName: vv.Get("JoinExpr.rarg.RangeVar.schemaname").String(),
									Type:       vv.Get("JoinExpr.rarg.RangeVar.relpersistence").String(),
								})
							}
							if vv.Get("JoinExpr.larg.RangeSubselect").Exists() {
								fromClause := vv.Get("JoinExpr.larg.RangeSubselect.subquery.SelectStmt.fromClause").Array()
								for _, vvv := range fromClause {
									sqlTree.Source = append(sqlTree.Source, &Record{
										RelName:    vvv.Get("RangeVar.relname").String(),
										SchemaName: vvv.Get("RangeVar.schemaname").String(),
										Type:       vvv.Get("RangeVar.relpersistence").String(),
									})
								}
							}
							if vv.Get("JoinExpr.rarg.RangeSubselect").Exists() {
								fromClause := vv.Get("JoinExpr.rarg.RangeSubselect.subquery.SelectStmt.fromClause").Array()
								for _, vvv := range fromClause {
									sqlTree.Source = append(sqlTree.Source, &Record{
										RelName:    vvv.Get("RangeVar.relname").String(),
										SchemaName: vvv.Get("RangeVar.schemaname").String(),
										Type:       vvv.Get("RangeVar.relpersistence").String(),
									})
								}
							}
							if vv.Get("JoinExpr.larg.JoinExpr").Exists() {
								JoinExpr := vv.Get("JoinExpr.larg.JoinExpr")
								sqlTree.Source = append(sqlTree.Source,
									&Record{
										RelName:    JoinExpr.Get("larg.RangeVar.relname").String(),
										SchemaName: JoinExpr.Get("larg.RangeVar.schemaname").String(),
										Type:       JoinExpr.Get("larg.RangeVar.relpersistence").String(),
									},
									&Record{
										RelName:    JoinExpr.Get("rarg.RangeVar.relname").String(),
										SchemaName: JoinExpr.Get("rarg.RangeVar.schemaname").String(),
										Type:       JoinExpr.Get("rarg.RangeVar.relpersistence").String(),
									})
							}
							if vv.Get("JoinExpr.rarg.JoinExpr").Exists() {
								JoinExpr := vv.Get("JoinExpr.rarg.JoinExpr")
								sqlTree.Source = append(sqlTree.Source,
									&Record{
										RelName:    JoinExpr.Get("larg.RangeVar.relname").String(),
										SchemaName: JoinExpr.Get("larg.RangeVar.schemaname").String(),
										Type:       JoinExpr.Get("larg.RangeVar.relpersistence").String(),
									},
									&Record{
										RelName:    JoinExpr.Get("rarg.RangeVar.relname").String(),
										SchemaName: JoinExpr.Get("rarg.RangeVar.schemaname").String(),
										Type:       JoinExpr.Get("rarg.RangeVar.relpersistence").String(),
									})
							}

						}
						if vv.Get("RangeVar").Exists() {
							sqlTree.Source = append(sqlTree.Source, &Record{
								RelName:    vv.Get("RangeVar.relname").String(),
								SchemaName: vv.Get("RangeVar.schemaname").String(),
								Type:       vv.Get("RangeVar.relpersistence").String(),
							})
						}
					}
				}
				if v.Get("stmt.CreateTableAsStmt.into").Exists() {
					sqlTree.Target = append(sqlTree.Target, &Record{
						RelName:    v.Get("stmt.CreateTableAsStmt.into.rel.relname").String(),
						SchemaName: v.Get("stmt.CreateTableAsStmt.into.rel.schemaname").String(),
						Type:       v.Get("stmt.CreateTableAsStmt.into.rel.relpersistence").String(),
					})
				}
			}

			// 如果该 SQL 为 insert into select 操作
			if v.Get("stmt.InsertStmt").Exists() {
				if v.Get("stmt.InsertStmt.selectStmt").Exists() {
					fromClause := v.Get("stmt.InsertStmt.selectStmt.SelectStmt.fromClause").Array()
					for _, vv := range fromClause {
						if vv.Get("JoinExpr").Exists() {

							if vv.Get("JoinExpr.larg.RangeVar").Exists() {
								sqlTree.Source = append(sqlTree.Source, &Record{
									RelName:    vv.Get("JoinExpr.larg.RangeVar.relname").String(),
									SchemaName: vv.Get("JoinExpr.larg.RangeVar.schemaname").String(),
									Type:       vv.Get("JoinExpr.larg.RangeVar.relpersistence").String(),
								})
							}
							if vv.Get("JoinExpr.rarg.RangeVar").Exists() {
								sqlTree.Source = append(sqlTree.Source, &Record{
									RelName:    vv.Get("JoinExpr.rarg.RangeVar.relname").String(),
									SchemaName: vv.Get("JoinExpr.rarg.RangeVar.schemaname").String(),
									Type:       vv.Get("JoinExpr.rarg.RangeVar.relpersistence").String(),
								})
							}
							if vv.Get("JoinExpr.larg.RangeSubselect").Exists() {
								fromClause := vv.Get("JoinExpr.larg.RangeSubselect.subquery.SelectStmt.fromClause").Array()
								for _, vvv := range fromClause {
									sqlTree.Source = append(sqlTree.Source, &Record{
										RelName:    vvv.Get("RangeVar.relname").String(),
										SchemaName: vvv.Get("RangeVar.schemaname").String(),
										Type:       vvv.Get("RangeVar.relpersistence").String(),
									})
								}
							}
							if vv.Get("JoinExpr.rarg.RangeSubselect").Exists() {
								fromClause := vv.Get("JoinExpr.rarg.RangeSubselect.subquery.SelectStmt.fromClause").Array()
								for _, vvv := range fromClause {
									sqlTree.Source = append(sqlTree.Source, &Record{
										RelName:    vvv.Get("RangeVar.relname").String(),
										SchemaName: vvv.Get("RangeVar.schemaname").String(),
										Type:       vvv.Get("RangeVar.relpersistence").String(),
									})
								}
							}
							if vv.Get("JoinExpr.larg.JoinExpr").Exists() {
								JoinExpr := vv.Get("JoinExpr.larg.JoinExpr")
								sqlTree.Source = append(sqlTree.Source,
									&Record{
										RelName:    JoinExpr.Get("larg.RangeVar.relname").String(),
										SchemaName: JoinExpr.Get("larg.RangeVar.schemaname").String(),
										Type:       JoinExpr.Get("larg.RangeVar.relpersistence").String(),
									},
									&Record{
										RelName:    JoinExpr.Get("rarg.RangeVar.relname").String(),
										SchemaName: JoinExpr.Get("rarg.RangeVar.schemaname").String(),
										Type:       JoinExpr.Get("rarg.RangeVar.relpersistence").String(),
									})
							}
							if vv.Get("JoinExpr.rarg.JoinExpr").Exists() {
								JoinExpr := vv.Get("JoinExpr.rarg.JoinExpr")
								sqlTree.Source = append(sqlTree.Source,
									&Record{
										RelName:    JoinExpr.Get("larg.RangeVar.relname").String(),
										SchemaName: JoinExpr.Get("larg.RangeVar.schemaname").String(),
										Type:       JoinExpr.Get("larg.RangeVar.relpersistence").String(),
									},
									&Record{
										RelName:    JoinExpr.Get("rarg.RangeVar.relname").String(),
										SchemaName: JoinExpr.Get("rarg.RangeVar.schemaname").String(),
										Type:       JoinExpr.Get("rarg.RangeVar.relpersistence").String(),
									})
							}
						}
						// TODO:"fromClause":[{"RangeVar":{"relname":"temp_invalid_data","inh":true,"relpersistence":"p","location":135}}],"limitOption":"LIMIT_OPTION_DEFAULT","op":"SETOP_NONE"}}
						if vv.Get("RangeVar").Exists() {
							sqlTree.Source = append(sqlTree.Source, &Record{
								RelName:    vv.Get("RangeVar.relname").String(),
								SchemaName: vv.Get("RangeVar.schemaname").String(),
								Type:       vv.Get("RangeVar.relpersistence").String(),
							})
						}
					}
				}

				sqlTree.Target = append(sqlTree.Target, &Record{
					RelName:    v.Get("stmt.InsertStmt.relation.relname").String(),
					SchemaName: v.Get("stmt.InsertStmt.relation.schemaname").String(),
					Type:       v.Get("stmt.InsertStmt.relation.relpersistence").String(),
				})
			}

			// update 语句
			if v.Get("stmt.UpdateStmt").Exists() {

				// TODO:需支持关联更新 "fromClause":[{"RangeVar":{"schemaname":"dm","relname":"hpe_mmp_workobjectinfo","inh":true,"relpersistence":"p","alias":{"aliasname":"s"},"location":58}}]}}}]}

				sqlTree.Target = append(sqlTree.Target, &Record{
					RelName:    v.Get("stmt.UpdateStmt.relation.relname").String(),
					SchemaName: v.Get("stmt.UpdateStmt.relation.schemaname").String(),
					Type:       v.Get("stmt.UpdateStmt.relation.relpersistence").String(),
				})
			}

			// ignore
			if v.Get("stmt.VacuumStmt").Exists() {
				break
			}
		}

	case "PLpgSQL_stmt_dynexecute":

		// 支持 execute dynamic sql

		subQuery := gjson.Get(plan, "query.PLpgSQL_expr.query").String()

		subTree, err := pg_query.ParseToJSON(subQuery)
		if err != nil {
			return err
		}
		log.Printf("%s: %s\n", subQuery, subTree)

	case "PLpgSQL_stmt_perform":

	}

	return nil
}
