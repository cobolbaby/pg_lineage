package main

import (
	"log"

	"github.com/cobolbaby/lineage/depgraph"

	pg_query "github.com/pganalyze/pg_query_go/v2"
	"github.com/tidwall/gjson"
)

const (
	REL_PERSIST     = "p"
	REL_PERSIST_NOT = "t"
)

func SQLParser(sqlTree *depgraph.Graph, operator, plan string) error {
	// log.Printf("%s: %s\n", operator, plan)

	var subTree string
	var subQuery string
	var err error

	switch operator {
	case "PLpgSQL_stmt_execsql":
		subQuery = gjson.Get(plan, "sqlstmt.PLpgSQL_expr.query").String()
		subTree, err = pg_query.ParseToJSON(subQuery)

	case "PLpgSQL_stmt_dynexecute":
		// 支持 execute dynamic sql
		subQuery = gjson.Get(plan, "query.PLpgSQL_expr.query").String()
		subTree, err = pg_query.ParseToJSON(subQuery)
	}

	if err != nil {
		return err
	}
	log.Printf("%s: %s\n", subQuery, subTree)

	stmts := gjson.Get(subTree, "stmts").Array()
	for _, v := range stmts {

		// 跳过 analyze 语句
		if v.Get("stmt.VacuumStmt").Exists() {
			break
		}
		// 跳过 drop table 语句
		if v.Get("stmt.DropStmt").Exists() {
			break
		}

		// create table ... as 操作
		if v.Get("stmt.CreateTableAsStmt").Exists() {
			cvv := v.Get("stmt.CreateTableAsStmt")
			if cvv.Get("query.SelectStmt.withClause").Exists() {
				ctes := cvv.Get("query.SelectStmt.withClause.ctes").Array()
				for _, vv := range ctes {
					tnode := &Record{
						RelName:    vv.Get("CommonTableExpr.ctename").String(),
						SchemaName: "",
						Type:       REL_PERSIST_NOT,
					}
					sqlTree.AddNode(tnode)

					// 如果存在 FROM 字句，则需要添加依赖关系
					for _, r := range parseFromClause(vv.Get("CommonTableExpr.ctequery.SelectStmt")) {
						sqlTree.DependOn(tnode, r)
					}
				}
			}
			if cvv.Get("into").Exists() {
				tnode := &Record{
					RelName:    cvv.Get("into.rel.relname").String(),
					SchemaName: cvv.Get("into.rel.schemaname").String(),
					Type:       cvv.Get("into.rel.relpersistence").String(),
				}
				sqlTree.AddNode(tnode)

				// 如果存在 FROM 字句，则需要添加依赖关系
				if cvv.Get("query.SelectStmt.fromClause").Exists() {
					for _, r := range parseFromClause(cvv.Get("query.SelectStmt")) {
						sqlTree.DependOn(tnode, r)
					}
				}
			}
		}

		// 单独创建 table
		if v.Get("stmt.CreateStmt").Exists() {
			vv := v.Get("stmt.CreateStmt")
			if r := parseRelname(vv); r != nil {
				sqlTree.AddNode(r)
			}
		}

		// 如果该 SQL 为 select 操作，则获取 from
		if v.Get("stmt.SelectStmt").Exists() {
			vv := v.Get("stmt.SelectStmt")
			for _, r := range parseFromClause(vv) {
				sqlTree.AddNode(r)
			}
		}

		// insert into tbl ... select * from ...
		// insert into tbl ...
		// with ... insert into tbl ... select * from ...
		if v.Get("stmt.InsertStmt").Exists() {
			ivv := v.Get("stmt.InsertStmt")
			if ivv.Get("withClause").Exists() {
				ctes := ivv.Get("withClause.ctes").Array()
				for _, vv := range ctes {
					tnode := &Record{
						RelName:    vv.Get("CommonTableExpr.ctename").String(),
						SchemaName: "",
						Type:       REL_PERSIST_NOT,
					}
					sqlTree.AddNode(tnode)

					// 如果存在 FROM 字句，则需要添加依赖关系
					for _, r := range parseFromClause(vv.Get("CommonTableExpr.ctequery.SelectStmt")) {
						sqlTree.DependOn(tnode, r)
					}
				}
			}

			tnode := parseRelname(ivv)
			sqlTree.AddNode(tnode)

			if ivv.Get("selectStmt").Exists() {
				for _, r := range parseFromClause(ivv.Get("selectStmt.SelectStmt")) {
					sqlTree.DependOn(tnode, r)
				}
			}
		}

		// update tbl set ...
		// update tbl set ... from tbl2
		// update tbl set ... from (select * from tbl2) tbl3 where ...
		if v.Get("stmt.UpdateStmt").Exists() {
			vv := v.Get("stmt.UpdateStmt")
			tnode := parseRelname(vv)
			sqlTree.AddNode(tnode)

			if vv.Get("fromClause").Exists() {
				for _, r := range parseFromClause(vv) {
					sqlTree.DependOn(tnode, r)
				}
			}
		}

		// 如果该 SQL 为 delete 操作，则填充目标节点
		if v.Get("stmt.DeleteStmt").Exists() {
			vv := v.Get("stmt.DeleteStmt")
			tnode := parseRelname(vv)
			sqlTree.AddNode(tnode)

			if vv.Get("fromClause").Exists() {
				for _, r := range parseFromClause(vv) {
					sqlTree.DependOn(tnode, r)
				}
			}
		}
	}

	return nil
}

// INSERT / UPDATE / DELETE / CREATE TABLE 简单操作
func parseRelname(v gjson.Result) *Record {
	if !v.Get("relation").Exists() {
		return nil
	}

	return &Record{
		RelName:    v.Get("relation.relname").String(),
		SchemaName: v.Get("relation.schemaname").String(),
		Type:       v.Get("relation.relpersistence").String(),
	}
}

// FROM Clause
func parseFromClause(v gjson.Result) []*Record {
	var records []*Record

	if !v.Get("fromClause").Exists() {
		return records
	}

	fromClause := v.Get("fromClause").Array()
	for _, vv := range fromClause {

		// 最简单的 select 查询，只有一个表
		if vv.Get("RangeVar").Exists() {
			records = append(records, &Record{
				RelName:    vv.Get("RangeVar.relname").String(),
				SchemaName: vv.Get("RangeVar.schemaname").String(),
				Type:       vv.Get("RangeVar.relpersistence").String(),
			})
		}

		// 子查询
		if vv.Get("RangeSubselect").Exists() {
			if r := parseFromClause(vv.Get("RangeSubselect.subquery.SelectStmt")); r != nil {
				records = append(records, r...)
			}
		}

		// 关联查询
		if vv.Get("JoinExpr").Exists() {
			if r := parseJoinClause(vv); r != nil {
				records = append(records, r...)
			}
		}
	}

	return records
}

// JOIN Clause
func parseJoinClause(v gjson.Result) []*Record {
	if !v.Get("JoinExpr").Exists() {
		return nil
	}

	var records []*Record

	lvv := v.Get("JoinExpr.larg")
	rvv := v.Get("JoinExpr.rarg")

	if lvv.Get("RangeVar").Exists() {
		records = append(records, &Record{
			RelName:    lvv.Get("RangeVar.relname").String(),
			SchemaName: lvv.Get("RangeVar.schemaname").String(),
			Type:       lvv.Get("RangeVar.relpersistence").String(),
		})
	}
	if rvv.Get("RangeVar").Exists() {
		records = append(records, &Record{
			RelName:    rvv.Get("RangeVar.relname").String(),
			SchemaName: rvv.Get("RangeVar.schemaname").String(),
			Type:       rvv.Get("RangeVar.relpersistence").String(),
		})
	}
	if lvv.Get("RangeSubselect").Exists() {
		if r := parseFromClause(lvv.Get("RangeSubselect.subquery.SelectStmt")); r != nil {
			records = append(records, r...)
		}
	}
	if rvv.Get("RangeSubselect").Exists() {
		if r := parseFromClause(rvv.Get("RangeSubselect.subquery.SelectStmt")); r != nil {
			records = append(records, r...)
		}
	}
	if lvv.Get("JoinExpr").Exists() {
		if r := parseJoinClause(lvv); r != nil {
			records = append(records, r...)
		}
	}
	if rvv.Get("JoinExpr").Exists() {
		if r := parseJoinClause(rvv); r != nil {
			records = append(records, r...)
		}
	}

	return records
}
