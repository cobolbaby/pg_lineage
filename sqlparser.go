package main

import (

	// "github.com/cobolbaby/lineage/depgraph"

	"errors"

	"github.com/cobolbaby/lineage/pkg/depgraph"
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

		// 跳过不必要的SQL，没啥解析的价值
		if subQuery == "select clock_timestamp()" {
			return nil
		}

		subTree, err = pg_query.ParseToJSON(subQuery)

	case "PLpgSQL_stmt_dynexecute":
		// 支持 execute dynamic sql
		subQuery = gjson.Get(plan, "query.PLpgSQL_expr.query").String()
		subTree, err = pg_query.ParseToJSON(subQuery)
	}

	if err != nil {
		return err
	}
	// log.Printf("%s: %s\n", subQuery, subTree)

	stmts := gjson.Get(subTree, "stmts").Array()
	for _, v := range stmts {

		// 跳过 analyze/drop/truncate/create index 语句
		if v.Get("stmt.VacuumStmt").Exists() ||
			v.Get("stmt.DropStmt").Exists() ||
			v.Get("stmt.TruncateStmt").Exists() ||
			v.Get("stmt.IndexStmt").Exists() {
			break
		}

		// create table ... as 操作
		if v.Get("stmt.CreateTableAsStmt").Exists() {
			cvv := v.Get("stmt.CreateTableAsStmt")

			tnode := &Record{
				RelName:    cvv.Get("into.rel.relname").String(),
				SchemaName: cvv.Get("into.rel.schemaname").String(),
				Type:       cvv.Get("into.rel.relpersistence").String(),
			}
			sqlTree.AddNode(tnode)

			if cvv.Get("query.SelectStmt").Exists() {
				vv := cvv.Get("query.SelectStmt")

				if vv.Get("withClause").Exists() {
					parseWithClause(vv, sqlTree)
				}

				for _, r := range parseFromClause(vv) {
					sqlTree.DependOn(tnode, r)
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

		// insert into tbl ...
		// insert into tbl ... select * from ...
		// with ... insert into tbl ... select * from ...
		// insert into tbl with ... select * from ...
		if v.Get("stmt.InsertStmt").Exists() {
			ivv := v.Get("stmt.InsertStmt")

			if ivv.Get("withClause").Exists() {
				parseWithClause(ivv, sqlTree)
			}

			tnode := parseRelname(ivv)
			sqlTree.AddNode(tnode)

			if ivv.Get("selectStmt.SelectStmt").Exists() {
				vv := ivv.Get("selectStmt.SelectStmt")

				if vv.Get("withClause").Exists() {
					parseWithClause(vv, sqlTree)
				}

				for _, r := range parseFromClause(vv) {
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
		// delete from tbl where ...
		// delete from tbl using tbl2 where ...
		if v.Get("stmt.DeleteStmt").Exists() {
			vv := v.Get("stmt.DeleteStmt")
			tnode := parseRelname(vv)
			sqlTree.AddNode(tnode)

			// 关联删除，依赖 using 关键词
			if vv.Get("usingClause").Exists() {
				for _, r := range parseUsingClause(vv) {
					sqlTree.DependOn(tnode, r)
				}
			}
		}
	}

	return nil
}

func parseUsingClause(v gjson.Result) []*Record {
	var records []*Record

	if !v.Get("usingClause").Exists() {
		return records
	}

	usingClause := v.Get("usingClause").Array()
	for _, vv := range usingClause {

		// 只有一个表
		if vv.Get("RangeVar").Exists() {
			records = append(records, &Record{
				RelName:    vv.Get("RangeVar.relname").String(),
				SchemaName: vv.Get("RangeVar.schemaname").String(),
				Type:       vv.Get("RangeVar.relpersistence").String(),
			})
		}

	}
	return records
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

// CTE 子句
func parseWithClause(v gjson.Result, sqlTree *depgraph.Graph) error {

	if !v.Get("withClause").Exists() {
		return errors.New("withClause not exists")
	}

	ctes := v.Get("withClause.ctes").Array()
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

	return nil
}

// UNION 解析
func parserUnionClause(v gjson.Result) []*Record {
	var records []*Record

	if v.Get("op").String() != "SETOP_UNION" {
		return records
	}

	if r := parseFromClause(v.Get("larg")); r != nil {
		records = append(records, r...)
	}
	if r := parseFromClause(v.Get("rarg")); r != nil {
		records = append(records, r...)
	}
	return records
}

// FROM Clause
func parseFromClause(v gjson.Result) []*Record {
	// 如果遇到 UNION，则调用 parserUnionClause 方法
	if v.Get("op").String() == "SETOP_UNION" {
		return parserUnionClause(v)
	}

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
	var records []*Record

	if !v.Get("JoinExpr").Exists() {
		return records
	}

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
