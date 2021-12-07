package main

import (

	// "github.com/cobolbaby/lineage/depgraph"

	"errors"
	"strings"
	"time"

	"github.com/cobolbaby/lineage/pkg/depgraph"
	"github.com/cobolbaby/lineage/pkg/log"
	pg_query "github.com/pganalyze/pg_query_go/v2"
	"github.com/tidwall/gjson"
)

var (
	PLPGSQL_BLACKLIST_STMTS = map[string]bool{
		"PLpgSQL_stmt_assign":     true,
		"PLpgSQL_stmt_raise":      true,
		"PLpgSQL_stmt_execsql":    false,
		"PLpgSQL_stmt_if":         true,
		"PLpgSQL_stmt_dynexecute": true, // 比较复杂，不太好支持
		"PLpgSQL_stmt_perform":    true, // 暂不支持
	}
)

type Owner struct {
	Username string
	Nickname string
	ID       string
}

type Record struct {
	SchemaName string
	RelName    string
	Type       string
	Columns    []string
	Comment    string
	Visited    string
	Size       int64
	Layer      string
	Database   string
	Owner      *Owner
	CreateTime time.Time
	Labels     []string
	ID         string
}

func (r *Record) GetID() string {
	if r.ID != "" {
		return r.ID
	}

	if r.SchemaName != "" {
		return r.SchemaName + "." + r.RelName
	} else {
		switch r.RelName {
		case "pg_namespace", "pg_class", "pg_attribute", "pg_type":
			r.SchemaName = "pg_catalog"
			return r.SchemaName + "." + r.RelName
		default:
			return r.RelName
		}
	}
}

func (r *Record) IsTemp() bool {
	return r.SchemaName == "" ||
		strings.HasPrefix(r.RelName, "temp_") ||
		strings.HasPrefix(r.RelName, "tmp_")
}

type Op struct {
	Type       string
	ProcName   string
	SchemaName string
	Database   string
	Comment    string
	Owner      *Owner
	SrcID      string
	DestID     string
	ID         string
}

func (o *Op) GetID() string {
	if o.ID != "" {
		return o.ID
	}

	if o.SchemaName == "" {
		o.SchemaName = "public"
	}
	return o.SchemaName + "." + o.ProcName
}

const (
	REL_PERSIST     = "p"
	REL_PERSIST_NOT = "t"
)

func ParseUDF(sqlTree *depgraph.Graph, plpgsql string) error {

	raw, err := pg_query.ParsePlPgSqlToJSON(plpgsql)
	if err != nil {
		return err
	}

	// log.Debugf("pg_query.ParsePlPgSqlToJSON: %s", raw)
	v := gjson.Parse(raw).Array()[0]

	for _, action := range v.Get("PLpgSQL_function.action.PLpgSQL_stmt_block.body").Array() {
		// 遍历属性
		action.ForEach(func(key, value gjson.Result) bool {
			// 没有配置，或者屏蔽掉的
			if enable, ok := PLPGSQL_BLACKLIST_STMTS[key.String()]; ok && enable {
				return false
			}

			// 递归调用 Parse
			if err := parseUDFOperator(sqlTree, key.String(), value.String()); err != nil {
				log.Errorf("pg_query.ParseToJSON err: %s, sql: %s", err, value.String())
				return false
			}

			return true
		})
	}

	return nil
}

func parseUDFOperator(sqlTree *depgraph.Graph, operator, plan string) error {
	// log.Printf("%s: %s\n", operator, plan)

	var subQuery string

	switch operator {
	case "PLpgSQL_stmt_execsql":
		subQuery = gjson.Get(plan, "sqlstmt.PLpgSQL_expr.query").String()

		// 跳过不必要的SQL，没啥解析的价值
		if subQuery == "select clock_timestamp()" {
			return nil
		}

	case "PLpgSQL_stmt_dynexecute":
		// 支持 execute dynamic sql
		subQuery = gjson.Get(plan, "query.PLpgSQL_expr.query").String()

	}

	if err := ParseSQL(sqlTree, subQuery); err != nil {
		return err
	}

	return nil
}

func ParseSQL(sqlTree *depgraph.Graph, sql string) error {

	raw, err := pg_query.ParseToJSON(sql)
	if err != nil {
		return err
	}
	// log.Debugf("%s: %s\n", sql, raw)

	stmts := gjson.Get(raw, "stmts.#.stmt").Array()
	for _, v := range stmts {

		// 跳过 analyze/drop/truncate/create index 语句
		if v.Get("VacuumStmt").Exists() ||
			v.Get("DropStmt").Exists() ||
			v.Get("TruncateStmt").Exists() ||
			v.Get("IndexStmt").Exists() {
			break
		}

		// create table ... as 操作
		if v.Get("CreateTableAsStmt").Exists() {
			cvv := v.Get("CreateTableAsStmt")

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
		if v.Get("CreateStmt").Exists() {
			vv := v.Get("CreateStmt")
			if r := parseRelname(vv); r != nil {
				sqlTree.AddNode(r)
			}
		}

		// 如果该 SQL 为 select 操作，则获取 from
		if v.Get("SelectStmt").Exists() {
			vv := v.Get("SelectStmt")
			for _, r := range parseFromClause(vv) {
				sqlTree.AddNode(r)
			}
		}

		// insert into tbl ...
		// insert into tbl ... select * from ...
		// with ... insert into tbl ... select * from ...
		// insert into tbl with ... select * from ...
		if v.Get("InsertStmt").Exists() {
			ivv := v.Get("InsertStmt")

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
		if v.Get("UpdateStmt").Exists() {
			vv := v.Get("UpdateStmt")
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
		if v.Get("DeleteStmt").Exists() {
			vv := v.Get("DeleteStmt")
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

func IdentifyFuncCall(sql string) (*Op, error) {
	log.Debugf("IdentifyFuncCall %s", sql)

	var records *Op

	raw, _ := pg_query.ParseToJSON(sql)
	stmts := gjson.Get(raw, "stmts.#.stmt").Array()
	for _, v := range stmts {

		// 支持通过 select 方式调用
		if v.Get("SelectStmt").Exists() {

			// 形如 select dw.func_insert_xxxx(a,b)
			targetList := v.Get("SelectStmt.targetList").Array()
			for _, vv := range targetList {

				if vv.Get("ResTarget.val.FuncCall").Exists() {
					// 只解析第一个结构
					records = parseFuncCall(vv.Get("ResTarget.val"))
					break
				}
			}

			// 形如 select * from report.query_xxxx(1,2,3)
			fromClause := v.Get("SelectStmt.fromClause").Array()
			for _, vv := range fromClause {

				if vv.Get("RangeFunction").Exists() {

					// https://github.com/tidwall/gjson#path-syntax
					udfs := vv.Get("RangeFunction.functions.#.List.items").Array()
					for _, vvv := range udfs {
						// 只解析第一个结构
						records = parseFuncCall(vvv.Array()[0])
						break
					}
				}
			}

		}

		// 支持通过 call 方式调用
	}

	if records == nil {
		return nil, errors.New("not a function call")
	}
	return records, nil
}

func parseFuncCall(v gjson.Result) *Op {
	if !v.Get("FuncCall").Exists() {
		return nil
	}

	funcname := v.Get("FuncCall.funcname.#.String.str").Array()

	if len(funcname) == 2 {
		return &Op{
			ProcName:   funcname[1].String(),
			SchemaName: funcname[0].String(),
			Type:       "plpgsql",
		}
	}
	if len(funcname) == 1 {
		return &Op{
			ProcName:   funcname[0].String(),
			SchemaName: "",
			Type:       "plpgsql",
		}
	}

	return nil
}
