package main

import (
	"crypto/md5"
	"encoding/json"
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v2"
)

func main() {

	// SELECT
	// 	*
	// FROM
	// 	demo.tbl1 a
	// 	JOIN demo.tbl2 b ON a.id = b.aid AND a.name = b.name
	// 	JOIN demo.tbl3 c ON a.id = c.aid;
	// SELECT
	// 	*
	// FROM
	// 	demo.tbl1 aa
	// 	JOIN demo.tbl3 cc ON aa.id = cc.aid;
	// SELECT
	// 	*
	// FROM
	// 	demo.tbl1 aaa
	// 	JOIN demo.tbl2 bbb ON aaa.id = bbb.aid;
	// SELECT
	// 	*
	// FROM
	// 	demo.tbl1 a
	// 	JOIN demo.tbl2 b ON a.id = b.aid AND a.name = b.name
	// 	JOIN demo.tbl3 c ON a.id = c.aid
	// 	LEFT JOIN demo.tbl4 d ON a.did = d.aid;

	sql := `
	SELECT
		*
	FROM
		demo.tbl1 aaaa
	WHERE
		aaaa.id IN (
			SELECT
				aid
			FROM
				demo.tbl2
		)
		and aaaa.name = 'name'
		and aaaa.cid IN (
			SELECT
				cid
			FROM
				demo.tbl3
			WHERE
				cid = 1
		);	
	`

	result, err := pg_query.Parse(sql)
	if err != nil {
		panic(err)
	}

	m := make(map[string]*RelationShip)
	for _, v := range result.Stmts {
		aliasMap := make(map[string]*Relation)

		// 从 JOIN 中获取关系
		fmt.Printf("GetFromClause %+v\n", v)
		for _, vv := range v.Stmt.GetSelectStmt().GetFromClause() {

			// 需要从 FromClause 中获取别名信息，以便在 WHERE 使用
			// 需要从 FromClause 中获取 JoinExpr 信息，以便提炼出关系
			plan := parseFromClause(vv, aliasMap)

			m = MergeMap(m, plan.GetRelationShip())
		}

		// 从 WHERE 中获取关系
		// fmt.Println(v.Stmt.GetSelectStmt().GetWhereClause())
		r := parseWhereClause(v.Stmt.GetSelectStmt().GetWhereClause(), aliasMap)
		m = MergeMap(m, r)
	}

	fmt.Printf("GetRelationShip: #%d\n", len(m))
	for _, vv := range m {
		fmt.Printf("%s\n", vv.ToString())
	}

	// filter...
	// 如果要写库，需要过滤掉临时表

}

func parseFromClause(node *pg_query.Node, aliasMap map[string]*Relation) *Record {
	// 如果包含 JOIN 则怎么样，如果不包含 JOIN 又怎么样
	if node.GetJoinExpr() != nil {
		return parseJoinClause(node, aliasMap)
	}

	if node.GetRangeVar() != nil {
		return parseRangeVar(node.GetRangeVar(), aliasMap)
	}

	return nil

}

func parseRangeVar(node *pg_query.RangeVar, aliasMap map[string]*Relation) *Record {
	lRelation := &Relation{
		RelName:        node.GetRelname(),
		Schema:         node.GetSchemaname(),
		Alias:          node.GetAlias().GetAliasname(),
		Relpersistence: node.GetRelpersistence(),
	}

	aliasMap[lRelation.Alias] = lRelation

	return &Record{
		LRelation: lRelation,
	}
}

type Relation struct {
	Schema         string
	RelName        string
	Alias          string
	Relpersistence string
}

type Column struct {
	Schema  string
	RelName string
	Field   string
	Alias   string
}

type RelationShip struct {
	SColumn *Column
	TColumn *Column
	Type    string
}

func (r *RelationShip) ToString() string {
	return fmt.Sprintf("%s.%s.%s = %s.%s.%s",
		r.SColumn.Schema, r.SColumn.RelName, r.SColumn.Field,
		r.TColumn.Schema, r.TColumn.RelName, r.TColumn.Field,
	)
}

type Record struct {
	LRelation    *Relation
	RRelation    *Relation
	LSubQuery    *Record
	RSubQuery    *Record
	RelationShip map[string]*RelationShip
}

func (records *Record) GetRelationShip() map[string]*RelationShip {
	r := records.RelationShip
	if records.LSubQuery != nil {
		r = MergeMap(r, records.LSubQuery.GetRelationShip())
	}
	if records.RSubQuery != nil {
		r = MergeMap(r, records.RSubQuery.GetRelationShip())
	}
	return r
}

func parseWhereClause(node *pg_query.Node, aliasMap map[string]*Relation) map[string]*RelationShip {
	var relationShip map[string]*RelationShip

	if node.GetSubLink() != nil {
		relationShip = parseSubLink(node.GetSubLink(), pg_query.JoinType_JOIN_INNER, aliasMap)
	} else if node.GetBoolExpr() != nil {
		relationShip = parseBoolExpr(node.GetBoolExpr(), pg_query.JoinType_JOIN_INNER, aliasMap)
	}

	return relationShip
}

func parseSubLink(node *pg_query.SubLink, jointype pg_query.JoinType, aliasMap map[string]*Relation) map[string]*RelationShip {
	var relationShip map[string]*RelationShip

	switch node.GetSubLinkType() {
	case pg_query.SubLinkType_ANY_SUBLINK:
		relationShip = parseAnySubLink(node, jointype, aliasMap)
	// case :
	// 扩展...
	default:
		fmt.Println(fmt.Sprintf("%s", node.GetSubLinkType()))
	}

	return relationShip
}

func parseAnySubLink(node *pg_query.SubLink, jointype pg_query.JoinType, aliasMap map[string]*Relation) map[string]*RelationShip {
	m := make(map[string]*RelationShip)

	relationship := &RelationShip{}

	lrel := aliasMap[node.GetTestexpr().GetColumnRef().GetFields()[0].GetString_().GetStr()]
	relationship.SColumn = &Column{
		Schema:  lrel.Schema,
		RelName: lrel.RelName,
		Field:   node.GetTestexpr().GetColumnRef().GetFields()[1].GetString_().GetStr(),
	}

	rrel := node.GetSubselect().GetSelectStmt().GetFromClause()[0].GetRangeVar()
	relationship.TColumn = &Column{
		Schema:  rrel.GetSchemaname(),
		RelName: rrel.GetRelname(),
		Field:   node.GetSubselect().GetSelectStmt().GetTargetList()[0].GetResTarget().GetVal().GetColumnRef().GetFields()[0].GetString_().GetStr(),
	}

	relationship.Type = jointype.String()

	// checksum
	key := Hash(relationship)
	m[key] = relationship

	return m
}

// 返回左右表间的关系，所以主体有两个表，外加关系，多张表的话，则需要返回子结果集
func parseJoinClause(node *pg_query.Node, aliasMap map[string]*Relation) *Record {

	if node.GetJoinExpr() == nil {
		return &Record{}
	}

	j := node.GetJoinExpr()
	var lRelation, rRelation *Relation
	var lSubQuery, rSubQuery *Record

	// 优先遍历内层JOIN
	if j.GetLarg().GetJoinExpr() != nil {
		lSubQuery = parseJoinClause(j.GetLarg(), aliasMap)
	}
	if j.GetRarg().GetJoinExpr() != nil {
		rSubQuery = parseJoinClause(j.GetRarg(), aliasMap)
	}
	if j.GetLarg().GetRangeVar() != nil {
		lnode := j.GetLarg().GetRangeVar()

		lRelation = &Relation{
			RelName:        lnode.GetRelname(),
			Schema:         lnode.GetSchemaname(),
			Alias:          lnode.GetAlias().GetAliasname(),
			Relpersistence: lnode.GetRelpersistence(),
		}

		aliasMap[lRelation.Alias] = lRelation
	}
	if j.GetRarg().GetRangeVar() != nil {
		rnode := j.GetRarg().GetRangeVar()

		rRelation = &Relation{
			RelName:        rnode.GetRelname(),
			Schema:         rnode.GetSchemaname(),
			Alias:          rnode.GetAlias().GetAliasname(),
			Relpersistence: rnode.GetRelpersistence(),
		}

		aliasMap[rRelation.Alias] = rRelation
	}
	// 记录关系
	relationShip := parseQuals(j, aliasMap)

	return &Record{
		LRelation:    lRelation,
		RRelation:    rRelation,
		LSubQuery:    lSubQuery,
		RSubQuery:    rSubQuery,
		RelationShip: relationShip,
	}
}

func parseQuals(node *pg_query.JoinExpr, aliasMap map[string]*Relation) map[string]*RelationShip {
	var relationShip map[string]*RelationShip

	if node.GetQuals() == nil {
		return relationShip
	}

	if node.GetQuals().GetAExpr() != nil {
		relationShip = parseAExpr(node.GetQuals().GetAExpr(), node.GetJointype(), aliasMap)
	} else if node.GetQuals().GetBoolExpr() != nil {
		relationShip = parseBoolExpr(node.GetQuals().GetBoolExpr(), node.GetJointype(), aliasMap)
	}
	// ...

	return relationShip
}

func MergeMap(mObj ...map[string]*RelationShip) map[string]*RelationShip {
	newObj := make(map[string]*RelationShip)
	for _, m := range mObj {
		for k, v := range m {
			newObj[k] = v
		}
	}
	return newObj
}

func parseBoolExpr(expr *pg_query.BoolExpr, joinType pg_query.JoinType, aliasMap map[string]*Relation) map[string]*RelationShip {
	m := make(map[string]*RelationShip)
	for _, v := range expr.GetArgs() {
		if v.GetAExpr() != nil {
			m = MergeMap(m, parseAExpr(v.GetAExpr(), joinType, aliasMap))
		} else if v.GetSubLink() != nil {
			m = MergeMap(m, parseSubLink(v.GetSubLink(), joinType, aliasMap))
		}
	}
	return m
}

func parseAExpr(expr *pg_query.A_Expr, joinType pg_query.JoinType, aliasMap map[string]*Relation) map[string]*RelationShip {
	m := make(map[string]*RelationShip)

	l := expr.GetLexpr()
	r := expr.GetRexpr()

	if r.GetAConst() != nil { // 可能是 <tbl>.<col> = 'value'，那直接跳过
		return m
	}

	relationship := &RelationShip{}

	if len(l.GetColumnRef().GetFields()) == 2 {
		rel := aliasMap[l.GetColumnRef().GetFields()[0].GetString_().GetStr()]

		relationship.SColumn = &Column{
			Schema:  rel.Schema,
			RelName: rel.RelName,
			Field:   l.GetColumnRef().GetFields()[1].GetString_().GetStr(),
		}
	}

	if len(r.GetColumnRef().GetFields()) == 2 {
		rel := aliasMap[r.GetColumnRef().GetFields()[0].GetString_().GetStr()]

		relationship.TColumn = &Column{
			Schema:  rel.Schema,
			RelName: rel.RelName,
			Field:   r.GetColumnRef().GetFields()[1].GetString_().GetStr(),
		}
	}

	relationship.Type = joinType.String()
	// checksum
	key := Hash(relationship)

	m[key] = relationship
	return m
}

func Hash(s *RelationShip) string {
	data, _ := json.Marshal(s)
	return fmt.Sprintf("%x", md5.Sum(data))
}
