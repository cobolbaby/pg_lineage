package main

import (
	"crypto/md5"
	"encoding/json"
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v2"
)

func main() {

	sql := `
	SELECT
    	*
	FROM
		demo.tbl1 a
		JOIN demo.tbl2 b ON a.id = b.aid AND a.name = b.name
		JOIN demo.tbl3 c ON a.id = c.aid;
	`

	result, err := pg_query.Parse(sql)
	if err != nil {
		panic(err)
	}

	for _, v := range result.Stmts[0].Stmt.GetSelectStmt().GetFromClause() {
		// fmt.Printf("GetFromClause %+v\n", v)

		joinRel := parseJoinClause(v, make(map[string]*Relation))
		// fmt.Printf("parseJoinClause res %+v\n", joinRel)

		r := joinRel.GetRelationShip()
		fmt.Printf("MergeMap res %+v\n", r)

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

// 返回左右表间的关系，所以主体有两个表，外加关系，不过如果返回的是一个子查询，理论上其也会有个别名
func parseJoinClause(node *pg_query.Node, aliasMap map[string]*Relation) *Record {

	if node.GetJoinExpr() == nil {
		return nil
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
	if node.GetQuals() == nil {
		return nil
	}

	var relationShip map[string]*RelationShip

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
		m = MergeMap(m, parseAExpr(v.GetAExpr(), joinType, aliasMap))
	}
	return m
}

func parseAExpr(expr *pg_query.A_Expr, joinType pg_query.JoinType, aliasMap map[string]*Relation) map[string]*RelationShip {
	m := make(map[string]*RelationShip)

	l := expr.GetLexpr()
	r := expr.GetRexpr()

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
