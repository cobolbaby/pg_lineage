package main

import (
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
		// fmt.Printf("%+v\n", v)
		fmt.Printf("%+v", parseJoinClause(v, make(map[string]*Relation)))
	}

}

type Relation struct {
	RelName        string
	Schema         string
	Alias          string
	Relpersistence string
}

type Column struct {
	Schema  string
	RelName string
	Field   string
}

type Record struct {
	LRelation    *Relation
	RRelation    *Relation
	RelationShip map[Column]Column
	Alias        string
	Type         string
}

// 返回左右表间的关系，所以主体有两个表，外加关系，不过如果返回的是一个子查询，理论上其也会有个别名
func parseJoinClause(node *pg_query.Node, aliasMap map[string]*Relation) *Record {

	if node.GetJoinExpr() == nil {
		return nil
	}

	j := node.GetJoinExpr()
	var lRelation, rRelation *Relation

	// 优先遍历内层JOIN
	if j.GetLarg().GetJoinExpr() != nil {
		lnode := parseJoinClause(j.GetLarg(), aliasMap)

		lRelation = &Relation{
			RelName: "", // ???
			Schema:  "", // ???
			Alias:   lnode.Alias,
		}
	}
	if j.GetRarg().GetJoinExpr() != nil {
		rnode := parseJoinClause(j.GetRarg(), aliasMap)

		rRelation = &Relation{
			RelName: "",
			Schema:  "",
			Alias:   rnode.Alias,
		}
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

	relationShip := parseQuals(j, aliasMap)

	r := &Record{
		LRelation:    lRelation,
		RRelation:    rRelation,
		RelationShip: relationShip,
		Type:         j.GetJointype().String(),
		Alias:        j.GetAlias().GetAliasname(),
	}

	return r
}

func parseQuals(node *pg_query.JoinExpr, aliasMap map[string]*Relation) map[Column]Column {
	if node.GetQuals() == nil {
		return nil
	}

	relationShip := make(map[Column]Column)

	if node.GetQuals().GetAExpr() != nil {
		relationShip = parseAExpr(node.GetQuals().GetAExpr(), aliasMap)
	} else if node.GetQuals().GetBoolExpr() != nil {
		relationShip = parseBoolExpr(node.GetQuals().GetBoolExpr(), aliasMap)
	}
	// ...

	return relationShip
}

func MergeMap(mObj ...map[Column]Column) map[Column]Column {
	newObj := map[Column]Column{}
	for _, m := range mObj {
		for k, v := range m {
			newObj[k] = v
		}
	}
	return newObj
}

func parseBoolExpr(expr *pg_query.BoolExpr, aliasMap map[string]*Relation) map[Column]Column {
	m := map[Column]Column{}
	for _, v := range expr.GetArgs() {
		m = MergeMap(m, parseAExpr(v.GetAExpr(), aliasMap))
	}
	return m
}

func parseAExpr(expr *pg_query.A_Expr, aliasMap map[string]*Relation) map[Column]Column {
	l := expr.GetLexpr()
	r := expr.GetRexpr()

	var key, value Column

	if len(l.GetColumnRef().GetFields()) == 2 {
		rel := aliasMap[l.GetColumnRef().GetFields()[0].GetString_().GetStr()]

		key = Column{
			Schema:  rel.Schema,
			RelName: rel.RelName,
			Field:   l.GetColumnRef().GetFields()[1].GetString_().GetStr(),
		}
	}

	if len(r.GetColumnRef().GetFields()) == 2 {
		rel := aliasMap[r.GetColumnRef().GetFields()[0].GetString_().GetStr()]

		value = Column{
			Schema:  rel.Schema,
			RelName: rel.RelName,
			Field:   r.GetColumnRef().GetFields()[1].GetString_().GetStr(),
		}
	}

	return map[Column]Column{
		key: value,
	}
}
