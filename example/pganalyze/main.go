package main

import (
	"crypto/md5"
	"encoding/json"
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v2"
)

func main() {

	sql := `
	with total as (
		select 
			total.*,
			wc.data_value as wc,
			op.data_value as opid
		from fbt.fbt_opentest_detail_total total 
			inner join fbt.fbt_opentest_detail_process_step_datarecords wc 
				on total.transactionid = wc.transactionid 
				and wc.name = 'workflow_id' 
			inner join fbt.fbt_opentest_detail_process_step_datarecords op 
				on total.transactionid = op.transactionid 
				and op.name = 'op_id'
		where 
			--total.end_datetime between ('2021-12-27T20:04:11.881Z' at time zone 'Asia/Shanghai') and ('2021-12-28T08:04:11.881Z' at time zone 'Asia/Shanghai')
			split_part(total.transactionid, '_', 3)::timestamp between ('2021-12-27T20:04:11.881Z' at time zone 'Asia/Shanghai') and ('2021-12-28T08:04:11.881Z' at time zone 'Asia/Shanghai')
			--and wc.data_value <> 'ZTS'
			--and op.data_value <> '9999998'
	), modellist as (
		select model from total group by model
	), start as (
		select 
			result.*,
			fis.family as fisfamily, 
			fis.model as fismodel
		from fbt.opentest_result result 
			inner join fisf3.fis2_pca_model fis on fis_code = substring(sno,1,2)||substring(sno,5,1)
		where 
			teststatus = 'start' 
			--and opid <> '9999998'
			--and endtime between ('2021-12-27T20:04:11.881Z' at time zone 'Asia/Shanghai') and ('2021-12-28T08:04:11.881Z' at time zone 'Asia/Shanghai')
			and split_part(result.transactionid, '_', 3)::timestamp between ('2021-12-27T20:04:11.881Z' at time zone 'Asia/Shanghai') and ('2021-12-28T08:04:11.881Z' at time zone 'Asia/Shanghai')
	), done as (
		select 
			result.*,
			fis.family as fisfamily, 
			fis.model as fismodel
		from fbt.opentest_result result 
			inner join fisf3.fis2_pca_model fis on fis_code = substring(sno,1,2)||substring(sno,5,1)
		where 
			teststatus = 'done' 
			--and opid <> '9999998'
			--and endtime between ('2021-12-27T20:04:11.881Z' at time zone 'Asia/Shanghai') and ('2021-12-28T08:04:11.881Z' at time zone 'Asia/Shanghai')
			and split_part(result.transactionid, '_', 3)::timestamp between ('2021-12-27T20:04:11.881Z' at time zone 'Asia/Shanghai') and ('2021-12-28T08:04:11.881Z' at time zone 'Asia/Shanghai')
	), fislog as (
		select 
			map.fromid as transactionid, 
			model.model, 
			model.family, 
			fislog.*
		from 
			fisf3.pca_sa_log fislog 
			inner join (select distinct fromid, cdt from fisf3.pca_refid_mapping_log where wc in ('15', '20', '37', '3S')) map
				on fislog.cdt = map.cdt 
			inner join fisf3.pca_pca_sno sno
				on fislog.mcbsno = sno.mcbsno 
			inner join fisf3.fis2_pca_model model 
				on sno.model = model.model
		where 
			fislog.wc in ('15', '20', '37', '3S','ZTS') 
			--and fislog.cdt between ('2021-12-27T20:04:11.881Z' at time zone 'Asia/Shanghai') and ('2021-12-28T08:04:11.881Z' at time zone 'Asia/Shanghai')
			and split_part(map.fromid, '_', 3)::timestamp between ('2021-12-27T20:04:11.881Z' at time zone 'Asia/Shanghai') and ('2021-12-28T08:04:11.881Z' at time zone 'Asia/Shanghai')
			and model.model in (select model from modellist)
	), transtable as (
		select 
			coalesce(total.transactionid, start.transactionid, fislog.transactionid)                                as transactionid,
			split_part(coalesce(total.transactionid, start.transactionid, fislog.transactionid), '_', 1)            as mcbsno,
			split_part(coalesce(total.transactionid, start.transactionid, fislog.transactionid), '_', 2)            as wc,
			split_part(coalesce(total.transactionid, start.transactionid, fislog.transactionid), '_', 3)::timestamp as starttime
		from 
			total 
			full outer join start on total.transactionid = start.transactionid 
			full outer join fislog on coalesce(total.transactionid, start.transactionid) = fislog.transactionid 
	), transtimes as (
		select 
			transactionid,
			mcbsno,
			wc,
			starttime, 
			--row_number() over (partition by mcbsno order by starttime) as trno
			row_number() over (partition by mcbsno,wc order by starttime) as trno
		from transtable 
	), steps as ( --增加 symp
		select 
			steps.transactionid, 
			to_char(steps.create_time,'yyyy-mm-dd hh24:mi:ss') as create_time,
			steps.step_status,
			steps.index_name,
			steps.step_name,
			steps.symptom_label,
			wc.data_value as wc,
			family.data_value as family,
			model.data_value as model,
			op.data_value as opid
	   from fbt.fbt_opentest_detail_process_steps steps 
			inner join fbt.fbt_opentest_detail_process_step_datarecords wc 
				on steps.transactionid = wc.transactionid 
				and wc.name = 'workflow_id' 
			inner join fbt.fbt_opentest_detail_process_step_datarecords family
				on steps.transactionid = family.transactionid 
				and family.name = 'family'
			inner join fbt.fbt_opentest_detail_process_step_datarecords model
				on steps.transactionid = model.transactionid 
				and model.name = 'model'
			inner join fbt.fbt_opentest_detail_process_step_datarecords op
				on steps.transactionid = op.transactionid 
				and op.name = 'op_id'
		where 
			split_part(steps.transactionid, '_', 3)::timestamp between ('2021-12-27T20:04:11.881Z' at time zone 'Asia/Shanghai') and ('2021-12-28T08:04:11.881Z' at time zone 'Asia/Shanghai')
			and steps.step_status <> 'PASSED'
			and wc.data_value in ('15', '20', '37', '3S','ZTS') 
			--and (op.data_value <> '9999998' or 'no' = 'no')
		order by transactionid
	), symp as (
		select 
			transactionid,
			string_agg(index_name || step_name || ' ('|| symptoms ||') ', ' | ') as symp
		from (
			select 
				transactionid,
				index_name, 
				step_name,
				string_agg(symptom_label, '; ') as symptoms
			from steps
			group by transactionid, index_name, step_name
			order by transactionid, index_name, step_name
		) as steps_sym
		group by transactionid
		order by transactionid
	)
	select 
		transtimes.mcbsno                                                           as "Mcbsno",
		transtimes.transactionid                                                    as "TransactionId",
		transtimes.trno                                                             as "TrNo",
		--coalesce(total.transactionid, start.transactionid, fislog.transactionid)    as "TransactionId",
		coalesce(total.family, start.fisfamily, fislog.family)                      as "Family",
		coalesce(total.model, start.fismodel, fislog.model)                         as "Model",
		transtimes.wc                                                               as "WC",
		--coalesce(total.wc, start.wc, fislog.wc)                                     as "WC",
		case when start.transactionid is null then 'NoData' else 'Start' end        as "Start",
		case when total.transactionid is null then 'NoData' else 'Log'   end        as "Log",
		coalesce(fislog.ispass::text, 'NoData')                                     as "Fis",
		--case when fislog.transactionid is null then 'NoData' else 'FIS'  end        as "Fis",
		fislog.rework                                                               as "FisRewk",
		case when total.status is null then 'NoData' else total.status   end        as "LogStatus",
		symp.symp                                                                   as "ErrSteps",
		total.start_datetime::timestamp at time zone 'asia/shanghai'                as "LogStarttime", 
		total.end_datetime::timestamp at time zone 'asia/shanghai'                  as "LogEndtime" ,
		coalesce(total.opid, start.opid, fislog.badgeno)                            as "OpId", 
		coalesce(total.station_name, start.fixtureid, fislog.fixno)                 as "Fixture", 
		done.exptime                                                                as "ResExpT"
	from 
		transtimes
		left join total on transtimes.transactionid = total.transactionid
		left join start on transtimes.transactionid = start.transactionid 
		left join fislog on transtimes.transactionid = fislog.transactionid 
		left join done on transtimes.transactionid = done.transactionid 
		left join symp on transtimes.transactionid = symp.transactionid
	where 
		(coalesce(total.opid, start.opid, fislog.badgeno) <> '9999998' or 'no' = 'no')
		and ('' = '' or symp.symp like '%'|| '' ||'%')
		and transtimes.wc = '15'
		and coalesce(total.family, start.fisfamily, fislog.family) in ('','DRAGONITE','MANDALORIAN','MANTINE','METAGROSS','RHYDON')
		and coalesce(total.model, start.fismodel, fislog.model) in ('','1395T3064801','1395T3065201','1395T3204501','1395T3303504','1395T3318501','1395T3318601','1395T3318701','1395T3323303','1395T3323304')
	order by transtimes.starttime desc 
	`

	// Debugger
	// resultJson, err := pg_query.ParseToJSON(sql)
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Println(resultJson)

	m := make(map[string]*RelationShip)

	// 解析sql，暂时不支持 PL/pgSQL
	result, err := pg_query.Parse(sql)
	if err != nil {
		panic(err)
	}

	for _, v := range result.Stmts {
		// TODO:判断为哪种类型SQL
		// truncate 跳过
		// drop   跳过
		// vacuum 跳过
		// analyz 跳过
		// alter  跳过
		// insert 解析其中的 select 子句
		// delete 仅解析关联删除
		// update 仅解析关联更新
		// create 解析其中的 select 子句
		// select 已经解析

		aliasMap := make(map[string]*Relation)

		// 解析 CTE
		r0 := parseWithClause(v.Stmt.GetSelectStmt().GetWithClause(), aliasMap)
		m = MergeMap(m, r0)

		// 解析 FROM 获取关系
		// 从 FromClause 中获取 JoinExpr 信息，以便提炼关系
		// 从 FromClause 中获取别名信息，可能在 WHERE 会用到
		for _, vv := range v.Stmt.GetSelectStmt().GetFromClause() {
			r1 := parseFromClause(vv, aliasMap)
			m = MergeMap(m, r1)
		}

		// 解析 WHERE IN 获取关系
		r2 := parseWhereClause(v.Stmt.GetSelectStmt().GetWhereClause(), aliasMap)
		m = MergeMap(m, r2)
	}

	counter := 0
	for _, vv := range m {
		// 过滤掉临时表
		if vv.SColumn == nil || vv.TColumn == nil || vv.SColumn.Schema == "" || vv.TColumn.Schema == "" {
			continue
		}
		counter += 1
		fmt.Printf("[%d] %s\n", counter, vv.ToString())
	}
	fmt.Printf("GetRelationShip: #%d\n", counter)

}

func parseWithClause(withClause *pg_query.WithClause, aliasMap map[string]*Relation) map[string]*RelationShip {
	m := make(map[string]*RelationShip)

	for _, v := range withClause.GetCtes() {

		// 解析 FROM 获取关系
		// 从 FromClause 中获取 JoinExpr 信息，以便提炼关系
		// 从 FromClause 中获取别名信息，可能在 WHERE 会用到
		for _, vv := range v.GetCommonTableExpr().GetCtequery().GetSelectStmt().GetFromClause() {
			r1 := parseFromClause(vv, aliasMap)
			m = MergeMap(m, r1)
		}

		// 解析 WHERE IN 获取关系
		r2 := parseWhereClause(v.GetCommonTableExpr().GetCtequery().GetSelectStmt().GetWhereClause(), aliasMap)
		m = MergeMap(m, r2)

		// 记录 CTE 的 Alias
		r := &Relation{
			Alias:   v.GetCommonTableExpr().GetCtename(),
			RelName: v.GetCommonTableExpr().GetCtename(),
		}
		aliasMap[r.Alias] = r
	}

	return m
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
	if r.SColumn == nil || r.TColumn == nil {
		return ""
	}

	var sDisplayName, tDisplayName string

	if r.SColumn.Schema == "" {
		sDisplayName = r.SColumn.RelName
	} else {
		sDisplayName = fmt.Sprintf("%s.%s", r.SColumn.Schema, r.SColumn.RelName)
	}
	if r.TColumn.Schema == "" {
		tDisplayName = r.TColumn.RelName
	} else {
		tDisplayName = fmt.Sprintf("%s.%s", r.TColumn.Schema, r.TColumn.RelName)
	}

	return fmt.Sprintf("%s.%s %s %s.%s",
		sDisplayName, r.SColumn.Field,
		r.Type,
		tDisplayName, r.TColumn.Field,
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

func parseFromClause(node *pg_query.Node, aliasMap map[string]*Relation) map[string]*RelationShip {
	// 如果包含 JOIN
	if node.GetJoinExpr() != nil {
		return parseJoinClause(node, aliasMap)
	}

	// 如果包含 SubQuery 。。。

	// 如果只是简单的一张表
	if node.GetRangeVar() != nil {
		return parseRangeVar(node.GetRangeVar(), aliasMap)
	}

	return nil
}

func parseRangeVar(node *pg_query.RangeVar, aliasMap map[string]*Relation) map[string]*RelationShip {
	lRelation := &Relation{
		RelName:        node.GetRelname(),
		Schema:         node.GetSchemaname(),
		Alias:          node.GetAlias().GetAliasname(),
		Relpersistence: node.GetRelpersistence(),
	}

	aliasMap[lRelation.Alias] = lRelation

	return nil
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
		fmt.Printf("node.GetSubLinkType(): %s", node.GetSubLinkType())
	}

	return relationShip
}

func parseAnySubLink(node *pg_query.SubLink, jointype pg_query.JoinType, aliasMap map[string]*Relation) map[string]*RelationShip {
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
	m := make(map[string]*RelationShip)
	key := Hash(relationship)
	m[key] = relationship

	return m
}

// 返回左右表间的关系，所以主体有两个表，外加关系，多张表的话，则需要返回子结果集
func parseJoinClause(node *pg_query.Node, aliasMap map[string]*Relation) map[string]*RelationShip {
	if node.GetJoinExpr() == nil {
		return nil
	}

	m := make(map[string]*RelationShip)
	j := node.GetJoinExpr()

	// 优先遍历内层JOIN
	if j.GetLarg().GetJoinExpr() != nil {
		lSubRelationShip := parseJoinClause(j.GetLarg(), aliasMap)
		m = MergeMap(m, lSubRelationShip)
	}
	if j.GetRarg().GetJoinExpr() != nil {
		rSubRelationShip := parseJoinClause(j.GetRarg(), aliasMap)
		m = MergeMap(m, rSubRelationShip)
	}
	// 处理子查询
	if j.GetLarg().GetRangeSubselect() != nil {
		// 解析 FROM
		for _, vv := range j.GetLarg().GetRangeSubselect().GetSubquery().GetSelectStmt().GetFromClause() {
			r1 := parseFromClause(vv, aliasMap)
			m = MergeMap(m, r1)
		}

		// 解析 WHERE IN 获取关系
		r2 := parseWhereClause(j.GetLarg().GetRangeSubselect().GetSubquery().GetSelectStmt().GetWhereClause(), aliasMap)
		m = MergeMap(m, r2)

		// 记录 SubQuery 的 Alias
		r := &Relation{
			Alias:   j.GetLarg().GetRangeSubselect().GetAlias().GetAliasname(),
			RelName: j.GetLarg().GetRangeSubselect().GetAlias().GetAliasname(),
		}
		aliasMap[r.Alias] = r
	}
	if j.GetRarg().GetRangeSubselect() != nil {
		// 解析 FROM
		for _, vv := range j.GetRarg().GetRangeSubselect().GetSubquery().GetSelectStmt().GetFromClause() {
			r1 := parseFromClause(vv, aliasMap)
			m = MergeMap(m, r1)
		}

		// 解析 WHERE IN 获取关系
		r2 := parseWhereClause(j.GetRarg().GetRangeSubselect().GetSubquery().GetSelectStmt().GetWhereClause(), aliasMap)
		m = MergeMap(m, r2)

		// 记录 SubQuery 的 Alias
		r := &Relation{
			Alias:   j.GetRarg().GetRangeSubselect().GetAlias().GetAliasname(),
			RelName: j.GetRarg().GetRangeSubselect().GetAlias().GetAliasname(),
		}
		aliasMap[r.Alias] = r
	}
	if j.GetLarg().GetRangeVar() != nil {
		parseRangeVar(j.GetLarg().GetRangeVar(), aliasMap)
	}
	if j.GetRarg().GetRangeVar() != nil {
		parseRangeVar(j.GetRarg().GetRangeVar(), aliasMap)
	}
	// 记录关系
	currRelationShip := parseQuals(j, aliasMap)
	m = MergeMap(m, currRelationShip)

	return m
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
		if v.GetAExpr() != nil {
			m = MergeMap(m, parseAExpr(v.GetAExpr(), joinType, aliasMap))
		} else if v.GetSubLink() != nil {
			m = MergeMap(m, parseSubLink(v.GetSubLink(), joinType, aliasMap))
		} else if v.GetBoolExpr() != nil {
			m = MergeMap(m, parseBoolExpr(v.GetBoolExpr(), joinType, aliasMap))
		}
	}
	return m
}

func parseAExpr(expr *pg_query.A_Expr, joinType pg_query.JoinType, aliasMap map[string]*Relation) map[string]*RelationShip {

	l := expr.GetLexpr()
	r := expr.GetRexpr()

	if r.GetAConst() != nil { // col = 'v1'，直接跳过
		return nil
	}
	if r.GetList() != nil { // col IN ('v1', 'v2', ...)
		return nil
	}
	if r.GetAExpr() != nil { // col = 'v1' || 'v2'，那直接跳过
		return nil
	}

	relationship := &RelationShip{}

	if len(l.GetColumnRef().GetFields()) == 2 {
		rel, ok := aliasMap[l.GetColumnRef().GetFields()[0].GetString_().GetStr()]
		if !ok {
			fmt.Printf("Relation not found: %s\n", l.GetColumnRef().GetFields()[0].GetString_().GetStr())
			return nil
		}

		relationship.SColumn = &Column{
			Schema:  rel.Schema,
			RelName: rel.RelName,
			Field:   l.GetColumnRef().GetFields()[1].GetString_().GetStr(),
		}
	}

	if len(r.GetColumnRef().GetFields()) == 2 {
		rel, ok := aliasMap[r.GetColumnRef().GetFields()[0].GetString_().GetStr()]
		if !ok {
			fmt.Printf("Relation not found: %s\n", r.GetColumnRef().GetFields()[0].GetString_().GetStr())
			return nil
		}

		relationship.TColumn = &Column{
			Schema:  rel.Schema,
			RelName: rel.RelName,
			Field:   r.GetColumnRef().GetFields()[1].GetString_().GetStr(),
		}
	}

	relationship.Type = joinType.String()

	// checksum
	m := make(map[string]*RelationShip)
	key := Hash(relationship)
	m[key] = relationship

	return m
}

func Hash(s *RelationShip) string {
	data, _ := json.Marshal(s)
	return fmt.Sprintf("%x", md5.Sum(data))
}
