package lineage

import (
	"strings"

	"pg_lineage/pkg/depgraph"
	"pg_lineage/pkg/log"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

func ResetGraph(session neo4j.Session) error {

	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		return tx.Run("MATCH (n:Lineage) DETACH DELETE n", nil)
	})

	return err
}

func CreateGraph(session neo4j.Session, graph *depgraph.Graph, udf *Udf) error {

	log.Infof("ShrinkGraph: %+v", graph)
	for i, layer := range graph.TopoSortedLayers() {
		log.Infof("ShrinkGraph %d: %s", i, strings.Join(layer, ", "))
	}

	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		// 创建点
		for _, v := range graph.GetNodes() {
			r, _ := v.(*Table)

			// fix: ShrinkGraph 中仍然存在临时节点
			if r.SchemaName == "" {
				log.Warnf("Invalid r.SchemaName: %+v", r)
				continue
			}

			r.Database = graph.GetNamespace()
			r.Calls = udf.Calls

			if _, err := CreateNode(tx, r); err != nil {
				return nil, err
			}
		}
		// 创建线
		for k, v := range graph.GetRelationships() {
			for kk := range v {

				udf.SrcID = k // 不含 namespace
				udf.DestID = kk
				udf.Database = graph.GetNamespace()

				if _, err := CreateEdge(tx, udf); err != nil {
					return nil, err
				}
			}
		}

		return nil, nil
	})

	return err
}

// 针对 Neo4j 建模，暂定的建模方案：
// 1. 点的 Label 最好是业务分类，但这得在元数据管理系统完备之后，前期先按照 Schema 做拆解
// 2. 线之前定义为了 UDF / Flink Job，但有一定的局限性，考虑将 UDF 转化为点保存
// 3. 对现有服务的改造，就时重写下面两个方法

// 创建图中节点
func CreateNode(tx neo4j.Transaction, r *Table) (*Table, error) {
	// 需要将 ID 作为唯一主键
	// CREATE CONSTRAINT ON (cc:Lineage:PG) ASSERT cc.id IS UNIQUE
	records, err := tx.Run(`
		MERGE (n:Lineage:PG:`+r.SchemaName+` {id: $id}) 
		ON CREATE SET n.database = $database, n.schemaname = $schemaname, n.relname = $relname, n.udt = timestamp(), 
					n.relpersistence = $relpersistence, n.calls = $calls
		ON MATCH SET n.udt = timestamp(), n.relpersistence = $relpersistence, n.calls = n.calls + $calls
		RETURN n.id
	`,
		map[string]interface{}{
			"id":             r.Database + "." + r.GetID(),
			"database":       r.Database,
			"schemaname":     r.SchemaName,
			"relname":        r.RelName,
			"relpersistence": r.RelPersistence,
			"calls":          r.Calls,
		})
	// In face of driver native errors, make sure to return them directly.
	// Depending on the error, the driver may try to execute the function again.
	if err != nil {
		return nil, err
	}
	record, err := records.Single()
	if err != nil {
		return nil, err
	}
	// You can also retrieve values by name, with e.g. `id, found := record.Get("n.id")`
	r.ID = record.Values[0].(string)
	return r, nil
}

// 创建图中边
func CreateEdge(tx neo4j.Transaction, r *Udf) (*Udf, error) {
	_, err := tx.Run(`
		MATCH (pnode {id: $pid}), (cnode {id: $cid})
		CREATE (pnode)-[e:DownStream {id: $id, database: $database, schemaname: $schemaname, procname: $procname, calls: $calls, udt: timestamp()}]->(cnode)
		RETURN e
	`, map[string]interface{}{
		"id":         r.Database + "." + r.GetID(),
		"database":   r.Database,
		"schemaname": r.SchemaName,
		"procname":   r.ProcName,
		"pid":        r.Database + "." + r.SrcID,
		"cid":        r.Database + "." + r.DestID,
		"calls":      r.Calls,
	})

	return nil, err
}

func CompleteLineageGraphInfo(session neo4j.Session, r *Table) error {
	// Create or update Neo4j node with PostgreSQL data
	cypher := `
		MERGE (n:Lineage:PG:` + r.SchemaName + ` {id: $id})
		ON CREATE SET n.database = $database, n.schemaname = $schemaname, n.relname = $relname, 
					n.udt = timestamp(), n.comment = $comment,  
					n.seq_scan = $seq_scan, n.seq_tup_read = $seq_tup_read, 
					n.idx_scan = $idx_scan, n.idx_tup_fetch = $idx_tup_fetch
		ON MATCH SET n.udt = timestamp(), n.comment = $comment, 
					n.seq_scan = $seq_scan, n.seq_tup_read = $seq_tup_read, 
					n.idx_scan = $idx_scan, n.idx_tup_fetch = $idx_tup_fetch
	`
	_, err := session.WriteTransaction(func(transaction neo4j.Transaction) (interface{}, error) {
		result, err := transaction.Run(cypher, map[string]interface{}{
			"id":            r.Database + "." + r.GetID(),
			"database":      r.Database,
			"schemaname":    r.SchemaName,
			"relname":       r.RelName,
			"seq_scan":      r.SeqScan, // Set your value for seq_scan
			"seq_tup_read":  r.SeqTupRead,
			"idx_scan":      r.IdxScan,
			"idx_tup_fetch": r.IdxTupFetch,
			"comment":       r.Comment,
		})
		if err != nil {
			return nil, err
		}
		return result.Consume()
	})
	return err
}
