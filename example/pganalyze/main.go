package main

import (
	"fmt"

	sqlparser "github.com/cobolbaby/lineage/pkg/sqlparser4join"
)

func main() {

	sql := `
	create temp table temp_new_key
	as
	select t.id, t.snoid,t.sourcetable,t.barcode
	,case when s.mcbsno is null  and c.mcbsno is null then t.barcode
			when c.mcbsno is null  then s.mcbsno
				else c.mcbsno end  as mcbsno,t.wc
	from temp_new_key_ictlog k
	join ictf6.ict_testlog_all t on k.id= t.id
	left join fis.pca_pca_snol s on t.barcode =s.idsno and s.type in ('PPID','CT') and s.ispass=1   -----custsno 转mcbsno
	left join fis.pca_pca_cust_sno  c  on  t.barcode = c.custsno and  c.type  in('CUST_SN','DES_TX_SNO' ) and c.inuse='1'
	`

	m := make(map[string]*sqlparser.RelationShip)

	m, _ = sqlparser.Parse(sql)

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
