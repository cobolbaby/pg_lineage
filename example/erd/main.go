package main

import (
	"fmt"
	"os"

	"pg_lineage/internal/erd"
	"pg_lineage/pkg/log"
)

func init() {
	if err := log.InitLogger(&log.LoggerConfig{
		Level: "debug",
		Path:  "./logs/lineage.log",
	}); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func main() {

	plpgsql := `
	select * from dw.func_insert_dim_cpu_dn_cto();
	`

	var m map[string]*erd.RelationShip

	m, _ = erd.ParseUDF(plpgsql)

	n := make(map[string]*erd.RelationShip)
	for kk, vv := range m {
		// 过滤掉临时表
		if vv.SColumn == nil || vv.TColumn == nil || vv.SColumn.Schema == "" || vv.TColumn.Schema == "" {
			continue
		}
		n[kk] = vv
	}
	log.Debugf("GetRelationShip: #%d\n", len(n))
	for _, v := range n {
		log.Debugf("%s\n", v.ToString())
	}

}
