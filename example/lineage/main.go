package main

import (
	"fmt"
	"os"
	"strings"

	"pg_lineage/internal/lineage"
	"pg_lineage/pkg/config"
	"pg_lineage/pkg/log"
)

func init() {
	if err := log.InitLogger(&config.LogConfig{
		Level: "debug",
		Path:  "./logs/lineage.log",
	}); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func main() {
	rawsql := `
		with a as
		(
			SELECT section_alias, post, sum(required) as required
			FROM scc.f6_post_allocation
			where required is not null and section_alias = 'Service'
			group by section_alias, post
		),
		b as
		(
			SELECT remark1, section, count(userid) as actual
			FROM scc.employee_line_class
			where title = 'DL' and inuse = 1
				and section = 'Service'
			group by section, remark1
		),
		c as
		(
			select a.section_alias as section, a.post, a.required, b.actual::int as "实际",
				case when a.required <= b.actual then null else a.required - coalesce(b.actual, 0) end as "差异"
			from a
				left join b on b.section = a.section_alias and b.remark1 = a.post
		)
		select section, case when post = 'BGA-Rework' then 'Rework' else post end as post, required,
		"实际", "差异"
		from c
		order by coalesce("实际", 0) + coalesce("差异", 0) desc 


	`

	sqlTree, err := lineage.Parse(rawsql)
	if err != nil {
		log.Error(err)
		return
	}

	// 拓扑排序
	for i, layer := range sqlTree.ShrinkGraph().TopoSortedLayers() {
		log.Debugf("%d: %s\n", i, strings.Join(layer, ", "))
	}
}
