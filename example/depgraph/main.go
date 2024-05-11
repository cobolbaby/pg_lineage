package main

import (
	"fmt"
	"strings"

	"pg_lineage/internal/lineage"
	"pg_lineage/pkg/depgraph"
)

func main() {
	g := depgraph.New()

	r1 := &lineage.Record{
		SchemaName:     "public",
		RelName:        "test_table",
		RelPersistence: "p",
		ID:             "public.test_table",
	}
	r2 := &lineage.Record{
		SchemaName:     "public",
		RelName:        "test_table2",
		RelPersistence: "p",
		ID:             "public.test_table2",
	}
	r3 := &lineage.Record{
		SchemaName:     "",
		RelName:        "temp_test_table3",
		RelPersistence: "t",
		ID:             "temp_test_table3",
	}
	r4 := &lineage.Record{
		SchemaName:     "",
		RelName:        "temp_test_table4",
		RelPersistence: "t",
		ID:             "temp_test_table4",
	}
	r5 := &lineage.Record{
		SchemaName:     "dw",
		RelName:        "test_table5",
		RelPersistence: "t",
		ID:             "dw.test_table5",
	}
	r6 := &lineage.Record{
		SchemaName:     "dw",
		RelName:        "test_table6",
		RelPersistence: "p",
		ID:             "dw.test_table6",
	}
	r7 := &lineage.Record{
		SchemaName:     "dw",
		RelName:        "test_table7",
		RelPersistence: "p",
		ID:             "dw.test_table7",
	}

	g.DependOn(r3, r1)
	g.DependOn(r4, r2)
	g.DependOn(r5, r3)
	g.DependOn(r5, r4)
	g.DependOn(r6, r5)
	g.DependOn(r7, r5)

	// 拓扑排序
	for i, layer := range g.ShrinkGraph().TopoSortedLayers() {
		fmt.Printf("%d: %s\n", i, strings.Join(layer, ", "))
	}
	// Output:
	// 0: public.test_table, public.test_table2
	// 1: dw.test_table5
	// 2: dw.test_table6, dw.test_table7

}
