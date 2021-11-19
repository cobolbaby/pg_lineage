package main

import (
	"fmt"
	"lineage/depgraph"
	"strings"
)

func main() {
	g := depgraph.New()

	r1 := &depgraph.Record{
		SchemaName: "public",
		RelName:    "test_table",
		Type:       "p",
		ID:         "public.test_table",
	}
	r2 := &depgraph.Record{
		SchemaName: "public",
		RelName:    "test_table2",
		Type:       "p",
		ID:         "public.test_table2",
	}
	r3 := &depgraph.Record{
		SchemaName: "",
		RelName:    "temp_test_table3",
		Type:       "t",
		ID:         "temp_test_table3",
	}
	r4 := &depgraph.Record{
		SchemaName: "",
		RelName:    "temp_test_table4",
		Type:       "t",
		ID:         "temp_test_table4",
	}
	r5 := &depgraph.Record{
		SchemaName: "dw",
		RelName:    "test_table5",
		Type:       "t",
		ID:         "dw.test_table5",
	}
	r6 := &depgraph.Record{
		SchemaName: "dw",
		RelName:    "test_table6",
		Type:       "p",
		ID:         "dw.test_table6",
	}
	r7 := &depgraph.Record{
		SchemaName: "dw",
		RelName:    "test_table7",
		Type:       "p",
		ID:         "dw.test_table7",
	}

	g.DependOn(r3, r1)
	g.DependOn(r5, r3)
	g.DependOn(r5, r4)
	g.DependOn(r6, r5)
	g.DependOn(r4, r2)
	g.DependOn(r7, r5)

	for i, layer := range g.ShrinkGraph().TopoSortedLayers() {
		fmt.Printf("%d: %s\n", i, strings.Join(layer, ", "))
	}
	// Output:
	// 0: feed, soil
	// 1: grain
	// 2: flour, chickens
	// 3: eggs
	// 4: cake
}
