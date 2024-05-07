package main

import (
	"fmt"
	"strings"
	"time"

	"pg_lineage/pkg/depgraph"
)

type Record struct {
	SchemaName string
	RelName    string
	Type       string
	Columns    []string
	Comment    string
	Visited    string
	Size       int64
	Layer      string
	Database   string
	CreateTime time.Time
	Labels     []string
	ID         string
}

func (r *Record) GetID() string {
	if r.SchemaName != "" {
		return r.SchemaName + "." + r.RelName
	} else {
		return r.RelName
	}
}

func (r *Record) IsTemp() bool {
	return strings.HasPrefix(r.RelName, "temp_") ||
		strings.HasPrefix(r.RelName, "tmp_") ||
		r.SchemaName == ""
}

func main() {
	g := depgraph.New()

	r1 := &Record{
		SchemaName: "public",
		RelName:    "test_table",
		Type:       "p",
		ID:         "public.test_table",
	}
	r2 := &Record{
		SchemaName: "public",
		RelName:    "test_table2",
		Type:       "p",
		ID:         "public.test_table2",
	}
	r3 := &Record{
		SchemaName: "",
		RelName:    "temp_test_table3",
		Type:       "t",
		ID:         "temp_test_table3",
	}
	r4 := &Record{
		SchemaName: "",
		RelName:    "temp_test_table4",
		Type:       "t",
		ID:         "temp_test_table4",
	}
	r5 := &Record{
		SchemaName: "dw",
		RelName:    "test_table5",
		Type:       "t",
		ID:         "dw.test_table5",
	}
	r6 := &Record{
		SchemaName: "dw",
		RelName:    "test_table6",
		Type:       "p",
		ID:         "dw.test_table6",
	}
	r7 := &Record{
		SchemaName: "dw",
		RelName:    "test_table7",
		Type:       "p",
		ID:         "dw.test_table7",
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
