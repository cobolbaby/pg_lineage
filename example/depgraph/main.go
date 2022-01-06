package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/cobolbaby/lineage/pkg/depgraph"
	"github.com/mitchellh/copystructure"
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

	// 深，浅拷贝
	// map[string]depgraph.Node
	src1 := g.GetNodes()
	dst11 := deepCopyWithPointer(src1)
	dst12 := deepCopyWithPointerV2(src1)
	fmt.Printf("deepCopyWithPointer GetNodes, src1: %+v, dst11: %+v, dst12: %+v\n", src1, dst11, dst12)

	// map[string]map[string]struct{}
	src2 := g.GetRelationships()
	dst21 := deepCopyWithStruc(src2)
	dst22 := deepCopyWithStrucV2(src2)
	fmt.Printf("deepCopyWithStruc GetRelationships, src2: %+v, dst21: %+v, dst22: %+v\n", src2, dst21, dst22)

	// &Graph{}
	gc, err := copystructure.Copy(g)
	if err != nil {
		panic(err)
	}
	fmt.Printf("copystructure.Copy(g), g: %+v, gc: %+v\n", g, gc.(*depgraph.Graph))
	gc2 := new(depgraph.Graph)
	*gc2 = *g
	fmt.Printf("new(depgraph.Graph) *gc2 = *g, g: %+v, gc: %+v\n", g, gc2)

}

// 如果 depgraph.Node 为指针，以下方法无法做到深拷贝
func deepCopyWithPointer(src map[string]depgraph.Node) map[string]depgraph.Node {
	dst := make(map[string]depgraph.Node)
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// 如果 depgraph.Node 为指针，以下方法可以做到深拷贝
func deepCopyWithPointerV2(src map[string]depgraph.Node) map[string]depgraph.Node {
	dst, _ := copystructure.Copy(src)
	return dst.(map[string]depgraph.Node)
}

// 可以做到深拷贝，主要原因是在与 struct{} 为非指针类型
func deepCopyWithStruc(src map[string]map[string]struct{}) map[string]map[string]struct{} {
	dst := make(map[string]map[string]struct{})
	for k, v := range src {
		tt := make(map[string]struct{}, len(v))
		for kk, vv := range v {
			tt[kk] = vv
		}
		dst[k] = tt
	}
	return dst
}

func deepCopyWithStrucV2(src map[string]map[string]struct{}) map[string]map[string]struct{} {
	dst, _ := copystructure.Copy(src)
	return dst.(map[string]map[string]struct{})
}
