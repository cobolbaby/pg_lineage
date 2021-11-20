package depgraph

import (
	"errors"
)

type Node interface {
	GetID() string
	IsTemp() bool
}

// 节点集合，为了方便扩展，Node定义为接口类型
type nodeset map[string]Node

// 采用邻接列表的方式存储节点间的依赖关系
type depmap map[string]map[string]struct{}

type Graph struct {
	nodes nodeset

	// Maintain dependency relationships in both directions. These
	// data structures are the edges of the graph.

	// `dependencies` tracks child -> parents. 依靠
	dependencies depmap
	// `dependents` tracks parent -> children. 家属
	dependents depmap
	// Keep track of the nodes of the graph themselves.
}

func New() *Graph {
	return &Graph{
		dependencies: make(depmap),
		dependents:   make(depmap),
		nodes:        make(nodeset),
	}
}

func (g *Graph) GetNodes() nodeset {
	return g.nodes
}

func (g *Graph) GetRelationships() depmap {
	return g.dependents
}

// 增加点，并添加边
func (g *Graph) DependOn(child Node, parent Node) error {
	if child.GetID() == parent.GetID() {
		return errors.New("self-referential dependencies not allowed")
	}

	if g.DependsOn(parent.GetID(), child.GetID()) {
		return errors.New("circular dependencies not allowed")
	}

	// Add nodes and edges
	g.AddNode(parent).AddNode(child).AddEdge(parent, child)

	return nil
}

func (g *Graph) AddEdge(parent Node, child Node) *Graph {
	addDependency(g.dependents, parent.GetID(), child.GetID())
	addDependency(g.dependencies, child.GetID(), parent.GetID())

	return g
}

func addDependency(dm depmap, key string, node string) {
	nodes, ok := dm[key]
	if !ok {
		nodes = make(map[string]struct{})
		dm[key] = nodes
	}
	nodes[node] = struct{}{}
}

func (g *Graph) AddNode(node Node) *Graph {
	g.nodes[node.GetID()] = node

	return g
}

func (g *Graph) DependsOn(child, parent string) bool {
	deps := g.Dependencies(child)
	_, ok := deps[parent]
	return ok
}

func (g *Graph) HasDependent(parent, child string) bool {
	deps := g.Dependents(parent)
	_, ok := deps[child]
	return ok
}

func (g *Graph) Leaves() []string {
	leaves := make([]string, 0)

	for node := range g.nodes {
		if _, ok := g.dependencies[node]; !ok {
			leaves = append(leaves, node)
		}
	}

	return leaves
}

// TopoSortedLayers returns a slice of all of the graph nodes in topological sort order. That is,
// if `B` depends on `A`, then `A` is guaranteed to come before `B` in the sorted output.
// The graph is guaranteed to be cycle-free because cycles are detected while building the
// graph. Additionally, the output is grouped into "layers", which are guaranteed to not have
// any dependencies within each layer. This is useful, e.g. when building an execution plan for
// some DAG, in which case each element within each layer could be executed in parallel. If you
// do not need this layered property, use `Graph.TopoSorted()`, which flattens all elements.
func (g *Graph) TopoSortedLayers() [][]string {
	layers := [][]string{}

	// Copy the graph
	shrinkingGraph := g.clone()
	for {
		leaves := shrinkingGraph.Leaves()
		if len(leaves) == 0 {
			break
		}

		layers = append(layers, leaves)
		for _, leafNode := range leaves {
			shrinkingGraph.remove(leafNode)
		}
	}

	return layers
}

func removeFromDepmap(dm depmap, key, node string) {
	nodes := dm[key]
	if len(nodes) == 1 {
		// The only element in the nodeset must be `node`, so we
		// can delete the entry entirely.
		delete(dm, key)
	} else {
		// Otherwise, remove the single node from the nodeset.
		delete(nodes, node)
	}
}

func (g *Graph) remove(node string) {
	// Remove edges from things that depend on `node`.
	for dependent := range g.dependents[node] {
		removeFromDepmap(g.dependencies, dependent, node)
	}
	delete(g.dependents, node)

	// Remove all edges from node to the things it depends on.
	for dependency := range g.dependencies[node] {
		removeFromDepmap(g.dependents, dependency, node)
	}
	delete(g.dependencies, node)

	// Finally, remove the node itself.
	delete(g.nodes, node)
}

// TopoSorted returns all the nodes in the graph is topological sort order.
// See also `Graph.TopoSortedLayers()`.
func (g *Graph) TopoSorted() []string {
	nodeCount := 0
	layers := g.TopoSortedLayers()
	for _, layer := range layers {
		nodeCount += len(layer)
	}

	allNodes := make([]string, 0, nodeCount)
	for _, layer := range layers {
		allNodes = append(allNodes, layer...)
	}

	return allNodes
}

func (g *Graph) Dependencies(child string) map[string]struct{} {
	return g.buildTransitive(child, g.immediateDependencies)
}

func (g *Graph) immediateDependencies(node string) map[string]struct{} {
	return g.dependencies[node]
}

func (g *Graph) Dependents(parent string) map[string]struct{} {
	return g.buildTransitive(parent, g.immediateDependents)
}

func (g *Graph) immediateDependents(node string) map[string]struct{} {
	return g.dependents[node]
}

// buildTransitive starts at `root` and continues calling `nextFn` to keep discovering more nodes until
// the graph cannot produce any more. It returns the set of all discovered nodes.
func (g *Graph) buildTransitive(root string, nextFn func(string) map[string]struct{}) map[string]struct{} {
	if _, ok := g.nodes[root]; !ok {
		return nil
	}

	out := make(map[string]struct{})
	searchNext := []string{root}
	for len(searchNext) > 0 {
		// List of new nodes from this layer of the dependency graph. This is
		// assigned to `searchNext` at the end of the outer "discovery" loop.
		discovered := []string{}
		for _, node := range searchNext {
			// For each node to discover, find the next nodes.
			for nextNode, _ := range nextFn(node) {
				// If we have not seen the node before, add it to the output as well
				// as the list of nodes to traverse in the next iteration.
				if _, ok := out[nextNode]; !ok {
					out[nextNode] = struct{}{}
					discovered = append(discovered, nextNode)
				}
			}
		}
		searchNext = discovered
	}

	return out
}

func (g *Graph) clone() *Graph {
	return &Graph{
		dependencies: copyDepmap(g.dependencies),
		dependents:   copyDepmap(g.dependents),
		nodes:        copyNodeset(g.nodes),
	}
}

// TODO:如果 Node 为接口类型，此时还能如此 copy 吗？
func copyNodeset(s nodeset) nodeset {
	out := make(nodeset, len(s))
	for k, v := range s {
		out[k] = v
	}
	return out
}

func copyDepmap(m depmap) depmap {
	out := make(depmap, len(m))
	for k, v := range m {
		tt := make(map[string]struct{}, len(v))
		for kk, vv := range v {
			tt[kk] = vv
		}
		out[k] = tt
	}
	return out
}

// 精简图，去掉其中的临时节点，单保留原图
// 思路: 遍历所有节点，如果节点为临时节点，则就将该节点的上游节点和下游节点连接起来，然后将该节点及其连线删除掉
func (g *Graph) ShrinkGraph() *Graph {
	shrinkingGraph := g.clone()
	for {
		tempNodeC := 0
		// TODO:map 遍历，如果写成 for node := 会是什么结果？
		for _, v := range shrinkingGraph.nodes {
			if v.IsTemp() {
				tempNodeC++
				// 先添加新边
				for pid := range shrinkingGraph.dependencies[v.GetID()] {
					for cid := range shrinkingGraph.dependents[v.GetID()] {
						shrinkingGraph.DependOn(shrinkingGraph.nodes[cid], shrinkingGraph.nodes[pid])
					}
				}
				// 再去掉该节点相关信息
				shrinkingGraph.remove(v.GetID())
			}
		}
		if tempNodeC == 0 {
			break
		}
	}
	return shrinkingGraph
}
