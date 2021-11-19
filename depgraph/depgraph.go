package depgraph

import (
	"errors"
)

type Node interface {
	GetID() string
	IsTemp() bool
}

// A node in this graph is just a string, so a nodeset is a map whose
// keys are the nodes that are present.
type nodeset map[string]Node

// depmap tracks the nodes that have some dependency relationship to
// some other node, represented by the key of the map.
type depmap map[string]map[string]struct{}

type Graph struct {
	nodes nodeset

	// Maintain dependency relationships in both directions. These
	// data structures are the edges of the graph.

	// `dependencies` tracks child -> parents.
	dependencies depmap
	// `dependents` tracks parent -> children.
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

func (g *Graph) DependOn(child, parent Node) error {
	if child.GetID() == parent.GetID() {
		return errors.New("self-referential dependencies not allowed")
	}

	if g.DependsOn(parent.GetID(), child.GetID()) {
		return errors.New("circular dependencies not allowed")
	}

	// Add nodes.
	g.nodes[parent.GetID()] = parent
	g.nodes[child.GetID()] = child

	// Add edges.
	addNodeToNodeset(g.dependents, parent.GetID(), child.GetID())
	addNodeToNodeset(g.dependencies, child.GetID(), parent.GetID())

	return nil
}

func (g *Graph) CreateNode(inode Node) error {
	g.nodes[inode.GetID()] = inode

	return nil
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
		for _, node := range layer {
			allNodes = append(allNodes, node)
		}
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

func (g *Graph) clone() *Graph {
	return &Graph{
		dependencies: copyDepmap(g.dependencies),
		dependents:   copyDepmap(g.dependents),
		nodes:        copyNodeset(g.nodes),
	}
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
			for nextNode := range nextFn(node) {
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

func addNodeToNodeset(dm depmap, key, node string) {
	nodes, ok := dm[key]
	if !ok {
		nodes = make(map[string]struct{})
		dm[key] = nodes
	}
	nodes[node] = struct{}{}
}

func (g *Graph) ShrinkGraph() *Graph {

	// 思路
	// 遍历所有节点，如果节点属性为 t，则计算该节点的上游有几个
	// 如果只有一个，则将该节点的上游和其下游若干个节点都连起来，然后将该连线删除掉
	// 循环遍历

	// Copy the graph
	shrinkingGraph := g.clone()

	for {

		tnum := 0
		for _, v := range shrinkingGraph.nodes {
			if v.IsTemp() {
				for pid := range shrinkingGraph.dependencies[v.GetID()] {
					pnode := shrinkingGraph.nodes[pid]
					for id := range shrinkingGraph.dependents[v.GetID()] {
						shrinkingGraph.DependOn(shrinkingGraph.nodes[id], pnode)
					}
				}
				// 去掉中间点
				shrinkingGraph.remove(v.GetID())

				tnum++
			}
		}

		if tnum == 0 {
			break
		}
	}

	return shrinkingGraph
}
