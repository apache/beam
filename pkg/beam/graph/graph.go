package graph

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
	"strings"
)

// Graph represents an in-progress deferred execution graph and is easily
// translatable to the model graph. The internal graph allows precise
// control over scope and connectivity.
type Graph struct {
	scopes []*Scope
	edges  []*MultiEdge
	nodes  []*Node

	root *Scope
}

func New() *Graph {
	root := &Scope{0, "root", nil}
	return &Graph{root: root}
}

func (b *Graph) Root() *Scope {
	return b.root
}

func (b *Graph) NewScope(parent *Scope, name string) *Scope {
	id := len(b.scopes) + 1
	g := &Scope{id: id, Label: name, Parent: parent}
	b.scopes = append(b.scopes, g)
	return g
}

func (b *Graph) NewEdge(parent *Scope) *MultiEdge {
	id := len(b.edges) + 1
	e := &MultiEdge{id: id, parent: parent}
	b.edges = append(b.edges, e)
	return e
}

func (b *Graph) NewNode(t typex.FullType) *Node {
	id := len(b.nodes) + 1
	n := &Node{id: id, t: t}
	b.nodes = append(b.nodes, n)
	return n
}

func (b *Graph) Build() ([]*MultiEdge, error) {
	// TODO: cycles, typecheck, connectedness, etc.

	return b.edges, nil
}

func (g *Graph) String() string {
	var nodes []string
	for _, node := range g.nodes {
		nodes = append(nodes, node.String())
	}
	var edges []string
	for _, edge := range g.edges {
		edges = append(edges, edge.String())
	}
	return fmt.Sprintf("Nodes: %v\nEdges: %v", strings.Join(nodes, "\n"), strings.Join(edges, "\n"))
}
