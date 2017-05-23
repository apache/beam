package graph

import (
	"fmt"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
)

// Graph represents an in-progress deferred execution graph and is easily
// translatable to the model graph. This graph representation allows precise
// control over scope and connectivity.
type Graph struct {
	scopes []*Scope
	edges  []*MultiEdge
	nodes  []*Node

	root *Scope
}

// New returns an empty graph with the scope set to the root.
func New() *Graph {
	root := &Scope{0, "root", nil}
	return &Graph{root: root}
}

// Root returns the root scope of the graph.
func (g *Graph) Root() *Scope {
	return g.root
}

// NewScope creates and returns a new scope that is a child of the supplied scope.
func (g *Graph) NewScope(parent *Scope, name string) *Scope {
	id := len(g.scopes) + 1
	s := &Scope{id: id, Label: name, Parent: parent}
	g.scopes = append(g.scopes, s)
	return s
}

// NewEdge creates a new edge of the graph in the supplied scope.
func (g *Graph) NewEdge(parent *Scope) *MultiEdge {
	id := len(g.edges) + 1
	e := &MultiEdge{id: id, parent: parent}
	g.edges = append(g.edges, e)
	return e
}

// NewNode creates a new node in the graph of the supplied fulltype.
func (g *Graph) NewNode(t typex.FullType) *Node {
	id := len(g.nodes) + 1
	n := &Node{id: id, t: t}
	g.nodes = append(g.nodes, n)
	return n
}

// Build performs finalization on the graph. It verifies the correctness of the
// graph structure, typechecks the plan and returns a slice of the edges in
// the graph.
func (g *Graph) Build() ([]*MultiEdge, error) {
	// TODO: cycles, typecheck, connectedness, etc.

	return g.edges, nil
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
