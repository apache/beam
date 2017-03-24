package graph

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/reflectx"
	"log"
	"reflect"
	"strings"
)

//go:generate protoc -I . v1.proto --go_out=plugins=grpc:v1

// TODO(herohde): perhaps switch terminology closer to the Fn Runner, when ready.

// Graph represents an in-progress deferred execution graph and is easily
// translatable to the model graph.
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
	e := &MultiEdge{id: id, Parent: parent}
	b.edges = append(b.edges, e)
	return e
}

func (b *Graph) NewNode(t reflect.Type) *Node {
	if reflectx.ClassOf(t).IsGeneric() {
		panic(fmt.Errorf("Invalid node type: %v", t))
	}

	id := len(b.nodes) + 1
	n := &Node{id: id, T: t}
	b.nodes = append(b.nodes, n)
	return n
}

func (b *Graph) Build() ([]*MultiEdge, error) {
	// TODO: cycles, typecheck, connectedness, etc.

	return b.edges, nil
}

// TODO(herohde): remove FakeBuild and Build + Parse instead. Used for quicker
// local runner.

func (b *Graph) FakeBuild() map[int]*MultiEdge {
	ret := make(map[int]*MultiEdge)
	for _, edge := range b.edges {
		ret[edge.id] = edge
	}

	for _, node := range b.nodes {
		log.Printf("Node: %v Class: %v", node, reflectx.ClassOf(node.T))
	}

	return ret
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

// Node is a typed connector, usually corresponding to PCollection<T>. The type
// may however differ, depending on whether it was introduced by GBK output,
// ParDo or "generic" ParDo, for example. The difference matters for the wireup
// at execution time: encoding/decoding must be correctly inserted, if a
// generic ParDo is fused with concretely-typed ones. Also, a generic transform
// may also need re-coding, if the input/output coders are not identical.
type Node struct {
	id int

	T     reflect.Type // type of underlying data, not representation.
	Coder *Coder
}

func (n *Node) ID() int {
	return n.id
}

func (n *Node) String() string {
	if n.Coder != nil {
		return fmt.Sprintf("{%v: %v/%v}", n.id, n.T, n.Coder.ID)
	} else {
		return fmt.Sprintf("{%v: %v/x}", n.id, n.T)
	}
}

// Scope is a syntactic Scope, such as arising from a composite PTransform. It
// has no semantic meaning at execution time. Used by monitoring.
type Scope struct {
	id     int
	Label  string
	Parent *Scope
}

func (s *Scope) ID() int {
	return s.id
}

func (s *Scope) String() string {
	if s.Parent == nil {
		return s.Label
	}
	return s.Parent.String() + "/" + s.Label
}

//go:generate $GOPATH/bin/stringer -type=Opcode

// Opcode represents a primitive Fn API instruction kind.
type Opcode int

const (
	External Opcode = iota
	ParDo
	GBK
	Source
	Sink
	Flatten
)

type Inbound struct {
	From *Node
	T    reflect.Type // actual, accepted type by DoFn
	// TODO: view info (if sideinput)
}

func (i *Inbound) String() string {
	return fmt.Sprintf("In: %v <- %v", i.T, i.From)
}

type Outbound struct {
	To *Node
	T  reflect.Type // actual, produced type by DoFn
}

func (o *Outbound) String() string {
	return fmt.Sprintf("Out: %v -> %v", o.T, o.To)
}

// TODO(herohde): perhaps promote the "generic" aspect to an edge instead?

// MultiEdge represents a primitive Fn API instruction.
type MultiEdge struct {
	id     int
	Parent *Scope

	Op   Opcode
	DoFn *UserFn

	// Data holds immediate values to be bound into "data" context field, if present.
	Data interface{}

	Input  []*Inbound
	Output []*Outbound
}

func (e *MultiEdge) ID() int {
	return e.id
}

func (e *MultiEdge) String() string {
	return fmt.Sprintf("%v: %v %v -> %v", e.id, e.Op, e.Input, e.Output)
}
