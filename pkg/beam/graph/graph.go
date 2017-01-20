package graph

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/model"
	"reflect"
)

// TODO(herohde): perhaps switch terminology closer to the Fn Runner, when ready.

// Graph represents an in-progress deferred execution graph and in easily
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
	g := &Scope{id: id, label: name, parent: parent}
	b.scopes = append(b.scopes, g)
	return g
}

func (b *Graph) NewEdge(parent *Scope) *MultiEdge {
	id := len(b.edges) + 1
	e := &MultiEdge{id: id, parent: parent}
	b.edges = append(b.edges, e)
	return e
}

func (b *Graph) NewNode(t reflect.Type) *Node {
	id := len(b.nodes) + 1
	n := &Node{id: id, T: t}
	b.nodes = append(b.nodes, n)
	return n
}

func (b *Graph) Build() (*model.Pipeline, error) {
	// TODO: cycles, typecheck, connectedness, etc.

	return nil, nil
}

// TODO(herohde): remove FakeBuild and Build + Parse instead. Used for quicker
// local runner.

func (b *Graph) FakeBuild() ([]*MultiEdge, []*Node) {
	return b.edges, b.nodes
}

// Node is a typed connector, usually corresponding to PCollection<T>. The type
// may however differ, depending on whether it was introduced by GBK output,
// ParDo or "generic" ParDo, for example. The difference matters for the wireup
// at execution time: encoding/decoding must be correctly inserted, if a
// generic ParDo is fused with concretely-typed ones. Also, a generic transform
// may also need re-coding, if the input/output coders are not identical.
type Node struct {
	id int

	T     reflect.Type
	Coder string
}

func (n *Node) ID() int {
	return n.id
}

func (n *Node) String() string {
	return fmt.Sprintf("{%v: %v/%v}", n.id, n.T, n.Coder)
}

// Scope is a syntactic Scope, such as arising from a composite PTransform. It
// has no semantic meaning at execution time. Used by monitoring.
type Scope struct {
	id     int
	label  string
	parent *Scope
}

func (s *Scope) ID() int {
	return s.id
}

func (s *Scope) String() string {
	if s.parent == nil {
		return s.label
	}
	return s.parent.String() + "/" + s.label
}

//go:generate $GOPATH/bin/stringer -type=Opcode

// Opcode represents a primitive Fn API instruction kind.
type Opcode int

const (
	ParDo Opcode = iota
	GBK
	Source
	Sink
	Flatten
)

type Inbound struct {
	From *Node
	// TODO: view info (if sideinput), coding info (if generic).
}

func (i *Inbound) String() string {
	return fmt.Sprintf("In: %v", i.From)
}

type Outbound struct {
	To *Node
	// TODO: coding info (if generic).
}

func (o *Outbound) String() string {
	return fmt.Sprintf("Out: %v", o.To)
}

// TODO(herohde): perhaps promote the "generic" aspect to an edge instead?

// MultiEdge represents a primitive Fn API instruction.
type MultiEdge struct {
	id     int
	parent *Scope

	Op   Opcode
	DoFn *UserFn

	// TODO(herohde): need immediate values, such as filenames for source.
	// Also figure out how to bind in context values in general.
	// Ctx *Context
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
