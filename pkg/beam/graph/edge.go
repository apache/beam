package graph

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/userfn"
	"log"
)

// Opcode represents a primitive Fn API instruction kind.
type Opcode string

const (
	ParDo      Opcode = "ParDo"
	GBK        Opcode = "GBK"
	Source     Opcode = "Source"
	Sink       Opcode = "Sink"
	Flatten    Opcode = "Flatten"
	DataSource Opcode = "DataSource"
	DataSink   Opcode = "DataSink"
)

// InputKind represents the role of the input and its shape.
type InputKind string

const (
	Main      InputKind = "Main"
	Singleton InputKind = "Singleton"
	Slice     InputKind = "Slice"
	Map       InputKind = "Map" // TODO: allow?
	Iter      InputKind = "Iter"
	ReIter    InputKind = "ReIter"
)

type Inbound struct {
	Kind InputKind
	From *Node
	Type typex.FullType // actual, accepted type by DoFn
}

func (i *Inbound) String() string {
	return fmt.Sprintf("In(%v): %v <- %v", i.Kind, i.Type, i.From)
}

type Outbound struct {
	To   *Node
	Type typex.FullType // actual, produced type by DoFn
}

func (o *Outbound) String() string {
	return fmt.Sprintf("Out: %v -> %v", o.Type, o.To)
}

// Port represents the connection port of external operations.
type Port struct {
	ID  string
	URL string
}

// Target represents the target of external operations.
type Target struct {
	ID   string
	Name string
}

// MultiEdge represents a primitive Fn API instruction.
type MultiEdge struct {
	id     int
	parent *Scope

	Op     Opcode
	DoFn   *userfn.UserFn // ParDo, Source.
	Port   *Port          // DataSource, DataSink.
	Target *Target        // DataSource, DataSink.

	Input  []*Inbound
	Output []*Outbound
}

func (e *MultiEdge) ID() int {
	return e.id
}

func (e *MultiEdge) Scope() *Scope {
	return e.parent
}

func (e *MultiEdge) String() string {
	return fmt.Sprintf("%v: %v %v -> %v", e.id, e.Op, e.Input, e.Output)
}

// NOTE(herohde) 4/28/2017: In general, we have no good coder guess for outgoing
// nodes, unless we add a notion of default coder for arbitrary types. We leave
// that to the beam layer.

// NewGBK inserts a new GBK edge into the graph.
func NewGBK(g *Graph, s *Scope, n *Node) (*MultiEdge, error) {
	// groupByKey inserts a GBK transform into the pipeline.

	if !typex.IsWKV(n.Type()) {
		return nil, fmt.Errorf("input type must be KV: %v", n)
	}

	// (1) Create GBK result type and coder: KV<T,U> -> GBK<T,U>.

	t := typex.NewWGBK(typex.SkipW(n.Type()).Components()...)
	out := g.NewNode(t)

	// (2) Add generic GBK edge

	inT := typex.New(typex.KVType, typex.New(typex.TType), typex.New(typex.UType))
	outT := typex.New(typex.GBKType, typex.New(typex.TType), typex.New(typex.UType))

	edge := g.NewEdge(s)
	edge.Op = GBK
	edge.Input = []*Inbound{{Kind: Main, From: n, Type: inT}}
	edge.Output = []*Outbound{{To: out, Type: outT}}

	log.Printf("EDGE: %v", edge)
	return edge, nil
}

// NewGBK inserts a new ParDo edge into the graph.
func NewParDo(g *Graph, s *Scope, u *userfn.UserFn, in []*Node) (*MultiEdge, error) {
	return newUserFnNode(ParDo, g, s, u, in)
}

// NewSource inserts a Source transform.
func NewSource(g *Graph, s *Scope, u *userfn.UserFn) (*MultiEdge, error) {
	return newUserFnNode(Source, g, s, u, nil)
}

func newUserFnNode(op Opcode, g *Graph, s *Scope, u *userfn.UserFn, in []*Node) (*MultiEdge, error) {
	inbound, kinds, outbound, out, err := Bind(u, NodeTypes(in)...)
	if err != nil {
		return nil, err
	}

	edge := g.NewEdge(s)
	edge.Op = op
	edge.DoFn = u
	for i := 0; i < len(in); i++ {
		edge.Input = append(edge.Input, &Inbound{Kind: kinds[i], From: in[i], Type: inbound[i]})
	}
	for i := 0; i < len(out); i++ {
		n := g.NewNode(out[i])
		edge.Output = append(edge.Output, &Outbound{To: n, Type: outbound[i]})
	}

	log.Printf("EDGE: %v", edge)
	return edge, nil
}

func determineInputKind() {

}
