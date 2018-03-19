// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package graph

import (
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

// Opcode represents a primitive Beam instruction kind.
type Opcode string

// Valid opcodes.
const (
	Impulse  Opcode = "Impulse"
	ParDo    Opcode = "ParDo"
	CoGBK    Opcode = "CoGBK"
	External Opcode = "External"
	Flatten  Opcode = "Flatten"
	Combine  Opcode = "Combine"
)

// InputKind represents the role of the input and its shape.
type InputKind string

// Valid input kinds.
const (
	Main      InputKind = "Main"
	Singleton InputKind = "Singleton"
	Slice     InputKind = "Slice"
	Map       InputKind = "Map"      // TODO: allow?
	MultiMap  InputKind = "MultiMap" // TODO: allow?
	Iter      InputKind = "Iter"
	ReIter    InputKind = "ReIter"
)

// Inbound represents an inbound data link from a Node.
type Inbound struct {
	// Kind presents the form of the data that the edge expects. Main input
	// must be processed element-wise, but side input may take several
	// convenient forms. For example, a DoFn that processes ints may choose
	// among the following parameter types:
	//
	//   * Main:      int
	//   * Singleton: int
	//   * Slice:     []int
	//   * Iter:      func(*int) bool
	//   * ReIter:    func() func(*int) bool
	//
	// If the DoFn is generic then int may be replaced by any of the type
	// variables. For example,
	//
	//   * Slice:     []typex.T
	//   * Iter:      func(*typex.X) bool
	//
	// If the input type is KV<int,string>, say, then the options are:
	//
	//   * Main:      int, string  (as two separate parameters)
	//   * Map:       map[int]string
	//   * MultiMap:  map[int][]string
	//   * Iter:      func(*int, *string) bool
	//   * ReIter:    func() func(*int, *string) bool
	//
	// As above, either int, string, or both can be replaced with type
	// variables. For example,
	//
	//   * Map:       map[typex.X]typex.Y
	//   * MultiMap:  map[typex.T][]string
	//   * Iter:      func(*typex.Z, *typex.Z) bool
	//
	// Note that in the last case the parameter type requires that both
	// the key and value types are identical. Bind enforces such constraints.
	Kind InputKind

	// From is the incoming node in the graph.
	From *Node

	// Type is the fulltype matching the actual type used by the transform.
	// Due to the loose signatures of DoFns, we can only determine the
	// inbound structure when the fulltypes of the incoming links are present.
	// For example,
	//
	//     func (ctx context.Context, key int, value typex.X) error
	//
	// is a generic DoFn that if bound to KV<int,string> would have one
	// Inbound link with type KV<int, X>.
	Type typex.FullType
}

func (i *Inbound) String() string {
	return fmt.Sprintf("In(%v): %v <- %v", i.Kind, i.Type, i.From)
}

// Outbound represents an outbound data link to a Node.
type Outbound struct {
	// To is the outgoing node in the graph.
	To *Node

	// Type is the fulltype matching the actual type used by the transform.
	// For DoFns, unlike inbound, the outbound types closely mimic the type
	// signature. For example,
	//
	//     func (ctx context.Context, emit func (key int, value typex.X)) error
	//
	// is a generic DoFn that produces one Outbound link of type KV<int,X>.
	Type typex.FullType // representation type of data
}

func (o *Outbound) String() string {
	return fmt.Sprintf("Out: %v -> %v", o.Type, o.To)
}

// Payload represents an external payload.
type Payload struct {
	URN  string
	Data []byte
}

// TODO(herohde) 5/24/2017: how should we represent/obtain the coder for Combine
// accumulator types? Coder registry? Assume JSON?

// MultiEdge represents a primitive data processing operation. Each non-user
// code operation may be implemented by either the harness or the runner.
type MultiEdge struct {
	id     int
	parent *Scope

	Op        Opcode
	DoFn      *DoFn      // ParDo
	CombineFn *CombineFn // Combine
	Value     []byte     // Impulse
	Payload   *Payload   // External

	Input  []*Inbound
	Output []*Outbound
}

// ID returns the graph-local identifier for the edge.
func (e *MultiEdge) ID() int {
	return e.id
}

// Name returns a not-necessarily-unique name for the edge.
func (e *MultiEdge) Name() string {
	if e.DoFn != nil {
		return e.DoFn.Name()
	}
	if e.CombineFn != nil {
		return e.CombineFn.Name()
	}
	return string(e.Op)
}

// Scope return the scope.
func (e *MultiEdge) Scope() *Scope {
	return e.parent
}

func (e *MultiEdge) String() string {
	return fmt.Sprintf("%v: %v %v -> %v", e.id, e.Op, e.Input, e.Output)
}

// NOTE(herohde) 4/28/2017: In general, we have no good coder guess for outgoing
// nodes, unless we add a notion of default coder for arbitrary types. We leave
// that to the beam layer.

// NewCoGBK inserts a new CoGBK edge into the graph.
func NewCoGBK(g *Graph, s *Scope, ns []*Node) (*MultiEdge, error) {
	if len(ns) == 0 {
		return nil, fmt.Errorf("cogbk needs at least 1 input")
	}
	if !typex.IsKV(ns[0].Type()) {
		return nil, fmt.Errorf("input type must be KV: %v", ns[0])
	}

	// (1) Create CoGBK result type: KV<T,U>, .., KV<T,Z> -> CoGBK<T,U,..,Z>.

	c := ns[0].Coder.Components[0]
	w := ns[0].Window()
	comp := []typex.FullType{c.T, ns[0].Type().Components()[1]}

	for i := 1; i < len(ns); i++ {
		n := ns[i]
		if !typex.IsKV(n.Type()) {
			return nil, fmt.Errorf("input type must be KV: %v", n)
		}
		if !n.Coder.Components[0].Equals(c) {
			return nil, fmt.Errorf("key coder for %v is %v, want %v", n, n.Coder.Components[0], c)
		}
		if !w.Equals(n.Window()) {
			return nil, fmt.Errorf("mismatched cogbk window types: %v, want %v", n.Window(), w)
		}

		comp = append(comp, n.Type().Components()[1])
	}

	t := typex.NewCoGBK(comp...)
	out := g.NewNode(t, w)

	// (2) Add CoGBK edge

	edge := g.NewEdge(s)
	edge.Op = CoGBK
	for i := 0; i < len(ns); i++ {
		edge.Input = append(edge.Input, &Inbound{Kind: Main, From: ns[i], Type: ns[i].Type()})
	}
	edge.Output = []*Outbound{{To: out, Type: t}}
	return edge, nil
}

// NewFlatten inserts a new Flatten edge in the graph. Flatten output type is
// the shared input type.
func NewFlatten(g *Graph, s *Scope, in []*Node) (*MultiEdge, error) {
	if len(in) < 2 {
		return nil, fmt.Errorf("flatten needs at least 2 input, got %v", len(in))
	}
	t := in[0].Type()
	w := in[0].Window()
	for _, n := range in {
		if !typex.IsEqual(t, n.Type()) {
			return nil, fmt.Errorf("mismatched flatten input types: %v, want %v", n.Type(), t)
		}
		if !w.Equals(n.Window()) {
			return nil, fmt.Errorf("mismatched flatten window types: %v, want %v", n.Window(), w)
		}
	}
	if typex.IsCoGBK(t) {
		return nil, fmt.Errorf("flatten input type cannot be CoGBK: %v", t)
	}

	edge := g.NewEdge(s)
	edge.Op = Flatten
	for _, n := range in {
		edge.Input = append(edge.Input, &Inbound{Kind: Main, From: n, Type: t})
	}
	edge.Output = []*Outbound{{To: g.NewNode(t, w), Type: t}}
	return edge, nil
}

// NewExternal inserts an External transform. The system makes no assumptions about
// what this transform might do.
func NewExternal(g *Graph, s *Scope, payload *Payload, in []*Node, out []typex.FullType) *MultiEdge {
	edge := g.NewEdge(s)
	edge.Op = External
	edge.Payload = payload
	for _, n := range in {
		edge.Input = append(edge.Input, &Inbound{Kind: Main, From: n, Type: n.Type()})
	}
	for _, t := range out {
		n := g.NewNode(t, inputWindow(in))
		edge.Output = append(edge.Output, &Outbound{To: n, Type: t})
	}
	return edge
}

// NewParDo inserts a new ParDo edge into the graph.
func NewParDo(g *Graph, s *Scope, u *DoFn, in []*Node, typedefs map[string]reflect.Type) (*MultiEdge, error) {
	return newDoFnNode(ParDo, g, s, u, in, typedefs)
}

func newDoFnNode(op Opcode, g *Graph, s *Scope, u *DoFn, in []*Node, typedefs map[string]reflect.Type) (*MultiEdge, error) {
	// TODO(herohde) 5/22/2017: revisit choice of ProcessElement as representative. We should
	// perhaps create a synthetic method for binding purposes? The main question is how to
	// tell which side input binds to which if the signatures differ, which is a downside of
	// positional binding.

	inbound, kinds, outbound, out, err := Bind(u.ProcessElementFn(), typedefs, NodeTypes(in)...)
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
		n := g.NewNode(out[i], inputWindow(in))
		edge.Output = append(edge.Output, &Outbound{To: n, Type: outbound[i]})
	}
	return edge, nil
}

// NewCombine inserts a new Combine edge into the graph. Combines cannot have side
// input.
func NewCombine(g *Graph, s *Scope, u *CombineFn, in *Node) (*MultiEdge, error) {
	// Create a synthetic function for binding purposes. It takes main input
	// and returns the output type -- but hides the accumulator.
	//
	//  (1) If AddInput exists, then it lists the main inputs. If not,
	//      the only main input type is the accumulator type.
	//  (2)	If ExtractOutput exists then it returns the output type. If not,
	//      then the accumulator is the output type.
	//
	// MergeAccumulators is guaranteed to exist. We do not allow the accumulator
	// to be a tuple type (i.e., so one can't define a inline KV merge function).

	synth := &funcx.Fn{}
	if f := u.AddInputFn(); f != nil {
		// drop accumulator and irrelevant parameters
		synth.Param = funcx.SubParams(f.Param, f.Params(funcx.FnValue)[1:]...)
	} else {
		synth.Param = []funcx.FnParam{{Kind: funcx.FnValue, T: u.MergeAccumulatorsFn().Ret[0].T}}
	}
	if f := u.ExtractOutputFn(); f != nil {
		synth.Ret = f.Ret
	} else {
		synth.Ret = u.MergeAccumulatorsFn().Ret
	}

	inT := in.Type()

	isPerKey := typex.IsCoGBK(inT)
	if isPerKey {
		// For per-key combine, the shape of the inbound type and the type of the
		// inbound node are different: a node type of CoGBK<A,B> will become B
		// or KV<A,B>, depending on whether the combineFn is keyed or not.
		// Per-key combines may omit the key in the signature. In such a case,
		// it is ignored for the purpose of binding. The runtime will later look at
		// these types to decide whether to add the key or not.
		//
		// However, the outbound type will be KV<A,O> (where O is the output
		// type) regardless of whether the combineFn is keyed or not.

		if len(inT.Components()) > 2 {
			return nil, fmt.Errorf("combine cannot follow multi-input CoGBK: %v", inT)
		}

		if len(synth.Param) == 1 {
			inT = inT.Components()[1] // Drop implicit key for binding purposes
		} else {
			inT = typex.NewKV(inT.Components()...)
		}

		// The runtime always adds the key for the output of per-key combiners.
		key := in.Type().Components()[0]
		synth.Ret = append([]funcx.ReturnParam{{Kind: funcx.RetValue, T: key.Type()}}, synth.Ret...)
	}

	inbound, kinds, outbound, out, err := Bind(synth, nil, inT)
	if err != nil {
		return nil, err
	}

	edge := g.NewEdge(s)
	edge.Op = Combine
	edge.CombineFn = u
	edge.Input = []*Inbound{{Kind: kinds[0], From: in, Type: inbound[0]}}
	for i := 0; i < len(out); i++ {
		n := g.NewNode(out[i], in.Window())
		edge.Output = append(edge.Output, &Outbound{To: n, Type: outbound[i]})
	}
	return edge, nil
}

// NewImpulse inserts a new Impulse edge into the graph. It must use the
// built-in bytes coder.
func NewImpulse(g *Graph, s *Scope, value []byte) *MultiEdge {
	ft := typex.New(reflectx.ByteSlice)
	w := window.NewGlobalWindow()
	n := g.NewNode(ft, w)
	n.Coder = coder.NewBytes()

	edge := g.NewEdge(s)
	edge.Op = Impulse
	edge.Value = value
	edge.Output = []*Outbound{{To: n, Type: ft}}
	return edge
}

func inputWindow(in []*Node) *window.Window {
	if len(in) == 0 {
		return window.NewGlobalWindow()
	}
	return in[0].Window()
}
