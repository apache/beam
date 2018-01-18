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

// Opcode represents a primitive Fn API instruction kind.
type Opcode string

// Valid opcodes.
const (
	Impulse    Opcode = "Impulse"
	ParDo      Opcode = "ParDo"
	GBK        Opcode = "GBK" // TODO: Unify with CoGBK?
	External   Opcode = "External"
	Flatten    Opcode = "Flatten"
	Combine    Opcode = "Combine"
	DataSource Opcode = "DataSource"
	DataSink   Opcode = "DataSink"
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
	// If the input type is W<KV<int,string>>, say, then the options are:
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
	// is a generic DoFn that if bound to W<KV<int,string>> would have one
	// Inbound link with type W<KV<int, X>>.
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
	// is a generic DoFn that produces one Outbound link of type W<KV<int,X>>.
	Type typex.FullType // representation type of data
}

func (o *Outbound) String() string {
	return fmt.Sprintf("Out: %v -> %v", o.Type, o.To)
}

// Port represents the connection port of external operations.
type Port struct {
	URL string
}

// Target represents the target of external operations.
type Target struct {
	ID   string
	Name string
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
	Port      *Port      // DataSource, DataSink
	Target    *Target    // DataSource, DataSink
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

// NewGBK inserts a new GBK edge into the graph.
func NewGBK(g *Graph, s *Scope, n *Node) (*MultiEdge, error) {
	if !typex.IsWKV(n.Type()) {
		return nil, fmt.Errorf("input type must be KV: %v", n)
	}

	// (1) Create GBK result type and coder: KV<T,U> -> GBK<T,U>.

	t := typex.NewWGBK(typex.SkipW(n.Type()).Components()...)
	out := g.NewNode(t, n.Window())

	// (2) Add generic GBK edge

	inT := typex.New(typex.KVType, typex.New(typex.TType), typex.New(typex.UType))
	outT := typex.New(typex.GBKType, typex.New(typex.TType), typex.New(typex.UType))

	edge := g.NewEdge(s)
	edge.Op = GBK
	edge.Input = []*Inbound{{Kind: Main, From: n, Type: inT}}
	edge.Output = []*Outbound{{To: out, Type: outT}}
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
	if typex.IsWGBK(t) || typex.IsWCoGBK(t) {
		return nil, fmt.Errorf("flatten input type cannot be GBK or CGBK: %v", t)
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

// NewCombine inserts a new Combine edge into the graph.
func NewCombine(g *Graph, s *Scope, u *CombineFn, in []*Node) (*MultiEdge, error) {
	if len(in) < 1 {
		return nil, fmt.Errorf("combine needs at least 1 input, got %v", len(in))
	}

	// Create a synthetic function for binding purposes. It takes main inputs,
	// side inputs and returns the output type -- but hides the accumulator.
	//
	//  (1) If AddInput exists, then it lists the main and side inputs. If not,
	//      the only main input type is the accumulator type.
	//  (2)	If ExtractOutput exists then it returns the output type. If not,
	//      then the accumulator is the output type.
	//
	// MergeAccumulators is guaranteed to exist. We do not allow the accumulator
	// to be a tuple type (i.e., so one can't define a inline KV merge function).

	synth := &funcx.Fn{}
	if f := u.AddInputFn(); f != nil {
		synth.Param = f.Param[1:] // drop accumulator parameter.
	} else {
		synth.Param = []funcx.FnParam{{Kind: funcx.FnValue, T: u.MergeAccumulatorsFn().Ret[0].T}}
	}
	if f := u.ExtractOutputFn(); f != nil {
		synth.Ret = f.Ret
	} else {
		synth.Ret = u.MergeAccumulatorsFn().Ret
	}

	ts := NodeTypes(in)
	t := typex.SkipW(in[0].Type())

	// Per-key combines are allowed as well and the key does not have to be
	// present in the signature. In that case, it is ignored for the
	// purpose of binding. The difficulty is that we can't easily tell the
	// difference between (Key, Accum) and (Accum, SideInput) in the
	// signature, so we simply try binding with and without a key.

	isPerKey := typex.IsWGBK(ts[0])
	if isPerKey {
		ts[0] = typex.NewW(t.Components()[1]) // Try W<GBK<A,B>> -> W<B>

		// The runtime always adds the key for the output of per-key combiners.
		key := t.Components()[0]
		synth.Ret = append([]funcx.ReturnParam{{Kind: funcx.RetValue, T: key.Type()}}, synth.Ret...)
	}

	inbound, kinds, outbound, out, err := Bind(synth, nil, ts...)
	if err != nil {
		if !isPerKey {
			return nil, err
		}

		ts[0] = typex.NewWKV(t.Components()...) // Try W<GBK<A,B>> -> W<KV<A,B>>

		inbound, kinds, outbound, out, err = Bind(synth, nil, ts...)
		if err != nil {
			return nil, err
		}
	}

	// For per-key combine, the shape of the inbound type and the type of the
	// node are different. A node type of W<GBK<A,B>> will become W<B> or
	// W<KV<A,B>>, depending on whether the combineFn is keyed or not. The
	// runtime will look at this type to decide whether to add the key.
	//
	// However, the outbound type will be W<KV<A,O>> (where O is the output
	// type) regardless of whether the combineFn is keyed or not.

	edge := g.NewEdge(s)
	edge.Op = Combine
	edge.CombineFn = u

	for i := 0; i < len(in); i++ {
		edge.Input = append(edge.Input, &Inbound{Kind: kinds[i], From: in[i], Type: inbound[i]})
	}
	for i := 0; i < len(out); i++ {
		n := g.NewNode(out[i], inputWindow(in))
		edge.Output = append(edge.Output, &Outbound{To: n, Type: outbound[i]})
	}
	return edge, nil
}

// NewImpulse inserts a new Impulse edge into the graph. It must use the
// built-in bytes coder.
func NewImpulse(g *Graph, s *Scope, value []byte) *MultiEdge {
	ft := typex.NewW(typex.New(reflectx.ByteSlice))
	w := window.NewGlobalWindow()
	n := g.NewNode(ft, w)
	n.Coder = coder.NewW(coder.NewBytes(), w)

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
