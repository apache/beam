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

package exec

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/coderx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

var intInput = []interface{}{int(1), int(2), int(3), int(4), int(5), int(6)}
var strInput = []interface{}{"1", "2", "3", "4", "5", "6"}

var tests = []struct {
	Fn         interface{}
	AccumCoder *coder.Coder
	Input      []interface{}
	Expected   interface{}
}{
	{Fn: mergeFn, AccumCoder: intCoder(reflectx.Int), Input: intInput, Expected: int(21)},
	{Fn: nonBinaryMergeFn, AccumCoder: intCoder(reflectx.Int), Input: intInput, Expected: int(21)},
	{Fn: &MyCombine{}, AccumCoder: intCoder(reflectx.Int64), Input: intInput, Expected: int(21)},
	{Fn: &MyOtherCombine{}, AccumCoder: intCoder(reflectx.Int64), Input: intInput, Expected: "21"},
	{Fn: &MyThirdCombine{}, AccumCoder: intCoder(reflectx.Int), Input: strInput, Expected: int(21)},
	{Fn: &MyContextCombine{}, AccumCoder: intCoder(reflectx.Int64), Input: intInput, Expected: int(21)},
	{Fn: &MyErrorCombine{}, AccumCoder: intCoder(reflectx.Int64), Input: intInput, Expected: int(21)},
}

func fnName(x interface{}) string {
	v := reflect.ValueOf(x)
	if v.Kind() != reflect.Func {
		return v.Type().String()
	}
	return runtime.FuncForPC(uintptr(v.Pointer())).Name()
}

// TestCombine verifies that the Combine node works correctly.
func TestCombine(t *testing.T) {
	for _, test := range tests {
		t.Run(fnName(test.Fn), func(t *testing.T) {
			edge := getCombineEdge(t, test.Fn, test.AccumCoder)

			out := &CaptureNode{UID: 1}
			combine := &Combine{UID: 2, Fn: edge.CombineFn, Out: out}
			n := &FixedRoot{UID: 3, Elements: makeKeyedInput(42, test.Input...), Out: combine}

			constructAndExecutePlan(t, []Unit{n, combine, out})

			expected := makeKV(42, test.Expected)
			if !equalList(out.Elements, expected) {
				t.Errorf("combine(%s) = %v, want %v", edge.CombineFn.Name(), extractKeyedValues(out.Elements...), extractKeyedValues(expected...))
			}
		})
	}
}

// TestLiftedCombine verifies that the LiftedCombine, MergeAccumulators, and
// ExtractOutput nodes work correctly after the lift has been performed.
func TestLiftedCombine(t *testing.T) {
	for _, test := range tests {
		t.Run(fnName(test.Fn), func(t *testing.T) {
			edge := getCombineEdge(t, test.Fn, test.AccumCoder)

			out := &CaptureNode{UID: 1}
			extract := &ExtractOutput{Combine: &Combine{UID: 2, Fn: edge.CombineFn, Out: out}}
			merge := &MergeAccumulators{Combine: &Combine{UID: 3, Fn: edge.CombineFn, Out: extract}}
			gbk := &simpleGBK{UID: 4, Out: merge}
			precombine := &LiftedCombine{Combine: &Combine{UID: 5, Fn: edge.CombineFn, Out: gbk}}
			n := &FixedRoot{UID: 6, Elements: makeKVInput(42, test.Input...), Out: precombine}

			constructAndExecutePlan(t, []Unit{n, precombine, gbk, merge, extract, out})
			expected := makeKV(42, test.Expected)
			if !equalList(out.Elements, expected) {
				t.Errorf("liftedCombineChain(%s) = %v, want %v", edge.CombineFn.Name(), extractKeyedValues(out.Elements...), extractKeyedValues(expected...))
			}
		})
	}
}

func getCombineEdge(t *testing.T, cfn interface{}, ac *coder.Coder) *graph.MultiEdge {
	t.Helper()
	fn, err := graph.NewCombineFn(cfn)
	if err != nil {
		t.Fatalf("invalid function: %v", err)
	}

	g := graph.New()
	var vtype reflect.Type
	if fn.AddInputFn() != nil {
		// This makes the assumption that the AddInput function is unkeyed.
		vtype = fn.AddInputFn().Param[1].T
	} else {
		vtype = fn.MergeAccumulatorsFn().Param[1].T
	}
	inT := typex.NewCoGBK(typex.New(reflectx.Int), typex.New(vtype))
	in := g.NewNode(inT, window.DefaultWindowingStrategy(), true)

	edge, err := graph.NewCombine(g, g.Root(), fn, in, ac)
	if err != nil {
		t.Fatalf("invalid combinefn: %v", err)
	}
	return edge
}

func constructAndExecutePlan(t *testing.T, us []Unit) {
	p, err := NewPlan("a", us)
	if err != nil {
		t.Fatalf("failed to construct plan: %v", err)
	}

	if err := p.Execute(context.Background(), "1", DataContext{}); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if err := p.Down(context.Background()); err != nil {
		t.Fatalf("down failed: %v", err)
	}
}

// The following functions and struct represent the combine contract implemented
// in various ways.
//
// Instead of a ProcessElement method, a liftable combine can be written by
// breaking down an operation into four component methods:
//
//   CreateAccumulator() AccumT
//   AddInput(a AccumT, v InputT) AccumT
//   MergeAccumulators(a, b AccumT) AccumT
//   ExtractOutput(v AccumT) OutputT
//
// In addition, depending there can be three distinct types, depending on where
// they are used in the combine, the input type, InputT, the output type, OutputT,
// and the accumulator type AccumT. Depending on the equality of the types, one
// or more of the methods can be unspecified.
//
// The only required method is MergeAccumulators.
//
// When InputT == OutputT == AccumT, the only required method is MergeAccumulators.
//
// When InputT == AccumT , then AddInput can be omitted, and MergeAccumulators used instead.
// When AccumT == Output , then ExtractOutput can be omitted, and the identity used instead.
//
// These two combined mean that when InputT == OutputT == AccumT, only MergeAccumulators
// needs to be specified.
//
// CreateAccumulator() is only required if the AccumT cannot be used in it's zero state,
// and needs initialization.

// mergeFn represents a combine that is just a binary merge, where
//
//  InputT == OutputT == AccumT == int
func mergeFn(a, b int) int {
	return a + b
}

// nonBinaryMergeFn represents a combine with a context parameter and an error return, where
//
//  InputT == OutputT == AccumT == int
func nonBinaryMergeFn(ctx context.Context, a, b int) (int, error) {
	return a + b, nil
}

// MyCombine represents a combine with the same Input and Output type (int), but a
// distinct accumulator type (int64).
//
//  InputT == OutputT == int
//  AccumT == int64
type MyCombine struct{}

func (*MyCombine) AddInput(a int64, v int) int64 {
	return a + int64(v)
}

func (*MyCombine) MergeAccumulators(a, b int64) int64 {
	return a + b
}

func (*MyCombine) ExtractOutput(a int64) int {
	return int(a)
}

// MyOtherCombine is the same as MyCombine, but has strings extracted as output.
//
//  InputT == int
//  AccumT == int64
//  OutputT == string
type MyOtherCombine struct {
	MyCombine // Embedding to re-use the exisitng AddInput and MergeAccumulators implementations
}

func (*MyOtherCombine) ExtractOutput(a int64) string {
	return fmt.Sprintf("%d", a)
}

// MyThirdCombine has parses strings as Input, and doesn't specify an ExtractOutput
//
//  InputT == string
//  AccumT == int
//  OutputT == int
type MyThirdCombine struct{}

func (c *MyThirdCombine) AddInput(a int, s string) (int, error) {
	v, err := strconv.ParseInt(s, 0, 0)
	if err != nil {
		return 0, err
	}
	return c.MergeAccumulators(a, int(v)), nil
}

func (*MyThirdCombine) MergeAccumulators(a, b int) int {
	return a + b
}

// MyContextCombine is the same as MyCombine, but requires a context parameter.
//
//  InputT == int
//  AccumT == int64
//  OutputT == string
type MyContextCombine struct {
	MyCombine // Embedding to re-use the exisitng AddInput implementations
}

func (*MyContextCombine) MergeAccumulators(_ context.Context, a, b int64) int64 {
	return a + b
}

// MyErrorCombine is the same as MyCombine, but may return an error.
//
//  InputT == int
//  AccumT == int64
//  OutputT == string
type MyErrorCombine struct {
	MyCombine // Embedding to re-use the exisitng AddInput implementations
}

func (*MyErrorCombine) MergeAccumulators(a, b int64) (int64, error) {
	return a + b, nil
}

func intCoder(t reflect.Type) *coder.Coder {
	c, err := coderx.NewVarIntZ(t)
	if err != nil {
		panic(fmt.Sprintf("Couldn't get VarInt coder for %v: %v", t, err))
	}
	return &coder.Coder{Kind: coder.Custom, T: typex.New(t), Custom: c}
}

// simpleGBK buffers all input and continues on FinishBundle. Use with small single-bundle data only.
type simpleGBK struct {
	UID  UnitID
	Edge *graph.MultiEdge
	Out  Node

	m map[interface{}]*group
}

type group struct {
	key    FullValue
	values []FullValue
}

func (n *simpleGBK) ID() UnitID {
	return n.UID
}

func (n *simpleGBK) Up(ctx context.Context) error {
	n.m = make(map[interface{}]*group)
	return nil
}

func (n *simpleGBK) StartBundle(ctx context.Context, id string, data DataContext) error {
	return n.Out.StartBundle(ctx, id, data)
}

func (n *simpleGBK) ProcessElement(ctx context.Context, elm FullValue, _ ...ReStream) error {
	key := elm.Elm.(int)
	value := elm.Elm2

	g, ok := n.m[key]
	if !ok {
		g = &group{
			key:    FullValue{Elm: key, Timestamp: elm.Timestamp, Windows: elm.Windows},
			values: make([]FullValue, 0),
		}
		n.m[key] = g
	}
	g.values = append(g.values, FullValue{Elm: value, Timestamp: elm.Timestamp})

	return nil
}

func (n *simpleGBK) FinishBundle(ctx context.Context) error {
	for _, g := range n.m {
		values := &FixedReStream{Buf: g.values}
		if err := n.Out.ProcessElement(ctx, g.key, values); err != nil {
			return err
		}
	}
	return n.Out.FinishBundle(ctx)
}

func (n *simpleGBK) Down(ctx context.Context) error {
	return nil
}

func (n *simpleGBK) String() string {
	return fmt.Sprintf("simpleGBK: %v Out:%v", n.ID(), n.Out.ID())
}
