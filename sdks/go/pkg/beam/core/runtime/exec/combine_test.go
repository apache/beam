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
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/coderx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

// TestCombine verifies that the Combine node works correctly.
func TestCombine(t *testing.T) {
	tests := []struct {
		Fn         interface{}
		AccumCoder *coder.Coder
	}{
		{Fn: mergeFn, AccumCoder: intCoder(reflectx.Int)},
		{Fn: &MyCombine{}, AccumCoder: intCoder(reflectx.Int64)},
	}
	for _, test := range tests {
		t.Run(reflect.TypeOf(test.Fn).Name(), func(t *testing.T) {
			edge := getCombineEdge(t, test.Fn, test.AccumCoder)

			out := &CaptureNode{UID: 1}
			combine := &Combine{UID: 2, Fn: edge.CombineFn, Out: out}
			n := &FixedRoot{UID: 3, Elements: makeKeyedInput(42, 1, 2, 3, 4, 5, 6), Out: combine}

			constructAndExecutePlan(t, []Unit{n, combine, out})

			expected := makeKV(42, 21)
			if !equalList(out.Elements, expected) {
				t.Errorf("pardo(sumFn) = %v, want %v", extractKeyedValues(out.Elements...), extractKeyedValues(expected...))
			}
		})
	}
}

// TestLiftedCombine verifies that the LiftedCombine node works correctly.
func TestLiftedCombine(t *testing.T) {
	tests := []struct {
		Fn         interface{}
		AccumCoder *coder.Coder
		Expected   interface{}
	}{
		{Fn: mergeFn, AccumCoder: intCoder(reflectx.Int), Expected: int(21)},
		{Fn: &MyCombine{}, AccumCoder: intCoder(reflectx.Int64), Expected: int64(21)},
	}
	for _, test := range tests {
		t.Run(reflect.TypeOf(test.Fn).Name(), func(t *testing.T) {
			edge := getCombineEdge(t, test.Fn, test.AccumCoder)

			out := &CaptureNode{UID: 1}
			precombine := &LiftedCombine{Combine: &Combine{UID: 2, Fn: edge.CombineFn, Out: out}}
			n := &FixedRoot{UID: 3, Elements: makeKVInput(42, 1, 2, 3, 4, 5, 6), Out: precombine}

			constructAndExecutePlan(t, []Unit{n, precombine, out})
			expected := makeKV(42, test.Expected)
			if !equalList(out.Elements, expected) {
				t.Errorf("precombine(combineFn) = %v, want %v", extractKeyedValues(out.Elements...), extractKeyedValues(expected...))
			}
		})
	}
}

// TestMergeAccumulators verifies that the MergeAccumulators node works correctly.
func TestMergeAccumulators(t *testing.T) {
	tests := []struct {
		Fn         interface{}
		AccumCoder *coder.Coder
		Expected   interface{}
		Values     []interface{}
	}{
		{Fn: mergeFn, AccumCoder: intCoder(reflectx.Int), Values: []interface{}{int(1), int(2), int(3), int(4), int(5), int(6)}, Expected: int(21)},
		{Fn: &MyCombine{}, AccumCoder: intCoder(reflectx.Int64), Values: []interface{}{int64(1), int64(2), int64(3), int64(4), int64(5), int64(6)}, Expected: int64(21)},
	}
	for _, test := range tests {
		t.Run(reflect.TypeOf(test.Fn).Name(), func(t *testing.T) {
			edge := getCombineEdge(t, test.Fn, test.AccumCoder)

			out := &CaptureNode{UID: 1}
			merge := &MergeAccumulators{Combine: &Combine{UID: 3, Fn: edge.CombineFn, Out: out}}
			n := &FixedRoot{UID: 3, Elements: makeKeyedInput(42, test.Values...), Out: merge}

			constructAndExecutePlan(t, []Unit{n, merge, out})

			expected := makeKV(42, test.Expected)
			if !equalList(out.Elements, expected) {
				t.Errorf("merge(combineFn) = %v, want %v", extractKeyedValues(out.Elements...), extractKeyedValues(expected...))
			}
		})
	}
}

// TestExtractOutput verifies that the ExtractOutput node works correctly.
func TestExtractOutput(t *testing.T) {
	tests := []struct {
		Fn         interface{}
		AccumCoder *coder.Coder
		Input      interface{}
	}{
		{Fn: mergeFn, AccumCoder: intCoder(reflectx.Int), Input: int(21)},
		{Fn: &MyCombine{}, AccumCoder: intCoder(reflectx.Int64), Input: int64(21)},
	}
	for _, test := range tests {
		t.Run(reflect.TypeOf(test.Fn).Name(), func(t *testing.T) {
			edge := getCombineEdge(t, test.Fn, test.AccumCoder)

			out := &CaptureNode{UID: 1}
			extract := &ExtractOutput{Combine: &Combine{UID: 2, Fn: edge.CombineFn, Out: out}}
			n := &FixedRoot{UID: 3, Elements: makeKVInput(42, test.Input), Out: extract}

			constructAndExecutePlan(t, []Unit{n, extract, out})

			expected := makeKV(42, 21)
			if !equalList(out.Elements, expected) {
				t.Errorf("extract(combineFn) = %v, want %v", extractKeyedValues(out.Elements...), extractKeyedValues(expected...))
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
	inT := typex.NewCoGBK(typex.New(reflectx.Int), typex.New(reflectx.Int))
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

	if err := p.Execute(context.Background(), "1", nil); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if err := p.Down(context.Background()); err != nil {
		t.Fatalf("down failed: %v", err)
	}
}

func mergeFn(a, b int) int {
	return a + b
}

type MyCombine struct{}

func (*MyCombine) AddInput(k int64, a int) int64 {
	return k + int64(a)
}

func (*MyCombine) MergeAccumulators(a, b int64) int64 {
	return a + b
}

func (*MyCombine) ExtractOutput(a int64) int {
	return int(a)
}

func intCoder(t reflect.Type) *coder.Coder {
	c, err := coderx.NewVarIntZ(t)
	if err != nil {
		panic(fmt.Sprintf("Couldn't get VarInt coder for %v: %v", t, err))
	}
	return &coder.Coder{Kind: coder.Custom, T: typex.New(t), Custom: c}
}
