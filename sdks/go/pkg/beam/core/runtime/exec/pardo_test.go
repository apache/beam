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
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
)

func sumFn(n int, a int, b []int, c func(*int) bool, d func() func(*int) bool, e func(int)) int {
	sum := a
	for _, i := range b {
		sum += i
	}

	var i int
	for c(&i) {
		sum += i
	}

	dfn := d()
	for dfn(&i) {
		sum += i
	}
	e(sum)
	return n
}

// TestParDo verifies that the ParDo node works correctly for side inputs and emitters. It uses a special
// dofn that uses all forms of sideinput.
func TestParDo(t *testing.T) {
	fn, err := graph.NewDoFn(sumFn)
	if err != nil {
		t.Fatalf("invalid function: %v", err)
	}

	g := graph.New()
	nN := g.NewNode(typex.New(reflectx.Int), window.DefaultWindowingStrategy(), true)
	aN := g.NewNode(typex.New(reflectx.Int), window.DefaultWindowingStrategy(), true)
	bN := g.NewNode(typex.New(reflectx.Int), window.DefaultWindowingStrategy(), true)
	cN := g.NewNode(typex.New(reflectx.Int), window.DefaultWindowingStrategy(), true)
	dN := g.NewNode(typex.New(reflectx.Int), window.DefaultWindowingStrategy(), true)

	edge, err := graph.NewParDo(g, g.Root(), fn, []*graph.Node{nN, aN, bN, cN, dN}, nil, nil)
	if err != nil {
		t.Fatalf("invalid pardo: %v", err)
	}

	out := &CaptureNode{UID: 1}
	sum := &CaptureNode{UID: 2}
	pardo := &ParDo{UID: 3, Fn: edge.DoFn, Inbound: edge.Input, Out: []Node{out, sum}, Side: []SideInputAdapter{
		&FixedSideInputAdapter{Val: &FixedReStream{Buf: makeValues(1)}},       // a
		&FixedSideInputAdapter{Val: &FixedReStream{Buf: makeValues(2, 3, 4)}}, // b
		&FixedSideInputAdapter{Val: &FixedReStream{Buf: makeValues(5, 6)}},    // c
		&FixedSideInputAdapter{Val: &FixedReStream{Buf: makeValues(7, 8, 9)}}, // d
	}}
	n := &FixedRoot{UID: 4, Elements: makeInput(10, 20, 30), Out: pardo}

	p, err := NewPlan("a", []Unit{n, pardo, out, sum})
	if err != nil {
		t.Fatalf("failed to construct plan: %v", err)
	}

	if err := p.Execute(context.Background(), "1", DataContext{}); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if err := p.Down(context.Background()); err != nil {
		t.Fatalf("down failed: %v", err)
	}

	expected := makeValues(10, 20, 30)
	if !equalList(out.Elements, expected) {
		t.Errorf("pardo(sumFn) = %v, want %v", extractValues(out.Elements...), extractValues(expected...))
	}
	expectedSum := makeValues(45, 45, 45)
	if !equalList(sum.Elements, expectedSum) {
		t.Errorf("pardo(sumFn) side input = %v, want %v", extractValues(sum.Elements...), extractValues(expectedSum...))
	}
}

func windowObserverFn(w typex.Window, word string) string {
	if _, ok := w.(window.GlobalWindow); ok {
		return fmt.Sprintf("%v-%v", word, "global")
	}
	if iw, ok := w.(window.IntervalWindow); ok {
		return fmt.Sprintf("%v-%v-%v", word, iw.Start, iw.End)
	}
	return fmt.Sprintf("Couldnt cast %v to a window type", w)
}

// TestParDo verifies that the ParDo node works correctly for side inputs and emitters. It uses a special
// dofn that uses all forms of sideinput.
func TestParDo_WindowObservation(t *testing.T) {
	tests := []struct {
		ws   []typex.Window
		out1 string
		out2 string
	}{
		{
			ws:   []typex.Window{window.GlobalWindow{}},
			out1: "testInput1-global",
			out2: "testInput2-global",
		},
		{
			ws: []typex.Window{window.IntervalWindow{
				Start: 100,
				End:   200,
			}},
			out1: "testInput1-100-200",
			out2: "testInput2-100-200",
		},
	}
	for _, test := range tests {
		fn, err := graph.NewDoFn(windowObserverFn)
		if err != nil {
			t.Fatalf("invalid function: %v", err)
		}

		g := graph.New()
		nN := g.NewNode(typex.New(reflectx.String), window.DefaultWindowingStrategy(), true)

		edge, err := graph.NewParDo(g, g.Root(), fn, []*graph.Node{nN}, nil, nil)
		if err != nil {
			t.Fatalf("invalid pardo: %v", err)
		}

		out := &CaptureNode{UID: 1}
		pardo := &ParDo{UID: 3, Fn: edge.DoFn, Inbound: edge.Input, Out: []Node{out}}
		n := &FixedRoot{UID: 4, Elements: makeWindowedInput(test.ws, "testInput1", "testInput2"), Out: pardo}

		p, err := NewPlan("a", []Unit{n, pardo, out})
		if err != nil {
			t.Fatalf("failed to construct plan: %v", err)
		}

		if err := p.Execute(context.Background(), "1", DataContext{}); err != nil {
			t.Fatalf("execute failed: %v", err)
		}
		if err := p.Down(context.Background()); err != nil {
			t.Fatalf("down failed: %v", err)
		}

		expected := makeWindowedValues(test.ws, test.out1, test.out2)
		if !equalList(out.Elements, expected) {
			t.Errorf("pardo(windowObserverFn) = %v, want %v", extractValues(out.Elements...), extractValues(expected...))
		}
	}
}

func emitSumFn(n int, emit func(int)) {
	emit(n + 1)
}

// BenchmarkParDo_EmitSumFn measures the overhead of invoking a ParDo in a plan.
//
// On @lostluck's desktop:
// BenchmarkParDo_EmitSumFn-12    	 1000000	      1070 ns/op	     481 B/op	       3 allocs/op
func BenchmarkParDo_EmitSumFn(b *testing.B) {
	fn, err := graph.NewDoFn(emitSumFn)
	if err != nil {
		b.Fatalf("invalid function: %v", err)
	}

	g := graph.New()
	nN := g.NewNode(typex.New(reflectx.Int), window.DefaultWindowingStrategy(), true)
	edge, err := graph.NewParDo(g, g.Root(), fn, []*graph.Node{nN}, nil, nil)
	if err != nil {
		b.Fatalf("invalid pardo: %v", err)
	}
	var in []int
	for i := 0; i < b.N; i++ {
		in = append(in)
	}

	process := make(chan MainInput, 1)
	errchan := make(chan string, 1)
	out := &CaptureNode{UID: 1}
	pardo := &ParDo{UID: 3, Fn: edge.DoFn, Inbound: edge.Input, Out: []Node{out}}
	n := &BenchRoot{UID: 4, Elements: process, Out: pardo}
	p, err := NewPlan("a", []Unit{n, pardo, out})
	if err != nil {
		b.Fatalf("failed to construct plan: %v", err)
	}
	go func() {
		defer close(errchan)
		if err := p.Execute(context.Background(), "1", DataContext{}); err != nil {
			errchan <- fmt.Sprintf("execute failed: %v", err)
			return
		}
		if err := p.Down(context.Background()); err != nil {
			errchan <- fmt.Sprintf("down failed: %v", err)
			return
		}
	}()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		process <- MainInput{Key: FullValue{
			Windows:   window.SingleGlobalWindow,
			Timestamp: mtime.ZeroTimestamp,
			Elm:       1,
		}}
	}
	close(process)
	for msg := range errchan {
		b.Fatal(msg)
	}
}
