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
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
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

func TestParDo_BundleFinalization(t *testing.T) {
	bf := bundleFinalizer{
		callbacks:         []bundleFinalizationCallback{},
		lastValidCallback: time.Now(),
	}
	testVar1 := 0
	testVar2 := 0
	bf.RegisterCallback(500*time.Minute, func() error {
		testVar1 += 5
		return nil
	})
	bf.RegisterCallback(2*time.Minute, func() error {
		testVar2 = 25
		return nil
	})

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
	pardo := &ParDo{UID: 3, Fn: edge.DoFn, Inbound: edge.Input, Out: []Node{out, sum}, bf: bf, Side: []SideInputAdapter{
		&FixedSideInputAdapter{Val: &FixedReStream{Buf: makeValues(1)}},       // a
		&FixedSideInputAdapter{Val: &FixedReStream{Buf: makeValues(2, 3, 4)}}, // b
		&FixedSideInputAdapter{Val: &FixedReStream{Buf: makeValues(5, 6)}},    // c
		&FixedSideInputAdapter{Val: &FixedReStream{Buf: makeValues(7, 8, 9)}}, // d
	}}

	// We can't do exact equality since this relies on real time, we'll give it a broad range
	if got := pardo.GetBundleExpirationTime(context.Background()); got.After(time.Now().Add(600*time.Minute)) || got.Before(time.Now().Add(400*time.Minute)) {
		t.Errorf("Before FinalizeBundle() getBundleExpirationTime=%v, want a time around 500 minutes from now", got)
	}
	if err := pardo.FinalizeBundle(context.Background()); err != nil {
		t.Fatalf("FinalizeBundle() failed with error %v", err)
	}
	if got, want := testVar1, 5; got != want {
		t.Errorf("After FinalizeBundle() testVar1=%v, want %v", got, want)
	}
	if got, want := testVar2, 25; got != want {
		t.Errorf("After FinalizeBundle() testVar2=%v, want %v", got, want)
	}
	if got := pardo.GetBundleExpirationTime(context.Background()); got.After(time.Now()) {
		t.Errorf("After FinalizeBundle() getBundleExpirationTime=%v, want a time before current time", got)
	}
}

func TestParDo_BundleFinalization_ExpiredCallback(t *testing.T) {
	bf := bundleFinalizer{
		callbacks:         []bundleFinalizationCallback{},
		lastValidCallback: time.Now(),
	}
	testVar1 := 0
	testVar2 := 0
	bf.RegisterCallback(-500*time.Minute, func() error {
		testVar1 += 5
		return nil
	})
	bf.RegisterCallback(200*time.Minute, func() error {
		testVar2 = 25
		return nil
	})

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
	pardo := &ParDo{UID: 3, Fn: edge.DoFn, Inbound: edge.Input, Out: []Node{out, sum}, bf: bf, Side: []SideInputAdapter{
		&FixedSideInputAdapter{Val: &FixedReStream{Buf: makeValues(1)}},       // a
		&FixedSideInputAdapter{Val: &FixedReStream{Buf: makeValues(2, 3, 4)}}, // b
		&FixedSideInputAdapter{Val: &FixedReStream{Buf: makeValues(5, 6)}},    // c
		&FixedSideInputAdapter{Val: &FixedReStream{Buf: makeValues(7, 8, 9)}}, // d
	}}

	// We can't do exact equality since this relies on real time, we'll give it a broad range
	if got := pardo.GetBundleExpirationTime(context.Background()); got.After(time.Now().Add(300*time.Minute)) || got.Before(time.Now().Add(100*time.Minute)) {
		t.Errorf("Before FinalizeBundle() getBundleExpirationTime=%v, want a time around 200 minutes from now", got)
	}
	if err := pardo.FinalizeBundle(context.Background()); err != nil {
		t.Fatalf("FinalizeBundle() failed with error %v", err)
	}
	if got, want := testVar1, 0; got != want {
		t.Errorf("After FinalizeBundle() testVar1=%v, want %v", got, want)
	}
	if got, want := testVar2, 25; got != want {
		t.Errorf("After FinalizeBundle() testVar2=%v, want %v", got, want)
	}
	if got := pardo.GetBundleExpirationTime(context.Background()); got.After(time.Now()) {
		t.Errorf("After FinalizeBundle() getBundleExpirationTime=%v, want a time before current time", got)
	}
}

func TestParDo_BundleFinalization_CallbackErrors(t *testing.T) {
	bf := bundleFinalizer{
		callbacks:         []bundleFinalizationCallback{},
		lastValidCallback: time.Now(),
	}
	testVar1 := 0
	testVar2 := 0
	errored := false
	bf.RegisterCallback(500*time.Minute, func() error {
		testVar1 += 5
		return nil
	})
	bf.RegisterCallback(200*time.Minute, func() error {
		if errored {
			return nil
		}
		errored = true
		return errors.New("Callback error")
	})
	bf.RegisterCallback(2*time.Minute, func() error {
		testVar2 = 25
		return nil
	})

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
	pardo := &ParDo{UID: 3, Fn: edge.DoFn, Inbound: edge.Input, Out: []Node{out, sum}, bf: bf, Side: []SideInputAdapter{
		&FixedSideInputAdapter{Val: &FixedReStream{Buf: makeValues(1)}},       // a
		&FixedSideInputAdapter{Val: &FixedReStream{Buf: makeValues(2, 3, 4)}}, // b
		&FixedSideInputAdapter{Val: &FixedReStream{Buf: makeValues(5, 6)}},    // c
		&FixedSideInputAdapter{Val: &FixedReStream{Buf: makeValues(7, 8, 9)}}, // d
	}}

	// We can't do exact equality since this relies on real time, we'll give it a broad range
	if got := pardo.GetBundleExpirationTime(context.Background()); got.After(time.Now().Add(600*time.Minute)) || got.Before(time.Now().Add(400*time.Minute)) {
		t.Errorf("Before FinalizeBundle() getBundleExpirationTime=%v, want a time around 500 minutes from now", got)
	}
	if err := pardo.FinalizeBundle(context.Background()); err == nil {
		t.Errorf("FinalizeBundle() succeeded, expected error")
	}
	if got, want := testVar1, 5; got != want {
		t.Errorf("After FinalizeBundle() testVar1=%v, want %v", got, want)
	}
	if got, want := testVar2, 25; got != want {
		t.Errorf("After FinalizeBundle() testVar2=%v, want %v", got, want)
	}
	if got := pardo.GetBundleExpirationTime(context.Background()); got.After(time.Now().Add(300*time.Minute)) || got.Before(time.Now().Add(100*time.Minute)) {
		t.Errorf("After FinalizeBundle() getBundleExpirationTime=%v, want a time around 200 minutes from now", got)
	}
	if err := pardo.FinalizeBundle(context.Background()); err != nil {
		t.Fatalf("FinalizeBundle() failed with error %v, expected to succeed the second time", err)
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
