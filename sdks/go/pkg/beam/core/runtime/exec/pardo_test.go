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
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/timers"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
	"github.com/google/go-cmp/cmp"
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

// TestParDo_IterableSideInputsAndEmitters verifies that the ParDo node works correctly for side inputs and emitters.
// It uses a special dofn that uses all forms of iterable sideinput.
func TestParDo_IterableSideInputsAndEmitters(t *testing.T) {
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

func shortFn(n int, iter func(*int) bool, emit func(int)) {
	var i int
	iter(&i)
	emit(i + n)
}

// TestParDo_ShortGBK verifies that the ParDo node works correctly for short read GBKs.
func TestParDo_ShortGBK(t *testing.T) {
	fn, err := graph.NewDoFn(shortFn)
	if err != nil {
		t.Fatalf("invalid function: %v", err)
	}

	g := graph.New()
	nN := g.NewNode(typex.NewCoGBK(typex.New(reflectx.Int), typex.New(reflectx.Int)), window.DefaultWindowingStrategy(), true)

	edge, err := graph.NewParDo(g, g.Root(), fn, []*graph.Node{nN}, nil, nil)
	if err != nil {
		t.Fatalf("invalid pardo: %v", err)
	}

	short := &CaptureNode{UID: 2}
	pardo := &ParDo{UID: 3, Fn: edge.DoFn, Inbound: edge.Input, Out: []Node{short}, Side: []SideInputAdapter{}}
	n := &FixedRoot{UID: 4, Elements: makeKeyedInput(42, 10, 20, 30), Out: pardo}

	p, err := NewPlan("a", []Unit{n, pardo, short})
	if err != nil {
		t.Fatalf("failed to construct plan: %v", err)
	}

	if err := p.Execute(context.Background(), "1", DataContext{}); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if err := p.Down(context.Background()); err != nil {
		t.Fatalf("down failed: %v", err)
	}

	expected := makeValues(52)
	if !equalList(short.Elements, expected) {
		t.Errorf("pardo(sumFn) = %v, want %v", extractValues(short.Elements...), extractValues(expected...))
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

func TestProcessSingleWindow_withOutputs(t *testing.T) {
	tests := []struct {
		name           string
		inputFn        any
		input          []any
		expectedOutput []any
	}{
		{
			"forwarding case",
			func(a string) (string, error) { return a, nil },
			[]any{"a", "b", "c"},
			[]any{"a", "b", "c"},
		},
		{
			"continuation case",
			func(a string) (string, sdf.ProcessContinuation, error) { return a, sdf.StopProcessing(), nil },
			[]any{"a", "b", "c"},
			[]any{"a", "b", "c"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fn, err := graph.NewDoFn(test.inputFn)
			if err != nil {
				t.Fatalf("invalid function %v", err)
			}
			g := graph.New()
			nN := g.NewNode(typex.New(reflectx.String), window.DefaultWindowingStrategy(), true)

			edge, err := graph.NewParDo(g, g.Root(), fn, []*graph.Node{nN}, nil, nil)
			if err != nil {
				t.Fatalf("invalid pardo: %v", err)
			}

			out := &CaptureNode{UID: 1}
			pardo := &ParDo{UID: 2, Fn: edge.DoFn, Inbound: edge.Input, Out: []Node{out}}
			n := &FixedRoot{UID: 3, Elements: makeInput(test.input...), Out: pardo}

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

			if !equalList(out.Elements, makeValues(test.expectedOutput...)) {
				t.Errorf("got elements %v, want %v", extractValues(out.Elements...), test.expectedOutput)
			}
		})
	}

}

func onlyPC(a string) (sdf.ProcessContinuation, error) {
	return sdf.StopProcessing(), nil
}

func TestProcessSingleWindow_noForward(t *testing.T) {
	fn, err := graph.NewDoFn(onlyPC)
	if err != nil {
		t.Fatalf("invalid function %v", err)
	}
	g := graph.New()
	nN := g.NewNode(typex.New(reflectx.String), window.DefaultWindowingStrategy(), true)

	edge, err := graph.NewParDo(g, g.Root(), fn, []*graph.Node{nN}, nil, nil)
	if err != nil {
		t.Fatalf("invalid pardo: %v", err)
	}

	pardo := &ParDo{UID: 2, Fn: edge.DoFn, Inbound: edge.Input, Out: []Node{}}
	n := &FixedRoot{UID: 3, Elements: makeInput("a"), Out: pardo}

	p, err := NewPlan("a", []Unit{n, pardo})
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

var errReturned = errors.New("a unique error")

func alwaysError(a string) (string, error) {
	return a, errReturned
}

func TestProcessSingleWindow_errorCase(t *testing.T) {
	fn, err := graph.NewDoFn(alwaysError)
	if err != nil {
		t.Fatalf("invalid function %v", err)
	}
	g := graph.New()
	nN := g.NewNode(typex.New(reflectx.String), window.DefaultWindowingStrategy(), true)

	edge, err := graph.NewParDo(g, g.Root(), fn, []*graph.Node{nN}, nil, nil)
	if err != nil {
		t.Fatalf("invalid pardo: %v", err)
	}
	out := &CaptureNode{UID: 1}
	pardo := &ParDo{UID: 2, Fn: edge.DoFn, Inbound: edge.Input, Out: []Node{out}}
	n := &FixedRoot{UID: 3, Elements: makeInput("a"), Out: pardo}

	p, err := NewPlan("a", []Unit{n, pardo, out})
	if err != nil {
		t.Fatalf("failed to construct plan: %v", err)
	}

	err = p.Execute(context.Background(), "1", DataContext{})
	if err == nil {
		t.Fatalf("plan execution succeeded when it should have failed")
	}

	if got, want := err.Error(), errReturned.Error(); !strings.Contains(got, want) {
		t.Errorf("got error %v, want error %v", got, want)
	}

	if len(out.Elements) != 0 {
		t.Errorf("got %v output elements, want 0", len(out.Elements))
	}
}

func makeInputsWithUnfinishedRestrictions(values ...any) []MainInput {
	initial := makeInput(values...)
	var restrictedIns []MainInput
	for _, mainIn := range initial {
		rest := offsetrange.Restriction{Start: 0, End: 10}
		rt := sdf.NewLockRTracker(offsetrange.NewTracker(rest))
		mainIn.RTracker = rt
		restrictedIns = append(restrictedIns, mainIn)
	}
	return restrictedIns
}

type returnSplittable struct{}

func (fn *returnSplittable) CreateInitialRestriction(_ string) offsetrange.Restriction {
	return offsetrange.Restriction{
		Start: 0,
		End:   10,
	}
}

func (fn *returnSplittable) CreateTracker(rest offsetrange.Restriction) *sdf.LockRTracker {
	return sdf.NewLockRTracker(offsetrange.NewTracker(rest))
}

func (fn *returnSplittable) SplitRestriction(_ string, rest offsetrange.Restriction) []offsetrange.Restriction {
	return []offsetrange.Restriction{rest}
}

func (fn *returnSplittable) RestrictionSize(_ string, rest offsetrange.Restriction) float64 {
	return rest.Size()
}

func (fn *returnSplittable) ProcessElement(rt *sdf.LockRTracker, a string) string { return a }

type noReturnSplittable struct{}

func (fn *noReturnSplittable) CreateInitialRestriction(_ string) offsetrange.Restriction {
	return offsetrange.Restriction{
		Start: 0,
		End:   10,
	}
}

func (fn *noReturnSplittable) CreateTracker(rest offsetrange.Restriction) *sdf.LockRTracker {
	return sdf.NewLockRTracker(offsetrange.NewTracker(rest))
}

func (fn *noReturnSplittable) SplitRestriction(_ string, rest offsetrange.Restriction) []offsetrange.Restriction {
	return []offsetrange.Restriction{rest}
}

func (fn *noReturnSplittable) RestrictionSize(_ string, rest offsetrange.Restriction) float64 {
	return rest.Size()
}

func (fn *noReturnSplittable) ProcessElement(rt *sdf.LockRTracker, a string, emit func(string)) error {
	return nil
}

func TestProcessSingleWindow_dataLossCase(t *testing.T) {
	tests := []struct {
		name    string
		inputFn any
		input   *MainInput
	}{
		{
			"returned value",
			&returnSplittable{},
			&makeInputsWithUnfinishedRestrictions("a")[0],
		},
		{
			"no returned value",
			&noReturnSplittable{},
			&makeInputsWithUnfinishedRestrictions("a")[0],
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fn, err := graph.NewDoFn(test.inputFn)
			if err != nil {
				t.Fatalf("invalid function %v", err)
			}
			g := graph.New()
			nN := g.NewNode(typex.New(reflectx.String), window.DefaultWindowingStrategy(), true)

			edge, err := graph.NewParDo(g, g.Root(), fn, []*graph.Node{nN}, nil, nil)
			if err != nil {
				t.Fatalf("invalid pardo: %v", err)
			}

			out := &CaptureNode{UID: 1}
			pardo := &ParDo{UID: 2, Fn: edge.DoFn, Inbound: edge.Input, Out: []Node{out}}

			if err := pardo.Up(context.Background()); err != nil {
				t.Fatalf("up failed: %v", err)
			}

			_, err = pardo.processSingleWindow(test.input)
			if err == nil {
				t.Errorf("processSingleWindow(%v) succeeded when it should have failed", test.input)
			}

			if !strings.Contains(err.Error(), "DoFn terminated without fully processing restriction") {
				t.Errorf("got unexpected error %v", err)
			}
		})
	}
}

// hasTimers is to do unit testing for ProcessTimers, and does not represent
// a canonical use of timers in a DoFn. Do not use as a starting point.
type hasTimers struct {
	Timer timers.EventTime

	emitOnlyOnTag bool
}

func (fn *hasTimers) ProcessElement(tp timers.Provider, k, v string, emit func(string)) {
}

func (fn *hasTimers) OnTimer(ts typex.EventTime, tp timers.Provider, k string, timer timers.Context, emit func(string)) {
	if timer.Tag == "" {
		// Sets another timer, but should be sent to the runner.
		fn.Timer.Set(tp, ts.ToTime().Add(-10*time.Millisecond), timers.WithTag("tag"))
	}

	if (timer.Tag != "" && fn.emitOnlyOnTag) || !fn.emitOnlyOnTag {
		emit(k)
	}
}

func TestProcessTimers(t *testing.T) {
	tests := []struct {
		name                 string
		inputFn              any
		timerKeys            []any
		wantEmitted, wantSet []any
		Coder                *coder.Coder
	}{
		{
			name:        "onTimer- different keys",
			Coder:       coder.NewT(coder.NewString(), coder.NewGlobalWindow()),
			inputFn:     &hasTimers{Timer: timers.InEventTime("testTimer"), emitOnlyOnTag: false},
			timerKeys:   []any{"1", "2", "3", "4", "5"},
			wantEmitted: []any{"1", "2", "3", "4", "5"},
		}, {
			name:        "onTimer- same keys",
			Coder:       coder.NewT(coder.NewString(), coder.NewGlobalWindow()),
			inputFn:     &hasTimers{Timer: timers.InEventTime("testTimer"), emitOnlyOnTag: true},
			timerKeys:   []any{"1", "2", "1", "3", "1", "2", "1", "1"},
			wantEmitted: []any{"1", "1", "2", "1", "1"}, // We only emit on inline firings,
			wantSet:     []any{"1", "2", "3"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fn, err := graph.NewDoFn(test.inputFn)
			if err != nil {
				t.Fatalf("invalid function %v", err)
			}
			g := graph.New()
			nN := g.NewNode(typex.NewKV(typex.New(reflectx.String), typex.New(reflectx.String)), window.DefaultWindowingStrategy(), true)

			edge, err := graph.NewParDo(g, g.Root(), fn, []*graph.Node{nN}, nil, nil)
			if err != nil {
				t.Fatalf("invalid pardo: %v", err)
			}

			out := &CaptureNode{UID: 1}

			ta := newUserTimerAdapter(StreamID{}, map[string]timerFamilySpec{
				"testTimer": {
					Domain:     timers.EventTimeDomain,
					KeyEncoder: MakeElementEncoder(test.Coder.Components[0]),
					KeyDecoder: MakeElementDecoder(test.Coder.Components[0]),
					WinEncoder: MakeWindowEncoder(test.Coder.Window),
					WinDecoder: MakeWindowDecoder(test.Coder.Window),
				},
			})
			pardo := &ParDo{UID: 2, Fn: edge.DoFn, Inbound: edge.Input, Out: []Node{out}, TimerTracker: ta}

			now := mtime.Now()
			tc := MakeElementEncoder(test.Coder)
			var buf bytes.Buffer
			for _, v := range test.timerKeys {
				timer := TimerRecv{Key: &FullValue{Elm: v}, Windows: window.SingleGlobalWindow, Pane: typex.NoFiringPane(),
					TimerMap: timers.TimerMap{
						Family:        "testTimer",
						Clear:         false,
						FireTimestamp: now,
						HoldTimestamp: now,
					}}
				if err := tc.Encode(&FullValue{Elm: timer}, &buf); err != nil {
					t.Fatalf("failed to encode timer for key %v", v)
				}
			}

			if err := pardo.Up(context.Background()); err != nil {
				t.Fatalf("pardo.Up failed: %v", err)
			}
			if err := out.Up(context.Background()); err != nil {
				t.Fatalf("capture.Up failed: %v", err)
			}
			if err := pardo.StartBundle(context.Background(), "testID", DataContext{}); err != nil {
				t.Fatalf("pardo.StartBundle failed: %v", err)
			}
			if err := pardo.ProcessTimers("testTimer", &buf); err != nil {
				t.Errorf("ProcessTimers failed when it should have succeeded: %v", err)
			}
			if diff := cmp.Diff(test.wantEmitted, extractValues(out.Elements...)); diff != "" {
				t.Errorf("ParDo.ProcessTimers diff: \n%v", diff)
			}

			// Hard check modifications for expected writes.
			for _, key := range test.wantSet {
				ks := key.(string)
				pair := windowKeyPair{key: string(append([]byte{byte(len(ks))}, []byte(ks)...)), window: window.GlobalWindow{}}
				mods, ok := pardo.TimerTracker.modifications[pair]
				if !ok {
					t.Errorf("can't find modified timer for pair %v for lenMods: %v", pair, pardo.TimerTracker.modifications)
				}
				_, ok = mods.modified[timerKey{family: "testTimer", tag: "tag"}]
				if !ok {
					t.Errorf("can't find modified timer for pair %v for testTimer+tag", pair)
				}
			}
		})
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
