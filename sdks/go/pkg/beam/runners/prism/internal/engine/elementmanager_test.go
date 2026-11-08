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

package engine

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"io"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/google/go-cmp/cmp"
)

func TestElementHeap(t *testing.T) {
	elements := elementHeap{
		element{timestamp: mtime.EndOfGlobalWindowTime},
		element{timestamp: mtime.MaxTimestamp},
		element{timestamp: 3},
		element{timestamp: mtime.MinTimestamp},
		element{timestamp: 2},
		element{timestamp: mtime.ZeroTimestamp},
		element{timestamp: 1},
	}
	heap.Init(&elements)
	heap.Push(&elements, element{timestamp: 4})

	if got, want := elements.Len(), len(elements); got != want {
		t.Errorf("elements.Len() = %v, want %v", got, want)
	}
	if got, want := elements[0].timestamp, mtime.MinTimestamp; got != want {
		t.Errorf("elements[0].timestamp = %v, want %v", got, want)
	}

	wanted := []mtime.Time{mtime.MinTimestamp, mtime.ZeroTimestamp, 1, 2, 3, 4, mtime.EndOfGlobalWindowTime, mtime.MaxTimestamp}
	for i, want := range wanted {
		if got := heap.Pop(&elements).(element).timestamp; got != want {
			t.Errorf("[%d] heap.Pop(&elements).(element).timestamp = %v, want %v", i, got, want)
		}
	}
}

func TestStatefulStageKind_buildEventTimeBundle_TimeSortedInput(t *testing.T) {
	// The key has data at 50 and 200 pending. Each step builds a bundle at the
	// given watermark, and checks the timestamps of the data it processes.
	// Data is buildable at a watermark exactly when the step processes some.
	type step struct {
		watermark mtime.Time
		want      []mtime.Time
	}
	tests := []struct {
		name            string
		timeSorted      bool
		allowedLateness time.Duration
		steps           []step
	}{
		{
			name: "not time sorted processes all data",
			steps: []step{
				{watermark: mtime.MinTimestamp, want: []mtime.Time{50, 200}},
			},
		}, {
			name:       "time sorted holds data until the watermark passes it",
			timeSorted: true,
			steps: []step{
				{watermark: 49, want: nil},
				{watermark: 50, want: []mtime.Time{50}},
				{watermark: 199, want: nil},
				{watermark: 200, want: []mtime.Time{200}},
			},
		}, {
			name:            "time sorted holds data for the allowed lateness",
			timeSorted:      true,
			allowedLateness: 100 * time.Millisecond,
			steps: []step{
				{watermark: 149, want: nil},
				{watermark: 150, want: []mtime.Time{50}},
				{watermark: 299, want: nil},
				{watermark: mtime.MaxTimestamp, want: []mtime.Time{200}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			em := NewElementManager(Config{})
			em.AddStage("dofn", []string{"input"}, nil, nil)
			em.StageStateful("dofn", nil)
			if test.timeSorted {
				em.StageRequiresTimeSortedInput("dofn", test.allowedLateness)
			}
			ss := em.stages["dofn"]
			win := window.GlobalWindow{}
			dnt := &dataAndTimers{
				elements: elementHeap{
					{window: win, timestamp: 200, elmBytes: []byte{2}, keyBytes: []byte("k")},
					{window: win, timestamp: 50, elmBytes: []byte{1}, keyBytes: []byte("k")},
				},
				timers: map[timerKey]timerTimes{},
			}
			heap.Init(&dnt.elements)
			ss.pendingByKeys = map[string]*dataAndTimers{"k": dnt}

			for _, s := range test.steps {
				ss.mu.Lock()
				buildable := ss.hasBuildableDataLocked(s.watermark)
				toProcess, _, _, _, _, _, _ := ss.kind.buildEventTimeBundle(ss, s.watermark)
				ss.mu.Unlock()
				var got []mtime.Time
				for _, e := range toProcess {
					got = append(got, e.timestamp)
				}
				if !cmp.Equal(got, s.want) {
					t.Errorf("buildEventTimeBundle(watermark=%v) = %v, want %v", s.watermark, got, s.want)
				}
				if want := len(s.want) > 0; buildable != want {
					t.Errorf("hasBuildableDataLocked(%v) = %v, want %v", s.watermark, buildable, want)
				}
			}
			if got := len(ss.pendingByKeys); got != 0 {
				t.Errorf("len(pendingByKeys) = %v, want 0", got)
			}
		})
	}
}

func TestStageState_minPendingTimestamp(t *testing.T) {

	newState := func() *stageState {
		return makeStageState("test", []string{"testInput"}, []string{"testOutput"}, nil)
	}
	t.Run("noElements", func(t *testing.T) {
		ss := newState()
		got := ss.minPendingTimestamp()
		want := mtime.MaxTimestamp
		if got != want {
			t.Errorf("ss.minPendingTimestamp() = %v, want %v", got, want)
		}
	})

	want := mtime.ZeroTimestamp - 20
	t.Run("onlyPending", func(t *testing.T) {
		ss := newState()
		ss.pending = elementHeap{
			element{timestamp: mtime.EndOfGlobalWindowTime},
			element{timestamp: mtime.MaxTimestamp},
			element{timestamp: 3},
			element{timestamp: want},
			element{timestamp: 2},
			element{timestamp: mtime.ZeroTimestamp},
			element{timestamp: 1},
		}
		heap.Init(&ss.pending)

		got := ss.minPendingTimestamp()
		if got != want {
			t.Errorf("ss.minPendingTimestamp() = %v, want %v", got, want)
		}
	})

	t.Run("onlyInProgress", func(t *testing.T) {
		ss := newState()
		ss.inprogress = map[string]elements{
			"a": {
				es: []element{
					{timestamp: mtime.EndOfGlobalWindowTime},
					{timestamp: mtime.MaxTimestamp},
				},
				minTimestamp: mtime.EndOfGlobalWindowTime,
			},
			"b": {
				es: []element{
					{timestamp: 3},
					{timestamp: want},
					{timestamp: 2},
					{timestamp: 1},
				},
				minTimestamp: want,
			},
			"c": {
				es: []element{
					{timestamp: mtime.ZeroTimestamp},
				},
				minTimestamp: mtime.ZeroTimestamp,
			},
		}

		got := ss.minPendingTimestamp()
		if got != want {
			t.Errorf("ss.minPendingTimestamp() = %v, want %v", got, want)
		}
	})

	t.Run("minInPending", func(t *testing.T) {
		ss := newState()
		ss.pending = elementHeap{
			{timestamp: 3},
			{timestamp: want},
			{timestamp: 2},
			{timestamp: 1},
		}
		heap.Init(&ss.pending)
		ss.inprogress = map[string]elements{
			"a": {
				es: []element{
					{timestamp: mtime.EndOfGlobalWindowTime},
					{timestamp: mtime.MaxTimestamp},
				},
				minTimestamp: mtime.EndOfGlobalWindowTime,
			},
			"c": {
				es: []element{
					{timestamp: mtime.ZeroTimestamp},
				},
				minTimestamp: mtime.ZeroTimestamp,
			},
		}

		got := ss.minPendingTimestamp()
		if got != want {
			t.Errorf("ss.minPendingTimestamp() = %v, want %v", got, want)
		}
	})
	t.Run("minInProgress", func(t *testing.T) {
		ss := newState()
		ss.pending = elementHeap{
			{timestamp: 3},
			{timestamp: 2},
			{timestamp: 1},
		}
		heap.Init(&ss.pending)
		ss.inprogress = map[string]elements{
			"a": {
				es: []element{
					{timestamp: want},
					{timestamp: mtime.EndOfGlobalWindowTime},
					{timestamp: mtime.MaxTimestamp},
				},
				minTimestamp: want,
			},
			"c": {
				es: []element{
					{timestamp: mtime.ZeroTimestamp},
				},
				minTimestamp: mtime.ZeroTimestamp,
			},
		}

		got := ss.minPendingTimestamp()
		if got != want {
			t.Errorf("ss.minPendingTimestamp() = %v, want %v", got, want)
		}
	})
}

func TestStageState_UpstreamWatermark(t *testing.T) {
	impulse := makeStageState("impulse", nil, []string{"output"}, nil)
	_, up := impulse.UpstreamWatermark()
	if got, want := up, mtime.MaxTimestamp; got != want {
		t.Errorf("impulse.UpstreamWatermark() = %v, want %v", got, want)
	}

	dofn := makeStageState("dofn", []string{"input"}, []string{"output"}, nil)
	dofn.updateUpstreamWatermark("input", 42)

	_, up = dofn.UpstreamWatermark()
	if got, want := up, mtime.Time(42); got != want {
		t.Errorf("dofn.UpstreamWatermark() = %v, want %v", got, want)
	}

	flatten := makeStageState("flatten", []string{"a", "b", "c"}, []string{"output"}, nil)
	flatten.updateUpstreamWatermark("a", 50)
	flatten.updateUpstreamWatermark("b", 42)
	flatten.updateUpstreamWatermark("c", 101)
	_, up = flatten.UpstreamWatermark()
	if got, want := up, mtime.Time(42); got != want {
		t.Errorf("flatten.UpstreamWatermark() = %v, want %v", got, want)
	}
}

func TestStageState_updateWatermarks(t *testing.T) {
	inputCol := "testInput"
	outputCol := "testOutput"
	newState := func() (*stageState, *stageState, *ElementManager) {
		underTest := makeStageState("underTest", []string{inputCol}, []string{outputCol}, nil)
		outStage := makeStageState("outStage", []string{outputCol}, nil, nil)
		em := &ElementManager{
			consumers: map[string][]string{
				inputCol:  {underTest.ID},
				outputCol: {outStage.ID},
			},
			stages: map[string]*stageState{
				outStage.ID:  outStage,
				underTest.ID: underTest,
			},
		}
		return underTest, outStage, em
	}

	tests := []struct {
		name                                  string
		initInput, initOutput                 mtime.Time
		upstream, minPending, minStateHold    mtime.Time
		wantInput, wantOutput, wantDownstream mtime.Time
	}{
		{
			name:           "initialized",
			initInput:      mtime.MinTimestamp,
			initOutput:     mtime.MinTimestamp,
			upstream:       mtime.MinTimestamp,
			minPending:     mtime.EndOfGlobalWindowTime,
			minStateHold:   mtime.EndOfGlobalWindowTime,
			wantInput:      mtime.MinTimestamp, // match default
			wantOutput:     mtime.MinTimestamp, // match upstream
			wantDownstream: mtime.MinTimestamp, // match upstream
		}, {
			name:           "upstream",
			initInput:      mtime.MinTimestamp,
			initOutput:     mtime.MinTimestamp,
			upstream:       mtime.ZeroTimestamp,
			minPending:     mtime.EndOfGlobalWindowTime,
			minStateHold:   mtime.EndOfGlobalWindowTime,
			wantInput:      mtime.ZeroTimestamp, // match upstream
			wantOutput:     mtime.ZeroTimestamp, // match upstream
			wantDownstream: mtime.ZeroTimestamp, // match upstream
		}, {
			name:           "useMinPending",
			initInput:      mtime.MinTimestamp,
			initOutput:     mtime.MinTimestamp,
			upstream:       mtime.ZeroTimestamp,
			minPending:     -20,
			minStateHold:   mtime.EndOfGlobalWindowTime,
			wantInput:      -20, // match minPending
			wantOutput:     -20, // match minPending
			wantDownstream: -20, // match minPending
		}, {
			name:           "useStateHold",
			initInput:      mtime.MinTimestamp,
			initOutput:     mtime.MinTimestamp,
			upstream:       mtime.ZeroTimestamp,
			minPending:     -20,
			minStateHold:   -30,
			wantInput:      -20, // match minPending
			wantOutput:     -30, // match state hold
			wantDownstream: -30, // match state hold
		}, {
			name:           "noAdvance",
			initInput:      20,
			initOutput:     30,
			upstream:       mtime.MinTimestamp,
			wantInput:      20,                 // match original input
			wantOutput:     30,                 // match original output
			wantDownstream: mtime.MinTimestamp, // not propagated
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ss, outStage, em := newState()
			ss.input = test.initInput
			ss.output = test.initOutput
			ss.updateUpstreamWatermark(inputCol, test.upstream)
			ss.pending = append(ss.pending, element{timestamp: test.minPending})
			ss.watermarkHolds.Add(test.minStateHold, 1)
			ss.updateWatermarks(em)
			if got, want := ss.input, test.wantInput; got != want {
				pcol, up := ss.UpstreamWatermark()
				t.Errorf("ss.updateWatermarks(%v,%v); ss.input = %v, want %v (upstream %v %v)", test.minPending, test.minStateHold, got, want, pcol, up)
			}
			if got, want := ss.output, test.wantOutput; got != want {
				pcol, up := ss.UpstreamWatermark()
				t.Errorf("ss.updateWatermarks(%v,%v); ss.output = %v, want %v (upstream %v %v)", test.minPending, test.minStateHold, got, want, pcol, up)
			}
			_, up := outStage.UpstreamWatermark()
			if got, want := up, test.wantDownstream; got != want {
				t.Errorf("outStage.UpstreamWatermark() = %v, want %v", got, want)
			}
		})
	}

}

func TestElementManager(t *testing.T) {
	t.Run("impulse", func(t *testing.T) {
		ctx, cancelFn := context.WithCancelCause(context.Background())
		em := NewElementManager(Config{})
		em.AddStage("impulse", nil, []string{"output"}, nil)
		em.AddStage("dofn", []string{"output"}, nil, nil)

		em.Impulse("impulse")

		if got, want := em.stages["impulse"].OutputWatermark(), mtime.MaxTimestamp; got != want {
			t.Fatalf("impulse.OutputWatermark() = %v, want %v", got, want)
		}

		var i int
		ch := em.Bundles(ctx, cancelFn, func() string {
			defer func() { i++ }()
			return fmt.Sprintf("%v", i)
		})
		rb, ok := <-ch
		if !ok {
			t.Error("Bundles channel unexpectedly closed")
		}
		if got, want := rb.StageID, "dofn"; got != want {
			t.Errorf("stage to execute = %v, want %v", got, want)
		}
		em.PersistBundle(rb, nil, TentativeData{}, PColInfo{}, Residuals{})
		_, ok = <-ch
		if ok {
			t.Error("Bundles channel expected to be closed")
		}
		if got, want := i, 1; got != want {
			t.Errorf("got %v bundles, want %v", got, want)
		}
	})

	info := PColInfo{
		GlobalID: "generic_info", // GlobalID isn't used except for debugging.
		WDec:     exec.MakeWindowDecoder(coder.NewGlobalWindow()),
		WEnc:     exec.MakeWindowEncoder(coder.NewGlobalWindow()),
		EDec: func(r io.Reader) []byte {
			b, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("error decoding \"generic_info\" data:%v", err)
			}
			return b
		},
	}
	es := elements{
		es: []element{{
			window:    window.GlobalWindow{},
			timestamp: mtime.MinTimestamp,
			pane:      typex.NoFiringPane(),
			elmBytes:  []byte{3, 65, 66, 67}, // "ABC"
		}},
		minTimestamp: mtime.MinTimestamp,
	}

	t.Run("dofn", func(t *testing.T) {
		ctx, cancelFn := context.WithCancelCause(context.Background())
		em := NewElementManager(Config{})
		em.AddStage("impulse", nil, []string{"input"}, nil)
		em.AddStage("dofn1", []string{"input"}, []string{"output"}, nil)
		em.AddStage("dofn2", []string{"output"}, nil, nil)
		em.Impulse("impulse")

		var i int
		ch := em.Bundles(ctx, cancelFn, func() string {
			defer func() { i++ }()
			t.Log("generating bundle", i)
			return fmt.Sprintf("%v", i)
		})
		rb, ok := <-ch
		if !ok {
			t.Error("Bundles channel unexpectedly closed")
		}
		t.Log("received bundle", i)

		td := TentativeData{}
		for _, d := range es.ToData(info) {
			td.WriteData("output", d)
		}
		outputCoders := map[string]PColInfo{
			"output": info,
		}

		em.PersistBundle(rb, outputCoders, td, info, Residuals{})
		rb, ok = <-ch
		if !ok {
			t.Error("Bundles channel not expected to be closed")
		}
		// Check the data is what's expected:
		data := em.InputForBundle(rb, info)
		if got, want := len(data), 1; got != want {
			t.Errorf("data len = %v, want %v", got, want)
		}
		if !cmp.Equal([]byte{127, 223, 59, 100, 90, 28, 172, 9, 0, 0, 0, 1, 15, 3, 65, 66, 67}, data[0]) {
			t.Errorf("unexpected data, got %v", data[0])
		}
		em.PersistBundle(rb, outputCoders, TentativeData{}, info, Residuals{})
		rb, ok = <-ch
		if ok {
			t.Error("Bundles channel expected to be closed", rb)
		}

		if got, want := i, 2; got != want {
			t.Errorf("got %v bundles, want %v", got, want)
		}
	})

	t.Run("side", func(t *testing.T) {
		ctx, cancelFn := context.WithCancelCause(context.Background())
		em := NewElementManager(Config{})
		em.AddStage("impulse", nil, []string{"input"}, nil)
		em.AddStage("dofn1", []string{"input"}, []string{"output"}, nil)
		em.AddStage("dofn2", []string{"input"}, nil, []LinkID{{Transform: "dofn2", Global: "output", Local: "local"}})
		em.Impulse("impulse")

		var i int
		ch := em.Bundles(ctx, cancelFn, func() string {
			defer func() { i++ }()
			t.Log("generating bundle", i)
			return fmt.Sprintf("%v", i)
		})
		rb, ok := <-ch
		if !ok {
			t.Error("Bundles channel unexpectedly closed")
		}
		t.Log("received bundle", i)

		if got, want := rb.StageID, "dofn1"; got != want {
			t.Fatalf("stage to execute = %v, want %v", got, want)
		}

		td := TentativeData{}
		for _, d := range es.ToData(info) {
			td.WriteData("output", d)
		}
		outputCoders := map[string]PColInfo{
			"output":  info,
			"input":   info,
			"impulse": info,
		}

		em.PersistBundle(rb, outputCoders, td, info, Residuals{})
		rb, ok = <-ch
		if !ok {
			t.Fatal("Bundles channel not expected to be closed")
		}
		if got, want := rb.StageID, "dofn2"; got != want {
			t.Fatalf("stage to execute = %v, want %v", got, want)
		}
		em.PersistBundle(rb, outputCoders, TentativeData{}, info, Residuals{})
		rb, ok = <-ch
		if ok {
			t.Error("Bundles channel expected to be closed")
		}

		if got, want := i, 2; got != want {
			t.Errorf("got %v bundles, want %v", got, want)
		}
	})
	t.Run("residual", func(t *testing.T) {
		ctx, cancelFn := context.WithCancelCause(context.Background())
		em := NewElementManager(Config{})
		em.AddStage("impulse", nil, []string{"input"}, nil)
		em.AddStage("dofn", []string{"input"}, nil, nil)
		em.Impulse("impulse")

		var i int
		ch := em.Bundles(ctx, cancelFn, func() string {
			defer func() { i++ }()
			t.Log("generating bundle", i)
			return fmt.Sprintf("%v", i)
		})
		rb, ok := <-ch
		if !ok {
			t.Error("Bundles channel unexpectedly closed")
		}
		t.Log("received bundle", i)

		// Add a residual
		resid := es.ToData(info)
		residuals := Residuals{}
		for _, r := range resid {
			residuals.Data = append(residuals.Data, Residual{Element: r})
		}
		em.PersistBundle(rb, nil, TentativeData{}, info, residuals)
		rb, ok = <-ch
		if !ok {
			t.Error("Bundles channel not expected to be closed")
		}
		// Check the data is what's expected:
		data := em.InputForBundle(rb, info)
		if got, want := len(data), 1; got != want {
			t.Errorf("data len = %v, want %v", got, want)
		}
		if !cmp.Equal([]byte{127, 223, 59, 100, 90, 28, 172, 9, 0, 0, 0, 1, 15, 3, 65, 66, 67}, data[0]) {
			t.Errorf("unexpected data, got %v", data[0])
		}
		em.PersistBundle(rb, nil, TentativeData{}, info, Residuals{})
		rb, ok = <-ch
		if ok {
			t.Error("Bundles channel expected to be closed", rb)
		}

		if got, want := i, 2; got != want {
			t.Errorf("got %v bundles, want %v", got, want)
		}
	})
}

func TestElementManager_OnWindowExpiration(t *testing.T) {
	t.Run("createOnWindowExpirationBundles", func(t *testing.T) {
		// Unlike the other tests above, we synthesize the input configuration,
		em := NewElementManager(Config{})
		var instID uint64
		em.nextBundID = func() string {
			return fmt.Sprintf("inst%03d", atomic.AddUint64(&instID, 1))
		}
		em.AddStage("impulse", nil, []string{"input"}, nil)
		em.AddStage("dofn", []string{"input"}, nil, nil)
		onWE := StaticTimerID{
			TransformID: "dofn1",
			TimerFamily: "onWinExp",
		}
		em.StageOnWindowExpiration("dofn", onWE)
		em.Impulse("impulse")

		stage := em.stages["dofn"]
		stage.pendingByKeys = map[string]*dataAndTimers{}
		stage.inprogressKeys = set[string]{}

		validateInProgressExpiredWindows := func(t *testing.T, win typex.Window, want int) {
			t.Helper()
			if got := stage.inProgressExpiredWindows[win]; got != want {
				t.Errorf("stage.inProgressExpiredWindows[%v] = %v, want %v", win, got, want)
			}
		}
		validateSideBundles := func(t *testing.T, keys set[string]) {
			t.Helper()
			if len(em.injectedBundles) == 0 {
				t.Errorf("no injectedBundles exist when checking keys: %v", keys)
			}
			// Check that all keys are marked as in progress
			for k := range keys {
				if !stage.inprogressKeys.present(k) {
					t.Errorf("key %q not marked as in progress", k)
				}
			}

			bundleID := ""
		sideBundles:
			for _, rb := range em.injectedBundles {
				// find that a side channel bundle exists with these keys.
				bkeys := stage.inprogressKeysByBundle[rb.BundleID]
				if len(bkeys) != len(keys) {
					continue sideBundles
				}
				for k := range keys {
					if !bkeys.present(k) {
						continue sideBundles
					}
				}
				bundleID = rb.BundleID
				break
			}
			if bundleID == "" {
				t.Errorf("no bundle found with all the given keys: %v: bundles: %v keysByBundle: %v", keys, em.injectedBundles, stage.inprogressKeysByBundle)
			}
		}

		newOut := mtime.EndOfGlobalWindowTime
		// No windows exist, so no side channel bundles should be set.
		if got, want := stage.createOnWindowExpirationBundles(newOut, em), false; got != want {
			t.Errorf("createOnWindowExpirationBundles(%v) = %v, want %v", newOut, got, want)
		}
		// Validate that no side channel bundles were created.
		if got, want := len(stage.inProgressExpiredWindows), 0; got != want {
			t.Errorf("len(stage.inProgressExpiredWindows) = %v, want %v", got, want)
		}
		if got, want := len(em.injectedBundles), 0; got != want {
			t.Errorf("len(em.injectedBundles) = %v, want %v", got, want)
		}

		// Configure a few conditions to validate in the call.
		// Each window is in it's own bundle, all are in the same bundle.
		// Bundle 1
		expiredWindow1 := window.IntervalWindow{Start: 0, End: newOut - 1}

		akey := "\u0004key1"
		keys1 := singleSet(akey)
		stage.keysToExpireByWindow[expiredWindow1] = keys1
		// Bundle 2
		expiredWindow2 := window.IntervalWindow{Start: 1, End: newOut - 1}
		keys2 := singleSet("\u0004key2")
		keys2.insert("\u0004key3")
		keys2.insert("\u0004key4")
		stage.keysToExpireByWindow[expiredWindow2] = keys2

		// We should never see this key and window combination, as the window is
		// not yet expired.
		liveWindow := window.IntervalWindow{Start: 2, End: newOut + 1}
		stage.keysToExpireByWindow[liveWindow] = singleSet("\u0010keyNotSeen")

		if got, want := stage.createOnWindowExpirationBundles(newOut, em), true; got != want {
			t.Errorf("createOnWindowExpirationBundles(%v) = %v, want %v", newOut, got, want)
		}

		// We should only see 2 injectedBundles at this point.
		if got, want := len(em.injectedBundles), 2; got != want {
			t.Errorf("len(em.injectedBundles) = %v, want %v", got, want)
		}

		validateInProgressExpiredWindows(t, expiredWindow1, 1)
		validateInProgressExpiredWindows(t, expiredWindow2, 1)
		validateSideBundles(t, keys1)
		validateSideBundles(t, keys2)

		// Bundle 3
		expiredWindow3 := window.IntervalWindow{Start: 3, End: newOut - 1}
		keys3 := singleSet(akey)   // We shouldn't see this key, since it's in progress.
		keys3.insert("\u0004key5") // We should see this key since it isn't.
		stage.keysToExpireByWindow[expiredWindow3] = keys3

		if got, want := stage.createOnWindowExpirationBundles(newOut, em), true; got != want {
			t.Errorf("createOnWindowExpirationBundles(%v) = %v, want %v", newOut, got, want)
		}

		// We should see 3 injectedBundles at this point.
		if got, want := len(em.injectedBundles), 3; got != want {
			t.Errorf("len(em.injectedBundles) = %v, want %v", got, want)
		}

		validateInProgressExpiredWindows(t, expiredWindow1, 1)
		validateInProgressExpiredWindows(t, expiredWindow2, 1)
		validateInProgressExpiredWindows(t, expiredWindow3, 1)
		validateSideBundles(t, keys1)
		validateSideBundles(t, keys2)
		validateSideBundles(t, singleSet("\u0004key5"))

		// remove key1 from "inprogress keys", and the associated bundle.
		stage.inprogressKeys.remove(akey)
		delete(stage.inProgressExpiredWindows, expiredWindow1)
		for bundID, bkeys := range stage.inprogressKeysByBundle {
			if bkeys.present(akey) {
				t.Logf("bundID: %v, bkeys: %v, keyByBundle: %v", bundID, bkeys, stage.inprogressKeysByBundle)
				delete(stage.inprogressKeysByBundle, bundID)
				win := stage.expiryWindowsByBundles[bundID]
				delete(stage.expiryWindowsByBundles, bundID)
				if win != expiredWindow1 {
					t.Fatalf("Unexpected window: got %v, want %v", win, expiredWindow1)
				}
				break
			}
		}

		// Now we should get another bundle for expiredWindow3, and have none for expiredWindow1
		if got, want := stage.createOnWindowExpirationBundles(newOut, em), true; got != want {
			t.Errorf("createOnWindowExpirationBundles(%v) = %v, want %v", newOut, got, want)
		}

		validateInProgressExpiredWindows(t, expiredWindow1, 0)
		validateInProgressExpiredWindows(t, expiredWindow2, 1)
		validateInProgressExpiredWindows(t, expiredWindow3, 2)
		validateSideBundles(t, keys1) // Should still have this key present, but with a different bundle.
		validateSideBundles(t, keys2)
		validateSideBundles(t, singleSet("\u0004key5")) // still exist..
	})
}

func TestElementManager_AccumulatingTriggerPanes(t *testing.T) {
	type tsStep struct {
		watermark mtime.Time
		key       byte
		vals      []byte
		eventTime mtime.Time
	}
	type gotPane struct {
		Win    typex.Window
		Key    string
		Pane   typex.PaneInfo
		Values []int
	}

	readFixed := func(n int) func(io.Reader) []byte {
		return func(r io.Reader) []byte {
			buf := make([]byte, n)
			if _, err := io.ReadFull(r, buf); err != nil {
				t.Fatalf("readFixed(%d) failed: %v", n, err)
			}
			return buf
		}
	}
	tsInfo := PColInfo{
		GlobalID: "tsOut",
		WDec:     exec.MakeWindowDecoder(coder.NewGlobalWindow()),
		WEnc:     exec.MakeWindowEncoder(coder.NewGlobalWindow()),
		EDec:     readFixed(2),
		KeyDec:   readFixed(1),
	}
	winInfo := PColInfo{
		GlobalID: "winOut",
		WDec:     exec.MakeWindowDecoder(coder.NewIntervalWindow()),
		WEnc:     exec.MakeWindowEncoder(coder.NewIntervalWindow()),
		EDec:     readFixed(2),
		KeyDec:   readFixed(1),
	}
	aggInfo := PColInfo{
		GlobalID: "aggOut",
		WDec:     exec.MakeWindowDecoder(coder.NewIntervalWindow()),
		WEnc:     exec.MakeWindowEncoder(coder.NewIntervalWindow()),
		EDec: func(r io.Reader) []byte {
			hdr := make([]byte, 2)
			if _, err := io.ReadFull(r, hdr); err != nil {
				t.Fatalf("aggInfo header read failed: %v", err)
			}
			vals := make([]byte, int(hdr[1]))
			if _, err := io.ReadFull(r, vals); err != nil {
				t.Fatalf("aggInfo values read failed: %v", err)
			}
			return append(hdr, vals...)
		},
		KeyDec: readFixed(1),
	}
	coders := map[string]PColInfo{
		"tsOut":  tsInfo,
		"winOut": winInfo,
		"aggOut": aggInfo,
	}

	win0to10 := window.IntervalWindow{Start: 0, End: 10}

	tests := []struct {
		name  string
		strat WinStrat
		steps []tsStep
		want  []gotPane
	}{
		{
			name: "early_count1_empty_ontime_then_late_count1",
			strat: WinStrat{
				AllowedLateness: 60 * time.Millisecond,
				Accumulating:    true,
				Trigger: &TriggerAfterEndOfWindow{
					Early: &TriggerElementCount{ElementCount: 1},
					Late:  &TriggerElementCount{ElementCount: 1},
				},
			},
			steps: []tsStep{
				{key: 'k', vals: []byte{1}, eventTime: 1},
				{watermark: 20},
				{key: 'k', vals: []byte{2}, eventTime: 2},
			},
			want: []gotPane{
				{
					Win:    win0to10,
					Key:    "k",
					Pane:   typex.PaneInfo{Timing: typex.PaneEarly, Index: 0, NonSpeculativeIndex: -1, IsFirst: true, IsLast: false},
					Values: []int{1},
				},
				{
					Win:    win0to10,
					Key:    "k",
					Pane:   typex.PaneInfo{Timing: typex.PaneOnTime, Index: 1, NonSpeculativeIndex: 0, IsFirst: false, IsLast: false},
					Values: []int{1},
				},
				{
					Win:    win0to10,
					Key:    "k",
					Pane:   typex.PaneInfo{Timing: typex.PaneLate, Index: 2, NonSpeculativeIndex: 1, IsFirst: false, IsLast: false},
					Values: []int{1, 2},
				},
			},
		},
		{
			name: "early_count2_nonempty_ontime_then_late_count1",
			strat: WinStrat{
				AllowedLateness: 60 * time.Millisecond,
				Accumulating:    true,
				Trigger: &TriggerAfterEndOfWindow{
					Early: &TriggerElementCount{ElementCount: 2},
					Late:  &TriggerElementCount{ElementCount: 1},
				},
			},
			steps: []tsStep{
				{key: 'k', vals: []byte{1, 2}, eventTime: 1},
				{key: 'k', vals: []byte{3}, eventTime: 3},
				{watermark: 20},
				{key: 'k', vals: []byte{4}, eventTime: 4},
			},
			want: []gotPane{
				{
					Win:    win0to10,
					Key:    "k",
					Pane:   typex.PaneInfo{Timing: typex.PaneEarly, Index: 0, NonSpeculativeIndex: -1, IsFirst: true, IsLast: false},
					Values: []int{1, 2},
				},
				{
					Win:    win0to10,
					Key:    "k",
					Pane:   typex.PaneInfo{Timing: typex.PaneOnTime, Index: 1, NonSpeculativeIndex: 0, IsFirst: false, IsLast: false},
					Values: []int{1, 2, 3},
				},
				{
					Win:    win0to10,
					Key:    "k",
					Pane:   typex.PaneInfo{Timing: typex.PaneLate, Index: 2, NonSpeculativeIndex: 1, IsFirst: false, IsLast: false},
					Values: []int{1, 2, 3, 4},
				},
			},
		},
		{
			name: "untriggered_late_element_emits_closing_pane",
			strat: WinStrat{
				AllowedLateness: 60 * time.Millisecond,
				Accumulating:    true,
				Trigger: &TriggerAfterEndOfWindow{
					Early: &TriggerElementCount{ElementCount: 1},
					Late:  &TriggerElementCount{ElementCount: 2},
				},
			},
			steps: []tsStep{
				{key: 'k', vals: []byte{1}, eventTime: 1},
				{watermark: 20},
				{key: 'k', vals: []byte{2}, eventTime: 2},
			},
			want: []gotPane{
				{
					Win:    win0to10,
					Key:    "k",
					Pane:   typex.PaneInfo{Timing: typex.PaneEarly, Index: 0, NonSpeculativeIndex: -1, IsFirst: true, IsLast: false},
					Values: []int{1},
				},
				{
					Win:    win0to10,
					Key:    "k",
					Pane:   typex.PaneInfo{Timing: typex.PaneOnTime, Index: 1, NonSpeculativeIndex: 0, IsFirst: false, IsLast: false},
					Values: []int{1},
				},
				{
					Win:    win0to10,
					Key:    "k",
					Pane:   typex.PaneInfo{Timing: typex.PaneLate, Index: 2, NonSpeculativeIndex: 1, IsFirst: false, IsLast: true},
					Values: []int{1, 2},
				},
			},
		},
		{
			name: "repeated_count2_resets_partial_count_at_ontime",
			strat: WinStrat{
				AllowedLateness: 60 * time.Millisecond,
				Accumulating:    true,
				Trigger: &TriggerRepeatedly{
					Repeated: &TriggerElementCount{ElementCount: 2},
				},
			},
			steps: []tsStep{
				{key: 'k', vals: []byte{1, 2}, eventTime: 1},
				{key: 'k', vals: []byte{3}, eventTime: 3},
				{watermark: 20},
				{key: 'k', vals: []byte{4}, eventTime: 4},
				{key: 'k', vals: []byte{5}, eventTime: 5},
			},
			want: []gotPane{
				{
					Win:    win0to10,
					Key:    "k",
					Pane:   typex.PaneInfo{Timing: typex.PaneEarly, Index: 0, NonSpeculativeIndex: -1, IsFirst: true, IsLast: false},
					Values: []int{1, 2},
				},
				{
					Win:    win0to10,
					Key:    "k",
					Pane:   typex.PaneInfo{Timing: typex.PaneOnTime, Index: 1, NonSpeculativeIndex: 0, IsFirst: false, IsLast: false},
					Values: []int{1, 2, 3},
				},
				{
					Win:    win0to10,
					Key:    "k",
					Pane:   typex.PaneInfo{Timing: typex.PaneLate, Index: 2, NonSpeculativeIndex: 1, IsFirst: false, IsLast: false},
					Values: []int{1, 2, 3, 4, 5},
				},
			},
		},
		{
			name: "early_count2_without_late_finishes_at_ontime",
			strat: WinStrat{
				AllowedLateness: 60 * time.Millisecond,
				Accumulating:    true,
				Trigger: &TriggerAfterEndOfWindow{
					Early: &TriggerElementCount{ElementCount: 2},
				},
			},
			steps: []tsStep{
				{key: 'k', vals: []byte{1, 2}, eventTime: 1},
				{key: 'k', vals: []byte{3}, eventTime: 3},
				{watermark: 20},
				{key: 'k', vals: []byte{4}, eventTime: 4},
			},
			want: []gotPane{
				{
					Win:    win0to10,
					Key:    "k",
					Pane:   typex.PaneInfo{Timing: typex.PaneEarly, Index: 0, NonSpeculativeIndex: -1, IsFirst: true, IsLast: false},
					Values: []int{1, 2},
				},
				{
					Win:    win0to10,
					Key:    "k",
					Pane:   typex.PaneInfo{Timing: typex.PaneOnTime, Index: 1, NonSpeculativeIndex: 0, IsFirst: false, IsLast: true},
					Values: []int{1, 2, 3},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancelFn := context.WithCancelCause(context.Background())
			defer cancelFn(nil)

			em := NewElementManager(Config{StreamingMode: true})
			em.AddStage("ts", nil, []string{"tsOut"}, nil)
			em.RegisterPColInfo("tsOut", tsInfo)
			em.AddStage("assign", []string{"tsOut"}, []string{"winOut"}, nil)
			em.AddStage("agg", []string{"winOut"}, []string{"aggOut"}, nil)
			em.StageAggregates("agg", tc.strat)
			em.AddStage("sink", []string{"aggOut"}, nil, nil)

			tsb := em.AddTestStream("ts", map[string]string{"": "tsOut"})
			for _, st := range tc.steps {
				if len(st.vals) > 0 {
					var elms []TestStreamElement
					for _, v := range st.vals {
						elms = append(elms, TestStreamElement{
							Encoded:   []byte{st.key, v},
							EventTime: st.eventTime,
						})
					}
					tsb.AddElementEvent("", elms)
				} else {
					tsb.AddWatermarkEvent("", st.watermark)
				}
			}

			var bundSeq uint64
			ch := em.Bundles(ctx, cancelFn, func() string {
				return fmt.Sprintf("b%d", atomic.AddUint64(&bundSeq, 1))
			})

			var got []gotPane
			for rb := range ch {
				switch rb.StageID {
				case "assign":
					var td TentativeData
					for _, datum := range em.InputForBundle(rb, tsInfo) {
						buf := bytes.NewBuffer(datum)
						for {
							_, et, pn, err := exec.DecodeWindowedValueHeader(tsInfo.WDec, buf)
							if err == io.EOF {
								break
							}
							if err != nil {
								t.Fatalf("assign decode failed: %v", err)
							}
							kv := tsInfo.EDec(buf)
							start := (et / 10) * 10
							iw := window.IntervalWindow{Start: start, End: start + 10}
							var out bytes.Buffer
							if err := exec.EncodeWindowedValueHeader(winInfo.WEnc, []typex.Window{iw}, et, pn, &out); err != nil {
								t.Fatalf("assign encode failed: %v", err)
							}
							out.Write(kv)
							td.WriteData("winOut", out.Bytes())
						}
					}
					em.PersistBundle(rb, coders, td, tsInfo, Residuals{})

				case "agg":
					type kw struct {
						key byte
						win typex.Window
					}
					var order []kw
					grouped := map[kw][]byte{}
					for _, datum := range em.InputForBundle(rb, winInfo) {
						buf := bytes.NewBuffer(datum)
						for {
							ws, _, _, err := exec.DecodeWindowedValueHeader(winInfo.WDec, buf)
							if err == io.EOF {
								break
							}
							if err != nil {
								t.Fatalf("agg decode failed: %v", err)
							}
							kv := winInfo.EDec(buf)
							for _, w := range ws {
								id := kw{key: kv[0], win: w}
								if _, ok := grouped[id]; !ok {
									order = append(order, id)
								}
								grouped[id] = append(grouped[id], kv[1])
							}
						}
					}
					var td TentativeData
					for _, id := range order {
						vals := grouped[id]
						slices.Sort(vals)
						var out bytes.Buffer
						if err := exec.EncodeWindowedValueHeader(aggInfo.WEnc, []typex.Window{id.win}, id.win.MaxTimestamp(), typex.NoFiringPane(), &out); err != nil {
							t.Fatalf("agg encode failed: %v", err)
						}
						out.WriteByte(id.key)
						out.WriteByte(byte(len(vals)))
						out.Write(vals)
						td.WriteData("aggOut", out.Bytes())
					}
					em.PersistBundle(rb, coders, td, winInfo, Residuals{})

				case "sink":
					for _, datum := range em.InputForBundle(rb, aggInfo) {
						buf := bytes.NewBuffer(datum)
						for {
							ws, _, pn, err := exec.DecodeWindowedValueHeader(aggInfo.WDec, buf)
							if err == io.EOF {
								break
							}
							if err != nil {
								t.Fatalf("sink decode failed: %v", err)
							}
							raw := aggInfo.EDec(buf)
							vals := make([]int, len(raw[2:]))
							for i, b := range raw[2:] {
								vals[i] = int(b)
							}
							got = append(got, gotPane{
								Win:    ws[0],
								Key:    string(raw[:1]),
								Pane:   pn,
								Values: vals,
							})
						}
					}
					em.PersistBundle(rb, coders, TentativeData{}, aggInfo, Residuals{})
				}
			}

			if err := context.Cause(ctx); err != nil && err.Error() != "elementManager out of elements, cleaning up" {
				t.Fatalf("pipeline failed: %v", err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("panes mismatch (-want +got):\n%s", diff)
			}
			aggStage := em.stages["agg"]
			if gotIn := aggStage.InputWatermark(); gotIn != mtime.MaxTimestamp {
				t.Errorf("agg InputWatermark = %v, want %v", gotIn, mtime.MaxTimestamp)
			}
			if gotState := len(aggStage.state[LinkID{}]); gotState != 0 {
				t.Errorf("len(agg.state[LinkID{}]) = %d, want 0 after window expiry", gotState)
			}
			if gotOnTime := len(aggStage.pendingOnTime); gotOnTime != 0 {
				t.Errorf("len(agg.pendingOnTime) = %d, want 0", gotOnTime)
			}
		})
	}
}

func TestEmptyPendingOnTimeCleanup(t *testing.T) {
	ctx, cancelFn := context.WithTimeoutCause(context.Background(), 5*time.Second, fmt.Errorf("timed out waiting for empty pendingOnTime cleanup"))
	defer cancelFn()

	em := NewElementManager(Config{StreamingMode: true})
	em.AddStage("ts", nil, []string{"tsOut"}, nil)
	em.AddStage("agg", []string{"tsOut"}, []string{"aggOut"}, nil)
	em.StageAggregates("agg", WinStrat{
		AllowedLateness: 60 * time.Millisecond,
		Accumulating:    true,
		Trigger: &TriggerAfterEndOfWindow{
			Early: &TriggerElementCount{ElementCount: 1},
			Late:  &TriggerElementCount{ElementCount: 1},
		},
	})

	tsb := em.AddTestStream("ts", map[string]string{"": "tsOut"})
	tsb.AddWatermarkEvent("", 20)

	aggStage := em.stages["agg"]
	aggStage.mu.Lock()
	delta := aggStage.addPendingOnTimeLocked(window.IntervalWindow{Start: 0, End: 10}, "k")
	aggStage.mu.Unlock()
	em.addPending(delta)

	var bundSeq uint64
	ch := em.Bundles(ctx, func(err error) {
		t.Errorf("unexpected cancel: %v", err)
	}, func() string {
		return fmt.Sprintf("b%d", atomic.AddUint64(&bundSeq, 1))
	})

	for rb := range ch {
		t.Fatalf("unexpected bundle emitted: %+v", rb)
	}

	if err := context.Cause(ctx); err != nil && err.Error() == "timed out waiting for empty pendingOnTime cleanup" {
		t.Fatalf("Bundles() hung: %v", err)
	}
	if gotOut := aggStage.OutputWatermark(); gotOut != mtime.MaxTimestamp {
		t.Errorf("agg OutputWatermark = %v, want %v", gotOut, mtime.MaxTimestamp)
	}
	if gotOnTime := len(aggStage.pendingOnTime); gotOnTime != 0 {
		t.Errorf("len(agg.pendingOnTime) = %d, want 0", gotOnTime)
	}
}
