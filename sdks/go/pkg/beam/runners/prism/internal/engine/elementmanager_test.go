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
	"container/heap"
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

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
			Transform:   "dofn1",
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
			if len(em.sideChannelBundles) == 0 {
				t.Errorf("no sideChannelBundles exist when checking keys: %v", keys)
			}
			// Check that all keys are marked as in progress
			for k := range keys {
				if !stage.inprogressKeys.present(k) {
					t.Errorf("key %q not marked as in progress", k)
				}
			}

			bundleID := ""
		sideBundles:
			for _, rb := range em.sideChannelBundles {
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
				t.Errorf("no bundle found with all the given keys: %v: bundles: %v keysByBundle: %v", keys, em.sideChannelBundles, stage.inprogressKeysByBundle)
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
		if got, want := len(em.sideChannelBundles), 0; got != want {
			t.Errorf("len(em.sideChannelBundles) = %v, want %v", got, want)
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

		// We should only see 2 sideChannelBundles at this point.
		if got, want := len(em.sideChannelBundles), 2; got != want {
			t.Errorf("len(em.sideChannelBundles) = %v, want %v", got, want)
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

		// We should see 3 sideChannelBundles at this point.
		if got, want := len(em.sideChannelBundles), 3; got != want {
			t.Errorf("len(em.sideChannelBundles) = %v, want %v", got, want)
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
