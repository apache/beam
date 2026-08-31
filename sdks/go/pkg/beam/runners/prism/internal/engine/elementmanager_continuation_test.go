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
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

// bundleBudget bounds how many bundles a continuation test drives. The source
// never terminates, so these tests stop as soon as the consumer is scheduled and
// use the budget only to declare starvation.
const bundleBudget = 50

// continuationInfo is a global window PColInfo, keyed for stateful consumers.
func continuationInfo(t *testing.T, keyed bool) PColInfo {
	t.Helper()
	readAll := func(r io.Reader) []byte {
		b, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("error decoding element: %v", err)
		}
		return b
	}
	info := PColInfo{
		GlobalID: "continuation_info",
		WDec:     exec.MakeWindowDecoder(coder.NewGlobalWindow()),
		WEnc:     exec.MakeWindowEncoder(coder.NewGlobalWindow()),
		EDec:     readAll,
	}
	if keyed {
		info.KeyDec = readAll
	}
	return info
}

// encodeElement produces a global window element at the given event time.
func encodeElement(t *testing.T, info PColInfo, et mtime.Time) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := exec.EncodeWindowedValueHeader(info.WEnc, []typex.Window{window.GlobalWindow{}}, et, typex.NoFiringPane(), &buf); err != nil {
		t.Fatalf("EncodeWindowedValueHeader: %v", err)
	}
	buf.Write([]byte{3, 65, 66, 67}) // "ABC"
	return buf.Bytes()
}

// TestPersistBundle_ContinuationResidualConsumers covers issue #39446: a source
// whose residual pins its watermark must still get its consumers scheduled.
func TestPersistBundle_ContinuationResidualConsumers(t *testing.T) {
	for _, test := range []struct {
		name     string
		keyed    bool
		stateful bool
	}{
		{name: "ordinary consumer"},
		{name: "stateful consumer", keyed: true, stateful: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			srcInfo := continuationInfo(t, false)
			outInfo := continuationInfo(t, test.keyed)

			ctx, cancelFn := context.WithCancelCause(context.Background())
			defer cancelFn(nil)

			em := NewElementManager(Config{})
			em.AddStage("impulse", nil, []string{"src_in"}, nil)
			em.AddStage("src", []string{"src_in"}, []string{"sink_in"}, nil)
			em.AddStage("sink", []string{"sink_in"}, nil, nil)
			if test.stateful {
				em.StageStateful("sink", nil)
			}
			em.Impulse("impulse")

			var i int
			ch := em.Bundles(ctx, cancelFn, func() string {
				defer func() { i++ }()
				return fmt.Sprintf("%v", i)
			})

			src := em.stages["src"]

			// The source emits a record and self checkpoints every round, so it
			// always has a residual outstanding and never terminates.
			var srcBundles, sinkBundles int
			for b := 0; b < bundleBudget && sinkBundles == 0; b++ {
				rb, ok := <-ch
				if !ok {
					t.Fatalf("bundle %d: bundles channel closed early", b)
				}
				switch rb.StageID {
				case "sink":
					sinkBundles++
					em.PersistBundle(rb, nil, TentativeData{}, outInfo, Residuals{})
				case "src":
					srcBundles++
					td := TentativeData{}
					td.WriteData("sink_in", encodeElement(t, outInfo, mtime.Time(100*srcBundles)))
					// No reported estimate means MIN_TIMESTAMP, so only the
					// arriving data can drive the consumer.
					em.PersistBundle(rb, map[string]PColInfo{"sink_in": outInfo}, td, srcInfo, Residuals{
						TransformID: "src",
						InputID:     "i0",
						Data:        []Residual{{Element: encodeElement(t, srcInfo, mtime.MinTimestamp)}},
					})
				default:
					t.Fatalf("bundle %d: unexpected stage %v", b, rb.StageID)
				}
			}

			if sinkBundles == 0 {
				t.Errorf("consumer stage was never scheduled across %d source bundles, src output watermark = %v; its pending elements are starved",
					srcBundles, src.OutputWatermark())
			}
		})
	}
}

// TestPersistBundle_ContinuationResidualTransitive covers a consumer two stages
// below the source. The middle stage returns no residual of its own, yet its
// watermark is still pinned by the source's.
func TestPersistBundle_ContinuationResidualTransitive(t *testing.T) {
	info := continuationInfo(t, false)
	ctx, cancelFn := context.WithCancelCause(context.Background())
	defer cancelFn(nil)

	em := NewElementManager(Config{})
	em.AddStage("impulse", nil, []string{"src_in"}, nil)
	em.AddStage("src", []string{"src_in"}, []string{"mid_in"}, nil)
	em.AddStage("mid", []string{"mid_in"}, []string{"sink_in"}, nil)
	em.AddStage("sink", []string{"sink_in"}, nil, nil)
	em.Impulse("impulse")

	var i int
	ch := em.Bundles(ctx, cancelFn, func() string {
		defer func() { i++ }()
		return fmt.Sprintf("%v", i)
	})

	var srcBundles, midBundles, sinkBundles int
	for b := 0; b < bundleBudget && sinkBundles == 0; b++ {
		rb, ok := <-ch
		if !ok {
			t.Fatalf("bundle %d: bundles channel closed early", b)
		}
		switch rb.StageID {
		case "src":
			srcBundles++
			td := TentativeData{}
			td.WriteData("mid_in", encodeElement(t, info, mtime.Time(100*srcBundles)))
			em.PersistBundle(rb, map[string]PColInfo{"mid_in": info}, td, info, Residuals{
				TransformID: "src",
				InputID:     "i0",
				Data:        []Residual{{Element: encodeElement(t, info, mtime.MinTimestamp)}},
			})
		case "mid":
			midBundles++
			td := TentativeData{}
			td.WriteData("sink_in", encodeElement(t, info, mtime.Time(100*midBundles)))
			em.PersistBundle(rb, map[string]PColInfo{"sink_in": info}, td, info, Residuals{})
		case "sink":
			sinkBundles++
			em.PersistBundle(rb, nil, TentativeData{}, info, Residuals{})
		default:
			t.Fatalf("bundle %d: unexpected stage %v", b, rb.StageID)
		}
	}

	if midBundles == 0 {
		t.Error("middle stage was never scheduled")
	}
	if sinkBundles == 0 {
		t.Errorf("stage two below the source was never scheduled across %d source and %d middle bundles; its pending elements are starved",
			srcBundles, midBundles)
	}
}

// TestPersistBundle_ContinuationResidualWatermark pins the BundleApplication
// output_watermarks contract: an unreported estimate means MIN_TIMESTAMP.
func TestPersistBundle_ContinuationResidualWatermark(t *testing.T) {
	for _, test := range []struct {
		name     string
		report   bool
		wantHeld bool
	}{
		{name: "reported estimate advances the watermark", report: true},
		{name: "no estimate holds the watermark", wantHeld: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			info := continuationInfo(t, false)
			ctx, cancelFn := context.WithCancelCause(context.Background())
			defer cancelFn(nil)

			em := NewElementManager(Config{})
			em.AddStage("impulse", nil, []string{"src_in"}, nil)
			em.AddStage("src", []string{"src_in"}, []string{"sink_in"}, nil)
			em.AddStage("sink", []string{"sink_in"}, nil, nil)
			em.Impulse("impulse")

			var i int
			ch := em.Bundles(ctx, cancelFn, func() string {
				defer func() { i++ }()
				return fmt.Sprintf("%v", i)
			})

			src := em.stages["src"]

			for round := 0; round < 3; round++ {
				rb, ok := <-ch
				if !ok {
					t.Fatalf("round %d: bundles channel closed early", round)
				}
				residuals := Residuals{
					TransformID: "src",
					InputID:     "i0",
					Data:        []Residual{{Element: encodeElement(t, info, mtime.MinTimestamp)}},
				}
				if test.report {
					residuals.MinOutputWatermarks = map[string]mtime.Time{"sink_in": mtime.Time(1000 * (round + 1))}
				}
				em.PersistBundle(rb, nil, TentativeData{}, info, residuals)
			}

			got := src.OutputWatermark()
			if test.wantHeld && got != mtime.MinTimestamp {
				t.Errorf("src.OutputWatermark() = %v, want %v: an unreported estimate defaults to MIN_TIMESTAMP", got, mtime.MinTimestamp)
			}
			if !test.wantHeld && got == mtime.MinTimestamp {
				t.Errorf("src.OutputWatermark() = %v, want it to follow the reported estimate", got)
			}
		})
	}
}

// TestStatefulBuildEventTimeBundle_OneKeyPerBundle checks that a key holding
// only a timer above the watermark does not consume the single key slot, which
// would build an empty bundle and reschedule the stage on the same key forever.
func TestStatefulBuildEventTimeBundle_OneKeyPerBundle(t *testing.T) {
	OneKeyPerBundle = true
	t.Cleanup(func() { OneKeyPerBundle = false })

	// Key iteration order is randomized, so repeat until the timer key leads.
	for i := 0; i < 20; i++ {
		em := NewElementManager(Config{})
		ss := makeStageState("stateful", []string{"input"}, nil, nil)
		ss.kind = &statefulStageKind{}
		ss.AddPending(em, []element{{
			window:        window.GlobalWindow{},
			timestamp:     mtime.MaxTimestamp - 1,
			holdTimestamp: mtime.MaxTimestamp - 1,
			pane:          typex.NoFiringPane(),
			transform:     "stateful",
			family:        "timer",
			keyBytes:      []byte("timerkey"),
			sequence:      0,
		}, {
			window:    window.GlobalWindow{},
			timestamp: 10,
			pane:      typex.NoFiringPane(),
			elmBytes:  []byte{3, 65, 66, 67},
			keyBytes:  []byte("datakey"),
		}})

		toProcess, minTs, newKeys, _, _, _, _ := ss.kind.buildEventTimeBundle(ss, mtime.Time(100))
		if len(toProcess) == 0 {
			t.Fatalf("iteration %d: built an empty bundle while a data key was pending", i)
		}
		// The skipped timer key must not be marked in progress, nor hold the
		// bundle's minimum timestamp.
		if len(newKeys) != 1 || !newKeys.present("datakey") {
			t.Fatalf("iteration %d: newKeys = %v, want only datakey", i, newKeys)
		}
		if want := mtime.Time(10); minTs != want {
			t.Fatalf("iteration %d: minTs = %v, want %v", i, minTs, want)
		}
	}
}

// delaySetup builds an impulse -> src pipeline and returns it with the first
// source bundle already received, ready for the test to persist a residual.
func delaySetup(t *testing.T, cfg Config) (*ElementManager, <-chan RunBundle, *stageState, RunBundle, context.CancelCauseFunc) {
	t.Helper()
	ctx, cancelFn := context.WithCancelCause(context.Background())
	em := NewElementManager(cfg)
	em.AddStage("impulse", nil, []string{"src_in"}, nil)
	em.AddStage("src", []string{"src_in"}, nil, nil)
	em.Impulse("impulse")
	var i int
	ch := em.Bundles(ctx, cancelFn, func() string {
		defer func() { i++ }()
		return fmt.Sprintf("%v", i)
	})
	rb, ok := <-ch
	if !ok {
		t.Fatal("bundles channel closed before the first source bundle")
	}
	if rb.StageID != "src" {
		t.Fatalf("first bundle stage = %v, want src", rb.StageID)
	}
	return em, ch, em.stages["src"], rb, cancelFn
}

// TestPersistBundle_ResidualResumeDelay covers the SDK requested resume delay:
// a delayed residual is parked with a watermark hold and released through the
// processing time queue instead of returning to pending immediately.
func TestPersistBundle_ResidualResumeDelay(t *testing.T) {
	info := continuationInfo(t, false)
	residual := func(delay time.Duration) Residuals {
		return Residuals{
			TransformID: "src",
			InputID:     "i0",
			Data:        []Residual{{Element: encodeElement(t, info, mtime.MinTimestamp), Delay: delay}},
		}
	}

	t.Run("parks with hold and schedules", func(t *testing.T) {
		em, _, src, rb, cancelFn := delaySetup(t, Config{EnableRTC: true})
		defer cancelFn(nil)
		const delay = 5 * time.Second
		before := time.Now()
		em.PersistBundle(rb, nil, TentativeData{}, info, residual(delay))

		src.mu.Lock()
		pendingLen := len(src.pending)
		parked := 0
		for _, elems := range src.delayedResiduals {
			parked += len(elems)
		}
		minPending := src.minPendingTimestampLocked()
		src.mu.Unlock()
		if pendingLen != 0 {
			t.Errorf("delayed residual returned to pending immediately: len(pending) = %v, want 0", pendingLen)
		}
		if parked != 1 {
			t.Errorf("parked residuals = %v, want 1", parked)
		}
		if minPending != mtime.MinTimestamp {
			t.Errorf("parked residual does not pin the input watermark: minPending = %v, want %v", minPending, mtime.MinTimestamp)
		}
		em.refreshCond.L.Lock()
		fireAt, ok := em.processTimeEvents.Peek()
		em.refreshCond.L.Unlock()
		if !ok {
			t.Fatal("no processing time event scheduled for the delayed residual")
		}
		if until := fireAt.ToTime().Sub(before); until <= 0 || until > delay {
			t.Errorf("residual release scheduled %v after persisting, want within (0, %v]", until, delay)
		}
	})

	t.Run("releases only after the delay", func(t *testing.T) {
		em, ch, _, rb, cancelFn := delaySetup(t, Config{EnableRTC: true})
		defer cancelFn(nil)
		_ = em
		const delay = 300 * time.Millisecond
		start := time.Now()
		em.PersistBundle(rb, nil, TentativeData{}, info, residual(delay))
		select {
		case rb2, ok := <-ch:
			if !ok {
				t.Fatal("bundles channel closed before the delayed residual fired")
			}
			if rb2.StageID != "src" {
				t.Fatalf("resumed bundle stage = %v, want src", rb2.StageID)
			}
			if elapsed := time.Since(start); elapsed < 250*time.Millisecond {
				t.Errorf("delayed residual fired after %v, want at least ~%v", elapsed, delay)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("delayed residual never fired; the real-time wake-up is missing")
		}
	})

	t.Run("fast forwards without a real-time clock", func(t *testing.T) {
		em, ch, _, rb, cancelFn := delaySetup(t, Config{})
		defer cancelFn(nil)
		start := time.Now()
		em.PersistBundle(rb, nil, TentativeData{}, info, residual(5*time.Second))
		select {
		case rb2, ok := <-ch:
			if !ok {
				t.Fatal("bundles channel closed before the delayed residual fired")
			}
			if rb2.StageID != "src" {
				t.Fatalf("resumed bundle stage = %v, want src", rb2.StageID)
			}
			if elapsed := time.Since(start); elapsed >= 4*time.Second {
				t.Errorf("fast forward mode waited %v in real time for a synthetic delay", elapsed)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("delayed residual never fired in fast forward mode")
		}
	})
}
