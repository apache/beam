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

package internal

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window/trigger"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/jobopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/jobservices"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/universal"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/teststream"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func init() {
	register.Function4x0(formatAccumPaneFn)
	register.Iter1[int64]()
	register.Emitter1[string]()
}

func formatAccumPaneFn(pn beam.PaneInfo, _ int, iter func(*int64) bool, emit func(string)) {
	var vals []int64
	var v int64
	for iter(&v) {
		vals = append(vals, v)
	}
	slices.Sort(vals)
	emit(fmt.Sprintf("timing=%s index=%d nonSpec=%d first=%t last=%t vals=%v",
		pn.Timing, pn.Index, pn.NonSpeculativeIndex, pn.IsFirst, pn.IsLast, vals))
}

func runLocalTestPipeline(t *testing.T, p *beam.Pipeline, runFn func(*jobservices.Job)) {
	t.Helper()
	prevEndpoint := *jobopts.Endpoint
	prevEnv := *jobopts.EnvironmentType
	prevBin := *jobopts.WorkerBinary
	prevJob := *jobopts.JobName

	s := jobservices.NewServer(0, runFn)
	go s.Serve()

	f, err := os.CreateTemp("", "prism-worker-dummy")
	if err != nil {
		s.Stop()
		t.Fatal(err)
	}
	_ = f.Close()

	*jobopts.Endpoint = s.Endpoint()
	*jobopts.EnvironmentType = "loopback"
	*jobopts.WorkerBinary = f.Name()
	*jobopts.JobName = strings.ReplaceAll(strings.ToLower(t.Name()), "/", "-")

	t.Cleanup(func() {
		*jobopts.Endpoint = prevEndpoint
		*jobopts.EnvironmentType = prevEnv
		*jobopts.WorkerBinary = prevBin
		*jobopts.JobName = prevJob
		os.Remove(f.Name())
		s.Stop()
	})

	if _, err := universal.Execute(context.Background(), p); err != nil {
		t.Fatalf("pipeline execution failed: %v", err)
	}
}

func TestTriggerAccumulating(t *testing.T) {
	t.Run("early_count1_empty_ontime_late_count1", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		con := teststream.NewConfig()
		if err := con.AddElements(1000, int64(1)); err != nil {
			t.Fatal(err)
		}
		if err := con.AdvanceWatermark(20000); err != nil {
			t.Fatal(err)
		}
		if err := con.AddElements(2000, int64(2)); err != nil {
			t.Fatal(err)
		}
		if err := con.AdvanceWatermarkToInfinity(); err != nil {
			t.Fatal(err)
		}

		col := teststream.Create(s, con)
		keyed := beam.AddFixedKey(s, col)
		trig := trigger.AfterEndOfWindow().
			EarlyFiring(trigger.AfterCount(1)).
			LateFiring(trigger.AfterCount(1))
		windowed := beam.WindowInto(s, window.NewFixedWindows(10*time.Second), keyed,
			beam.Trigger(trig),
			beam.PanesAccumulate(),
			beam.AllowedLateness(60*time.Second),
		)
		grouped := beam.GroupByKey(s, windowed)
		panes := beam.ParDo(s, formatAccumPaneFn, grouped)
		globalPanes := beam.WindowInto(s, window.NewGlobalWindows(), panes)
		passert.Equals(s, globalPanes,
			"timing=early index=0 nonSpec=-1 first=true last=false vals=[1]",
			"timing=ontime index=1 nonSpec=0 first=false last=false vals=[1]",
			"timing=late index=2 nonSpec=1 first=false last=false vals=[1 2]",
		)

		runLocalTestPipeline(t, p, RunPipeline)
	})

	t.Run("early_count2_nonempty_ontime_late_count1", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		con := teststream.NewConfig()
		if err := con.AddElements(1000, int64(1), int64(2)); err != nil {
			t.Fatal(err)
		}
		if err := con.AddElements(3000, int64(3)); err != nil {
			t.Fatal(err)
		}
		if err := con.AdvanceWatermark(20000); err != nil {
			t.Fatal(err)
		}
		if err := con.AddElements(4000, int64(4)); err != nil {
			t.Fatal(err)
		}
		if err := con.AdvanceWatermarkToInfinity(); err != nil {
			t.Fatal(err)
		}

		col := teststream.Create(s, con)
		keyed := beam.AddFixedKey(s, col)
		trig := trigger.AfterEndOfWindow().
			EarlyFiring(trigger.AfterCount(2)).
			LateFiring(trigger.AfterCount(1))
		windowed := beam.WindowInto(s, window.NewFixedWindows(10*time.Second), keyed,
			beam.Trigger(trig),
			beam.PanesAccumulate(),
			beam.AllowedLateness(60*time.Second),
		)
		grouped := beam.GroupByKey(s, windowed)
		panes := beam.ParDo(s, formatAccumPaneFn, grouped)
		globalPanes := beam.WindowInto(s, window.NewGlobalWindows(), panes)
		passert.Equals(s, globalPanes,
			"timing=early index=0 nonSpec=-1 first=true last=false vals=[1 2]",
			"timing=ontime index=1 nonSpec=0 first=false last=false vals=[1 2 3]",
			"timing=late index=2 nonSpec=1 first=false last=false vals=[1 2 3 4]",
		)

		runLocalTestPipeline(t, p, RunPipeline)
	})

	t.Run("early_count2_nonempty_ontime_late_count2", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		con := teststream.NewConfig()
		if err := con.AddElements(1000, int64(1), int64(2)); err != nil {
			t.Fatal(err)
		}
		if err := con.AddElements(3000, int64(3)); err != nil {
			t.Fatal(err)
		}
		if err := con.AdvanceWatermark(20000); err != nil {
			t.Fatal(err)
		}
		if err := con.AddElements(4000, int64(4)); err != nil {
			t.Fatal(err)
		}
		if err := con.AddElements(5000, int64(5)); err != nil {
			t.Fatal(err)
		}
		if err := con.AdvanceWatermarkToInfinity(); err != nil {
			t.Fatal(err)
		}

		col := teststream.Create(s, con)
		keyed := beam.AddFixedKey(s, col)
		trig := trigger.AfterEndOfWindow().
			EarlyFiring(trigger.AfterCount(2)).
			LateFiring(trigger.AfterCount(2))
		windowed := beam.WindowInto(s, window.NewFixedWindows(10*time.Second), keyed,
			beam.Trigger(trig),
			beam.PanesAccumulate(),
			beam.AllowedLateness(60*time.Second),
		)
		grouped := beam.GroupByKey(s, windowed)
		panes := beam.ParDo(s, formatAccumPaneFn, grouped)
		globalPanes := beam.WindowInto(s, window.NewGlobalWindows(), panes)
		passert.Equals(s, globalPanes,
			"timing=early index=0 nonSpec=-1 first=true last=false vals=[1 2]",
			"timing=ontime index=1 nonSpec=0 first=false last=false vals=[1 2 3]",
			"timing=late index=2 nonSpec=1 first=false last=false vals=[1 2 3 4 5]",
		)

		runLocalTestPipeline(t, p, RunPipeline)
	})

	t.Run("repeated_count2_resets_partial_count_at_ontime", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		con := teststream.NewConfig()
		if err := con.AddElements(1000, int64(1), int64(2)); err != nil {
			t.Fatal(err)
		}
		if err := con.AddElements(3000, int64(3)); err != nil {
			t.Fatal(err)
		}
		if err := con.AdvanceWatermark(20000); err != nil {
			t.Fatal(err)
		}
		if err := con.AddElements(4000, int64(4)); err != nil {
			t.Fatal(err)
		}
		if err := con.AddElements(5000, int64(5)); err != nil {
			t.Fatal(err)
		}
		if err := con.AdvanceWatermarkToInfinity(); err != nil {
			t.Fatal(err)
		}

		col := teststream.Create(s, con)
		keyed := beam.AddFixedKey(s, col)
		trig := trigger.Repeat(trigger.AfterCount(2))
		windowed := beam.WindowInto(s, window.NewFixedWindows(10*time.Second), keyed,
			beam.Trigger(trig),
			beam.PanesAccumulate(),
			beam.AllowedLateness(60*time.Second),
		)
		grouped := beam.GroupByKey(s, windowed)
		panes := beam.ParDo(s, formatAccumPaneFn, grouped)
		globalPanes := beam.WindowInto(s, window.NewGlobalWindows(), panes)
		passert.Equals(s, globalPanes,
			"timing=early index=0 nonSpec=-1 first=true last=false vals=[1 2]",
			"timing=ontime index=1 nonSpec=0 first=false last=false vals=[1 2 3]",
			"timing=late index=2 nonSpec=1 first=false last=false vals=[1 2 3 4 5]",
		)

		runLocalTestPipeline(t, p, RunPipeline)
	})

	t.Run("closing_emit_always_emits_final_pane_without_new_elements", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		con := teststream.NewConfig()
		if err := con.AddElements(1000, int64(1), int64(2), int64(3)); err != nil {
			t.Fatal(err)
		}
		if err := con.AdvanceWatermark(20000); err != nil {
			t.Fatal(err)
		}
		if err := con.AddElements(2000, int64(4)); err != nil {
			t.Fatal(err)
		}
		if err := con.AddElements(3000, int64(5)); err != nil {
			t.Fatal(err)
		}
		if err := con.AdvanceWatermark(80000); err != nil {
			t.Fatal(err)
		}
		if err := con.AdvanceWatermarkToInfinity(); err != nil {
			t.Fatal(err)
		}

		col := teststream.Create(s, con)
		keyed := beam.AddFixedKey(s, col)
		trig := trigger.AfterEndOfWindow().
			EarlyFiring(trigger.AfterProcessingTime().PlusDelay(5 * time.Minute)).
			LateFiring(trigger.AfterCount(1))
		windowed := beam.WindowInto(s, window.NewFixedWindows(10*time.Second), keyed,
			beam.Trigger(trig),
			beam.PanesAccumulate(),
			beam.AllowedLateness(60*time.Second),
		)
		grouped := beam.GroupByKey(s, windowed)
		panes := beam.ParDo(s, formatAccumPaneFn, grouped)
		globalPanes := beam.WindowInto(s, window.NewGlobalWindows(), panes)
		passert.Equals(s, globalPanes,
			"timing=ontime index=0 nonSpec=0 first=true last=false vals=[1 2 3]",
			"timing=late index=1 nonSpec=1 first=false last=false vals=[1 2 3 4]",
			"timing=late index=2 nonSpec=2 first=false last=false vals=[1 2 3 4 5]",
			"timing=late index=3 nonSpec=3 first=false last=true vals=[1 2 3 4 5]",
		)

		runLocalTestPipeline(t, p, runWithClosingBehavior(pipepb.ClosingBehavior_EMIT_ALWAYS))
	})
}

func runWithClosingBehavior(mode pipepb.ClosingBehavior_Enum) func(*jobservices.Job) {
	return func(j *jobservices.Job) {
		for _, ws := range j.Pipeline.GetComponents().GetWindowingStrategies() {
			if ws.GetAccumulationMode() == pipepb.AccumulationMode_ACCUMULATING {
				ws.ClosingBehavior = mode
			}
		}
		RunPipeline(j)
	}
}

func TestHandleReshuffle(t *testing.T) {
	h := &runner{
		config: RunnerCharacteristic{
			SDKReshuffle: false,
		},
	}

	comps := &pipepb.Components{
		Transforms: map[string]*pipepb.PTransform{
			"reshuffle": {
				UniqueName: "reshuffle",
				Inputs: map[string]string{
					"in": "pcol_in",
				},
				Outputs: map[string]string{
					"out": "pcol_out",
				},
				Subtransforms: []string{"sub_1"},
			},
			"sub_1": {
				UniqueName:    "sub_1",
				Subtransforms: []string{"sub_2"},
			},
			"sub_2": {
				UniqueName: "sub_2",
			},
			"consumer": {
				UniqueName: "consumer",
				Inputs: map[string]string{
					"in": "pcol_out",
				},
			},
		},
	}

	got := h.handleReshuffle("reshuffle", comps.GetTransforms()["reshuffle"], comps)

	want := prepareResult{
		SubbedComps:   nil,
		RemovedLeaves: []string{"sub_1", "sub_2"},
		ForcedRoots:   []string{"consumer"},
	}

	if d := cmp.Diff(want, got, protocmp.Transform()); d != "" {
		t.Errorf("handleReshuffle() diff (-want, +got):\n%v", d)
	}

	// Verify that the consumer's input has been remapped to the input of the reshuffle
	gotInput := comps.GetTransforms()["consumer"].GetInputs()["in"]
	if gotInput != "pcol_in" {
		t.Errorf("consumer input got %q, want %q", gotInput, "pcol_in")
	}
}
