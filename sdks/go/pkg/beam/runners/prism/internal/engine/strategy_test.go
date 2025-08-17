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
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

func TestEarliestCompletion(t *testing.T) {
	tests := []struct {
		strat WinStrat
		input typex.Window
		want  mtime.Time
	}{
		{WinStrat{}, window.GlobalWindow{}, mtime.EndOfGlobalWindowTime},
		{WinStrat{}, window.IntervalWindow{Start: 0, End: 4}, 3},
		{WinStrat{}, window.IntervalWindow{Start: mtime.MinTimestamp, End: mtime.MaxTimestamp}, mtime.MaxTimestamp - 1},
		{WinStrat{AllowedLateness: 5 * time.Second}, window.GlobalWindow{}, mtime.EndOfGlobalWindowTime.Add(5 * time.Second)},
		{WinStrat{AllowedLateness: 5 * time.Millisecond}, window.IntervalWindow{Start: 0, End: 4}, 8},
		{WinStrat{AllowedLateness: 5 * time.Second}, window.IntervalWindow{Start: mtime.MinTimestamp, End: mtime.MaxTimestamp}, mtime.MaxTimestamp.Add(5 * time.Second)},
	}

	for _, test := range tests {
		if got, want := test.strat.EarliestCompletion(test.input), test.want; got != want {
			t.Errorf("%v.EarliestCompletion(%v)) = %v, want %v", test.strat, test.input, got, want)
		}
	}
}

func TestTriggers_isReady(t *testing.T) {
	type io struct {
		input      triggerInput
		shouldFire bool
	}
	tests := []struct {
		name   string
		trig   Trigger
		inputs []io
	}{
		{
			name: "never", trig: &TriggerNever{},
			inputs: []io{
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 2}, false},
				{triggerInput{newElementCount: 4}, false},
			},
		}, {
			name: "count[1]", trig: &TriggerElementCount{ElementCount: 1},
			inputs: []io{
				{triggerInput{newElementCount: 1}, true},  // First should fire.
				{triggerInput{newElementCount: 1}, false}, // Subsequent ones should not since the trigger is finished, and not reset.
				{triggerInput{newElementCount: 1}, false},
			},
		}, {
			name: "count[2]", trig: &TriggerElementCount{ElementCount: 2},
			inputs: []io{
				{triggerInput{newElementCount: 1}, false}, // Shouldn't fire because we havne't hit the threshold yet.
				{triggerInput{newElementCount: 1}, true},  // Should fire, because the count will hit the threshold.
				{triggerInput{newElementCount: 1}, false}, // Subsequent ones should not since the trigger is finished, and not reset.
				{triggerInput{newElementCount: 1}, false},
			},
		}, {
			name: "count[2]_jumpover", trig: &TriggerElementCount{ElementCount: 2},
			inputs: []io{
				{triggerInput{newElementCount: 1}, false}, // Shouldn't fire because we havne't hit the threshold yet.
				{triggerInput{newElementCount: 2}, true},  // Should fire, because the count will hit and pass the threshold.
				{triggerInput{newElementCount: 1}, false}, // Subsequent ones should not since the trigger is finished, and not reset.
				{triggerInput{newElementCount: 1}, false},
			},
		}, {
			name: "count[2]_repeated", trig: &TriggerRepeatedly{&TriggerElementCount{ElementCount: 2}},
			inputs: []io{
				{triggerInput{newElementCount: 1}, false}, // Shouldn't fire because we havne't hit the threshold yet.
				{triggerInput{newElementCount: 2}, true},  // Should fire, because the count will hit and pass the threshold.
				{triggerInput{newElementCount: 1}, false}, // Insufficient incrementing, so not fired.
				{triggerInput{newElementCount: 1}, true},  // Threshold hit, it should fire.
				{triggerInput{newElementCount: 2}, true},  // Automatically hit, it should fire.
			},
		}, {
			name: "always", trig: &TriggerAlways{}, // Equivalent to Repeat { ElementCount(1) }
			inputs: []io{
				{triggerInput{newElementCount: 1}, true},
				{triggerInput{newElementCount: 2}, true},
				{triggerInput{newElementCount: 4}, true},
			},
		}, {
			name: "afterEach_2_1_3",
			trig: &TriggerAfterEach{
				SubTriggers: []Trigger{
					&TriggerElementCount{2},
					&TriggerElementCount{1},
					&TriggerElementCount{3},
				},
			},
			inputs: []io{
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true}, // first is ready
				{triggerInput{newElementCount: 1}, true}, // second is ready
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true},  // third is ready
				{triggerInput{newElementCount: 1}, false}, // never resets after this.
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
			},
		}, {
			name: "afterAny_2_3_4",
			trig: &TriggerAfterAny{
				SubTriggers: []Trigger{
					&TriggerElementCount{2},
					&TriggerElementCount{3},
					&TriggerElementCount{4},
				},
			},
			inputs: []io{
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true},  // ElmCount 2 is ready
				{triggerInput{newElementCount: 1}, false}, // Should never fire again as a result.
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
			},
		}, {
			name: "afterAll_2_3_4",
			trig: &TriggerAfterAll{
				SubTriggers: []Trigger{
					&TriggerElementCount{2},
					&TriggerElementCount{3},
					&TriggerElementCount{4},
				},
			},
			inputs: []io{
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false}, // ElmCount 2 is ready
				{triggerInput{newElementCount: 1}, false}, // ElmCount 3 is ready.
				{triggerInput{newElementCount: 1}, true},  // ElmCount 4 is ready, so fire now.
				{triggerInput{newElementCount: 1}, false}, // Should never fire again as a result.
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
			},
		}, {
			name: "afterAll_afterEach_3_never",
			trig: &TriggerAfterAll{
				SubTriggers: []Trigger{
					&TriggerAfterEach{SubTriggers: []Trigger{&TriggerElementCount{3}, &TriggerNever{}}},
					&TriggerElementCount{5},
				},
			},
			inputs: []io{
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false}, // ElmCount 2 is ready
				{triggerInput{newElementCount: 1}, false}, // AfterEach(ElmCount 3) is ready,
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true},  // ElmCount 5 is ready, so fire now.
				{triggerInput{newElementCount: 1}, false}, // Should never fire again as a result.
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
			},
		}, {
			name: "afterEach_afterEach",
			trig: &TriggerAfterEach{
				SubTriggers: []Trigger{
					&TriggerAfterEach{SubTriggers: []Trigger{&TriggerElementCount{3}, &TriggerElementCount{1}}},
					&TriggerAfterEach{SubTriggers: []Trigger{&TriggerElementCount{3}, &TriggerElementCount{2}}},
				},
			},
			inputs: []io{
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true}, // ElmCount 3 is ready
				{triggerInput{newElementCount: 1}, true}, // ElmCount 1 is ready
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true}, // ElmCount 3 is ready
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true}, // ElmCount 2 is ready
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
			},
		}, {
			name: "orFinally_2_7",
			trig: &TriggerOrFinally{
				Main:    &TriggerElementCount{2},
				Finally: &TriggerElementCount{7},
			},
			inputs: []io{
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true}, // Main is ready
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true}, // Main is ready
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true},  // Main is ready
				{triggerInput{newElementCount: 1}, true},  // Finally is Ready
				{triggerInput{newElementCount: 1}, false}, // Should never fire again as a result.
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
			},
		}, {
			name: "orFinally_afterEach_2_1_7_afterEach_4_5",
			trig: &TriggerOrFinally{
				Main: &TriggerAfterEach{
					SubTriggers: []Trigger{&TriggerElementCount{2}, &TriggerElementCount{1}, &TriggerElementCount{7}},
				},
				Finally: &TriggerAfterEach{
					SubTriggers: []Trigger{&TriggerElementCount{4}, &TriggerElementCount{5}},
				},
			},
			inputs: []io{
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true},  // Main is ready
				{triggerInput{newElementCount: 1}, true},  // Main is ready
				{triggerInput{newElementCount: 1}, true},  // Finally is ready
				{triggerInput{newElementCount: 1}, false}, // Should never fire again as a result.
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
			},
		}, {
			name: "repeated_afterEach_2_1_3",
			trig: &TriggerRepeatedly{&TriggerAfterEach{
				SubTriggers: []Trigger{
					&TriggerElementCount{2},
					&TriggerElementCount{1},
					&TriggerElementCount{3},
				},
			}},
			inputs: []io{
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true}, // first is ready
				{triggerInput{newElementCount: 1}, true}, // second is ready
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true}, // third is ready
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true}, // first is ready again
				{triggerInput{newElementCount: 1}, true}, // second is ready again
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true}, // third is ready again
				{triggerInput{newElementCount: 1}, false},
			},
		}, {
			name: "repeated_afterAny_2_3_4",
			trig: &TriggerRepeatedly{&TriggerAfterAny{
				SubTriggers: []Trigger{
					&TriggerElementCount{2},
					&TriggerElementCount{3},
					&TriggerElementCount{4},
				},
			}},
			inputs: []io{
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true}, // ElmCount 2 is ready
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true}, // ElmCount 2 is ready again
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true}, // ElmCount 2 is ready again
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true}, // ElmCount 2 is ready again
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true}, // ElmCount 2 is ready again
			},
		}, {
			name: "repeated_afterAll_2_3_4",
			trig: &TriggerRepeatedly{&TriggerAfterAll{
				SubTriggers: []Trigger{
					&TriggerElementCount{2},
					&TriggerElementCount{3},
					&TriggerElementCount{4},
				},
			}},
			inputs: []io{
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false}, // ElmCount 2 is ready
				{triggerInput{newElementCount: 1}, false}, // ElmCount 3 is ready.
				{triggerInput{newElementCount: 1}, true},  // ElmCount 4 is ready, so fire now.
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true}, // all ready again.
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
			},
		}, {
			name: "repeated_orFinally_2_7",
			trig: &TriggerRepeatedly{&TriggerOrFinally{
				Main:    &TriggerElementCount{2},
				Finally: &TriggerElementCount{7},
			}},
			inputs: []io{
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true}, // Main is ready
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true}, // Main is ready
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true}, // Main is ready
				{triggerInput{newElementCount: 1}, true}, // Finally is Ready
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true}, // Main is ready
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true}, // Main is ready
			},
		}, {
			name: "afterEndOfWindow_Early2",
			trig: &TriggerAfterEndOfWindow{
				Early: &TriggerElementCount{2},
			},
			inputs: []io{
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true}, // Early is ready
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true},                            // Early is ready
				{triggerInput{newElementCount: 1, endOfWindowReached: true}, false}, // End of window
				{triggerInput{newElementCount: 1, endOfWindowReached: true}, false},
				{triggerInput{newElementCount: 1, endOfWindowReached: true}, false},
				{triggerInput{newElementCount: 1, endOfWindowReached: true}, false},
			},
		}, {
			name: "afterEndOfWindow_EarlyNever_Late2",
			trig: &TriggerAfterEndOfWindow{
				Early: &TriggerNever{},
				Late:  &TriggerElementCount{2},
			},
			inputs: []io{
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1, endOfWindowReached: true}, false}, // End of window
				{triggerInput{newElementCount: 1, endOfWindowReached: true}, true},  // Late
				{triggerInput{newElementCount: 1, endOfWindowReached: true}, false},
				{triggerInput{newElementCount: 1, endOfWindowReached: true}, true}, // Late
				{triggerInput{newElementCount: 1, endOfWindowReached: true}, false},
				{triggerInput{newElementCount: 1, endOfWindowReached: true}, true}, // Late
			},
		}, {
			name: "afterEndOfWindow_NeitherSet",
			trig: &TriggerAfterEndOfWindow{},
			inputs: []io{
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1, endOfWindowReached: true}, false}, // End of window
				{triggerInput{newElementCount: 1, endOfWindowReached: true}, false}, // Late
				{triggerInput{newElementCount: 1, endOfWindowReached: true}, false},
				{triggerInput{newElementCount: 1, endOfWindowReached: true}, false}, // Late
				{triggerInput{newElementCount: 1, endOfWindowReached: true}, false},
				{triggerInput{newElementCount: 1, endOfWindowReached: true}, false}, // Late
			},
		}, {
			name: "afterEndOfWindow_EarlyUnset_Late2",
			trig: &TriggerAfterEndOfWindow{
				Late: &TriggerElementCount{2},
			},
			inputs: []io{
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1, endOfWindowReached: true}, false}, // End of window
				{triggerInput{newElementCount: 1, endOfWindowReached: true}, true},  // Late
				{triggerInput{newElementCount: 1, endOfWindowReached: true}, false},
				{triggerInput{newElementCount: 1, endOfWindowReached: true}, true}, // Late
				{triggerInput{newElementCount: 1, endOfWindowReached: true}, false},
				{triggerInput{newElementCount: 1, endOfWindowReached: true}, true}, // Late
			},
		}, {
			name: "default",
			trig: &TriggerDefault{},
			inputs: []io{
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1, endOfWindowReached: true}, true}, // End of window
				{triggerInput{newElementCount: 2, endOfWindowReached: true}, true},
				{triggerInput{newElementCount: 3, endOfWindowReached: true}, true},
				{triggerInput{newElementCount: 4, endOfWindowReached: true}, true},
				{triggerInput{newElementCount: 5, endOfWindowReached: true}, true},
				{triggerInput{newElementCount: 6, endOfWindowReached: true}, true},
				{triggerInput{newElementCount: 7, endOfWindowReached: true}, true},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := StateData{}
			for i, in := range test.inputs {
				ws := WinStrat{Trigger: test.trig}
				if got, want := ws.IsTriggerReady(in.input, &state), in.shouldFire; got != want {
					t.Errorf("%v[%d]: %#v.isReady(%+v)) = %v, want %v; state: %v", test.name, i, test.trig, in, got, want, state.Trigger)
				}
			}
		})
	}
}
