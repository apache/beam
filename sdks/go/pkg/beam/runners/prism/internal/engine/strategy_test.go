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
			name: "afterEach_2_Always_1",
			trig: &TriggerAfterEach{
				SubTriggers: []Trigger{
					&TriggerElementCount{2},
					&TriggerAfterAny{SubTriggers: []Trigger{&TriggerAlways{}}},
					&TriggerElementCount{1},
				},
			},
			inputs: []io{
				{triggerInput{newElementCount: 1}, false},
				{triggerInput{newElementCount: 1}, true},  // first is ready
				{triggerInput{newElementCount: 1}, true},  // second is ready
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
			name: "afterProcessingTime_Delay_Exact",
			trig: &TriggerAfterProcessingTime{
				Transforms: []TimestampTransform{
					{Delay: 3 * time.Second},
				},
			},
			inputs: []io{
				{triggerInput{emNow: 0}, false}, // the trigger is set to fire at 3s after 0
				{triggerInput{emNow: 1000}, false},
				{triggerInput{emNow: 2000}, false},
				{triggerInput{emNow: 3000}, true}, // fire
				{triggerInput{emNow: 4000}, false},
				{triggerInput{emNow: 5000}, false},
				{triggerInput{emNow: 6000}, false},
				{triggerInput{emNow: 7000}, false},
			},
		}, {
			name: "afterProcessingTime_Delay_Late",
			trig: &TriggerAfterProcessingTime{
				Transforms: []TimestampTransform{
					{Delay: 3 * time.Second},
				},
			},
			inputs: []io{
				{triggerInput{emNow: 0}, false}, // the trigger is set to fire at 3s after 0
				{triggerInput{emNow: 1000}, false},
				{triggerInput{emNow: 2000}, false},
				{triggerInput{emNow: 3001}, true}, // fire a little after the preset time
				{triggerInput{emNow: 4000}, false},
			},
		}, {
			name: "afterProcessingTime_AlignToPeriodOnly",
			trig: &TriggerAfterProcessingTime{
				Transforms: []TimestampTransform{
					{AlignToPeriod: 5 * time.Second},
				},
			},
			inputs: []io{
				{triggerInput{emNow: 1500}, false}, // align 1.5s to 5s
				{triggerInput{emNow: 2000}, false},
				{triggerInput{emNow: 4999}, false},
				{triggerInput{emNow: 5000}, true}, // fire at 5
				{triggerInput{emNow: 5001}, false},
			},
		}, {
			name: "afterProcessingTime_AlignToPeriodAndOffset",
			trig: &TriggerAfterProcessingTime{
				Transforms: []TimestampTransform{
					{AlignToPeriod: 5 * time.Second, AlignToOffset: 200 * time.Millisecond},
				},
			},
			inputs: []io{
				{triggerInput{emNow: 1500}, false}, // align 1.5s to 5s plus an 0.2 offset
				{triggerInput{emNow: 2000}, false},
				{triggerInput{emNow: 5119}, false},
				{triggerInput{emNow: 5200}, true}, // fire at 5.2s
				{triggerInput{emNow: 5201}, false},
			},
		}, {
			name: "afterProcessingTime_TwoTransforms",
			trig: &TriggerAfterProcessingTime{
				Transforms: []TimestampTransform{
					{AlignToPeriod: 5 * time.Second, AlignToOffset: 200 * time.Millisecond},
					{Delay: 1 * time.Second},
				},
			},
			inputs: []io{
				{triggerInput{emNow: 1500}, false}, // align 1.5s to 5s plus an 0.2 offset and a 1s delay
				{triggerInput{emNow: 2000}, false},
				{triggerInput{emNow: 5119}, false},
				{triggerInput{emNow: 5200}, false},
				{triggerInput{emNow: 5201}, false},
				{triggerInput{emNow: 6119}, false},
				{triggerInput{emNow: 6200}, true}, // fire
				{triggerInput{emNow: 6201}, false},
			},
		}, {
			name: "afterProcessingTime_Repeated", trig: &TriggerRepeatedly{
				&TriggerAfterProcessingTime{
					Transforms: []TimestampTransform{
						{Delay: 3 * time.Second},
					}}},
			inputs: []io{
				{triggerInput{emNow: 0}, false},
				{triggerInput{emNow: 1000}, false},
				{triggerInput{emNow: 2000}, false},
				{triggerInput{emNow: 3000}, true}, // firing the first time, trigger set again
				{triggerInput{emNow: 4000}, false},
				{triggerInput{emNow: 5000}, false},
				{triggerInput{emNow: 6000}, true}, // firing the second time
			},
		}, {
			name: "afterProcessingTime_Repeated_AcrossWindows", trig: &TriggerRepeatedly{
				&TriggerAfterProcessingTime{
					Transforms: []TimestampTransform{
						{Delay: 3 * time.Second},
					}}},
			inputs: []io{
				{triggerInput{emNow: 0}, false},
				{triggerInput{emNow: 1000}, false},
				{triggerInput{emNow: 2000}, false},
				{triggerInput{emNow: 3000}, true}, // fire the first time, trigger is set again
				{triggerInput{emNow: 4000}, false},
				{triggerInput{emNow: 5000}, false},
				{triggerInput{emNow: 6000,
					endOfWindowReached: true}, true}, // fire the second time, reach end of window and start over
				{triggerInput{emNow: 7000}, false}, // trigger firing time is set to 7s + 3s = 10s
				{triggerInput{emNow: 8000}, false},
				{triggerInput{emNow: 9000}, false},
				{triggerInput{emNow: 10000}, true}, // fire in the new window
			},
		}, {
			name: "afterProcessingTime_Repeated_Composite", trig: &TriggerRepeatedly{
				&TriggerAfterAny{SubTriggers: []Trigger{
					&TriggerAfterProcessingTime{
						Transforms: []TimestampTransform{
							{Delay: 3 * time.Second},
						},
					},
					&TriggerElementCount{ElementCount: 2},
				}}},
			inputs: []io{
				{triggerInput{emNow: 0, newElementCount: 1}, false},    // ElmCount = 1, set AfterProcessingTime trigger firing time to 3s
				{triggerInput{emNow: 1000, newElementCount: 1}, true},  // ElmCount = 2, fire ElmCount trigger and reset ElmCount and AfterProcessingTime firing time (4s)
				{triggerInput{emNow: 4000, newElementCount: 1}, true},  // ElmCount = 1, fire AfterProcessingTime trigger and reset ElmCount and AfterProcessingTime firing time (7s)
				{triggerInput{emNow: 5000, newElementCount: 1}, false}, // ElmCount = 1
				{triggerInput{emNow: 5500, newElementCount: 1}, true},  // ElmCount = 2, fire ElmCount trigger
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

func TestOnWatermarkFire(t *testing.T) {
	t.Run("nil_and_leaf_triggers", func(t *testing.T) {
		var state StateData
		(WinStrat{}).OnWatermarkFire(&state)
		(WinStrat{Trigger: &TriggerAlways{}}).OnWatermarkFire(&state)
		(WinStrat{Trigger: &TriggerNever{}}).OnWatermarkFire(&state)
		(WinStrat{Trigger: &TriggerAfterSynchronizedProcessingTime{}}).OnWatermarkFire(&state)

		def := &TriggerDefault{}
		if def.shouldFire(&state) {
			t.Fatal("TriggerDefault.shouldFire before elements = true, want false")
		}
		(WinStrat{Trigger: def}).OnWatermarkFire(&state)
		if !def.shouldFire(&state) {
			t.Fatal("TriggerDefault.shouldFire after OnWatermarkFire = false, want true")
		}

		cnt := &TriggerElementCount{ElementCount: 2}
		wsCnt := WinStrat{Trigger: cnt}
		if wsCnt.IsTriggerReady(triggerInput{newElementCount: 1}, &state) {
			t.Fatal("ElementCount(2) fired on 1 element")
		}
		wsCnt.OnWatermarkFire(&state)
		if wsCnt.IsTriggerReady(triggerInput{newElementCount: 1}, &state) {
			t.Fatal("ElementCount(2) fired on 1 element after OnWatermarkFire reset")
		}
		if !wsCnt.IsTriggerReady(triggerInput{newElementCount: 1}, &state) {
			t.Fatal("ElementCount(2) did not fire on second element")
		}
		wsCnt.OnWatermarkFire(&state)

		pt := &TriggerAfterProcessingTime{Transforms: []TimestampTransform{{Delay: 5 * time.Second}}}
		wsPT := WinStrat{Trigger: pt}
		wsPT.IsTriggerReady(triggerInput{newElementCount: 1, emNow: 1000}, &state)
		if state.getTriggerState(pt).extra == nil {
			t.Fatal("AfterProcessingTime did not set firing state")
		}
		wsPT.OnWatermarkFire(&state)
		if state.getTriggerState(pt).extra != nil {
			t.Fatal("AfterProcessingTime kept uncommitted timer state after OnWatermarkFire")
		}
		wsPT.IsTriggerReady(triggerInput{newElementCount: 1, emNow: 1000}, &state)
		if !wsPT.IsTriggerReady(triggerInput{emNow: 6000}, &state) {
			t.Fatal("AfterProcessingTime did not fire at deadline")
		}
		wsPT.OnWatermarkFire(&state)
	})

	t.Run("after_all", func(t *testing.T) {
		var state StateData
		partial := &TriggerAfterAll{SubTriggers: []Trigger{
			&TriggerAfterEndOfWindow{},
			&TriggerElementCount{ElementCount: 2},
		}}
		wsPartial := WinStrat{Trigger: partial}
		wsPartial.IsTriggerReady(triggerInput{newElementCount: 1}, &state)
		wsPartial.OnWatermarkFire(&state)
		if state.getTriggerState(partial).finished {
			t.Fatal("AfterAll finished while ElementCount(2) remained unfinished")
		}

		both := &TriggerAfterAll{SubTriggers: []Trigger{
			&TriggerAfterEndOfWindow{},
			&TriggerAfterEndOfWindow{},
		}}
		wsBoth := WinStrat{Trigger: both}
		wsBoth.OnWatermarkFire(&state)
		if !state.getTriggerState(both).finished {
			t.Fatal("AfterAll did not finish when all subtriggers finished on watermark")
		}
		wsBoth.OnWatermarkFire(&state)
	})

	t.Run("after_any", func(t *testing.T) {
		var state StateData
		none := &TriggerAfterAny{SubTriggers: []Trigger{
			&TriggerElementCount{ElementCount: 2},
		}}
		wsNone := WinStrat{Trigger: none}
		wsNone.IsTriggerReady(triggerInput{newElementCount: 1}, &state)
		wsNone.OnWatermarkFire(&state)
		if state.getTriggerState(none).finished {
			t.Fatal("AfterAny finished when no subtrigger finished")
		}

		fin := &TriggerAfterAny{SubTriggers: []Trigger{
			&TriggerElementCount{ElementCount: 2},
			&TriggerAfterEndOfWindow{},
		}}
		wsFin := WinStrat{Trigger: fin}
		wsFin.OnWatermarkFire(&state)
		if !state.getTriggerState(fin).finished {
			t.Fatal("AfterAny did not finish when subtrigger finished on watermark")
		}
		wsFin.OnWatermarkFire(&state)
	})

	t.Run("after_each", func(t *testing.T) {
		var state StateData
		seq := &TriggerAfterEach{SubTriggers: []Trigger{
			&TriggerAfterEndOfWindow{},
			&TriggerAfterEndOfWindow{},
		}}
		ws := WinStrat{Trigger: seq}
		ws.OnWatermarkFire(&state)
		if state.getTriggerState(seq).finished {
			t.Fatal("AfterEach finished after first subtrigger")
		}
		ws.OnWatermarkFire(&state)
		if !state.getTriggerState(seq).finished {
			t.Fatal("AfterEach did not finish after last subtrigger")
		}
		ws.OnWatermarkFire(&state)
	})

	t.Run("or_finally_and_repeatedly_and_eow", func(t *testing.T) {
		var state StateData
		orfFin := &TriggerOrFinally{
			Main:    &TriggerElementCount{ElementCount: 2},
			Finally: &TriggerAfterEndOfWindow{},
		}
		wsOrfFin := WinStrat{Trigger: orfFin}
		wsOrfFin.OnWatermarkFire(&state)
		if !state.getTriggerState(orfFin).finished {
			t.Fatal("OrFinally did not finish when Finally finished on watermark")
		}
		wsOrfFin.OnWatermarkFire(&state)

		var state2 StateData
		mainEOW := &TriggerAfterEndOfWindow{}
		orfKeep := &TriggerOrFinally{
			Main: mainEOW,
			Finally: &TriggerAfterEndOfWindow{
				Late: &TriggerElementCount{ElementCount: 2},
			},
		}
		wsOrfKeep := WinStrat{Trigger: orfKeep}
		wsOrfKeep.OnWatermarkFire(&state2)
		if state2.getTriggerState(orfKeep).finished {
			t.Fatal("OrFinally finished while Finally still had Late firings")
		}
		if state2.getTriggerState(mainEOW).finished {
			t.Fatal("OrFinally did not reset finished Main subtrigger")
		}

		var state3 StateData
		rep := &TriggerRepeatedly{Repeated: &TriggerAfterEndOfWindow{}}
		(WinStrat{Trigger: rep}).OnWatermarkFire(&state3)
		if state3.getTriggerState(rep.Repeated).finished {
			t.Fatal("Repeatedly did not reset finished subtrigger on watermark")
		}

		var state4 StateData
		eowNoLate := &TriggerAfterEndOfWindow{Early: &TriggerElementCount{ElementCount: 2}}
		wsNoLate := WinStrat{Trigger: eowNoLate}
		wsNoLate.OnWatermarkFire(&state4)
		if !state4.getTriggerState(eowNoLate).finished {
			t.Fatal("AfterEndOfWindow without Late did not finish at watermark")
		}
		wsNoLate.OnWatermarkFire(&state4)

		var state5 StateData
		lateOnce := &TriggerAfterEndOfWindow{}
		eowLate := &TriggerAfterEndOfWindow{Late: lateOnce}
		wsLate := WinStrat{Trigger: eowLate}
		wsLate.OnWatermarkFire(&state5)
		wsLate.OnWatermarkFire(&state5)
		if state5.getTriggerState(lateOnce).finished {
			t.Fatal("AfterEndOfWindow did not reset finished Late subtrigger")
		}
	})
}
