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
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/google/go-cmp/cmp"
)

// testTimestamp is a constant used to check that timestamps are retained.
const testTimestamp = 15

// testWindow is a constant used to check that windows are retained
var testWindows = []typex.Window{window.IntervalWindow{Start: 10, End: 20}}

// TestSdfNodes verifies that the various SDF nodes fulfill each of their
// described contracts, that they each successfully invoke any SDF methods
// needed, and that they preserve timestamps and windows correctly.
func TestSdfNodes(t *testing.T) {
	// Setup. The DoFns created below are defined in sdf_invokers_test.go and
	// have testable behavior to confirm that they got correctly invoked.
	// Without knowing the expected behavior of these DoFns, the desired outputs
	// in the unit tests below will not make much sense.
	dfn, err := graph.NewDoFn(&VetSdf{}, graph.NumMainInputs(graph.MainSingle))
	if err != nil {
		t.Fatalf("invalid function: %v", err)
	}
	kvdfn, err := graph.NewDoFn(&VetKvSdf{}, graph.NumMainInputs(graph.MainKv))
	if err != nil {
		t.Fatalf("invalid function: %v", err)
	}

	// Validate PairWithRestriction matches its contract and properly invokes
	// SDF method CreateInitialRestriction.
	t.Run("PairWithRestriction", func(t *testing.T) {
		tests := []struct {
			name string
			fn   *graph.DoFn
			in   FullValue
			want FullValue
		}{
			{
				name: "SingleElem",
				fn:   dfn,
				in: FullValue{
					Elm:       1,
					Elm2:      nil,
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
				want: FullValue{
					Elm: &FullValue{
						Elm:       1,
						Elm2:      nil,
						Timestamp: testTimestamp,
						Windows:   testWindows,
					},
					Elm2:      &VetRestriction{ID: "Sdf", CreateRest: true, Val: 1},
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
			},
			{
				name: "KvElem",
				fn:   kvdfn,
				in: FullValue{
					Elm:       1,
					Elm2:      2,
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
				want: FullValue{
					Elm: &FullValue{
						Elm:       1,
						Elm2:      2,
						Timestamp: testTimestamp,
						Windows:   testWindows,
					},
					Elm2:      &VetRestriction{ID: "KvSdf", CreateRest: true, Key: 1, Val: 2},
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
			},
		}
		for _, test := range tests {
			test := test
			t.Run(test.name, func(t *testing.T) {
				capt := &CaptureNode{UID: 2}
				node := &PairWithRestriction{UID: 1, Fn: test.fn, Out: capt}
				root := &FixedRoot{UID: 0, Elements: []MainInput{{Key: test.in}}, Out: node}
				units := []Unit{root, node, capt}
				constructAndExecutePlan(t, units)

				got := capt.Elements[0]
				if !cmp.Equal(got, test.want) {
					t.Errorf("PairWithRestriction(%v) has incorrect output: (-got, +want)\n%v",
						test.in, cmp.Diff(got, test.want))
				}
			})
		}
	})

	// Validate SplitAndSizeRestrictions matches its contract and properly
	// invokes SDF methods SplitRestriction and RestrictionSize.
	t.Run("SplitAndSizeRestrictions", func(t *testing.T) {
		tests := []struct {
			name string
			fn   *graph.DoFn
			in   FullValue
			want []FullValue
		}{
			{
				name: "SingleElem",
				fn:   dfn,
				in: FullValue{
					Elm: &FullValue{
						Elm:       1,
						Elm2:      nil,
						Timestamp: testTimestamp,
						Windows:   testWindows,
					},
					Elm2:      &VetRestriction{ID: "Sdf"},
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
				want: []FullValue{
					{
						Elm: &FullValue{
							Elm: &FullValue{
								Elm:       1,
								Elm2:      nil,
								Timestamp: testTimestamp,
								Windows:   testWindows,
							},
							Elm2: &VetRestriction{ID: "Sdf.1", SplitRest: true, RestSize: true, Val: 1},
						},
						Elm2:      1.0,
						Timestamp: testTimestamp,
						Windows:   testWindows,
					},
					{
						Elm: &FullValue{
							Elm: &FullValue{
								Elm:       1,
								Elm2:      nil,
								Timestamp: testTimestamp,
								Windows:   testWindows,
							},
							Elm2: &VetRestriction{ID: "Sdf.2", SplitRest: true, RestSize: true, Val: 1},
						},
						Elm2:      1.0,
						Timestamp: testTimestamp,
						Windows:   testWindows,
					},
				},
			},
			{
				name: "KvElem",
				fn:   kvdfn,
				in: FullValue{
					Elm: &FullValue{
						Elm:       1,
						Elm2:      2,
						Timestamp: testTimestamp,
						Windows:   testWindows,
					},
					Elm2:      &VetRestriction{ID: "KvSdf"},
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
				want: []FullValue{
					{
						Elm: &FullValue{
							Elm: &FullValue{
								Elm:       1,
								Elm2:      2,
								Timestamp: testTimestamp,
								Windows:   testWindows,
							},
							Elm2: &VetRestriction{ID: "KvSdf.1", SplitRest: true, RestSize: true, Key: 1, Val: 2},
						},
						Elm2:      3.0,
						Timestamp: testTimestamp,
						Windows:   testWindows,
					},
					{
						Elm: &FullValue{
							Elm: &FullValue{
								Elm:       1,
								Elm2:      2,
								Timestamp: testTimestamp,
								Windows:   testWindows,
							},
							Elm2: &VetRestriction{ID: "KvSdf.2", SplitRest: true, RestSize: true, Key: 1, Val: 2},
						},
						Elm2:      3.0,
						Timestamp: testTimestamp,
						Windows:   testWindows,
					},
				},
			},
		}
		for _, test := range tests {
			test := test
			t.Run(test.name, func(t *testing.T) {
				capt := &CaptureNode{UID: 2}
				node := &SplitAndSizeRestrictions{UID: 1, Fn: test.fn, Out: capt}
				root := &FixedRoot{UID: 0, Elements: []MainInput{{Key: test.in}}, Out: node}
				units := []Unit{root, node, capt}
				constructAndExecutePlan(t, units)

				for i, got := range capt.Elements {
					if !cmp.Equal(got, test.want[i]) {
						t.Errorf("SplitAndSizeRestrictions(%v) has incorrect output %v: got: %v, want: %v",
							test.in, i, got, test.want)
					}
				}
			})
		}
	})

	// Validate ProcessSizedElementsAndRestrictions matches its contract and
	// properly invokes SDF methods CreateTracker and ProcessElement.
	t.Run("ProcessSizedElementsAndRestrictions", func(t *testing.T) {
		tests := []struct {
			name string
			fn   *graph.DoFn
			in   FullValue
			want FullValue
		}{
			{
				name: "SingleElem",
				fn:   dfn,
				in: FullValue{
					Elm: &FullValue{
						Elm:  1,
						Elm2: &VetRestriction{ID: "Sdf"},
					},
					Elm2:      1.0,
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
				want: FullValue{
					Elm:       &VetRestriction{ID: "Sdf", CreateTracker: true, ProcessElm: true, Val: 1},
					Elm2:      nil,
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
			},
			{
				name: "KvElem",
				fn:   kvdfn,
				in: FullValue{
					Elm: &FullValue{
						Elm: &FullValue{
							Elm:       1,
							Elm2:      2,
							Timestamp: testTimestamp,
							Windows:   testWindows,
						},
						Elm2: &VetRestriction{ID: "KvSdf"},
					},
					Elm2:      3.0,
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
				want: FullValue{
					Elm:       &VetRestriction{ID: "KvSdf", CreateTracker: true, ProcessElm: true, Key: 1, Val: 2},
					Elm2:      nil,
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
			},
		}
		for _, test := range tests {
			test := test
			t.Run(test.name, func(t *testing.T) {
				capt := &CaptureNode{UID: 2}
				n := &ParDo{UID: 1, Fn: test.fn, Out: []Node{capt}}
				node := &ProcessSizedElementsAndRestrictions{PDo: n}
				root := &FixedRoot{UID: 0, Elements: []MainInput{{Key: test.in}}, Out: node}
				units := []Unit{root, node, capt}
				constructAndExecutePlan(t, units)

				got := capt.Elements[0]
				if !cmp.Equal(got, test.want) {
					t.Errorf("ProcessSizedElementsAndRestrictions(%v) has incorrect output: got: %v, want: %v",
						test.in, got, test.want)
				}
			})
		}
	})

	// Validate SdfFallback matches its contract and properly invokes SDF
	// methods CreateRestriction, SplitRestriction, CreateTracker and
	// ProcessElement.
	t.Run("SdfFallback", func(t *testing.T) {
		tests := []struct {
			name string
			fn   *graph.DoFn
			in   FullValue
			want []FullValue
		}{
			{
				name: "SingleElem",
				fn:   dfn,
				in: FullValue{
					Elm:       1,
					Elm2:      nil,
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
				want: []FullValue{
					{
						Elm:       &VetRestriction{ID: "Sdf.1", CreateRest: true, SplitRest: true, CreateTracker: true, ProcessElm: true, Val: 1},
						Elm2:      nil,
						Timestamp: testTimestamp,
						Windows:   testWindows,
					},
					{
						Elm:       &VetRestriction{ID: "Sdf.2", CreateRest: true, SplitRest: true, CreateTracker: true, ProcessElm: true, Val: 1},
						Elm2:      nil,
						Timestamp: testTimestamp,
						Windows:   testWindows,
					},
				},
			},
			{
				name: "KvElem",
				fn:   kvdfn,
				in: FullValue{
					Elm:       1,
					Elm2:      2,
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
				want: []FullValue{
					{
						Elm:       &VetRestriction{ID: "KvSdf.1", CreateRest: true, SplitRest: true, CreateTracker: true, ProcessElm: true, Key: 1, Val: 2},
						Elm2:      nil,
						Timestamp: testTimestamp,
						Windows:   testWindows,
					},
					{
						Elm:       &VetRestriction{ID: "KvSdf.2", CreateRest: true, SplitRest: true, CreateTracker: true, ProcessElm: true, Key: 1, Val: 2},
						Elm2:      nil,
						Timestamp: testTimestamp,
						Windows:   testWindows,
					},
				},
			},
		}
		for _, test := range tests {
			test := test
			t.Run(test.name, func(t *testing.T) {
				capt := &CaptureNode{UID: 2}
				n := &ParDo{UID: 1, Fn: test.fn, Out: []Node{capt}}
				node := &SdfFallback{PDo: n}
				root := &FixedRoot{UID: 0, Elements: []MainInput{{Key: test.in}}, Out: node}
				units := []Unit{root, node, capt}
				constructAndExecutePlan(t, units)

				for i, got := range capt.Elements {
					if !cmp.Equal(got, test.want[i]) {
						t.Errorf("SdfFallback(%v) has incorrect output %v: got: %v, want: %v",
							test.in, i, got, test.want)
					}
				}
			})
		}
	})
}

// TestAsSplittableUnit tests ProcessSizedElementsAndRestrictions' implementation
// of the SplittableUnit interface.
func TestAsSplittableUnit(t *testing.T) {
	dfn, err := graph.NewDoFn(&VetSdf{}, graph.NumMainInputs(graph.MainSingle))
	if err != nil {
		t.Fatalf("invalid function: %v", err)
	}
	kvdfn, err := graph.NewDoFn(&VetKvSdf{}, graph.NumMainInputs(graph.MainKv))
	if err != nil {
		t.Fatalf("invalid function: %v", err)
	}
	multiWindows := []typex.Window{
		window.IntervalWindow{Start: 10, End: 20},
		window.IntervalWindow{Start: 11, End: 21},
		window.IntervalWindow{Start: 12, End: 22},
		window.IntervalWindow{Start: 13, End: 23},
	}

	// Test that progress returns expected results and respects windows.
	t.Run("Progress", func(t *testing.T) {
		tests := []struct {
			name          string
			windows       []typex.Window
			doneWork      float64 // Will be output by RTracker's GetProgress.
			remainingWork float64 // Will be output by RTracker's GetProgress.
			currWindow    int
			wantProgress  float64
		}{
			{
				name:          "SingleWindow",
				windows:       testWindows,
				doneWork:      1.0,
				remainingWork: 1.0,
				currWindow:    0,
				wantProgress:  0.5,
			},
			{
				name:          "MultipleWindows",
				windows:       multiWindows,
				doneWork:      1.0,
				remainingWork: 1.0,
				currWindow:    1,
				// Progress should be halfway through second window.
				wantProgress: 1.5 / 4.0,
			},
		}
		for _, test := range tests {
			test := test
			t.Run(test.name, func(t *testing.T) {
				// The contents of the element don't really matter in the test,
				// but the element is still built to be valid.
				elm := FullValue{
					Elm: &FullValue{
						Elm:  1,
						Elm2: &VetRestriction{ID: "Sdf"},
					},
					Elm2:      1.0,
					Timestamp: testTimestamp,
					Windows:   test.windows,
				}

				// Setup, create transforms, inputs, and desired outputs.
				n := &ParDo{UID: 1, Fn: dfn, Out: []Node{}}
				node := &ProcessSizedElementsAndRestrictions{PDo: n}
				node.rt = &SplittableUnitRTracker{
					VetRTracker: VetRTracker{Rest: elm.Elm.(*FullValue).Elm2.(*VetRestriction)},
					Done:        test.doneWork,
					Remaining:   test.remainingWork,
				}
				node.elm = &elm
				node.currW = test.currWindow

				// Call from SplittableUnit and check results.
				su := SplittableUnit(node)
				if err := node.Up(context.Background()); err != nil {
					t.Fatalf("ProcessSizedElementsAndRestrictions.Up() failed: %v", err)
				}
				got := su.GetProgress()
				if !floatEquals(got, test.wantProgress, 0.00001) {
					t.Fatalf("SplittableUnit.GetProgress() got incorrect progress: got %v, want %v", got, test.wantProgress)
				}
			})
		}
	})

	// Test that Split returns properly structured results and calls Split on
	// the restriction tracker.
	t.Run("Split", func(t *testing.T) {
		tests := []struct {
			name         string
			fn           *graph.DoFn
			in           FullValue
			wantPrimary  FullValue
			wantResidual FullValue
		}{
			{
				name: "SingleElem",
				fn:   dfn,
				in: FullValue{
					Elm: &FullValue{
						Elm:  1,
						Elm2: &VetRestriction{ID: "Sdf"},
					},
					Elm2:      1.0,
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
				wantPrimary: FullValue{
					Elm: &FullValue{
						Elm:  1,
						Elm2: &VetRestriction{ID: "Sdf.1", RestSize: true, Val: 1},
					},
					Elm2:      1.0,
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
				wantResidual: FullValue{
					Elm: &FullValue{
						Elm:  1,
						Elm2: &VetRestriction{ID: "Sdf.2", RestSize: true, Val: 1},
					},
					Elm2:      1.0,
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
			},
			{
				name: "KvElem",
				fn:   kvdfn,
				in: FullValue{
					Elm: &FullValue{
						Elm: &FullValue{
							Elm:  1,
							Elm2: 2,
						},
						Elm2: &VetRestriction{ID: "KvSdf"},
					},
					Elm2:      3.0,
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
				wantPrimary: FullValue{
					Elm: &FullValue{
						Elm: &FullValue{
							Elm:  1,
							Elm2: 2,
						},
						Elm2: &VetRestriction{ID: "KvSdf.1", RestSize: true, Key: 1, Val: 2},
					},
					Elm2:      3.0,
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
				wantResidual: FullValue{
					Elm: &FullValue{
						Elm: &FullValue{
							Elm:  1,
							Elm2: 2,
						},
						Elm2: &VetRestriction{ID: "KvSdf.2", RestSize: true, Key: 1, Val: 2},
					},
					Elm2:      3.0,
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
			},
		}
		for _, test := range tests {
			test := test
			t.Run(test.name, func(t *testing.T) {
				// Setup, create transforms, inputs, and desired outputs.
				n := &ParDo{UID: 1, Fn: test.fn, Out: []Node{}}
				node := &ProcessSizedElementsAndRestrictions{PDo: n}
				node.rt = &SplittableUnitRTracker{
					VetRTracker: VetRTracker{Rest: test.in.Elm.(*FullValue).Elm2.(*VetRestriction)},
				}
				node.elm = &test.in

				// Call from SplittableUnit and check results.
				su := SplittableUnit(node)
				frac := 0.5
				if err := node.Up(context.Background()); err != nil {
					t.Fatalf("ProcessSizedElementsAndRestrictions.Up() failed: %v", err)
				}
				gotPrimary, gotResidual, err := su.Split(frac)
				if err != nil {
					t.Fatalf("SplittableUnit.Split(%v) failed: %v", frac, err)
				}
				if diff := cmp.Diff(gotPrimary, &test.wantPrimary); diff != "" {
					t.Errorf("SplittableUnit.Split(%v) has incorrect primary: %v", frac, diff)
				}
				if diff := cmp.Diff(gotResidual, &test.wantResidual); diff != "" {
					t.Errorf("SplittableUnit.Split(%v) has incorrect residual: %v", frac, diff)
				}
			})
		}
	})
}

// SplittableUnitRTracker is a VetRTracker with some added behavior needed for
// TestAsSplittableUnit.
type SplittableUnitRTracker struct {
	VetRTracker
	Done, Remaining float64 // Allows manually setting progress.
}

func (rt *SplittableUnitRTracker) IsDone() bool { return false }

func (rt *SplittableUnitRTracker) TrySplit(_ float64) (interface{}, interface{}, error) {
	rest1 := rt.Rest.copy()
	rest1.ID += ".1"
	rest2 := rt.Rest.copy()
	rest2.ID += ".2"
	return &rest1, &rest2, nil
}

func (rt *SplittableUnitRTracker) GetProgress() (float64, float64) {
	return rt.Done, rt.Remaining
}
