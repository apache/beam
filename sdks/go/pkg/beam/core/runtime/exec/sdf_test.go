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
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
	"github.com/google/go-cmp/cmp"
)

// testTimestamp is a constant used to check that timestamps are retained.
const testTimestamp = 15

// testWindows is a constant used to check that windows are retained.
var testWindows = []typex.Window{window.IntervalWindow{Start: 10, End: 20}}

// testMultiWindows is used for tests that care about multiple windows.
var testMultiWindows = []typex.Window{
	window.IntervalWindow{Start: 10, End: 20},
	window.IntervalWindow{Start: 11, End: 21},
	window.IntervalWindow{Start: 12, End: 22},
	window.IntervalWindow{Start: 13, End: 23},
}

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
	emptydfn, err := graph.NewDoFn(&VetEmptyInitialSplitSdf{}, graph.NumMainInputs(graph.MainSingle))
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
			{
				name: "Empty",
				fn:   emptydfn,
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
				want: []FullValue{},
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

				if len(capt.Elements) != len(test.want) {
					t.Errorf("SplitAndSizeRestrictions(%v) has incorrect number of outputs got: %v, want: %v",
						test.in, len(capt.Elements), len(test.want))
				}
				for i, got := range capt.Elements {
					if !cmp.Equal(got, test.want[i]) {
						t.Errorf("SplitAndSizeRestrictions(%v) has incorrect output %v: got: %v, want: %v",
							test.in, i, got, test.want)
					}
				}
			})
		}
	})

	// Validate SplitAndSizeRestrictions matches its contract and properly
	// invokes SDF methods SplitRestriction and RestrictionSize.
	t.Run("InvalidSplitAndSizeRestrictions", func(t *testing.T) {
		idfn, err := graph.NewDoFn(&NegativeSizeSdf{rest: offsetrange.Restriction{Start: 0, End: 4}}, graph.NumMainInputs(graph.MainSingle))
		if err != nil {
			t.Fatalf("invalid function: %v", err)
		}
		tests := []struct {
			name string
			fn   *graph.DoFn
			in   FullValue
		}{
			{
				name: "InvalidSplit",
				fn:   idfn,
				in: FullValue{
					Elm: &FullValue{
						Elm:       1,
						Elm2:      nil,
						Timestamp: testTimestamp,
						Windows:   testWindows,
					},
					Elm2:      offsetrange.Restriction{Start: 0, End: 4},
					Timestamp: testTimestamp,
					Windows:   testWindows,
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
				p, err := NewPlan("a", units)
				if err != nil {
					t.Fatalf("failed to construct plan: %v", err)
				}
				err = p.Execute(context.Background(), "1", DataContext{})
				if err == nil {
					t.Errorf("execution was expected to fail.")
				}
				if !strings.Contains(err.Error(), "size returned expected to be non-negative but received") {
					t.Errorf("SplitAndSizeRestrictions(%v) failed, got: %v, wanted: 'size returned expected to be non-negative but received'.", test.in, err)
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
	pdfn, err := graph.NewDoFn(&NegativeSizeSdf{rest: offsetrange.Restriction{Start: 0, End: 2}}, graph.NumMainInputs(graph.MainSingle))
	if err != nil {
		t.Fatalf("invalid function: %v", err)
	}
	rdfn, err := graph.NewDoFn(&NegativeSizeSdf{rest: offsetrange.Restriction{Start: 2, End: 4}}, graph.NumMainInputs(graph.MainSingle))
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
					ThisIsDone:  false,
				}
				node.elm = &elm
				node.currW = test.currWindow
				node.numW = len(test.windows)

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
			name          string
			fn            *graph.DoFn
			frac          float64
			doneRt        bool // Result that RTracker will return for IsDone.
			in            FullValue
			wantPrimaries []*FullValue
			wantResiduals []*FullValue
		}{
			{
				name: "SingleElem",
				fn:   dfn,
				frac: 0.5,
				in: FullValue{
					Elm: &FullValue{
						Elm:  1,
						Elm2: &VetRestriction{ID: "Sdf"},
					},
					Elm2:      1.0,
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
				wantPrimaries: []*FullValue{{
					Elm: &FullValue{
						Elm:  1,
						Elm2: &VetRestriction{ID: "Sdf.1", RestSize: true, Val: 1},
					},
					Elm2:      1.0,
					Timestamp: testTimestamp,
					Windows:   testWindows,
				}},
				wantResiduals: []*FullValue{{
					Elm: &FullValue{
						Elm:  1,
						Elm2: &VetRestriction{ID: "Sdf.2", RestSize: true, Val: 1},
					},
					Elm2:      1.0,
					Timestamp: testTimestamp,
					Windows:   testWindows,
				}},
			},
			{
				name: "KvElem",
				fn:   kvdfn,
				frac: 0.5,
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
				wantPrimaries: []*FullValue{{
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
				}},
				wantResiduals: []*FullValue{{
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
				}},
			},
			{
				name:   "DoneRTracker",
				fn:     dfn,
				doneRt: true,
				frac:   0.5,
				in: FullValue{
					Elm: &FullValue{
						Elm:  1,
						Elm2: &VetRestriction{ID: "Sdf"},
					},
					Elm2:      1.0,
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
				wantPrimaries: []*FullValue{},
				wantResiduals: []*FullValue{},
			},
			{
				// MultiWindow split where split point lands inside currently
				// processing restriction tracker.
				name: "MultiWindow/RestrictionSplit",
				fn:   dfn,
				frac: 0.125, // Should be in the middle of the first (current) window.
				in: FullValue{
					Elm: &FullValue{
						Elm:  1,
						Elm2: &VetRestriction{ID: "Sdf"},
					},
					Elm2:      1.0,
					Timestamp: testTimestamp,
					Windows:   testMultiWindows,
				},
				wantPrimaries: []*FullValue{{
					Elm: &FullValue{
						Elm:  1,
						Elm2: &VetRestriction{ID: "Sdf.1", RestSize: true, Val: 1},
					},
					Elm2:      1.0,
					Timestamp: testTimestamp,
					Windows:   testMultiWindows[0:1],
				}},
				wantResiduals: []*FullValue{{
					Elm: &FullValue{
						Elm:  1,
						Elm2: &VetRestriction{ID: "Sdf.2", RestSize: true, Val: 1},
					},
					Elm2:      1.0,
					Timestamp: testTimestamp,
					Windows:   testMultiWindows[0:1],
				}, {
					Elm: &FullValue{
						Elm:  1,
						Elm2: &VetRestriction{ID: "Sdf", RestSize: true, Val: 1},
					},
					Elm2:      1.0,
					Timestamp: testTimestamp,
					Windows:   testMultiWindows[1:4],
				}},
			},
			{
				// MultiWindow split where the split lands outside the current
				// window, and performs a window boundary split instead.
				name: "MultiWindow/WindowBoundarySplit",
				fn:   dfn,
				frac: 0.55,
				in: FullValue{
					Elm: &FullValue{
						Elm:  1,
						Elm2: &VetRestriction{ID: "Sdf"},
					},
					Elm2:      1.0,
					Timestamp: testTimestamp,
					Windows:   testMultiWindows,
				},
				wantPrimaries: []*FullValue{{
					Elm: &FullValue{
						Elm:  1,
						Elm2: &VetRestriction{ID: "Sdf", RestSize: true, Val: 1},
					},
					Elm2:      1.0,
					Timestamp: testTimestamp,
					Windows:   testMultiWindows[0:2],
				}},
				wantResiduals: []*FullValue{{
					Elm: &FullValue{
						Elm:  1,
						Elm2: &VetRestriction{ID: "Sdf", RestSize: true, Val: 1},
					},
					Elm2:      1.0,
					Timestamp: testTimestamp,
					Windows:   testMultiWindows[2:4],
				}},
			},
			{
				// Tests that a MultiWindow split with a Done RTracker will
				// fallback to a window boundary split.
				name:   "MultiWindow/DoneRTrackerSplit",
				fn:     dfn,
				frac:   0.125,
				doneRt: true,
				in: FullValue{
					Elm: &FullValue{
						Elm:  1,
						Elm2: &VetRestriction{ID: "Sdf"},
					},
					Elm2:      1.0,
					Timestamp: testTimestamp,
					Windows:   testMultiWindows,
				},
				wantPrimaries: []*FullValue{{
					Elm: &FullValue{
						Elm:  1,
						Elm2: &VetRestriction{ID: "Sdf", RestSize: true, Val: 1},
					},
					Elm2:      1.0,
					Timestamp: testTimestamp,
					Windows:   testMultiWindows[0:1],
				}},
				wantResiduals: []*FullValue{{
					Elm: &FullValue{
						Elm:  1,
						Elm2: &VetRestriction{ID: "Sdf", RestSize: true, Val: 1},
					},
					Elm2:      1.0,
					Timestamp: testTimestamp,
					Windows:   testMultiWindows[1:4],
				}},
			},
			{
				// Test that if a window boundary split lands at the end of an
				// element, it results in a no-op.
				name: "MultiWindow/NoResidual",
				fn:   dfn,
				frac: 0.95, // Should round to end of element and cause a no-op.
				in: FullValue{
					Elm: &FullValue{
						Elm:  1,
						Elm2: &VetRestriction{ID: "Sdf"},
					},
					Elm2:      1.0,
					Timestamp: testTimestamp,
					Windows:   testMultiWindows,
				},
				wantPrimaries: []*FullValue{},
				wantResiduals: []*FullValue{},
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
					Done:        0,
					Remaining:   1.0,
					ThisIsDone:  test.doneRt,
				}
				node.elm = &test.in
				node.numW = len(test.in.Windows)
				node.currW = 0

				// Call from SplittableUnit and check results.
				su := SplittableUnit(node)
				if err := node.Up(context.Background()); err != nil {
					t.Fatalf("ProcessSizedElementsAndRestrictions.Up() failed: %v", err)
				}
				gotPrimaries, gotResiduals, err := su.Split(test.frac)
				if err != nil {
					t.Fatalf("SplittableUnit.Split(%v) failed: %v", test.frac, err)
				}
				if diff := cmp.Diff(gotPrimaries, test.wantPrimaries); diff != "" {
					t.Errorf("SplittableUnit.Split(%v) has incorrect primary (-got, +want)\n%v", test.frac, diff)
				}
				if diff := cmp.Diff(gotResiduals, test.wantResiduals); diff != "" {
					t.Errorf("SplittableUnit.Split(%v) has incorrect residual (-got, +want)\n%v", test.frac, diff)
				}
			})
		}
	})

	// Test that Split properly validates the results and returns an error if invalid
	t.Run("InvalidSplitSize", func(t *testing.T) {
		tests := []struct {
			name string
			fn   *graph.DoFn
			in   FullValue
		}{
			{
				name: "Primary",
				fn:   pdfn,
				in: FullValue{
					Elm: &FullValue{
						Elm:  1,
						Elm2: &offsetrange.Restriction{Start: 0, End: 4},
					},
					Elm2:      1.0,
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
			},
			{
				name: "Residual",
				fn:   rdfn,
				in: FullValue{
					Elm: &FullValue{
						Elm:  1,
						Elm2: &offsetrange.Restriction{Start: 0, End: 4},
					},
					Elm2:      1.0,
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
				node.rt = sdf.RTracker(offsetrange.NewTracker(*test.in.Elm.(*FullValue).Elm2.(*offsetrange.Restriction)))
				node.elm = &test.in
				node.numW = len(test.in.Windows)
				node.currW = 0

				// Call from SplittableUnit and check results.
				su := SplittableUnit(node)
				if err := node.Up(context.Background()); err != nil {
					t.Fatalf("ProcessSizedElementsAndRestrictions.Up() failed: %v", err)
				}
				_, _, err := su.Split(0.5)
				if err == nil {
					t.Errorf("SplittableUnit.Split(%v) was expected to fail.", test.in)
				}
				if !strings.Contains(err.Error(), "size returned expected to be non-negative but received") {
					t.Errorf("SplittableUnit.Split(%v) failed, got: %v, wanted: 'size returned expected to be non-negative but received'.", test.in, err)
				}
			})
		}
	})
}

// TestMultiWindowProcessing tests that ProcessSizedElementsAndRestrictions
// handles processing multiple windows correctly, even when progress is
// reported and splits are performed during processing.
func TestMultiWindowProcessing(t *testing.T) {
	// Set up our SDF to block on the second window of four, at the second
	// position of the restriction. (i.e. window at 0.5 progress, full element
	// at 0.375 progress)
	blockW := 1
	wsdf := WindowBlockingSdf{
		block: make(chan struct{}),
		claim: 1,
		w:     testMultiWindows[blockW],
	}
	dfn, err := graph.NewDoFn(&wsdf, graph.NumMainInputs(graph.MainSingle))
	if err != nil {
		t.Fatalf("invalid function: %v", err)
	}

	// Create a plan with a single valid element as input to ProcessElement.
	in := FullValue{
		Elm: &FullValue{
			Elm:  1,
			Elm2: offsetrange.Restriction{Start: 0, End: 4},
		},
		Elm2:      4.0,
		Timestamp: testTimestamp,
		Windows:   testMultiWindows,
	}
	capt := &CaptureNode{UID: 2}
	n := &ParDo{UID: 1, Fn: dfn, Out: []Node{capt}}
	node := &ProcessSizedElementsAndRestrictions{PDo: n}
	root := &FixedRoot{UID: 0, Elements: []MainInput{{Key: in}}, Out: node}
	units := []Unit{root, node, capt}
	p, err := NewPlan("a", units)
	if err != nil {
		t.Fatalf("failed to construct plan: %v", err)
	}

	// Start a goroutine for processing, expecting to synchronize with it once
	// while processing is blocked (to validate processing) and a second time
	// it's done (to validate final outputs).
	done := make(chan struct{})
	errchan := make(chan string, 1)
	go func() {
		defer close(errchan)
		if err := p.Execute(context.Background(), "1", DataContext{}); err != nil {
			errchan <- fmt.Sprintf("execute failed: %v", err)
			return
		}
		done <- struct{}{}
	}()

	// Once SDF is blocked, check that it is tracking windows properly, and that
	// getting progress and splitting works as expected.
	<-wsdf.block

	if got, want := node.currW, blockW; got != want {
		t.Errorf("Incorrect current window during processing, got %v, want %v", got, want)
	}
	if got, want := node.numW, len(testMultiWindows); got != want {
		t.Errorf("Incorrect total number of windows during processing, got %v, want %v", got, want)
	}

	su := <-node.SU
	if got, want := su.GetProgress(), 1.5/4.0; !floatEquals(got, want, 0.00001) {
		t.Errorf("Incorrect result from GetProgress() during processing, got %v, want %v", got, want)
	}
	// Split should hit window boundary between 2 and 3. We don't need to check
	// the split result here, just the effects it has on currW and numW.
	frac := 0.5
	if _, _, err := su.Split(frac); err != nil {
		t.Errorf("Split(%v) failed with error: %v", frac, err)
	}
	if got, want := node.currW, blockW; got != want {
		t.Errorf("Incorrect current window after splitting, got %v, want %v", got, want)
	}
	if got, want := node.numW, 3; got != want {
		t.Errorf("Incorrect total number of windows after splitting, got %v, want %v", got, want)
	}

	// Now we can unblock SDF and finish processing, then check that the results
	// respected the windowed split.
	node.SU <- su
	wsdf.block <- struct{}{}
	<-done

	for msg := range errchan {
		t.Fatal(msg)
	}

	gotOut := capt.Elements
	wantOut := []FullValue{{ // Only 3 windows, 4th should be gone after split.
		Elm:       1,
		Timestamp: testTimestamp,
		Windows:   testMultiWindows[0:1],
	}, {
		Elm:       1,
		Timestamp: testTimestamp,
		Windows:   testMultiWindows[1:2],
	}, {
		Elm:       1,
		Timestamp: testTimestamp,
		Windows:   testMultiWindows[2:3],
	}}
	if diff := cmp.Diff(gotOut, wantOut); diff != "" {
		t.Errorf("ProcessSizedElementsAndRestrictions produced incorrect outputs (-got, +want):\n%v", diff)
	}
}

// NegativeSizeSdf is a very basic SDF that returns a negative restriction size
// if the passed in restriction matches otherwise it uses offsetrange.Restriction's default size.
type NegativeSizeSdf struct {
	rest offsetrange.Restriction
}

// CreateInitialRestriction creates a four-element offset range.
func (fn *NegativeSizeSdf) CreateInitialRestriction(_ int) offsetrange.Restriction {
	return offsetrange.Restriction{Start: 0, End: 4}
}

// SplitRestriction is a no-op, and does not split.
func (fn *NegativeSizeSdf) SplitRestriction(_ int, rest offsetrange.Restriction) []offsetrange.Restriction {
	return []offsetrange.Restriction{rest}
}

// RestrictionSize returns the passed in size that should be used.
func (fn *NegativeSizeSdf) RestrictionSize(_ int, rest offsetrange.Restriction) float64 {
	if fn.rest == rest {
		return -1
	}
	return rest.Size()
}

// CreateTracker creates a LockRTracker wrapping an offset range RTracker.
func (fn *NegativeSizeSdf) CreateTracker(rest offsetrange.Restriction) *offsetrange.Tracker {
	return offsetrange.NewTracker(rest)
}

// ProcessElement emits the element after consuming the entire restriction tracker.
func (fn *NegativeSizeSdf) ProcessElement(rt *offsetrange.Tracker, elm int, emit func(int)) {
	i := rt.GetRestriction().(offsetrange.Restriction).Start
	for rt.TryClaim(i) {
		i++
	}
	emit(elm)
}

// WindowBlockingSdf is a very basic SDF that blocks execution once, in one
// window and at one position within the restriction.
type WindowBlockingSdf struct {
	block chan struct{}
	claim int64 // The SDF will block after claiming this position.
	w     typex.Window
}

// CreateInitialRestriction creates a four-element offset range.
func (fn *WindowBlockingSdf) CreateInitialRestriction(_ int) offsetrange.Restriction {
	return offsetrange.Restriction{Start: 0, End: 4}
}

// SplitRestriction is a no-op, and does not split.
func (fn *WindowBlockingSdf) SplitRestriction(_ int, rest offsetrange.Restriction) []offsetrange.Restriction {
	return []offsetrange.Restriction{rest}
}

// RestrictionSize defers to the default offset range restriction size.
func (fn *WindowBlockingSdf) RestrictionSize(_ int, rest offsetrange.Restriction) float64 {
	return rest.Size()
}

// CreateTracker creates a LockRTracker wrapping an offset range RTracker.
func (fn *WindowBlockingSdf) CreateTracker(rest offsetrange.Restriction) *sdf.LockRTracker {
	return sdf.NewLockRTracker(offsetrange.NewTracker(rest))
}

// ProcessElement observes windows, and, while claiming positions in the
// restriction, checks each position+window pair to see if it matches the
// block position and window. If it does, then this sends a token through the
// block channel and receives it back, to give another thread a chance to
// perform work before unblocking. Finally, once all work has been claimed, it
// outputs the original input element.
func (fn *WindowBlockingSdf) ProcessElement(w typex.Window, rt *sdf.LockRTracker, elm int, emit func(int)) {
	i := rt.GetRestriction().(offsetrange.Restriction).Start
	for rt.TryClaim(i) {
		if w == fn.w && i == fn.claim {
			fn.block <- struct{}{}
			<-fn.block
		}
		i++
	}
	emit(elm)
}

// SplittableUnitRTracker is a VetRTracker with some added behavior needed for
// TestAsSplittableUnit.
type SplittableUnitRTracker struct {
	VetRTracker
	Done, Remaining float64 // Allows manually setting progress.
	ThisIsDone      bool
}

func (rt *SplittableUnitRTracker) IsDone() bool {
	return rt.ThisIsDone
}

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
