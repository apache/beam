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
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/google/go-cmp/cmp"
	"testing"
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
