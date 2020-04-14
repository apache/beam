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
	dfn, err := graph.NewDoFn(&Sdf{}, graph.NumMainInputs(graph.MainSingle))
	if err != nil {
		t.Fatalf("invalid function: %v", err)
	}
	kvdfn, err := graph.NewDoFn(&KvSdf{}, graph.NumMainInputs(graph.MainKv))
	if err != nil {
		t.Fatalf("invalid function: %v", err)
	}

	// Validate PairWithRestriction matches its contract and properly invokes
	// SDF method CreateInitialRestriction.
	t.Run("PairWithRestriction", func(t *testing.T) {
		tests := []struct {
			name string
			fn   *graph.DoFn
			in   *FullValue
			want *FullValue
		}{
			{
				name: "SingleElem",
				fn:   dfn,
				in: &FullValue{
					Elm:       5,
					Elm2:      nil,
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
				want: &FullValue{
					Elm: &FullValue{
						Elm:       5,
						Elm2:      nil,
						Timestamp: testTimestamp,
						Windows:   testWindows,
					},
					Elm2:      Restriction{5},
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
			},
			{
				name: "KvElem",
				fn:   kvdfn,
				in: &FullValue{
					Elm:       5,
					Elm2:      2,
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
				want: &FullValue{
					Elm: &FullValue{
						Elm:       5,
						Elm2:      2,
						Timestamp: testTimestamp,
						Windows:   testWindows,
					},
					Elm2:      Restriction{7},
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
			},
		}
		for _, test := range tests {
			test := test
			t.Run(test.name, func(t *testing.T) {
				fake := &FakeNode{UID: 2}
				node := &PairWithRestriction{UID: 1, Fn: test.fn, Out: fake}
				root := &FixedRoot{UID: 0, Elements: []MainInput{{Key: *test.in}}, Out: node}
				units := []Unit{root, node, fake}
				constructAndExecutePlan(t, units)

				got := fake.Vals[0]
				if !cmp.Equal(got, test.want) {
					t.Errorf("ProcessElement(%v) has incorrect output: got: %v, want: %v",
						test.in, got, test.want)
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
			in   *FullValue
			want []*FullValue
		}{
			{
				name: "SingleElem",
				fn:   dfn,
				in: &FullValue{
					Elm: &FullValue{
						Elm:       2,
						Elm2:      nil,
						Timestamp: testTimestamp,
						Windows:   testWindows,
					},
					Elm2:      Restriction{5},
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
				want: []*FullValue{
					{
						Elm: &FullValue{
							Elm: &FullValue{
								Elm:       2,
								Elm2:      nil,
								Timestamp: testTimestamp,
								Windows:   testWindows,
							},
							Elm2: Restriction{7},
						},
						Elm2:      9.0,
						Timestamp: testTimestamp,
						Windows:   testWindows,
					},
					{
						Elm: &FullValue{
							Elm: &FullValue{
								Elm:       2,
								Elm2:      nil,
								Timestamp: testTimestamp,
								Windows:   testWindows,
							},
							Elm2: Restriction{8},
						},
						Elm2:      10.0,
						Timestamp: testTimestamp,
						Windows:   testWindows,
					},
				},
			},
			{
				name: "KvElem",
				fn:   kvdfn,
				in: &FullValue{
					Elm: &FullValue{
						Elm:       2,
						Elm2:      5,
						Timestamp: testTimestamp,
						Windows:   testWindows,
					},
					Elm2:      Restriction{3},
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
				want: []*FullValue{
					{
						Elm: &FullValue{
							Elm: &FullValue{
								Elm:       2,
								Elm2:      5,
								Timestamp: testTimestamp,
								Windows:   testWindows,
							},
							Elm2: Restriction{5},
						},
						Elm2:      12.0,
						Timestamp: testTimestamp,
						Windows:   testWindows,
					},
					{
						Elm: &FullValue{
							Elm: &FullValue{
								Elm:       2,
								Elm2:      5,
								Timestamp: testTimestamp,
								Windows:   testWindows,
							},
							Elm2: Restriction{8},
						},
						Elm2:      15.0,
						Timestamp: testTimestamp,
						Windows:   testWindows,
					},
				},
			},
		}
		for _, test := range tests {
			test := test
			t.Run(test.name, func(t *testing.T) {
				fake := &FakeNode{UID: 2}
				node := &SplitAndSizeRestrictions{UID: 1, Fn: test.fn, Out: fake}
				root := &FixedRoot{UID: 0, Elements: []MainInput{{Key: *test.in}}, Out: node}
				units := []Unit{root, node, fake}
				constructAndExecutePlan(t, units)

				for i, got := range fake.Vals {
					if !cmp.Equal(got, test.want[i]) {
						t.Errorf("ProcessElement(%v) has incorrect output %v: got: %v, want: %v",
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
			in   *FullValue
			want *FullValue
		}{
			{
				name: "SingleElem",
				fn:   dfn,
				in: &FullValue{
					Elm: &FullValue{
						Elm: &FullValue{
							Elm:       3,
							Elm2:      nil,
							Timestamp: testTimestamp,
							Windows:   testWindows,
						},
						Elm2: Restriction{5},
					},
					Elm2:      8.0,
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
				want: &FullValue{
					Elm:       8,
					Elm2:      4,
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
			},
			{
				name: "KvElem",
				fn:   kvdfn,
				in: &FullValue{
					Elm: &FullValue{
						Elm: &FullValue{
							Elm:       3,
							Elm2:      10,
							Timestamp: testTimestamp,
							Windows:   testWindows,
						},
						Elm2: Restriction{5},
					},
					Elm2:      18.0,
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
				want: &FullValue{
					Elm:       8,
					Elm2:      12,
					Timestamp: testTimestamp,
					Windows:   testWindows,
				},
			},
		}
		for _, test := range tests {
			test := test
			t.Run(test.name, func(t *testing.T) {
				fake := &FakeNode{UID: 2}
				n := &ParDo{UID: 1, Fn: test.fn, Out: []Node{fake}}
				node := &ProcessSizedElementsAndRestrictions{PDo: n}
				root := &FixedRoot{UID: 0, Elements: []MainInput{{Key: *test.in}}, Out: node}
				units := []Unit{root, node, fake}
				constructAndExecutePlan(t, units)

				got := fake.Vals[0]
				if !cmp.Equal(got, test.want) {
					t.Errorf("ProcessElement(%v) has incorrect output: got: %v, want: %v",
						test.in, got, test.want)
				}
			})
		}
	})
}

// TestSplitAndSizeRestrictions verifies that the SplitAndSizeRestrictions node
// both fulfills its contract and successfully invokes the accompanying SDF's
// SplitRestriction and RestrictionSize methods to do so.
func TestSplitAndSizeRestrictions(t *testing.T) {
}

// TestProcessSizedElementsAndRestrictions verifies that the
// ProcessSizedElementsAndRestrictions node both fulfills its contract and
// successfully invokes the accompanying SDF's CreateTracker and ProcessElement
// methods to do so.
func TestProcessSizedElementsAndRestrictions(t *testing.T) {
}

// FakeNode is used to capture the outputs of the node being tested by
// TestSdfNodes. FakeNode appends each element that it receives in its
// ProcessElement method to its Vals struct, which can then be read by the test.
type FakeNode struct {
	UID  UnitID
	Vals []*FullValue
}

// ID is a no-op.
func (n *FakeNode) ID() UnitID {
	return n.UID
}

// Up is a no-op.
func (*FakeNode) Up(ctx context.Context) error {
	return nil
}

// StartBundle is a no-op.
func (*FakeNode) StartBundle(ctx context.Context, id string, data DataContext) error {
	return nil
}

// ProcessElement appends elm to this FakeNode's slice of elements received.
func (n *FakeNode) ProcessElement(ctx context.Context, elm *FullValue, values ...ReStream) error {
	n.Vals = append(n.Vals, elm)
	return nil
}

// FinishBundle is a no-op.
func (*FakeNode) FinishBundle(ctx context.Context) error {
	return nil
}

// Down is a no-op.
func (*FakeNode) Down(ctx context.Context) error {
	return nil
}
