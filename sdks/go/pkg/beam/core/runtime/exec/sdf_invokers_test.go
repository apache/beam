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
	"github.com/google/go-cmp/cmp"
	"testing"
)

// TestInvokes runs tests on each SDF method invoker, using the SDFs defined
// in this file. Tests both single-element and KV element cases.
func TestInvokes(t *testing.T) {
	// Setup.
	dfn, err := graph.NewDoFn(&Sdf{}, graph.NumMainInputs(graph.MainSingle))
	if err != nil {
		t.Fatalf("invalid function: %v", err)
	}
	sdf := (*graph.SplittableDoFn)(dfn)

	dfn, err = graph.NewDoFn(&KvSdf{}, graph.NumMainInputs(graph.MainKv))
	if err != nil {
		t.Fatalf("invalid function: %v", err)
	}
	kvsdf := (*graph.SplittableDoFn)(dfn)

	// Tests.
	t.Run("createInitialRestrictionCallFn", func(t *testing.T) {
		tests := []struct {
			name string
			sdf  *graph.SplittableDoFn
			elms *FullValue
			want Restriction
		}{
			{"SingleElem", sdf, &FullValue{Elm: 5}, Restriction{5}},
			{"KvElem", kvsdf, &FullValue{Elm: 5, Elm2: 2}, Restriction{7}},
		}
		for _, test := range tests {
			test := test
			fn := test.sdf.CreateInitialRestrictionFn()
			t.Run(test.name, func(t *testing.T) {
				invoker, err := newCreateInitialRestrictionInvoker(fn)
				if err != nil {
					t.Fatalf("newCreateInitialRestrictionInvoker failed: %v", err)
				}
				got := invoker.Invoke(test.elms)
				if !cmp.Equal(got, test.want) {
					t.Errorf("Invoke(%v) has incorrect output: got: %v, want: %v",
						test.elms, got, test.want)
				}
				invoker.Reset()
				for i, arg := range invoker.args {
					if arg != nil {
						t.Errorf("Reset() failed to empty all args. args[%v] = %v", i, arg)
					}
				}
			})
		}
	})

	t.Run("invokeSplitRestriction", func(t *testing.T) {
		tests := []struct {
			name string
			sdf  *graph.SplittableDoFn
			elms *FullValue
			rest Restriction
			want []interface{}
		}{
			{
				"SingleElem",
				sdf,
				&FullValue{Elm: 5},
				Restriction{3},
				[]interface{}{Restriction{8}, Restriction{9}},
			}, {
				"KvElem",
				kvsdf,
				&FullValue{Elm: 5, Elm2: 2},
				Restriction{3},
				[]interface{}{Restriction{8}, Restriction{5}},
			},
		}
		for _, test := range tests {
			test := test
			fn := test.sdf.SplitRestrictionFn()
			t.Run(test.name, func(t *testing.T) {
				invoker, err := newSplitRestrictionInvoker(fn)
				if err != nil {
					t.Fatalf("newSplitRestrictionInvoker failed: %v", err)
				}
				got := invoker.Invoke(test.elms, test.rest)
				if !cmp.Equal(got, test.want) {
					t.Errorf("Invoke(%v, %v) has incorrect output: got: %v, want: %v",
						test.elms, test.rest, got, test.want)
				}
				invoker.Reset()
				for i, arg := range invoker.args {
					if arg != nil {
						t.Errorf("Reset() failed to empty all args. args[%v] = %v", i, arg)
					}
				}
			})
		}
	})

	t.Run("invokeRestrictionSize", func(t *testing.T) {
		tests := []struct {
			name string
			sdf  *graph.SplittableDoFn
			elms *FullValue
			rest Restriction
			want float64
		}{
			{
				"SingleElem",
				sdf,
				&FullValue{Elm: 5},
				Restriction{3},
				8,
			}, {
				"KvElem",
				kvsdf,
				&FullValue{Elm: 5, Elm2: 2},
				Restriction{3},
				10,
			},
		}
		for _, test := range tests {
			test := test
			fn := test.sdf.RestrictionSizeFn()
			t.Run(test.name, func(t *testing.T) {
				invoker, err := newRestrictionSizeInvoker(fn)
				if err != nil {
					t.Fatalf("newRestrictionSizeInvoker failed: %v", err)
				}
				got := invoker.Invoke(test.elms, test.rest)
				if !cmp.Equal(got, test.want) {
					t.Errorf("Invoke(%v, %v) has incorrect output: got: %v, want: %v",
						test.elms, test.rest, got, test.want)
				}
			})
		}
	})

	t.Run("invokeCreateTracker", func(t *testing.T) {
		tests := []struct {
			name string
			sdf  *graph.SplittableDoFn
			rest Restriction
			want *RTracker
		}{
			{
				"SingleElem",
				sdf,
				Restriction{3},
				&RTracker{
					Restriction{3},
					1,
				},
			}, {
				"KvElem",
				kvsdf,
				Restriction{5},
				&RTracker{
					Restriction{5},
					2,
				},
			},
		}
		for _, test := range tests {
			test := test
			fn := test.sdf.CreateTrackerFn()
			t.Run(test.name, func(t *testing.T) {
				invoker, err := newCreateTrackerInvoker(fn)
				if err != nil {
					t.Fatalf("newCreateTrackerInvoker failed: %v", err)
				}
				got := invoker.Invoke(test.rest)
				if !cmp.Equal(got, test.want) {
					t.Errorf("Invoke(%v) has incorrect output: got: %v, want: %v",
						test.rest, got, test.want)
				}
			})
		}
	})
}

type Restriction struct {
	Val int
}

// RTracker's methods can all be no-ops, we just need it to implement sdf.RTracker.
type RTracker struct {
	Rest Restriction
	Val  int
}

func (rt *RTracker) TryClaim(interface{}) bool                      { return false }
func (rt *RTracker) GetError() error                                { return nil }
func (rt *RTracker) TrySplit(fraction float64) (interface{}, error) { return nil, nil }
func (rt *RTracker) GetProgress() float64                           { return 0 }
func (rt *RTracker) IsDone() bool                                   { return false }

// In order to test that these methods get called properly, each one has an
// implementation that lets us confirm that each argument was passed properly.

type Sdf struct {
}

// CreateInitialRestriction creates a restriction with the given value.
func (fn *Sdf) CreateInitialRestriction(i int) Restriction {
	return Restriction{i}
}

// SplitRestriction outputs two restrictions, the first containing the sum of i
// and rest.Val, the second containing the same value plus 1.
func (fn *Sdf) SplitRestriction(i int, rest Restriction) []Restriction {
	return []Restriction{{rest.Val + i}, {rest.Val + i + 1}}
}

// RestrictionSize returns the sum of i and rest.Val as a float64.
func (fn *Sdf) RestrictionSize(i int, rest Restriction) float64 {
	return (float64)(i + rest.Val)
}

// CreateTracker creates an RTracker containing the given restriction and a Val
// of 1.
func (fn *Sdf) CreateTracker(rest Restriction) *RTracker {
	return &RTracker{rest, 1}
}

// ProcessElement is a no-op, it's only included to pass validation.
func (fn *Sdf) ProcessElement(*RTracker, int) int {
	return 0
}

type KvSdf struct {
}

// CreateInitialRestriction creates a restriction with the sum of the given
// values.
func (fn *KvSdf) CreateInitialRestriction(i int, j int) Restriction {
	return Restriction{i + j}
}

// SplitRestriction outputs two restrictions, the first containing the sum of i
// and rest.Val, the second containing the sum of j and rest.Val.
func (fn *KvSdf) SplitRestriction(i int, j int, rest Restriction) []Restriction {
	return []Restriction{{rest.Val + i}, {rest.Val + j}}
}

// RestrictionSize returns the sum of i, j, and rest.Val as a float64.
func (fn *KvSdf) RestrictionSize(i int, j int, rest Restriction) float64 {
	return (float64)(i + j + rest.Val)
}

// CreateTracker creates an RTracker containing the given restriction and a Val
// of 2.
func (fn *KvSdf) CreateTracker(rest Restriction) *RTracker {
	return &RTracker{rest, 2}
}

// ProcessElement is a no-op, it's only included to pass validation.
func (fn *KvSdf) ProcessElement(*RTracker, int, int) int {
	return 0
}
