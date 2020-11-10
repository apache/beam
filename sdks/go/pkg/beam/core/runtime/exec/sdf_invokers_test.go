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
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/google/go-cmp/cmp"
)

// TestInvokes runs tests on each SDF method invoker, using the SDFs defined
// in this file. Tests both single-element and KV element cases.
func TestInvokes(t *testing.T) {
	// Setup.
	dfn, err := graph.NewDoFn(&VetSdf{}, graph.NumMainInputs(graph.MainSingle))
	if err != nil {
		t.Fatalf("invalid function: %v", err)
	}
	sdf := (*graph.SplittableDoFn)(dfn)

	dfn, err = graph.NewDoFn(&VetKvSdf{}, graph.NumMainInputs(graph.MainKv))
	if err != nil {
		t.Fatalf("invalid function: %v", err)
	}
	kvsdf := (*graph.SplittableDoFn)(dfn)

	// Tests.
	t.Run("CreateInitialRestriction Invoker (cirInvoker)", func(t *testing.T) {
		tests := []struct {
			name string
			sdf  *graph.SplittableDoFn
			elms *FullValue
			want *VetRestriction
		}{
			{
				name: "SingleElem",
				sdf:  sdf,
				elms: &FullValue{Elm: 1},
				want: &VetRestriction{ID: "Sdf", CreateRest: true, Val: 1},
			},
			{
				name: "KvElem",
				sdf:  kvsdf,
				elms: &FullValue{Elm: 1, Elm2: 2},
				want: &VetRestriction{ID: "KvSdf", CreateRest: true, Key: 1, Val: 2},
			},
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

	t.Run("SplitRestriction Invoker (srInvoker)", func(t *testing.T) {
		tests := []struct {
			name string
			sdf  *graph.SplittableDoFn
			elms *FullValue
			rest *VetRestriction
			want []interface{}
		}{
			{
				name: "SingleElem",
				sdf:  sdf,
				elms: &FullValue{Elm: 1},
				rest: &VetRestriction{ID: "Sdf"},
				want: []interface{}{
					&VetRestriction{ID: "Sdf.1", SplitRest: true, Val: 1},
					&VetRestriction{ID: "Sdf.2", SplitRest: true, Val: 1},
				},
			}, {
				name: "KvElem",
				sdf:  kvsdf,
				elms: &FullValue{Elm: 1, Elm2: 2},
				rest: &VetRestriction{ID: "KvSdf"},
				want: []interface{}{
					&VetRestriction{ID: "KvSdf.1", SplitRest: true, Key: 1, Val: 2},
					&VetRestriction{ID: "KvSdf.2", SplitRest: true, Key: 1, Val: 2},
				},
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
				rest := *test.rest // Create a copy because our test SDF edits the restriction.
				got := invoker.Invoke(test.elms, &rest)
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

	t.Run("RestrictionSize Invoker (rsInvoker)", func(t *testing.T) {
		tests := []struct {
			name     string
			sdf      *graph.SplittableDoFn
			elms     *FullValue
			rest     *VetRestriction
			want     float64
			restWant *VetRestriction
		}{
			{
				name:     "SingleElem",
				sdf:      sdf,
				elms:     &FullValue{Elm: 1},
				rest:     &VetRestriction{ID: "Sdf"},
				want:     1,
				restWant: &VetRestriction{ID: "Sdf", RestSize: true, Val: 1},
			}, {
				name:     "KvElem",
				sdf:      kvsdf,
				elms:     &FullValue{Elm: 1, Elm2: 2},
				rest:     &VetRestriction{ID: "KvSdf"},
				want:     3,
				restWant: &VetRestriction{ID: "KvSdf", RestSize: true, Key: 1, Val: 2},
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
				rest := *test.rest // Create a copy because our test SDF edits the restriction.
				got := invoker.Invoke(test.elms, &rest)
				if !cmp.Equal(got, test.want) {
					t.Errorf("Invoke(%v, %v) has incorrect output: got: %v, want: %v",
						test.elms, test.rest, got, test.want)
				}
				if got, want := &rest, test.restWant; !cmp.Equal(got, want) {
					t.Errorf("Invoke(%v, %v) has incorrectly handled restriction: got: %v, want: %v",
						test.elms, test.rest, got, want)
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

	t.Run("CreateTracker Invoker (ctInvoker)", func(t *testing.T) {
		tests := []struct {
			name string
			sdf  *graph.SplittableDoFn
			rest *VetRestriction
			want *VetRTracker
		}{
			{
				name: "SingleElem",
				sdf:  sdf,
				rest: &VetRestriction{ID: "Sdf"},
				want: &VetRTracker{&VetRestriction{ID: "Sdf", CreateTracker: true}},
			}, {
				name: "KvElem",
				sdf:  kvsdf,
				rest: &VetRestriction{ID: "KvSdf"},
				want: &VetRTracker{&VetRestriction{ID: "KvSdf", CreateTracker: true}},
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
				invoker.Reset()
				for i, arg := range invoker.args {
					if arg != nil {
						t.Errorf("Reset() failed to empty all args. args[%v] = %v", i, arg)
					}
				}
			})
		}
	})
}

// VetRestriction is a restriction used for validating that SDF methods get
// called with it. When using VetRestriction, the SDF methods it's used in
// should pass it as a pointer so the method can make changes to the restriction
// even if it doesn't output one directly (such as RestrictionSize).
//
type VetRestriction struct {
	// An identifier to differentiate restrictions on the same elements. When
	// split, a suffix in the form of ".#" is appended to this ID.
	ID string

	// Key and Val just copy the last seen input element's key and value to
	// confirm that the restriction saw the expected element.
	Key, Val interface{}

	// These booleans should be flipped to true by the corresponding SDF methods
	// to prove that the methods got called on the restriction.
	CreateRest, SplitRest, RestSize, CreateTracker, ProcessElm bool
}

func (r VetRestriction) copy() VetRestriction {
	return r
}

// VetRTracker's methods can all be no-ops, we just need it to implement
// sdf.RTracker and allow validating that it was passed to ProcessElement.
type VetRTracker struct {
	Rest *VetRestriction
}

func (rt *VetRTracker) TryClaim(interface{}) bool       { return false }
func (rt *VetRTracker) GetError() error                 { return nil }
func (rt *VetRTracker) GetProgress() (float64, float64) { return 0, 0 }
func (rt *VetRTracker) IsDone() bool                    { return true }
func (rt *VetRTracker) GetRestriction() interface{}     { return nil }
func (rt *VetRTracker) TrySplit(_ float64) (interface{}, interface{}, error) {
	return nil, nil, nil
}

// VetSdf runs an SDF In order to test that these methods get called properly,
// each method will flip the corresponding flag in the passed in VetRestriction,
// overwrite the restriction's Key and Val with the last seen input elements,
// and retain the other fields in the VetRestriction.
type VetSdf struct {
}

// CreateInitialRestriction creates a restriction with the given values and
// with the appropriate flags to track that this was called.
func (fn *VetSdf) CreateInitialRestriction(i int) *VetRestriction {
	return &VetRestriction{ID: "Sdf", Val: i, CreateRest: true}
}

// SplitRestriction outputs two identical restrictions, each being a copy of the
// initial one, but with the appropriate flags to track this was called. The
// split restrictions add a suffix of the form ".#" to the ID.
func (fn *VetSdf) SplitRestriction(i int, rest *VetRestriction) []*VetRestriction {
	rest.SplitRest = true
	rest.Val = i

	rest1 := rest.copy()
	rest1.ID += ".1"
	rest2 := rest.copy()
	rest2.ID += ".2"

	return []*VetRestriction{&rest1, &rest2}
}

// RestrictionSize just returns i as the size, as well as flipping appropriate
// flags on the restriction to track that this was called.
func (fn *VetSdf) RestrictionSize(i int, rest *VetRestriction) float64 {
	rest.Key = nil
	rest.Val = i
	rest.RestSize = true
	return (float64)(i)
}

// CreateTracker creates an RTracker containing the given restriction and flips
// the appropriate flags on the restriction to track that this was called.
func (fn *VetSdf) CreateTracker(rest *VetRestriction) *VetRTracker {
	rest.CreateTracker = true
	return &VetRTracker{rest}
}

// ProcessElement emits the restriction from the restriction tracker it
// received, with the appropriate flags flipped to track that this was called.
// Note that emitting restrictions is discouraged in normal usage. It is only
// done here to allow validating that ProcessElement is being executed
// properly.
func (fn *VetSdf) ProcessElement(rt *VetRTracker, i int, emit func(*VetRestriction)) {
	rest := rt.Rest
	rest.Key = nil
	rest.Val = i
	rest.ProcessElm = true
	emit(rest)
}

// VetKvSdf runs an SDF In order to test that these methods get called properly,
// each method will flip the corresponding flag in the passed in VetRestriction,
// overwrite the restriction's Key and Val with the last seen input elements,
// and retain the other fields in the VetRestriction.
type VetKvSdf struct {
}

// CreateInitialRestriction creates a restriction with the given values and
// with the appropriate flags to track that this was called.
func (fn *VetKvSdf) CreateInitialRestriction(i, j int) *VetRestriction {
	return &VetRestriction{ID: "KvSdf", Key: i, Val: j, CreateRest: true}
}

// SplitRestriction outputs two identical restrictions, each being a copy of the
// initial one, but with the appropriate flags to track this was called. The
// split restrictions add a suffix of the form ".#" to the ID.
func (fn *VetKvSdf) SplitRestriction(i, j int, rest *VetRestriction) []*VetRestriction {
	rest.SplitRest = true
	rest.Key = i
	rest.Val = j

	rest1 := rest.copy()
	rest1.ID += ".1"
	rest2 := rest.copy()
	rest2.ID += ".2"

	return []*VetRestriction{&rest1, &rest2}
}

// RestrictionSize just returns the sum of i and j as the size, as well as
// flipping appropriate flags on the restriction to track that this was called.
func (fn *VetKvSdf) RestrictionSize(i, j int, rest *VetRestriction) float64 {
	rest.Key = i
	rest.Val = j
	rest.RestSize = true
	return (float64)(i + j)
}

// CreateTracker creates an RTracker containing the given restriction and flips
// the appropriate flags on the restriction to track that this was called.
func (fn *VetKvSdf) CreateTracker(rest *VetRestriction) *VetRTracker {
	rest.CreateTracker = true
	return &VetRTracker{rest}
}

// ProcessElement emits the restriction from the restriction tracker it
// received, with the appropriate flags flipped to track that this was called.
// Note that emitting restrictions is discouraged in normal usage. It is only
// done here to allow validating that ProcessElement is being executed
// properly.
func (fn *VetKvSdf) ProcessElement(rt *VetRTracker, i, j int, emit func(*VetRestriction)) {
	rest := rt.Rest
	rest.Key = i
	rest.Val = j
	rest.ProcessElm = true
	emit(rest)
}
