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
	"errors"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
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

	dfn, err = graph.NewDoFn(&VetSdfStatefulWatermark{}, graph.NumMainInputs(graph.MainSingle))
	if err != nil {
		t.Fatalf("invalid function: %v", err)
	}
	statefulWeFn := (*graph.SplittableDoFn)(dfn)

	initialRestErrDfn, err := graph.NewDoFn(
		&VetCreateInitialRestrictionErrSdf{},
		graph.NumMainInputs(graph.MainSingle),
	)
	if err != nil {
		t.Fatalf("invalid function: %v", err)
	}
	initialRestErrSdf := (*graph.SplittableDoFn)(initialRestErrDfn)

	splitRestErrDfn, err := graph.NewDoFn(
		&VetSplitRestrictionErrSdf{},
		graph.NumMainInputs(graph.MainSingle),
	)
	if err != nil {
		t.Fatalf("invalid function: %v", err)
	}
	splitRestErrSdf := (*graph.SplittableDoFn)(splitRestErrDfn)

	restSizeErrDfn, err := graph.NewDoFn(
		&VetRestrictionSizeErrSdf{},
		graph.NumMainInputs(graph.MainSingle),
	)
	if err != nil {
		t.Fatalf("invalid function: %v", err)
	}
	restSizeErrSdf := (*graph.SplittableDoFn)(restSizeErrDfn)

	trackerErrDfn, err := graph.NewDoFn(
		&VetCreateTrackerErrSdf{},
		graph.NumMainInputs(graph.MainSingle),
	)
	if err != nil {
		t.Fatalf("invalid function: %v", err)
	}
	trackerErrSdf := (*graph.SplittableDoFn)(trackerErrDfn)

	truncateRestErrDfn, err := graph.NewDoFn(
		&VetTruncateRestrictionErrSdf{},
		graph.NumMainInputs(graph.MainSingle),
	)
	if err != nil {
		t.Fatalf("invalid function: %v", err)
	}
	truncateRestErrSdf := (*graph.SplittableDoFn)(truncateRestErrDfn)

	// Tests.
	t.Run("CreateInitialRestriction Invoker (cirInvoker)", func(t *testing.T) {
		tests := []struct {
			name    string
			sdf     *graph.SplittableDoFn
			elms    *FullValue
			want    *VetRestriction
			wantErr bool
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
			{
				name:    "Error",
				sdf:     initialRestErrSdf,
				elms:    &FullValue{Elm: 1},
				wantErr: true,
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

				got, err := invoker.Invoke(context.Background(), test.elms)
				if (err != nil) != test.wantErr {
					t.Fatalf("Invoke(%v) error = %v, wantErr %v", test.elms, err, test.wantErr)
				}
				if test.wantErr {
					return
				}

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
			name    string
			sdf     *graph.SplittableDoFn
			elms    *FullValue
			rest    *VetRestriction
			want    []any
			wantErr bool
		}{
			{
				name: "SingleElem",
				sdf:  sdf,
				elms: &FullValue{Elm: 1},
				rest: &VetRestriction{ID: "Sdf"},
				want: []any{
					&VetRestriction{ID: "Sdf.1", SplitRest: true, Val: 1},
					&VetRestriction{ID: "Sdf.2", SplitRest: true, Val: 1},
				},
			}, {
				name: "KvElem",
				sdf:  kvsdf,
				elms: &FullValue{Elm: 1, Elm2: 2},
				rest: &VetRestriction{ID: "KvSdf"},
				want: []any{
					&VetRestriction{ID: "KvSdf.1", SplitRest: true, Key: 1, Val: 2},
					&VetRestriction{ID: "KvSdf.2", SplitRest: true, Key: 1, Val: 2},
				},
			},
			{
				name:    "Error",
				sdf:     splitRestErrSdf,
				elms:    &FullValue{Elm: 1},
				rest:    &VetRestriction{ID: "Sdf"},
				wantErr: true,
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
				got, err := invoker.Invoke(context.Background(), test.elms, &rest)
				if (err != nil) != test.wantErr {
					t.Fatalf("Invoke(%v, %v) error = %v, wantErr %v", test.elms, test.rest, err, test.wantErr)
				}
				if test.wantErr {
					return
				}

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
			wantErr  bool
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
			{
				name:    "Error",
				sdf:     restSizeErrSdf,
				elms:    &FullValue{Elm: 1},
				rest:    &VetRestriction{ID: "Sdf"},
				wantErr: true,
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

				got, err := invoker.Invoke(context.Background(), test.elms, &rest)
				if (err != nil) != test.wantErr {
					t.Fatalf("Invoke(%v, %v) error = %v, wantErr %v", test.elms, test.rest, err, test.wantErr)
				}
				if test.wantErr {
					return
				}

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
			name    string
			sdf     *graph.SplittableDoFn
			rest    *VetRestriction
			want    *VetRTracker
			wantErr bool
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
			{
				name:    "Error",
				sdf:     trackerErrSdf,
				rest:    &VetRestriction{ID: "Sdf"},
				wantErr: true,
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

				got, err := invoker.Invoke(context.Background(), test.rest)
				if (err != nil) != test.wantErr {
					t.Fatalf("Invoke(%v) error = %v, wantErr %v", test.rest, err, test.wantErr)
				}
				if test.wantErr {
					return
				}

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

	t.Run("CreateWatermarkEstimator Invoker (cweInvoker)", func(t *testing.T) {
		tests := []struct {
			name  string
			sdf   *graph.SplittableDoFn
			state int
			want  VetWatermarkEstimator
		}{
			{
				name:  "Non-stateful",
				sdf:   sdf,
				state: 1,
				want:  VetWatermarkEstimator{State: -1},
			}, {
				name:  "Stateful",
				sdf:   statefulWeFn,
				state: 11,
				want:  VetWatermarkEstimator{State: 11},
			},
		}

		for _, test := range tests {
			test := test
			fn := test.sdf.CreateWatermarkEstimatorFn()
			t.Run(test.name, func(t *testing.T) {
				invoker, err := newCreateWatermarkEstimatorInvoker(fn)
				if err != nil {
					t.Fatalf("newCreateWatermarkEstimatorInvoker failed: %v", err)
				}
				got := invoker.Invoke(test.state)
				want := &test.want
				if !cmp.Equal(got, want) {
					t.Errorf("Invoke() has incorrect output: got: %v, want: %v", got, want)
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

	t.Run("InitialWatermarkEstimatorState Invoker (iwesInvoker)", func(t *testing.T) {
		fn := statefulWeFn.InitialWatermarkEstimatorStateFn()
		invoker, err := newInitialWatermarkEstimatorStateInvoker(fn)
		if err != nil {
			t.Fatalf("newInitialWatermarkEstimatorStateInvoker failed: %v", err)
		}
		got := invoker.Invoke(&VetRestriction{ID: "Sdf"}, &FullValue{Elm: 1, Timestamp: mtime.ZeroTimestamp})
		want := 1
		if got != want {
			t.Errorf("Invoke() has incorrect output: got: %v, want: %v", got, want)
		}
	})

	t.Run("WatermarkEstimatorState Invoker (wesInvoker)", func(t *testing.T) {
		fn := statefulWeFn.WatermarkEstimatorStateFn()
		invoker, err := newWatermarkEstimatorStateInvoker(fn)
		if err != nil {
			t.Fatalf("newWatermarkEstimatorStateInvoker failed: %v", err)
		}
		got := invoker.Invoke(&VetWatermarkEstimator{State: 11})
		want := 11
		if got != want {
			t.Errorf("Invoke() has incorrect output: got: %v, want: %v", got, want)
		}
	})

	t.Run("TruncateRestriction Invoker (trInvoker)", func(t *testing.T) {
		tests := []struct {
			name    string
			sdf     *graph.SplittableDoFn
			elms    *FullValue
			rest    *VetRestriction
			want    any
			wantErr bool
		}{
			{
				name: "SingleElem",
				sdf:  sdf,
				elms: &FullValue{Elm: 1},
				rest: &VetRestriction{ID: "Sdf"},
				want: &VetRestriction{ID: "Sdf", CreateTracker: true, TruncateRest: true, RestSize: true, Val: 1},
			}, {
				name: "KvElem",
				sdf:  kvsdf,
				elms: &FullValue{Elm: 1, Elm2: 2},
				rest: &VetRestriction{ID: "KvSdf"},
				want: &VetRestriction{ID: "KvSdf", CreateTracker: true, TruncateRest: true, RestSize: true, Key: 1, Val: 2},
			},
			{
				name:    "Error",
				sdf:     truncateRestErrSdf,
				elms:    &FullValue{Elm: 1},
				rest:    &VetRestriction{ID: "Sdf"},
				wantErr: true,
			},
		}
		for _, test := range tests {
			test := test
			fn := test.sdf.TruncateRestrictionFn()
			ctFn := test.sdf.CreateTrackerFn()
			rsFn := test.sdf.RestrictionSizeFn()
			t.Run(test.name, func(t *testing.T) {
				ctx := context.Background()
				rest := test.rest // Create a copy because our test SDF edits the restriction.
				ctInvoker, err := newCreateTrackerInvoker(ctFn)
				if err != nil {
					t.Fatalf("newCreateTrackerInvoker failed: %v", err)
				}
				rt, err := ctInvoker.Invoke(ctx, rest)
				if err != nil {
					t.Fatalf("ctInvoker.Invoke(%v) failed: %v", rest, err)
				}

				trInvoker, err := newTruncateRestrictionInvoker(fn)
				if err != nil {
					t.Fatalf("newTruncateRestrictionInvoker failed: %v", err)
				}

				trRest, err := trInvoker.Invoke(ctx, rt, test.elms)
				if (err != nil) != test.wantErr {
					t.Fatalf("trInvoker.Invoke(%v, %v) = %v, wantErr %v", rt, test.elms, err, test.wantErr)
				}
				if test.wantErr {
					return
				}

				rsInvoker, err := newRestrictionSizeInvoker(rsFn)
				if err != nil {
					t.Fatalf("newRestrictionSizeInvoker failed: %v", err)
				}
				if _, err := rsInvoker.Invoke(ctx, test.elms, trRest); err != nil {
					t.Fatalf("rsInvoker.Invoke(%v, %v) failed: %v", test.elms, trRest, err)
				}

				if !cmp.Equal(trRest, test.want) {
					t.Errorf("Invoke(%v, %v) has incorrect output: got: %v, want: %v",
						test.elms, test.rest, trRest, test.want)
				}
				trInvoker.Reset()
				for i, arg := range trInvoker.args {
					if arg != nil {
						t.Errorf("Reset() failed to empty all args. args[%v] = %v", i, arg)
					}
				}
			})
		}
	})

	t.Run("Default TruncateRestriction Invoker", func(t *testing.T) {
		tests := []struct {
			name string
			sdf  *graph.SplittableDoFn
			elms *FullValue
			rest *VetRestriction
			want any
		}{
			{
				name: "SingleElem",
				sdf:  sdf,
				elms: &FullValue{Elm: 1},
				rest: &VetRestriction{ID: "Sdf", Bounded: true},
				want: &VetRestriction{ID: "Sdf", Bounded: true, CreateTracker: true, RestSize: true, Val: 1},
			},
			{
				name: "SingleElem",
				sdf:  sdf,
				elms: &FullValue{Elm: 1},
				rest: &VetRestriction{ID: "Sdf", Bounded: false},
				want: &VetRestriction{ID: "Sdf", Bounded: false, CreateTracker: true, RestSize: false, Val: 1},
			},
			{
				name: "KvElem",
				sdf:  kvsdf,
				elms: &FullValue{Elm: 1, Elm2: 2},
				rest: &VetRestriction{ID: "KvSdf", Bounded: true},
				want: &VetRestriction{ID: "KvSdf", Bounded: true, CreateTracker: true, RestSize: true, Key: 1, Val: 2},
			},
			{
				name: "KvElem",
				sdf:  kvsdf,
				elms: &FullValue{Elm: 1, Elm2: 2},
				rest: &VetRestriction{ID: "KvSdf", Bounded: false},
				want: &VetRestriction{ID: "KvSdf", Bounded: false, CreateTracker: true, RestSize: false, Key: 1, Val: 2},
			},
		}

		for _, test := range tests {
			test := test
			ctFn := test.sdf.CreateTrackerFn()
			rsFn := test.sdf.RestrictionSizeFn()
			t.Run(test.name, func(t *testing.T) {
				ctx := context.Background()
				rest := test.rest // Create a copy because our test SDF edits the restriction.
				ctInvoker, err := newCreateTrackerInvoker(ctFn)
				if err != nil {
					t.Fatalf("newCreateTrackerInvoker failed: %v", err)
				}
				rt, err := ctInvoker.Invoke(ctx, rest)
				if err != nil {
					t.Fatalf("ctInvoker.Invoke(%v) failed: %v", rest, err)
				}

				trInvoker, err := newDefaultTruncateRestrictionInvoker()
				if err != nil {
					t.Fatalf("newTruncateRestrictionInvoker failed: %v", err)
				}
				trRest, err := trInvoker.Invoke(ctx, rt, test.elms)
				if err != nil {
					t.Fatalf("trInvoker.Invoke(%v, %v) failed: %v", rt, test.elms, err)
				}

				if trRest != nil {
					rsInvoker, err := newRestrictionSizeInvoker(rsFn)
					if err != nil {
						t.Fatalf("newRestrictionSizeInvoker failed: %v", err)
					}
					if _, err := rsInvoker.Invoke(ctx, test.elms, trRest); err != nil {
						t.Fatalf("rsInvoker.Invoke(%v, %v) failed: %v", test.elms, trRest, err)
					}

					if !cmp.Equal(trRest, test.want) {
						t.Errorf("Invoke(%v, %v) has incorrect output: got: %v, want: %v",
							test.elms, test.rest, trRest, test.want)
					}
					trInvoker.Reset()
					for i, arg := range trInvoker.args {
						if arg != nil {
							t.Errorf("Reset() failed to empty all args. args[%v] = %v", i, arg)
						}
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
type VetRestriction struct {
	// An identifier to differentiate restrictions on the same elements. When
	// split, a suffix in the form of ".#" is appended to this ID.
	ID string

	// Key and Val just copy the last seen input element's key and value to
	// confirm that the restriction saw the expected element.
	Key, Val any

	// Bounded just tells if the restriction is bounded or not
	Bounded bool

	// These booleans should be flipped to true by the corresponding SDF methods
	// to prove that the methods got called on the restriction.
	CreateRest, SplitRest, RestSize, CreateTracker, ProcessElm, TruncateRest bool
}

func (r VetRestriction) copy() VetRestriction {
	return r
}

// VetRTracker's methods can all be no-ops, we just need it to implement
// sdf.RTracker and allow validating that it was passed to ProcessElement.
type VetRTracker struct {
	Rest *VetRestriction
}

func (rt *VetRTracker) TryClaim(any) bool               { return false }
func (rt *VetRTracker) GetError() error                 { return nil }
func (rt *VetRTracker) GetProgress() (float64, float64) { return 0, 0 }
func (rt *VetRTracker) IsDone() bool                    { return true }
func (rt *VetRTracker) GetRestriction() any             { return nil }
func (rt *VetRTracker) TrySplit(_ float64) (any, any, error) {
	return nil, nil, nil
}
func (rt *VetRTracker) IsBounded() bool { return rt.Rest.Bounded }

type VetWatermarkEstimator struct {
	State int
}

func (e *VetWatermarkEstimator) CurrentWatermark() time.Time {
	return time.Date(2022, time.January, 1, 1, 0, 0, 0, time.UTC)
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

// TruncateRestriction truncates the restriction into half.
func (fn *VetSdf) TruncateRestriction(rest *VetRTracker, i int) *VetRestriction {
	rest.Rest.TruncateRest = true
	return rest.Rest
}

// CreateTracker creates an RTracker containing the given restriction and flips
// the appropriate flags on the restriction to track that this was called.
func (fn *VetSdf) CreateTracker(rest *VetRestriction) *VetRTracker {
	rest.CreateTracker = true
	return &VetRTracker{rest}
}

// CreateWatermarkEstimator creates a watermark estimator to be used by the Sdf
func (fn *VetSdf) CreateWatermarkEstimator() *VetWatermarkEstimator {
	return &VetWatermarkEstimator{State: -1}
}

// ProcessElement emits the restriction from the restriction tracker it
// received, with the appropriate flags flipped to track that this was called.
// Note that emitting restrictions is discouraged in normal usage. It is only
// done here to allow validating that ProcessElement is being executed
// properly.
func (fn *VetSdf) ProcessElement(rt *VetRTracker, i int, emit func(*VetRestriction)) sdf.ProcessContinuation {
	rest := rt.Rest
	rest.Key = nil
	rest.Val = i
	rest.ProcessElm = true
	emit(rest)
	return sdf.ResumeProcessingIn(1 * time.Second)
}

type VetSdfStatefulWatermark struct {
}

func (fn *VetSdfStatefulWatermark) CreateInitialRestriction(i int) *VetRestriction {
	return &VetRestriction{ID: "Sdf", Val: i, CreateRest: true}
}

func (fn *VetSdfStatefulWatermark) SplitRestriction(i int, rest *VetRestriction) []*VetRestriction {
	rest.SplitRest = true
	rest.Val = i

	rest1 := rest.copy()
	rest1.ID += ".1"
	rest2 := rest.copy()
	rest2.ID += ".2"

	return []*VetRestriction{&rest1, &rest2}
}

func (fn *VetSdfStatefulWatermark) RestrictionSize(i int, rest *VetRestriction) float64 {
	rest.Key = nil
	rest.Val = i
	rest.RestSize = true
	return (float64)(i)
}

func (fn *VetSdfStatefulWatermark) CreateTracker(rest *VetRestriction) *VetRTracker {
	rest.CreateTracker = true
	return &VetRTracker{rest}
}

func (fn *VetSdfStatefulWatermark) InitialWatermarkEstimatorState(_ typex.EventTime, _ *VetRestriction, element int) int {
	return 1
}

func (fn *VetSdfStatefulWatermark) CreateWatermarkEstimator(state int) *VetWatermarkEstimator {
	return &VetWatermarkEstimator{State: state}
}

func (fn *VetSdfStatefulWatermark) WatermarkEstimatorState(e *VetWatermarkEstimator) int {
	return e.State
}

func (fn *VetSdfStatefulWatermark) ProcessElement(rt *VetRTracker, i int, emit func(*VetRestriction)) {
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

// TruncateRestriction truncates the restriction tracked by VetRTracker.
func (fn *VetKvSdf) TruncateRestriction(rest *VetRTracker, i, j int) *VetRestriction {
	rest.Rest.TruncateRest = true
	return rest.Rest
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
func (fn *VetKvSdf) ProcessElement(rt *VetRTracker, i, j int, emit func(*VetRestriction)) sdf.ProcessContinuation {
	rest := rt.Rest
	rest.Key = i
	rest.Val = j
	rest.ProcessElm = true
	emit(rest)
	return sdf.ResumeProcessingIn(1 * time.Second)
}

// VetEmptyInitialSplitSdf runs an SDF in order to test that these methods get called properly,
// each method will flip the corresponding flag in the passed in VetRestriction,
// overwrite the restriction's Key and Val with the last seen input elements,
// and retain the other fields in the VetRestriction.
type VetEmptyInitialSplitSdf struct {
}

// CreateInitialRestriction creates a restriction with the given values and
// with the appropriate flags to track that this was called.
func (fn *VetEmptyInitialSplitSdf) CreateInitialRestriction(i int) *VetRestriction {
	return &VetRestriction{ID: "EmptySdf", Val: i, CreateRest: true}
}

// SplitRestriction outputs zero restrictions.
func (fn *VetEmptyInitialSplitSdf) SplitRestriction(i int, rest *VetRestriction) []*VetRestriction {
	return []*VetRestriction{}
}

// RestrictionSize just returns i as the size, as well as flipping appropriate
// flags on the restriction to track that this was called.
func (fn *VetEmptyInitialSplitSdf) RestrictionSize(i int, rest *VetRestriction) float64 {
	rest.Key = nil
	rest.Val = i
	rest.RestSize = true
	return (float64)(i)
}

// TruncateRestriction truncates the restriction into half.
func (fn *VetEmptyInitialSplitSdf) TruncateRestriction(rest *VetRTracker, i int) *VetRestriction {
	rest.Rest.TruncateRest = true
	return rest.Rest
}

// CreateTracker creates an RTracker containing the given restriction and flips
// the appropriate flags on the restriction to track that this was called.
func (fn *VetEmptyInitialSplitSdf) CreateTracker(rest *VetRestriction) *VetRTracker {
	rest.CreateTracker = true
	return &VetRTracker{rest}
}

// ProcessElement emits the restriction from the restriction tracker it
// received, with the appropriate flags flipped to track that this was called.
// Note that emitting restrictions is discouraged in normal usage. It is only
// done here to allow validating that ProcessElement is being executed
// properly.
func (fn *VetEmptyInitialSplitSdf) ProcessElement(rt *VetRTracker, i int, emit func(*VetRestriction)) sdf.ProcessContinuation {
	rest := rt.Rest
	rest.Key = nil
	rest.Val = i
	rest.ProcessElm = true
	emit(rest)
	return sdf.ResumeProcessingIn(1 * time.Second)
}

var errSdf = errors.New("SDF error")

// VetCreateInitialRestrictionErrSdf is an SDF with a CreateInitialRestriction method
// that returns a non-nil error.
type VetCreateInitialRestrictionErrSdf struct {
	VetSdf
}

func (fn *VetCreateInitialRestrictionErrSdf) CreateInitialRestriction(i int) (*VetRestriction, error) {
	return nil, errSdf
}

// VetSplitRestrictionErrSdf is an SDF with a SplitRestriction method
// that returns a non-nil error.
type VetSplitRestrictionErrSdf struct {
	VetSdf
}

func (fn *VetSplitRestrictionErrSdf) SplitRestriction(int, *VetRestriction) ([]*VetRestriction, error) {
	return nil, errSdf
}

// VetRestrictionSizeErrSdf is an SDF with a RestrictionSize method
// that returns a non-nil error.
type VetRestrictionSizeErrSdf struct {
	VetSdf
}

func (fn *VetRestrictionSizeErrSdf) RestrictionSize(int, *VetRestriction) (float64, error) {
	return -1, errSdf
}

// VetCreateTrackerErrSdf is an SDF with a CreateTracker method
// that returns a non-nil error.
type VetCreateTrackerErrSdf struct {
	VetSdf
}

func (fn *VetCreateTrackerErrSdf) CreateTracker(*VetRestriction) (*VetRTracker, error) {
	return nil, errSdf
}

// VetTruncateRestrictionErrSdf is an SDF with a TruncateRestriction method
// that returns a non-nil error.
type VetTruncateRestrictionErrSdf struct {
	VetSdf
}

func (fn *VetTruncateRestrictionErrSdf) TruncateRestriction(*VetRTracker, int) (*VetRestriction, error) {
	return nil, errSdf
}
