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

package offsetrange

import (
	"fmt"
	"math"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// TestRestriction_EvenSplits tests various splits and checks that they all
// follow the contract for EvenSplits. This means that all restrictions are
// evenly split, that each restriction has at least one element, and that each
// element is present in the split restrictions.
func TestRestriction_EvenSplits(t *testing.T) {
	tests := []struct {
		rest Restriction
		num  int64
	}{
		{rest: Restriction{Start: 0, End: 21}, num: 4},
		{rest: Restriction{Start: 21, End: 42}, num: 4},
		{rest: Restriction{Start: 0, End: 5}, num: 10},
		{rest: Restriction{Start: 0, End: 21}, num: -1},
	}
	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("(rest[%v, %v], splits = %v)",
			test.rest.Start, test.rest.End, test.num), func(t *testing.T) {
			r := test.rest

			// Get the minimum size that a split restriction can be. Max size
			// should be min + 1. This way we can check the size of each split.
			num := test.num
			if num <= 1 {
				num = 1
			}
			min := (r.End - r.Start) / num

			splits := r.EvenSplits(test.num)
			prevEnd := r.Start
			for _, split := range splits {
				size := split.End - split.Start
				// Check: Each restriction has at least 1 element.
				if size == 0 {
					t.Errorf("split restriction [%v, %v] is empty, size must be greater than 0.",
						split.Start, split.End)
				}
				// Check: Restrictions are evenly split.
				if size != min && size != min+1 {
					t.Errorf("split restriction [%v, %v] has unexpected size. got: %v, want: %v or %v",
						split.Start, split.End, size, min, min+1)
				}
				// Check: All elements are still in a split restriction and
				// the restrictions are in the appropriate ascending order.
				if split.Start != prevEnd {
					t.Errorf("restriction range [%v, %v] missing after splits.",
						prevEnd, split.Start)
				} else {
					prevEnd = split.End
				}
			}
			if prevEnd != r.End {
				t.Errorf("restriction range [%v, %v] missing after splits.",
					prevEnd, r.End)
			}
		})
	}
}

// TestRestriction_SizedSplits tests various splits and checks that they all
// follow the contract for SizedSplits. This means that all restrictions match
// the given size unless it is a remainder, and that each element is present
// in the split restrictions.
func TestRestriction_SizedSplits(t *testing.T) {
	tests := []struct {
		name string
		rest Restriction
		size int64
		want []Restriction
	}{
		{
			name: "Remainder",
			rest: Restriction{Start: 0, End: 11},
			size: 5,
			want: []Restriction{{0, 5}, {5, 10}, {10, 11}},
		},
		{
			name: "OffsetRemainder",
			rest: Restriction{Start: 11, End: 22},
			size: 5,
			want: []Restriction{{11, 16}, {16, 21}, {21, 22}},
		},
		{
			name: "OffsetExact",
			rest: Restriction{Start: 11, End: 21},
			size: 5,
			want: []Restriction{{11, 16}, {16, 21}},
		},
		{
			name: "LargeValues",
			rest: Restriction{Start: 0, End: 1024 * 1024 * 1024},
			size: 400 * 1024 * 1024,
			want: []Restriction{
				{0, 400 * 1024 * 1024},
				{400 * 1024 * 1024, 800 * 1024 * 1024},
				{800 * 1024 * 1024, 1024 * 1024 * 1024},
			},
		},
		{
			name: "OverlyLargeSize",
			rest: Restriction{Start: 0, End: 5},
			size: 10,
			want: []Restriction{{0, 5}},
		},
		{
			name: "InvalidSize",
			rest: Restriction{Start: 0, End: 21},
			size: 0,
			want: []Restriction{{0, 21}},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("%v (rest[%v, %v], size = %v)",
			test.name, test.rest.Start, test.rest.End, test.size), func(t *testing.T) {
			r := test.rest

			// Get the minimum size that a split restriction can be. Max size
			// should be min + 1. This way we can check the size of each split.
			splits := r.SizedSplits(test.size)
			prevEnd := r.Start
			for i, split := range splits {
				size := split.End - split.Start
				// Check: Each restriction has at least 1 element.
				if size == 0 {
					t.Errorf("split restriction [%v, %v] is empty, size must be greater than 0.",
						split.Start, split.End)
				}
				// Check: Restrictions (except for the last one) must match the split size.
				if i != len(splits)-1 && size != test.size {
					t.Errorf("split restriction [%v, %v] has unexpected size. got: %v, want: %v",
						split.Start, split.End, size, test.size)
				}
				// Check: All elements are still in a split restriction and
				// the restrictions are in the appropriate ascending order.
				if split.Start != prevEnd {
					t.Errorf("restriction range [%v, %v] missing after splits.",
						prevEnd, split.Start)
				} else {
					prevEnd = split.End
				}
			}
			if prevEnd != r.End {
				t.Errorf("restriction range [%v, %v] missing after splits.",
					prevEnd, r.End)
			}
		})
	}
}

// TestTracker_TryClaim validates both success and failure cases for TryClaim.
func TestTracker_TryClaim(t *testing.T) {
	// Test that TryClaim works as expected when called correctly.
	t.Run("Correctness", func(t *testing.T) {
		tests := []struct {
			rest   Restriction
			claims []int64
		}{
			{rest: Restriction{Start: 0, End: 3}, claims: []int64{0, 1, 2, 3}},
			{rest: Restriction{Start: 10, End: 40}, claims: []int64{15, 20, 50}},
			{rest: Restriction{Start: 0, End: 3}, claims: []int64{4}},
		}
		for _, test := range tests {
			test := test
			t.Run(fmt.Sprintf("(rest[%v, %v], claims = %v)",
				test.rest.Start, test.rest.End, test.claims), func(t *testing.T) {
				rt := NewTracker(test.rest)
				for _, pos := range test.claims {
					// If TryClaim returns false, check if there was an error.
					if !rt.TryClaim(pos) && !rt.IsDone() {
						t.Fatalf("tracker claiming %v failed, error: %v", pos, rt.GetError())
					}
				}
			})
		}
	})

	// Test that each invalid error case actually results in an error.
	t.Run("Errors", func(t *testing.T) {
		tests := []struct {
			rest   Restriction
			claims []int64
		}{
			// Claiming backwards.
			{rest: Restriction{Start: 0, End: 3}, claims: []int64{0, 2, 1}},
			// Claiming before start of restriction.
			{rest: Restriction{Start: 10, End: 40}, claims: []int64{8}},
			// Claiming after tracker signalled to stop.
			{rest: Restriction{Start: 0, End: 3}, claims: []int64{4, 5}},
		}
		for _, test := range tests {
			test := test
			t.Run(fmt.Sprintf("(rest[%v, %v], claims = %v)",
				test.rest.Start, test.rest.End, test.claims), func(t *testing.T) {
				rt := NewTracker(test.rest)
				for _, pos := range test.claims {
					// Finish successfully if we got an error.
					if !rt.TryClaim(pos) && !rt.IsDone() && rt.GetError() != nil {
						return
					}
				}
				t.Fatal("tracker did not fail on invalid claim")
			})
		}
	})
}

// TestTracker_TrySplit tests that TrySplit follows its contract, meaning that
// splits don't lose any elements, split fractions are clamped to 0 or 1, and
// that TrySplit always splits at the nearest integer greater than the given
// fraction.
func TestTracker_TrySplit(t *testing.T) {
	tests := []struct {
		rest     Restriction
		claimed  int64
		fraction float64
		// Index where we want the split to happen. This will be the end
		// (exclusive) of the primary and first element of the residual.
		splitPt int64
	}{
		{
			rest:     Restriction{Start: 0, End: 1},
			claimed:  0,
			fraction: 0.5,
			splitPt:  1,
		},
		{
			rest:     Restriction{Start: 0, End: 5},
			claimed:  0,
			fraction: 0.5,
			splitPt:  3,
		},
		{
			rest:     Restriction{Start: 0, End: 10},
			claimed:  5,
			fraction: 0.5,
			splitPt:  8,
		},
		{
			rest:     Restriction{Start: 0, End: 10},
			claimed:  5,
			fraction: -0.5,
			splitPt:  6,
		},
		{
			rest:     Restriction{Start: 0, End: 10},
			claimed:  5,
			fraction: 1.5,
			splitPt:  10,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("(split at %v of [%v, %v])",
			test.fraction, test.claimed, test.rest.End), func(t *testing.T) {
			rt := NewTracker(test.rest)
			ok := rt.TryClaim(test.claimed)
			if !ok {
				t.Fatalf("tracker failed on initial claim: %v", test.claimed)
			}
			gotP, gotR, err := rt.TrySplit(test.fraction)
			if err != nil {
				t.Fatalf("tracker failed on split: %v", err)
			}
			var wantP any = Restriction{Start: test.rest.Start, End: test.splitPt}
			var wantR any = Restriction{Start: test.splitPt, End: test.rest.End}
			if test.splitPt == test.rest.End {
				wantR = nil // When residuals are empty we should get nil.
			}
			if !cmp.Equal(gotP, wantP) {
				t.Errorf("split got incorrect primary: got: %v, want: %v", gotP, wantP)
			}
			if !cmp.Equal(gotR, wantR) {
				t.Errorf("split got incorrect residual: got: %v, want: %v", gotR, wantR)
			}
		})
	}
}

// TestTracker_TrySplit_WithoutClaiming tests that TrySplit follows its contract
// when no TryClaim calls have been made, meaning that
// splits don't lose any elements, split fractions are clamped to 0 or 1, and
// that TrySplit always splits at the nearest integer greater than the given
// fraction.
func TestTracker_TrySplit_WithoutClaiming(t *testing.T) {
	tests := []struct {
		rest     Restriction
		claimed  int64
		fraction float64
		// Index where we want the split to happen. This will be the end
		// (exclusive) of the primary and first element of the residual.
		splitPt int64
	}{
		{
			rest:     Restriction{Start: 0, End: 1},
			fraction: 0.5,
			splitPt:  0,
		},
		{
			rest:     Restriction{Start: 0, End: 1},
			fraction: 0.0,
			splitPt:  0,
		},
		{
			rest:     Restriction{Start: 0, End: 5},
			fraction: 0.5,
			splitPt:  2,
		},
		{
			rest:     Restriction{Start: 5, End: 10},
			fraction: 0.5,
			splitPt:  7,
		},
		{
			rest:     Restriction{Start: 5, End: 10},
			fraction: -0.5,
			splitPt:  5,
		},
		{
			rest:     Restriction{Start: 5, End: 10},
			fraction: 1.5,
			splitPt:  10,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("(split at %v of [%v, %v])",
			test.fraction, test.rest.Start, test.rest.End), func(t *testing.T) {
			rt := NewTracker(test.rest)
			gotP, gotR, err := rt.TrySplit(test.fraction)
			if err != nil {
				t.Fatalf("tracker failed on split: %v", err)
			}
			var wantP any = Restriction{Start: test.rest.Start, End: test.splitPt}
			var wantR any = Restriction{Start: test.splitPt, End: test.rest.End}
			if test.splitPt == test.rest.End {
				wantR = nil // When residuals are empty we should get nil.
			}
			if !cmp.Equal(gotP, wantP) {
				t.Errorf("split got incorrect primary: got: %v, want: %v", gotP, wantP)
			}
			if !cmp.Equal(gotR, wantR) {
				t.Errorf("split got incorrect residual: got: %v, want: %v", gotR, wantR)
			}
		})
	}
}

type offsetRangeEndEstimator struct {
	EstimateRangeEnd int64
}

// Estimate provides the estimated end for unbounded offset range.
func (o *offsetRangeEndEstimator) Estimate() int64 {
	return o.EstimateRangeEnd
}

// TestNewGrowableTracker_Bad tests the behavior of NewGrowableTracker when wrong arguments are passed.
func TestNewGrowableTracker_Bad(t *testing.T) {
	rest := Restriction{Start: 0, End: math.MaxInt64}
	_, err := NewGrowableTracker(rest, nil)
	if err == nil {
		t.Errorf("NewGrowableTracker() should have failed when nil is passed as a paramter for RangeEndEstimator.")
	}
}

// TestGrowableTracker_TryClaim tests the TryClaim method for GrowableTracker.
func TestGrowableTracker_TryClaim(t *testing.T) {
	estimator := offsetRangeEndEstimator{EstimateRangeEnd: 0}
	rest := Restriction{Start: 0, End: math.MaxInt64}
	tests := []struct {
		claim  int64
		result bool
	}{
		{
			claim:  0,
			result: true,
		},
		{
			claim:  10,
			result: true,
		},
		{
			claim:  100,
			result: true,
		},
		{
			claim:  math.MaxInt64,
			result: false,
		},
		{
			claim:  -1,
			result: false,
		},
	}

	for _, test := range tests {
		tracker, err := NewGrowableTracker(rest, &estimator)
		if err != nil {
			t.Fatalf("error in creating a new GrowableTracker: %v", err)
		}
		if got, want := tracker.TryClaim(test.claim), test.result; got != want {
			t.Errorf("tracker.TryClaim(%d) = %v, want: %v", test.claim, got, want)
		}
	}
}

// TestGrowableTracker_SplitBeforeStart tests TrySplit() method for GrowableTracker
// before claiming anything.
func TestGrowableTracker_SplitBeforeStart(t *testing.T) {
	estimator := offsetRangeEndEstimator{EstimateRangeEnd: 0}
	rest := Restriction{Start: 0, End: math.MaxInt64}
	tracker, err := NewGrowableTracker(rest, &estimator)
	if err != nil {
		t.Fatalf("error creating new GrowableTracker: %v", err)
	}
	estimator.EstimateRangeEnd = 10
	gotP, gotR, err := tracker.TrySplit(0)
	if err != nil {
		t.Fatalf("error in tracker.TrySplit(0): %v", err)
	}

	want := Restriction{0, 0}
	if got := gotP.(Restriction); got != want {
		t.Errorf("wrong primaries after TrySplit(0), got: %v, want: %v", got, want)
	}
	if got := tracker.GetRestriction().(Restriction); got != want {
		t.Errorf("wrong restriction tracked by tracker after TrySplit(0), got: %v, want: %v", got, want)
	}
	if got, want := gotR.(Restriction), (Restriction{0, math.MaxInt64}); got != want {
		t.Errorf("wrong residual after TrySplit(0), got: %v, want: %v", got, want)
	}
}

// TestGrowableTracker_Splits tests that TrySplit follows its contract, meaning that
// splits don't lose any elements, split fractions are clamped to 0 or 1, and
// that TrySplit always splits at the nearest integer greater than the given
// fraction.
func TestGrowableTracker_Splits(t *testing.T) {
	tests := []struct {
		rest              Restriction
		claimed           int64
		fraction          float64
		rangeEndEstimator offsetRangeEndEstimator
		// Index where we want the split to happen. This will be the end
		// (exclusive) of the primary and first element of the residual.
		splitPt int64
	}{
		{
			rest:              Restriction{Start: 0, End: math.MaxInt64},
			claimed:           0,
			fraction:          0.5,
			rangeEndEstimator: offsetRangeEndEstimator{EstimateRangeEnd: 10},
			splitPt:           5,
		},
		{
			rest:              Restriction{Start: 0, End: math.MaxInt64},
			claimed:           100,
			fraction:          0.5,
			rangeEndEstimator: offsetRangeEndEstimator{EstimateRangeEnd: 100},
			splitPt:           101,
		},
		{
			rest:              Restriction{Start: 0, End: math.MaxInt64},
			claimed:           5,
			fraction:          0.5,
			rangeEndEstimator: offsetRangeEndEstimator{EstimateRangeEnd: 0},
			splitPt:           6,
		},
		{
			rest:              Restriction{Start: 0, End: 10},
			claimed:           5,
			fraction:          -0.5,
			rangeEndEstimator: offsetRangeEndEstimator{EstimateRangeEnd: 10},
			splitPt:           6,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("(split at %v of [%v, %v])",
			test.fraction, test.claimed, test.rest.End), func(t *testing.T) {
			rt, err := NewGrowableTracker(test.rest, &test.rangeEndEstimator)
			if err != nil {
				t.Fatalf("error in creating a new growable tracker: %v", err)
			}
			ok := rt.TryClaim(test.claimed)
			if !ok {
				t.Fatalf("tracker failed on rt.TryClaim(%v)", test.claimed)
			}
			gotP, gotR, err := rt.TrySplit(test.fraction)
			if err != nil {
				t.Fatalf("tracker failed on split rt.TrySplit(%v): %v", test.fraction, err)
			}
			var wantP any = Restriction{Start: test.rest.Start, End: test.splitPt}
			var wantR any = Restriction{Start: test.splitPt, End: test.rest.End}
			if test.splitPt == test.rest.End {
				wantR = nil // When residuals are empty we should get nil.
			}
			if !cmp.Equal(gotP, wantP) {
				t.Errorf("split got incorrect primary: got: %v, want: %v", gotP, wantP)
			}
			if !cmp.Equal(gotR, wantR) {
				t.Errorf("split got incorrect residual: got: %v, want: %v", gotR, wantR)
			}
		})
	}
}

// TestGrowableTracker_IsBounded tests IsBounded method for GrowableTracker.
func TestGrowableTracker_IsBounded(t *testing.T) {
	estimator := offsetRangeEndEstimator{EstimateRangeEnd: 0}
	rest := Restriction{Start: 0, End: math.MaxInt64}
	tracker, err := NewGrowableTracker(rest, &estimator)
	if err != nil {
		t.Fatalf("error creating new GrowableTracker: %v", err)
	}

	if tracker.IsBounded() {
		t.Errorf("GrowableTracker is bounded, want unbounded initially.")
	}

	if got, want := tracker.TryClaim(int64(0)), true; got != want {
		t.Errorf("tracker.TryClaim(0) = %v, want: %v", got, want)
	}

	if tracker.IsBounded() {
		t.Errorf("GrowableTracker should've been unbounded.")
	}

	estimator.EstimateRangeEnd = 16
	tracker.TrySplit(0.5)
	if !tracker.IsBounded() {
		t.Errorf("tracker should've been bounded after split")
	}
}

// TestGrowableTracker_Progress tests GetProgess method for GrowableTracker.
func TestGrowableTracker_Progress(t *testing.T) {
	estimator := offsetRangeEndEstimator{0}
	tests := []struct {
		tracker         GrowableTracker
		done, remaining float64
		estimator       int64
	}{
		{
			tracker:   GrowableTracker{Tracker{rest: Restriction{Start: 0, End: math.MaxInt64}, claimed: 20, attempted: 20}, &estimator},
			done:      21,
			remaining: 0,
			estimator: 0,
		},
		{
			tracker:   GrowableTracker{Tracker{rest: Restriction{Start: 0, End: 20}, claimed: 15, attempted: 15}, &estimator},
			done:      16,
			remaining: 4,
			estimator: 0,
		},
		{
			tracker:   GrowableTracker{Tracker{rest: Restriction{Start: 0, End: math.MaxInt64}, claimed: 20, attempted: 20}, &estimator},
			done:      21,
			remaining: math.MaxInt64 - 20,
			estimator: math.MaxInt64,
		},
	}

	for _, test := range tests {
		estimator.EstimateRangeEnd = test.estimator
		done, remaining := test.tracker.GetProgress()
		if got, want := done, test.done; got != want {
			t.Errorf("wrong amount of work done, got: %v, want: %v", got, want)
		}
		if got, want := remaining, test.remaining; got != want {
			t.Errorf("wrong amount of work remaining, got:%v, want: %v", got, want)
		}
	}
}
