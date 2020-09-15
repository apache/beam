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
	"github.com/google/go-cmp/cmp"
	"testing"
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
				// Check: All elements are still in a split restrictions. This
				// logic assumes that the splits are returned in order which
				// isn't guaranteed by EvenSplits, but this check is way easier
				// with the assumption.
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
			splitPt:  5,
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
			var wantP interface{} = Restriction{Start: test.rest.Start, End: test.splitPt}
			var wantR interface{} = Restriction{Start: test.splitPt, End: test.rest.End}
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
