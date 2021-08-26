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

package synthetic

import (
	"fmt"
	"math/rand"
	"testing"
)

// TestStepConfig_OutputPerInput tests that setting the number of output per
// input works correctly, in both SDF and non-SDF synthetic steps.
func TestStepConfig_OutputPerInput(t *testing.T) {
	tests := []struct {
		outPer int
	}{
		{outPer: 0},
		{outPer: 1},
		{outPer: 10},
	}
	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("(outPer = %v)", test.outPer), func(t *testing.T) {
			cfg := DefaultStepConfig().OutputPerInput(test.outPer).Build()

			// Non-splittable StepFn.
			dfn := stepFn{cfg: cfg}
			var keys [][]byte
			emitFn := func(key []byte, val []byte) {
				keys = append(keys, key)
			}
			elm := []byte{0, 0, 0, 0}
			dfn.Setup()
			dfn.ProcessElement(elm, elm, emitFn)
			if got := len(keys); got != test.outPer {
				t.Errorf("stepFn emitted wrong number of outputs: got: %v, want: %v",
					got, test.outPer)
			}

			// SDF StepFn.
			cfg = DefaultStepConfig().OutputPerInput(test.outPer).Splittable(true).Build()
			sdf := sdfStepFn{cfg: cfg}
			keys, _ = simulateSdfStepFn(t, &sdf)
			if got := len(keys); got != test.outPer {
				t.Errorf("sdfStepFn emitted wrong number of outputs: got: %v, want: %v",
					got, test.outPer)
			}
		})
	}
}

// fakeRand is a rand.Rand implementation used for testing the filter ratio.
// It outputs the stored values in their corresponding random methods.
type fakeRand struct {
	*rand.Rand
	f64 float64 // Output for Float64().
}

func (r *fakeRand) Float64() float64 {
	return r.f64
}

// TestStepConfig_FilterRatio tests that the element is filtered properly based
// on the ratio given, in both SDF and non-SDF synthetic steps.
func TestStepConfig_FilterRatio(t *testing.T) {
	tests := []struct {
		rand     float64
		ratio    float64
		filtered bool
	}{
		{ratio: 0.5, rand: 0.49, filtered: true},
		{ratio: 0.5, rand: 0.51, filtered: false},
		{ratio: 0, rand: 0, filtered: false},
	}
	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("(ratio = %v, rand = %v)", test.ratio, test.rand), func(t *testing.T) {
			// Set up emits and elements for running the DoFns.
			var keys [][]byte
			emitFn := func(key []byte, val []byte) {
				keys = append(keys, key)
			}
			elm := []byte{0, 0, 0, 0}

			// Non-splittable StepFn.
			cfg := DefaultStepConfig().FilterRatio(test.ratio).Build()
			dfn := stepFn{cfg: cfg}
			dfn.Setup()
			dfn.rng = &fakeRand{f64: test.rand}
			dfn.ProcessElement(elm, elm, emitFn)

			got := len(keys) == 0 // True if element was filtered out.
			if got != test.filtered {
				t.Errorf("stepFn filtered incorrectly: got: %v, wanted: %v",
					got, test.filtered)
			}

			// SDF StepFn.
			cfg = DefaultStepConfig().FilterRatio(test.ratio).Splittable(true).Build()
			sdf := sdfStepFn{cfg: cfg}
			keys = nil
			rest := sdf.CreateInitialRestriction(elm, elm)
			splits := sdf.SplitRestriction(elm, elm, rest)
			sdf.Setup()
			sdf.rng = &fakeRand{f64: test.rand}
			for _, split := range splits {
				rt := sdf.CreateTracker(split)
				sdf.ProcessElement(rt, elm, elm, emitFn)
			}

			got = len(keys) == 0 // True if element was filtered out.
			if got != test.filtered {
				t.Errorf("sdfStepFn filtered incorrectly: got: %v, wanted: %v",
					got, test.filtered)
			}
		})
	}
}

// TestStepConfig_InitialSplits tests that the InitialSplits config option
// works correctly.
func TestStepConfig_InitialSplits(t *testing.T) {
	// Test that SplitRestriction creates the expected number of restrictions.
	t.Run("NumSplits", func(t *testing.T) {
		tests := []struct {
			outPer int
			splits int
			want   int
		}{
			{outPer: 100, splits: 10, want: 10},
			{outPer: 4, splits: 10, want: 4},
			{outPer: 0, splits: 10, want: 0},
		}
		for _, test := range tests {
			test := test
			t.Run(fmt.Sprintf("(outPer = %v, splits = %v)", test.outPer, test.splits), func(t *testing.T) {
				cfg := DefaultStepConfig().
					OutputPerInput(test.outPer).
					Splittable(true).
					InitialSplits(test.splits).
					Build()
				elm := []byte{0, 0, 0, 0}

				sdf := sdfStepFn{cfg: cfg}
				rest := sdf.CreateInitialRestriction(elm, elm)
				splits := sdf.SplitRestriction(elm, elm, rest)
				if got := len(splits); got != test.want {
					t.Errorf("SplitRestriction output the wrong number of splits: got: %v, want: %v",
						got, test.want)
				}
			})
		}
	})

	// Tests correctness of the splitting. In this case, that means that even
	// after splitting, the same amount of elements are output.
	t.Run("Correctness", func(t *testing.T) {
		tests := []struct {
			outPer int
			want   int
			splits int
		}{
			{outPer: 100, want: 100, splits: 10},
			{outPer: 4, want: 4, splits: 10},
			{outPer: 0, want: 0, splits: 10},
		}
		for _, test := range tests {
			test := test
			t.Run(fmt.Sprintf("(outPer = %v)", test.outPer), func(t *testing.T) {
				cfg := DefaultStepConfig().
					OutputPerInput(test.outPer).
					Splittable(true).
					InitialSplits(test.splits).
					Build()

				sdf := sdfStepFn{cfg: cfg}
				keys, _ := simulateSdfStepFn(t, &sdf)
				if got := len(keys); got != test.want {
					t.Errorf("SourceFn emitted wrong number of outputs: got: %v, want: %v",
						got, test.want)
				}
			})
		}
	})
}

// simulateSdfStepFn calls CreateInitialRestriction, SplitRestriction,
// CreateTracker, and ProcessElement on the given sdfStepFn with the given
// StepConfig, and outputs the resulting output elements. This method isn't
// expected to accurately reflect how SDFs are executed in practice (that
// should be done via integration tests), but to validate the implementations of
// those methods.
func simulateSdfStepFn(t *testing.T, sdf *sdfStepFn) (keys [][]byte, vals [][]byte) {
	t.Helper()

	emitFn := func(key []byte, val []byte) {
		keys = append(keys, key)
		vals = append(vals, key)
	}

	elm := []byte{0, 0, 0, 0}
	rest := sdf.CreateInitialRestriction(elm, elm)
	splits := sdf.SplitRestriction(elm, elm, rest)
	sdf.Setup()
	for _, split := range splits {
		rt := sdf.CreateTracker(split)
		sdf.ProcessElement(rt, elm, elm, emitFn)
	}
	return keys, vals
}
