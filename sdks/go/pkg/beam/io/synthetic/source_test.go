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
	"testing"
)

// TestSourceConfig_NumElements tests that setting the number of produced
// elements for a synthetic source works correctly.
func TestSourceConfig_NumElements(t *testing.T) {
	tests := []struct {
		elms int
		want int
	}{
		{elms: 1, want: 1},
		{elms: 42, want: 42},
	}
	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("(elm = %v)", test.elms), func(t *testing.T) {
			dfn := sourceFn{}
			cfg := DefaultSourceConfig().NumElements(test.elms).Build()

			keys, _, err := simulateSourceFn(t, &dfn, cfg)
			if err != nil {
				t.Errorf("Failure processing sourceFn: %v", err)
			}
			if got := len(keys); got != test.want {
				t.Errorf("SourceFn emitted wrong number of outputs: got: %v, want: %v",
					got, test.want)
			}
		})
	}
}

// TestSourceConfig_InitialSplits tests that the InitialSplits config option
// works correctly.
func TestSourceConfig_InitialSplits(t *testing.T) {
	// Test that SplitRestriction creates the expected number of restrictions.
	t.Run("NumSplits", func(t *testing.T) {
		tests := []struct {
			elms   int
			splits int
			want   int
		}{
			{elms: 42, splits: 10, want: 10},
			{elms: 4, splits: 10, want: 4},
		}
		for _, test := range tests {
			test := test
			t.Run(fmt.Sprintf("(elm = %v, splits = %v)", test.elms, test.splits), func(t *testing.T) {
				dfn := sourceFn{}
				cfg := DefaultSourceConfig().NumElements(test.elms).InitialSplits(test.splits).Build()

				rest := dfn.CreateInitialRestriction(cfg)
				splits := dfn.SplitRestriction(cfg, rest)
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
			elms   int
			want   int
			splits int
		}{
			{elms: 42, want: 42, splits: 10},
			{elms: 4, want: 4, splits: 10},
		}
		for _, test := range tests {
			test := test
			t.Run(fmt.Sprintf("(elm = %v)", test.elms), func(t *testing.T) {
				dfn := sourceFn{}
				cfg := DefaultSourceConfig().NumElements(test.elms).InitialSplits(test.splits).Build()

				keys, _, err := simulateSourceFn(t, &dfn, cfg)
				if err != nil {
					t.Errorf("Failure processing sourceFn: %v", err)
				}
				if got := len(keys); got != test.want {
					t.Errorf("SourceFn emitted wrong number of outputs: got: %v, want: %v",
						got, test.want)
				}
			})
		}
	})
}

// simulateSourceFn calls CreateInitialRestriction, SplitRestriction,
// CreateTracker, and ProcessElement on the given sourceFn with the given
// SourceConfig, and outputs the resulting output elements. This method isn't
// expected to accurately reflect how SDFs are executed in practice (that
// should be done via integration tests), but to validate the implementations of
// those methods.
func simulateSourceFn(t *testing.T, dfn *sourceFn, cfg SourceConfig) (keys [][]byte, vals [][]byte, err error) {
	t.Helper()

	emitFn := func(key []byte, val []byte) {
		keys = append(keys, key)
		vals = append(vals, key)
	}

	rest := dfn.CreateInitialRestriction(cfg)
	splits := dfn.SplitRestriction(cfg, rest)
	dfn.Setup()
	for _, split := range splits {
		rt := dfn.CreateTracker(split)
		if err := dfn.ProcessElement(rt, cfg, emitFn); err != nil {
			return nil, nil, err
		}
	}
	return keys, vals, nil
}
