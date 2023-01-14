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

package main

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestGenericTypingRepresentation(t *testing.T) {
	tests := []struct {
		in             int
		out            int
		includeType    bool
		representation string
	}{
		{0, 0, true, ""},
		{0, 0, false, ""},
		{0, 1, true, "[R0 any]"},
		{0, 1, false, "[R0]"},
		{0, 3, true, "[R0, R1, R2 any]"},
		{0, 3, false, "[R0, R1, R2]"},
		{1, 0, true, "[I0 any]"},
		{1, 0, false, "[I0]"},
		{3, 0, true, "[I0, I1, I2 any]"},
		{3, 0, false, "[I0, I1, I2]"},
		{1, 1, true, "[I0, R0 any]"},
		{1, 1, false, "[I0, R0]"},
		{4, 1, true, "[I0, I1, I2, I3, R0 any]"},
		{4, 1, false, "[I0, I1, I2, I3, R0]"},
		{4, 2, true, "[I0, I1, I2, I3, R0, R1 any]"},
		{4, 2, false, "[I0, I1, I2, I3, R0, R1]"},
		{1, 2, true, "[I0, R0, R1 any]"},
		{1, 2, false, "[I0, R0, R1]"},
	}

	for _, test := range tests {
		if got, want := genericTypingRepresentation(test.in, test.out, test.includeType), test.representation; got != want {
			t.Errorf("genericTypingRepresentation(%v, %v, %v) = %v, want: %v", test.in, test.out, test.includeType, got, want)
		}
	}
}

func TestPossibleBundleLifecycleParameterCombos(t *testing.T) {
	tests := []struct {
		numIn            int
		processElementIn int
		representation   [][]string
	}{
		{0, 0, [][]string{[]string{}}},
		{0, 1, [][]string{[]string{}}},
		{1, 0, [][]string{[]string{"context.Context"}, []string{"typex.PaneInfo"}, []string{"[]typex.Window"}, []string{"typex.EventTime"}, []string{"typex.BundleFinalization"}}},
		{1, 1, [][]string{[]string{"I0"}, []string{"context.Context"}, []string{"typex.PaneInfo"}, []string{"[]typex.Window"}, []string{"typex.EventTime"}, []string{"typex.BundleFinalization"}}},
		{2, 1, [][]string{[]string{"context.Context", "I0"}, []string{"context.Context", "typex.PaneInfo"}, []string{"context.Context", "[]typex.Window"}, []string{"context.Context", "typex.EventTime"}, []string{"context.Context", "typex.BundleFinalization"},
			[]string{"typex.PaneInfo", "I0"}, []string{"typex.PaneInfo", "[]typex.Window"}, []string{"typex.PaneInfo", "typex.EventTime"}, []string{"typex.PaneInfo", "typex.BundleFinalization"},
			[]string{"[]typex.Window", "I0"}, []string{"[]typex.Window", "typex.EventTime"}, []string{"[]typex.Window", "typex.BundleFinalization"},
			[]string{"typex.EventTime", "I0"}, []string{"typex.EventTime", "typex.BundleFinalization"},
			[]string{"typex.BundleFinalization", "I0"}}},
		{2, 2, [][]string{[]string{"context.Context", "I1"}, []string{"context.Context", "typex.PaneInfo"}, []string{"context.Context", "[]typex.Window"}, []string{"context.Context", "typex.EventTime"}, []string{"context.Context", "typex.BundleFinalization"},
			[]string{"typex.PaneInfo", "I1"}, []string{"typex.PaneInfo", "[]typex.Window"}, []string{"typex.PaneInfo", "typex.EventTime"}, []string{"typex.PaneInfo", "typex.BundleFinalization"},
			[]string{"[]typex.Window", "I1"}, []string{"[]typex.Window", "typex.EventTime"}, []string{"[]typex.Window", "typex.BundleFinalization"},
			[]string{"typex.EventTime", "I1"}, []string{"typex.EventTime", "typex.BundleFinalization"},
			[]string{"typex.BundleFinalization", "I1"},
			[]string{"I0", "I1"}}},
		{2, 3, [][]string{[]string{"context.Context", "I2"}, []string{"context.Context", "typex.PaneInfo"}, []string{"context.Context", "[]typex.Window"}, []string{"context.Context", "typex.EventTime"}, []string{"context.Context", "typex.BundleFinalization"},
			[]string{"typex.PaneInfo", "I2"}, []string{"typex.PaneInfo", "[]typex.Window"}, []string{"typex.PaneInfo", "typex.EventTime"}, []string{"typex.PaneInfo", "typex.BundleFinalization"},
			[]string{"[]typex.Window", "I2"}, []string{"[]typex.Window", "typex.EventTime"}, []string{"[]typex.Window", "typex.BundleFinalization"},
			[]string{"typex.EventTime", "I2"}, []string{"typex.EventTime", "typex.BundleFinalization"},
			[]string{"typex.BundleFinalization", "I2"},
			[]string{"I1", "I2"}}},
	}

	for _, test := range tests {
		got, want := possibleBundleLifecycleParameterCombos(test.numIn, test.processElementIn), test.representation
		if len(got) != len(want) {
			t.Errorf("possibleBundleLifecycleParameterCombos(%v, %v) returned list of length %v, want: %v. Full list: %v", test.numIn, test.processElementIn, len(got), len(want), got)
		} else {
			for _, l1 := range got {
				found := false
				for _, l2 := range want {
					// cmp.Equal doesn't seem to handle the empty list case correctly
					if (len(l1) == 0 && len(l2) == 0) || cmp.Equal(l1, l2) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("possibleBundleLifecycleParameterCombos(%v, %v) does not contain list %v", test.numIn, test.processElementIn, l1)
				}
			}
		}
	}
}
