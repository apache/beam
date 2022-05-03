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
	"fmt"
	"testing"
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
		assertRepresentationEquals(t, got, want, fmt.Sprintf("possibleBundleLifecycleParameterCombos(%v, %v)", test.numIn, test.processElementIn))
	}
}

func TestMakeStructRegisterEntry(t *testing.T) {
	tests := []struct {
		structName   string
		functionName string
		ins          []string
		outs         []string
		entry        string
	}{
		{"StartBundle", "startBundle", []string{}, []string{}, "reflectx.MakeFunc(func() { fn.(StartBundle0x0).startBundle() })"},
		{"StartBundle", "startBundle", []string{"context.Context"}, []string{}, "reflectx.MakeFunc(func(a0 context.Context) { fn.(StartBundle1x0[context.Context]).startBundle(a0) })"},
		{"StartBundle", "startBundle", []string{"context.Context", "I0"}, []string{}, "reflectx.MakeFunc(func(a0 context.Context, a1 I0) { fn.(StartBundle2x0[context.Context, I0]).startBundle(a0, a1) })"},
		{"FinishBundle", "finishBundle", []string{}, []string{"[]typex.Window"}, "reflectx.MakeFunc(func() ([]typex.Window){ return fn.(FinishBundle0x1[[]typex.Window]).finishBundle() })"},
		{"FinishBundle", "finishBundle", []string{}, []string{"[]typex.Window", "R0"}, "reflectx.MakeFunc(func() ([]typex.Window, R0){ return fn.(FinishBundle0x2[[]typex.Window, R0]).finishBundle() })"},
		{"FinishBundle", "finishBundle", []string{"I0"}, []string{"[]typex.Window", "R0"}, "reflectx.MakeFunc(func(a0 I0) ([]typex.Window, R0){ return fn.(FinishBundle1x2[I0, []typex.Window, R0]).finishBundle(a0) })"},
		{"FinishBundle", "finishBundle", []string{"context.Context", "I0"}, []string{"[]typex.Window", "R0"}, "reflectx.MakeFunc(func(a0 context.Context, a1 I0) ([]typex.Window, R0){ return fn.(FinishBundle2x2[context.Context, I0, []typex.Window, R0]).finishBundle(a0, a1) })"},
	}

	for _, test := range tests {
		if got, want := makeStructRegisterEntry(test.structName, test.functionName, test.ins, test.outs), test.entry; got != want {
			t.Errorf("makeStructRegisterEntry(\"%v\", \"%v\", %v, %v) = %v, want: %v", test.structName, test.functionName, test.ins, test.outs, got, want)
		}
	}
}

func assertRepresentationEquals(t *testing.T, got [][]string, want [][]string, functionInvocation string) {
	if len(got) != len(want) {
		t.Fatalf("%v returned list of length %v, want: %v. Full list: %v", functionInvocation, len(got), len(want), got)
	}
	for _, l1 := range got {
		found := false
		for _, l2 := range want {
			if listEquals(l1, l2) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("%v does not contain list %v", functionInvocation, l1)
		}
	}
}

func listEquals(got []string, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for idx, e1 := range got {
		e2 := want[idx]
		if e1 != e2 {
			return false
		}
	}

	return true
}
