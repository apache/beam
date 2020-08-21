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

package graph

import (
	"testing"
)

func assertPanic(t *testing.T, f func(), err string) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic; %v", err)
		}
	}()
	f()
}

func TestWithInputs(t *testing.T) {
	inputsMap := map[string]int{"x": 1}

	t.Run("InputsMap initialized", func(t *testing.T) {
		e := ExternalTransform{InputsMap: make(map[string]int)}
		testPanic := func() {
			e.WithNamedInputs(inputsMap)
		}
		assertPanic(t, testPanic, "inserting into initialized map should fail")
	})

	t.Run("InputsMap nil", func(t *testing.T) {
		e := ExternalTransform{}
		newE := e.WithNamedInputs(inputsMap)
		for tag, idx := range inputsMap {
			if v, exists := newE.InputsMap[tag]; !exists || v != idx {
				t.Errorf("expected inputs map %v; got inputs map %v", inputsMap, newE.InputsMap)
			}
		}
	})
}

func TestWithOutputs(t *testing.T) {
	outputsMap := map[string]int{"x": 1}

	t.Run("OutputsMap initialized", func(t *testing.T) {
		e := ExternalTransform{OutputsMap: make(map[string]int)}
		testPanic := func() {
			e.WithNamedOutputs(outputsMap)
		}
		assertPanic(t, testPanic, "inserting into initialized map should fail")
	})

	t.Run("OutputsMap nil", func(t *testing.T) {
		e := ExternalTransform{}
		newE := e.WithNamedOutputs(outputsMap)
		for tag, idx := range outputsMap {
			if v, exists := newE.OutputsMap[tag]; !exists || v != idx {
				t.Errorf("expected outputs map %v; got outputs map %v", outputsMap, newE.OutputsMap)
			}
		}
	})
}

func TestNewNamespaceGenerator(t *testing.T) {
	seen := make(map[string]bool)
	runs := 1000000
	n := 10

	gen := NewNamespaceGenerator(n)

	for i := 0; i < runs; i++ {
		k := gen()
		if len(k) < n {
			t.Errorf("expected string of length %v; got string of length %v", n, len(k))
		}
		seen[gen()] = true
	}

	if len(seen) < runs {
		t.Errorf("repeated random strings generated; could cause namespace collision")
	}
}

// func TestNewNamespaceGenerator(t *testing.T) {
// 	var tests = []struct {
// 		a, b int
// 		want int
// 	}{
// 		{0, 1, 0},
// 		{1, 0, 0},
// 		{2, -2, -2},
// 		{0, -1, -1},
// 		{-1, 0, -1},
// 	}

// 	for _, tt := range tests {
// 		testname := fmt.Sprintf("%d,%d", tt.a, tt.b)
// 		t.Run(testname, func(t *testing.T) {
// 			ans := IntMin(tt.a, tt.b)
// 			if ans != tt.want {
// 				t.Errorf("got %d, want %d", ans, tt.want)
// 			}
// 		})
// 	}
// }
