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
	"testing"
)

var toStringTestCases = []struct {
	Input    []any
	Expected []any
}{
	{Input: strInput, Expected: strInput},
	{Input: intInput, Expected: strInput},
	{Input: int64Input, Expected: strInput},
}

func TestToString(t *testing.T) {
	for _, testCase := range toStringTestCases {
		ctx := context.Background()
		out := &CaptureNode{UID: 1}
		toString := &ToString{UID: 2, Out: out}
		a := &FixedRoot{UID: 3, Elements: makeKVInput("key", testCase.Input...), Out: toString}

		p, err := NewPlan("a", []Unit{a, toString, out})
		if err != nil {
			t.Fatalf("failed to construct plan: %v", err)
		}

		if err := p.Execute(ctx, "1", DataContext{}); err != nil {
			t.Fatalf("execute failed: %v", err)
		}

		if err := p.Down(ctx); err != nil {
			t.Fatalf("down failed: %v", err)
		}

		expected := makeKVValues("key", testCase.Expected...)
		if !equalList(out.Elements, expected) {
			t.Errorf("tostring returned %v, want %v", extractKeyedValues(out.Elements...), extractKeyedValues(expected...))
		}
	}
}
