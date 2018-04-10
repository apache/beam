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

// TestFlatten verifies that the Flatten node works correctly.
func TestFlatten(t *testing.T) {
	// capture := Flatten(a, b, c)

	out := &CaptureNode{UID: 1}
	flatten := &Flatten{UID: 2, N: 3, Out: out}
	a := &FixedRoot{UID: 3, Elements: makeValues(1, 2), Out: flatten}
	b := &FixedRoot{UID: 4, Elements: makeValues(3, 4), Out: flatten}
	c := &FixedRoot{UID: 5, Elements: makeValues(5, 6), Out: flatten}

	p, err := NewPlan("a", []Unit{a, b, c, flatten, out}) // order matters in this test
	if err != nil {
		t.Fatalf("failed to construct plan: %v", err)
	}

	if err := p.Execute(context.Background(), "1", nil); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if err := p.Down(context.Background()); err != nil {
		t.Fatalf("down failed: %v", err)
	}

	expected := makeValues(1, 2, 3, 4, 5, 6)
	if !equalList(out.Elements, expected) {
		t.Errorf("flatten returned %v, want %v", extractValues(out.Elements...), extractValues(expected...))
	}
}
