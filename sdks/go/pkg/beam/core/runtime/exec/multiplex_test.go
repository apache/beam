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

// TestMultiplex verifies that the Multiplex node works correctly.
func TestMultiplex(t *testing.T) {
	// a, b, c := Mulitplex(in)

	a := &CaptureNode{UID: 1}
	b := &CaptureNode{UID: 2}
	c := &CaptureNode{UID: 3}
	multiplex := &Multiplex{UID: 4, Out: []Node{a, b, c}}
	in := &FixedRoot{UID: 5, Elements: makeValues(1, 2, 3), Out: multiplex}

	p, err := NewPlan("a", []Unit{a, b, c, multiplex, in})
	if err != nil {
		t.Fatalf("failed to construct plan: %v", err)
	}

	if err := p.Execute(context.Background(), "1", nil); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if err := p.Down(context.Background()); err != nil {
		t.Fatalf("down failed: %v", err)
	}

	expected := makeValues(1, 2, 3)
	if !equalList(a.Elements, expected) {
		t.Errorf("multiplex returned %v for a, want %v", extractValues(a.Elements...), extractValues(expected...))
	}
	if !equalList(b.Elements, expected) {
		t.Errorf("multiplex returned %v for b, want %v", extractValues(b.Elements...), extractValues(expected...))
	}
	if !equalList(c.Elements, expected) {
		t.Errorf("multiplex returned %v for c, want %v", extractValues(c.Elements...), extractValues(expected...))
	}
}
