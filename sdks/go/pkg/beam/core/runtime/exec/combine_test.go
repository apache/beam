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

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

func mergeFn(a, b int) int {
	return a + b
}

// TestCombine verifies that the Combine node works correctly.
func TestCombine(t *testing.T) {
	fn, err := graph.NewCombineFn(mergeFn)
	if err != nil {
		t.Fatalf("invalid function: %v", err)
	}

	g := graph.New()
	in := g.NewNode(typex.New(reflectx.Int), window.NewGlobalWindow())

	edge, err := graph.NewCombine(g, g.Root(), fn, in)
	if err != nil {
		t.Fatalf("invalid pardo: %v", err)
	}

	out := &CaptureNode{UID: 1}
	combine := &Combine{UID: 2, Fn: edge.CombineFn, Out: out}
	n := &FixedRoot{UID: 3, Elements: makeValues(1, 2, 3, 4, 5, 6), Out: combine}

	p, err := NewPlan("a", []Unit{n, combine, out})
	if err != nil {
		t.Fatalf("failed to construct plan: %v", err)
	}

	if err := p.Execute(context.Background(), "1", nil); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if err := p.Down(context.Background()); err != nil {
		t.Fatalf("down failed: %v", err)
	}

	expected := makeValues(21)
	if !equalList(out.Elements, expected) {
		t.Errorf("pardo(sumFn) = %v, want %v", extractValues(out.Elements...), extractValues(expected...))
	}
}
