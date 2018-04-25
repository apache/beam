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

func sumFn(n int, a int, b []int, c func(*int) bool, d func() func(*int) bool, e func(int)) int {
	sum := a
	for _, i := range b {
		sum += i
	}

	var i int
	for c(&i) {
		sum += i
	}

	dfn := d()
	for dfn(&i) {
		sum += i
	}
	e(sum)
	return n
}

// TestParDo verifies that the ParDo node works correctly for side inputs and emitters. It uses a special
// dofn that uses all forms of sideinput.
func TestParDo(t *testing.T) {
	fn, err := graph.NewDoFn(sumFn)
	if err != nil {
		t.Fatalf("invalid function: %v", err)
	}

	g := graph.New()
	nN := g.NewNode(typex.New(reflectx.Int), window.NewGlobalWindow())
	aN := g.NewNode(typex.New(reflectx.Int), window.NewGlobalWindow())
	bN := g.NewNode(typex.New(reflectx.Int), window.NewGlobalWindow())
	cN := g.NewNode(typex.New(reflectx.Int), window.NewGlobalWindow())
	dN := g.NewNode(typex.New(reflectx.Int), window.NewGlobalWindow())

	edge, err := graph.NewParDo(g, g.Root(), fn, []*graph.Node{nN, aN, bN, cN, dN}, nil)
	if err != nil {
		t.Fatalf("invalid pardo: %v", err)
	}

	out := &CaptureNode{UID: 1}
	sum := &CaptureNode{UID: 2}
	pardo := &ParDo{UID: 3, Fn: edge.DoFn, Inbound: edge.Input, Out: []Node{out, sum}, Side: []ReStream{
		&FixedReStream{Buf: makeValues(1)},       // a
		&FixedReStream{Buf: makeValues(2, 3, 4)}, // b
		&FixedReStream{Buf: makeValues(5, 6)},    // c
		&FixedReStream{Buf: makeValues(7, 8, 9)}, // d
	}}
	n := &FixedRoot{UID: 4, Elements: makeValues(10, 20, 30), Out: pardo}

	p, err := NewPlan("a", []Unit{n, pardo, out, sum})
	if err != nil {
		t.Fatalf("failed to construct plan: %v", err)
	}

	if err := p.Execute(context.Background(), "1", nil); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if err := p.Down(context.Background()); err != nil {
		t.Fatalf("down failed: %v", err)
	}

	expected := makeValues(10, 20, 30)
	if !equalList(out.Elements, expected) {
		t.Errorf("pardo(sumFn) = %v, want %v", extractValues(out.Elements...), extractValues(expected...))
	}
	expectedSum := makeValues(45, 45, 45)
	if !equalList(sum.Elements, expectedSum) {
		t.Errorf("pardo(sumFn) side input = %v, want %v", extractValues(sum.Elements...), extractValues(expectedSum...))
	}
}
