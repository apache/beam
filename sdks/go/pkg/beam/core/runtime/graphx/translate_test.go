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

package graphx_test

import (
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
)

func init() {
	ptest.RegisterFn(pickFn)
}

func pickFn(a int, small, big func(int)) {
	if a < 3 {
		small(a)
	} else {
		big(a)
	}
}

func pick(t *testing.T, g *graph.Graph) *graph.MultiEdge {
	dofn, err := graph.NewDoFn(pickFn)
	if err != nil {
		t.Fatal(err)
	}

	in := g.NewNode(intT(), window.NewGlobalWindow())
	in.Coder = intCoder()

	e, err := graph.NewParDo(g, g.Root(), dofn, []*graph.Node{in}, nil)
	if err != nil {
		t.Fatal(err)
	}
	e.Output[0].To.Coder = intCoder()
	e.Output[1].To.Coder = intCoder()
	return e
}

func intT() typex.FullType {
	return typex.NewW(typex.New(reflectx.Int))
}

func intCoder() *coder.Coder {
	return coder.NewW(custom("int", reflectx.Int), window.NewGlobalWindow())
}

// TestParDoRoundtrip verifies that ParDo can be serialized and deserialized.
func TestParDoRoundtrip(t *testing.T) {
	g := graph.New()
	pick(t, g)

	edges, _, err := g.Build()
	if err != nil {
		t.Fatal(err)
	}
	if len(edges) != 1 {
		t.Fatal("expected a single edge")
	}

	edges2 := roundtrip(t, edges)

	if !areEdgesSimilar(edges[0], edges2[0]) {
		t.Errorf("bad ParDo translation: %v, want %v", edges2[0], edges[0])
	}
}

func roundtrip(t *testing.T, edges []*graph.MultiEdge) []*graph.MultiEdge {
	p, err := graphx.Marshal(edges, &graphx.Options{ContainerImageURL: "foo"})
	if err != nil {
		t.Fatal(err)
	}

	// t.Logf("P: %v", proto.MarshalTextString(p))

	g2, err := graphx.Unmarshal(p)
	if err != nil {
		t.Fatal(err)
	}

	edges2, _, err := g2.Build()
	if err != nil {
		t.Fatal(err)
	}

	if len(edges2) != len(edges) {
		t.Fatalf("Graph sizes = %v, want %v", len(edges2), len(edges))
	}
	return edges2
}

// areEdgesSimilar compares two edges from different graphs. It returns true iff
// they have identical opcode, function and shape.
func areEdgesSimilar(a, b *graph.MultiEdge) bool {
	if a.Op != b.Op {
		return false
	}
	if a.Name() != b.Name() {
		return false
	}

	if len(a.Input) != len(b.Input) {
		return false
	}
	for i, in := range a.Input {
		in2 := b.Input[i]

		if in.Kind != in2.Kind {
			return false
		}
		if !in.From.Coder.Equals(in2.From.Coder) {
			return false
		}
	}

	if len(a.Output) != len(b.Output) {
		return false
	}
	for i, out := range a.Output {
		out2 := b.Output[i]

		if !out.To.Coder.Equals(out2.To.Coder) {
			return false
		}
	}

	if a.Op == graph.DataSink || a.Op == graph.DataSource {
		if a.Port.URL != b.Port.URL {
			return false
		}
	}

	return true
}
