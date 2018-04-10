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
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	"github.com/golang/protobuf/proto"
)

func init() {
	runtime.RegisterFunction(pickFn)
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
	return typex.New(reflectx.Int)
}

func intCoder() *coder.Coder {
	return custom("int", reflectx.Int)
}

// TestParDo verifies that ParDo can be serialized.
func TestParDo(t *testing.T) {
	g := graph.New()
	pick(t, g)

	edges, _, err := g.Build()
	if err != nil {
		t.Fatal(err)
	}
	if len(edges) != 1 {
		t.Fatal("expected a single edge")
	}

	p, err := graphx.Marshal(edges, &graphx.Options{ContainerImageURL: "foo"})
	if err != nil {
		t.Fatal(err)
	}

	if len(p.GetComponents().GetTransforms()) != 1 {
		t.Errorf("bad ParDo translation: %v", proto.MarshalTextString(p))
	}
}
