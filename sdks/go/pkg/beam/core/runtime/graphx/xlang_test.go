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

package graphx

import (
	"fmt"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func newNode(g *graph.Graph) *graph.Node {
	n := g.NewNode(typex.New(reflectx.Int), window.DefaultWindowingStrategy(), true)
	return n
}

func newIn(g *graph.Graph) *graph.Inbound {
	return &graph.Inbound{
		From: newNode(g),
	}
}

func newIns(g *graph.Graph, n int) []*graph.Inbound {
	var ins []*graph.Inbound
	for i := 0; i < n; i++ {
		ins = append(ins, newIn(g))
	}
	return ins
}

func newOut(g *graph.Graph) *graph.Outbound {
	return &graph.Outbound{
		To: newNode(g),
	}
}

func newOuts(g *graph.Graph, n int) []*graph.Outbound {
	var outs []*graph.Outbound
	for i := 0; i < n; i++ {
		outs = append(outs, newOut(g))
	}
	return outs
}

func newEdge(g *graph.Graph, ins, outs int) *graph.MultiEdge {
	return &graph.MultiEdge{
		Input:  newIns(g, ins),
		Output: newOuts(g, outs),
	}
}

func newExternal(ins, outs map[string]int) *graph.ExternalTransform {
	return &graph.ExternalTransform{
		InputsMap:  ins,
		OutputsMap: outs,
	}
}

type testExternalConf struct {
	i    int
	o    int
	iMap map[string]int
	oMap map[string]int
}

func TestExternalInputs(t *testing.T) {
	tt := testExternalConf{i: 2, o: 3, iMap: map[string]int{"x": 1}, oMap: map[string]int{"y": 1}}

	g := graph.New()
	e := newEdge(g, tt.i, tt.o)
	e.External = newExternal(tt.iMap, tt.oMap)

	i := ExternalInputs(e)

	for tag, idx := range tt.iMap {
		got, exists := i[tag]
		want := e.Input[idx].From

		if !exists {
			t.Errorf("input absent for key %v; expected %v", tag, want)
		}

		if got.ID() != want.ID() {
			t.Errorf("wrong input associated with key %v; want %v but got %v", tag, want, got)
		}
	}
}

func TestExternalOutputs(t *testing.T) {
	tt := testExternalConf{i: 2, o: 3, iMap: map[string]int{"x": 1}, oMap: map[string]int{"y": 1}}

	g := graph.New()
	e := newEdge(g, tt.i, tt.o)
	e.External = newExternal(tt.iMap, tt.oMap)

	o := ExternalOutputs(e)

	for tag, idx := range tt.oMap {
		got, exists := o[tag]
		want := e.Output[idx].To

		if !exists {
			t.Errorf("output absent for key %v; expected %v", tag, want)
		}

		if got.ID() != want.ID() {
			t.Errorf("wrong output associated with key %v; want %v but got %v", tag, want, got)
		}
	}
}

func newTransform(name string) *pipepb.PTransform {
	return &pipepb.PTransform{
		UniqueName: name,
	}
}

func newComponents(ts []string) *pipepb.Components {
	components := &pipepb.Components{}

	components.Transforms = make(map[string]*pipepb.PTransform)
	for id, t := range ts {
		components.Transforms[fmt.Sprint(id)] = newTransform(t)
	}

	return components
}

func expectPanic(t *testing.T, err string) {
	if r := recover(); r == nil {
		t.Errorf("expected panic; %v", err)
	}
}

func TestExpandedTransform(t *testing.T) {
	t.Run("Correct PTransform", func(t *testing.T) {
		want := newTransform("x")
		exp := &graph.ExpandedTransform{Transform: want}

		got := ExpandedTransform(exp)

		if d := cmp.Diff(want, got, protocmp.Transform()); d != "" {
			t.Errorf("diff (-want, +got): %v", d)
		}

	})

	t.Run("Malformed PTransform", func(t *testing.T) {
		defer expectPanic(t, "string can't be type asserted into a pipeline PTransform")
		exp := &graph.ExpandedTransform{Transform: "gibberish"}
		ExpandedTransform(exp)
	})
}

func TestExpandedComponents(t *testing.T) {
	t.Run("Correct Components", func(t *testing.T) {
		want := newComponents([]string{"x"})
		exp := &graph.ExpandedTransform{Components: want}

		got := ExpandedComponents(exp)

		if d := cmp.Diff(want, got, protocmp.Transform()); d != "" {
			t.Errorf("diff (-want, +got): %v", d)
		}

	})

	t.Run("Malformed Components", func(t *testing.T) {
		defer expectPanic(t, "string can't be type asserted into a pipeline Components")
		exp := &graph.ExpandedTransform{Transform: "gibberish"}
		ExpandedComponents(exp)
	})
}
