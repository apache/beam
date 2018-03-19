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

package direct

import (
	"context"
	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
)

func init() {
	beam.RegisterRunner("direct", Execute)
}

// Execute runs the pipeline in-process.
func Execute(ctx context.Context, p *beam.Pipeline) error {
	log.Info(ctx, "Pipeline:")
	log.Info(ctx, p)

	edges, _, err := p.Build()
	if err != nil {
		return fmt.Errorf("invalid pipeline: %v", err)
	}
	plan, err := Compile(edges)
	if err != nil {
		return fmt.Errorf("translation failed: %v", err)
	}

	if err = plan.Execute(ctx, "", nil); err != nil {
		plan.Down(ctx) // ignore any teardown errors
		return err
	}
	return plan.Down(ctx)
}

// Compile translates a pipeline to a multi-bundle execution plan.
func Compile(edges []*graph.MultiEdge) (*exec.Plan, error) {
	// (1) Preprocess graph structure to allow insertion of Multiplex,
	// Flatten and Discard.

	prev := make(map[int]int)      // nodeID -> #incoming
	succ := make(map[int][]linkID) // nodeID -> []linkID
	edgeMap := make(map[int]*graph.MultiEdge)

	for _, edge := range edges {
		edgeMap[edge.ID()] = edge
		for i, in := range edge.Input {
			from := in.From.ID()
			succ[from] = append(succ[from], linkID{edge.ID(), i})
		}
		for _, out := range edge.Output {
			to := out.To.ID()
			prev[to]++
		}
	}

	// (2) Constructs the plan units recursively.

	b := &builder{
		prev:  prev,
		succ:  succ,
		edges: edgeMap,
		nodes: make(map[int]exec.Node),
		links: make(map[linkID]exec.Node),
		idgen: &exec.GenID{},
	}

	var roots []exec.Unit

	for _, edge := range edges {
		switch edge.Op {
		case graph.Impulse:
			out, err := b.makeNode(edge.Output[0].To.ID())
			if err != nil {
				return nil, err
			}

			u := &Impulse{UID: b.idgen.New(), Value: edge.Value, Out: out}
			roots = append(roots, u)

		default:
			// skip non-roots
		}
	}

	return exec.NewPlan("plan", append(roots, b.units...))
}

// linkID represents an incoming data link to an Edge.
type linkID struct {
	to    int // graph.MultiEdge
	input int // input index. If > 0, it's a side or CoGBK input.
}

// builder is the recursive builder for non-root execution nodes.
type builder struct {
	prev  map[int]int              // nodeID -> #incoming
	succ  map[int][]linkID         // nodeID -> []linkID
	edges map[int]*graph.MultiEdge // edgeID -> Edge

	nodes map[int]exec.Node    // nodeID -> Node (cache)
	links map[linkID]exec.Node // linkID -> Node (cache)

	units []exec.Unit // result
	idgen *exec.GenID
}

func (b *builder) makeNodes(out []*graph.Outbound) ([]exec.Node, error) {
	var ret []exec.Node
	for _, o := range out {
		n, err := b.makeNode(o.To.ID())
		if err != nil {
			return nil, err
		}
		ret = append(ret, n)
	}
	return ret, nil
}

func (b *builder) makeNode(id int) (exec.Node, error) {
	if n, ok := b.nodes[id]; ok {
		return n, nil
	}

	list := b.succ[id]

	var u exec.Node
	switch len(list) {
	case 0:
		// Discard.

		u = &exec.Discard{UID: b.idgen.New()}

	case 1:
		return b.makeLink(list[0])

	default:
		// Multiplex.

		out, err := b.makeLinks(list)
		if err != nil {
			return nil, err
		}
		u = &exec.Multiplex{UID: b.idgen.New(), Out: out}
	}

	if count := b.prev[id]; count > 1 {
		// Guard node with Flatten, if needed.

		b.units = append(b.units, u)
		u = &exec.Flatten{UID: b.idgen.New(), N: count, Out: u}
	}

	b.nodes[id] = u
	b.units = append(b.units, u)
	return u, nil
}

func (b *builder) makeLinks(ids []linkID) ([]exec.Node, error) {
	var ret []exec.Node
	for _, id := range ids {
		n, err := b.makeLink(id)
		if err != nil {
			return nil, err
		}
		ret = append(ret, n)
	}
	return ret, nil
}

func (b *builder) makeLink(id linkID) (exec.Node, error) {
	if n, ok := b.links[id]; ok {
		return n, nil
	}

	// Process all incoming links for the edge and cache them. It thus doesn't matter
	// which exact link triggers the Node generation. The link caching is only needed
	// to process ParDo side inputs and CoGBK.

	edge := b.edges[id.to]

	out, err := b.makeNodes(edge.Output)
	if err != nil {
		return nil, err
	}

	var u exec.Node
	switch edge.Op {
	case graph.ParDo:
		pardo := &exec.ParDo{UID: b.idgen.New(), Fn: edge.DoFn, Inbound: edge.Input, Out: out}
		if len(edge.Input) == 1 {
			u = pardo
			break
		}

		// ParDo w/ side input. We need to insert buffering and wait. We also need to
		// ensure that we return the correct link node.

		b.units = append(b.units, pardo)

		w := &wait{UID: b.idgen.New(), need: len(edge.Input) - 1, next: pardo}
		b.units = append(b.units, w)
		b.links[linkID{edge.ID(), 0}] = w

		for i := 1; i < len(edge.Input); i++ {
			n := &buffer{uid: b.idgen.New(), next: w.ID(), read: pardo.ID(), notify: w.notify}
			pardo.Side = append(pardo.Side, n)

			b.units = append(b.units, n)
			b.links[linkID{edge.ID(), i}] = n
		}

		return b.links[id], nil

	case graph.Combine:
		isPerKey := typex.IsCoGBK(edge.Input[0].From.Type())
		usesKey := typex.IsKV(edge.Input[0].Type)

		u = &exec.Combine{UID: b.idgen.New(), Fn: edge.CombineFn, IsPerKey: isPerKey, UsesKey: usesKey, Out: out[0]}

	case graph.CoGBK:
		u = &CoGBK{UID: b.idgen.New(), Edge: edge, Out: out[0]}
		b.units = append(b.units, u)

		// CoGBK needs injection of each incoming index. If > 1 incoming,
		// insert Flatten as well.

		if len(edge.Input) > 1 {
			u = &exec.Flatten{UID: b.idgen.New(), N: len(edge.Input), Out: u}
			b.units = append(b.units, u)
		}

		for i := 0; i < len(edge.Input); i++ {
			n := &Inject{UID: b.idgen.New(), N: i, Out: u}

			b.units = append(b.units, n)
			b.links[linkID{edge.ID(), i}] = n
		}

		return b.links[id], nil

	case graph.Flatten:
		u = &exec.Flatten{UID: b.idgen.New(), N: len(edge.Input), Out: out[0]}

		for i := 0; i < len(edge.Input); i++ {
			b.links[linkID{edge.ID(), i}] = u
		}

	default:
		return nil, fmt.Errorf("unexpected edge: %v", edge)
	}

	b.links[id] = u
	b.units = append(b.units, u)
	return u, nil
}
