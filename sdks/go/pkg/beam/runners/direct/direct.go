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

// Package direct contains the direct runner for running single-bundle
// pipelines in the current process. Useful for testing.
package direct

import (
	"context"
	"path"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/options/jobopts"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/vet"
)

func init() {
	beam.RegisterRunner("direct", Execute)
}

// Execute runs the pipeline in-process.
func Execute(ctx context.Context, p *beam.Pipeline) error {
	log.Info(ctx, "Executing pipeline with the direct runner.")

	if !beam.Initialized() {
		log.Warn(ctx, "Beam has not been initialized. Call beam.Init() before pipeline construction.")
	}

	log.Info(ctx, "Pipeline:")
	log.Info(ctx, p)

	if *jobopts.Strict {
		log.Info(ctx, "Strict mode enabled, applying additional validation.")
		if err := vet.Execute(ctx, p); err != nil {
			return errors.Wrap(err, "strictness check failed")
		}
		log.Info(ctx, "Strict mode validation passed.")
	}

	edges, _, err := p.Build()
	if err != nil {
		return errors.Wrap(err, "invalid pipeline")
	}
	plan, err := Compile(edges)
	if err != nil {
		return errors.Wrap(err, "translation failed")
	}
	log.Info(ctx, plan)

	if err = plan.Execute(ctx, "", exec.DataContext{}); err != nil {
		plan.Down(ctx) // ignore any teardown errors
		return err
	}
	if err = plan.Down(ctx); err != nil {
		return err
	}
	// TODO(lostluck) 2020/01/24: What's the right way to expose the
	// metrics store for the direct runner?
	metrics.DumpToLogFromStore(ctx, plan.Store())
	return nil
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
		pardo := &exec.ParDo{
			UID:     b.idgen.New(),
			Fn:      edge.DoFn,
			Inbound: edge.Input,
			Out:     out,
			PID:     path.Base(edge.DoFn.Name()),
		}
		u = pardo
		if edge.DoFn.IsSplittable() {
			u = &exec.SdfFallback{PDo: pardo}
		}
		if len(edge.Input) == 1 {
			break
		}

		// ParDo w/ side input. We need to insert buffering and wait. We also need to
		// ensure that we return the correct link node.

		b.units = append(b.units, u)

		w := &wait{UID: b.idgen.New(), need: len(edge.Input) - 1, next: u}
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
		usesKey := typex.IsKV(edge.Input[0].Type)

		u = &exec.Combine{
			UID:     b.idgen.New(),
			Fn:      edge.CombineFn,
			UsesKey: usesKey,
			Out:     out[0],
			PID:     path.Base(edge.CombineFn.Name()),
		}

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

	case graph.Reshuffle:
		// Reshuffle is a no-op in the direct runner, as there's only a single bundle
		// on a single worker. Hoist the next node up in the cache.
		b.links[id] = out[0]
		return b.links[id], nil

	case graph.Flatten:
		u = &exec.Flatten{UID: b.idgen.New(), N: len(edge.Input), Out: out[0]}

		for i := 0; i < len(edge.Input); i++ {
			b.links[linkID{edge.ID(), i}] = u
		}

	case graph.WindowInto:
		u = &exec.WindowInto{UID: b.idgen.New(), Fn: edge.WindowFn, Out: out[0]}

	default:
		return nil, errors.Errorf("unexpected edge: %v", edge)
	}

	b.links[id] = u
	b.units = append(b.units, u)
	return u, nil
}
