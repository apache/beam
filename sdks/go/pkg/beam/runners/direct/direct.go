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
	"github.com/apache/beam/sdks/go/pkg/beam/log"
)

func init() {
	beam.RegisterRunner("direct", Execute)
}

// Execute runs the pipeline in-process.
func Execute(ctx context.Context, p *beam.Pipeline) error {
	log.Info(ctx, "Pipeline:")
	log.Info(ctx, p)

	list, _, err := p.Build()
	if err != nil {
		return fmt.Errorf("invalid pipeline: %v", err)
	}
	return ExecuteInternal(ctx, nil, "", list)
}

// TODO(herohde) 4/29/2017: Cleaner separation of direct (vs other runners) and core exec.
// How to bind in data manager (and state later).

// ExecuteInternal executes the instructions using the supplied contexts and manager. It's exported to
// support being called from in a remote worker configuration.
func ExecuteInternal(ctx context.Context, mgr exec.DataManager, instID string, list []*graph.MultiEdge) error {
	units, err := build(mgr, instID, list)
	if err != nil {
		return fmt.Errorf("translation failed: %v", err)
	}

	log.Debug(ctx, "Execution units:")
	for _, u := range units {
		log.Debugf(ctx, "%v: %v", u.ID(), u)
	}
	return exec.Execute(ctx, units)
}

type linkID struct {
	to    int // graph.MultiEdge
	input int // input index. If > 0, it's a side input.
}

func build(mgr exec.DataManager, instID string, list []*graph.MultiEdge) ([]exec.Unit, error) {
	// (1) Preprocess graph structure

	succ := make(map[int][]linkID) // nodeID -> []linkID
	for _, edge := range list {
		for i, in := range edge.Input {
			from := in.From.ID()
			succ[from] = append(succ[from], linkID{edge.ID(), i})
		}
	}

	next := make(map[linkID]exec.Node) // linkID -> Node
	idgen := &exec.GenID{}

	var units []exec.Unit
	var aux []exec.Unit

	// (2) Create units for each MultiEdge.

	for _, edge := range list {
		switch edge.Op {
		case graph.Impulse:
			unit := &Impulse{UID: idgen.New(), Edge: edge}
			units = append(units, unit)

		case graph.ParDo:
			unit := &exec.ParDo{UID: idgen.New(), Edge: edge}
			units = append(units, unit)

			if len(edge.Input) > 1 {
				// If side inputs are present, we need to buffer them and delay
				// the main processing until all side input are available.

				w := &Wait{UID: idgen.New(), need: len(edge.Input) - 1, next: unit}
				aux = append(aux, w)
				next[linkID{edge.ID(), 0}] = w

				for i := 1; i < len(edge.Input); i++ {
					b := &Buffer{UID: idgen.New(), next: w.ID(), read: unit.ID(), notify: w.notify}
					unit.Side = append(unit.Side, b)
					aux = append(aux, b)
					next[linkID{edge.ID(), i}] = b
				}
			} else {
				next[linkID{edge.ID(), 0}] = unit
			}

		case graph.Combine:
			unit := &exec.Combine{UID: idgen.New(), Edge: edge}
			units = append(units, unit)

			if len(edge.Input) > 1 {
				// If side inputs are present, we need to buffer them and delay
				// the main processing until all side input are available.

				w := &Wait{UID: idgen.New(), need: len(edge.Input) - 1, next: unit}
				aux = append(aux, w)
				next[linkID{edge.ID(), 0}] = w

				for i := 1; i < len(edge.Input); i++ {
					b := &Buffer{UID: idgen.New(), next: w.ID(), read: unit.ID(), notify: w.notify}
					unit.Side = append(unit.Side, b)
					aux = append(aux, b)
					next[linkID{edge.ID(), i}] = b
				}
			} else {
				next[linkID{edge.ID(), 0}] = unit
			}

		case graph.GBK:
			unit := &GBK{UID: idgen.New(), Edge: edge}
			units = append(units, unit)

			next[linkID{edge.ID(), 0}] = unit

		case graph.DataSource:
			unit := &exec.DataSource{UID: idgen.New(), Edge: edge, InstID: instID, Source: mgr}
			units = append(units, unit)

		case graph.DataSink:
			unit := &exec.DataSink{UID: idgen.New(), Edge: edge, InstID: instID, Sink: mgr}
			units = append(units, unit)

			next[linkID{edge.ID(), 0}] = unit

		case graph.Flatten:
			// nop

		default:
			return nil, fmt.Errorf("unexpected opcode: %v", edge)
		}
	}

	// (3) Eliminate flatten

	done := false
	for !done {
		done = true
		for _, edge := range list {
			switch edge.Op {
			case graph.Flatten:
				list := succ[edge.Output[0].To.ID()]
				unit, ok := next[list[0]]
				if !ok {
					// Next is a flatten that is not yet processed. Iterate again.
					done = false
					continue
				}
				for i := range edge.Input {
					next[linkID{edge.ID(), i}] = unit
				}
			}
		}
	}

	// (4) Fixup output.

	for _, unit := range units {
		edge, ok := getEdge(unit)
		if !ok {
			continue
		}

		var out []exec.Node
		for _, o := range edge.Output {
			var n exec.Node

			list := succ[o.To.ID()]
			switch len(list) {
			case 0:
				// Insert discard to ensure that there are no loose ends
				// in the execution graph.
				n = &exec.Discard{UID: idgen.New()}
				aux = append(aux, n)
			case 1:
				n = next[list[0]]
			default:
				// Insert multiplexer if fanout > 1.

				var tmp []exec.Node
				for _, elm := range list {
					tmp = append(tmp, next[elm])
				}
				n = &exec.Multiplex{UID: idgen.New(), Out: tmp}
				aux = append(aux, n)
			}
			out = append(out, n)
		}
		setOut(unit, out)
	}
	return append(units, aux...), nil
}

// TODO(herohde) 6/26/2017: get rid of the below functions. Maybe just add
// methods on units?

func getEdge(unit exec.Unit) (*graph.MultiEdge, bool) {
	switch unit.(type) {
	case *exec.ParDo:
		return unit.(*exec.ParDo).Edge, true
	case *exec.Combine:
		return unit.(*exec.Combine).Edge, true
	case *GBK:
		return unit.(*GBK).Edge, true
	case *exec.DataSource:
		return unit.(*exec.DataSource).Edge, true
	case *exec.DataSink:
		return unit.(*exec.DataSink).Edge, true
	case *Impulse:
		return unit.(*Impulse).Edge, true
	default:
		return nil, false
	}
}

func setOut(unit exec.Unit, out []exec.Node) {
	switch unit.(type) {
	case *exec.ParDo:
		unit.(*exec.ParDo).Out = out
	case *exec.Combine:
		unit.(*exec.Combine).Out = out
	case *GBK:
		if len(out) != 1 {
			panic(fmt.Errorf("bad outputs for GBK: %v", out))
		}
		unit.(*GBK).Out = out[0]
	case *exec.DataSource:
		unit.(*exec.DataSource).Out = out[0]
	case *exec.DataSink:
		// nop
	case *Impulse:
		unit.(*Impulse).Out = out
	default:
		panic(fmt.Sprintf("Unit %v has no output", unit))
	}
}
