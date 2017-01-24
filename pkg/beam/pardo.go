package beam

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/reflectx"
	"log"
)

// ParDo inserts a ParDo transformation into the pipeline.
func ParDo(p *Pipeline, dofn interface{}, col PCollection, opts ...Option) ([]PCollection, error) {
	fn, err := graph.ReflectFn(dofn)
	if err != nil {
		return nil, err
	}
	side, _, err := parseOpts(opts)
	if err != nil {
		return nil, err
	}

	log.Printf("ParDo: %v::%v -> %v", col, side, fn)

	// (1) Match and typecheck inputs.

	in, views := fn.Input()
	bound, boundViews, ok := graph.Bind(col.Type(), in, views)
	if !ok {
		return nil, fmt.Errorf("Cannot bind input (%v, %v) to fn: %v", col, side, fn)
	}

	if len(boundViews) != len(side) {
		return nil, fmt.Errorf("Mismatch number of side inputs: %v, expected %v", side, boundViews)
	}
	for i := 0; i < len(boundViews); i++ {
		if !graph.BindSide(boundViews[i], side[i].Input.Type()) {
			return nil, fmt.Errorf("Mismatch type for side input %v: %v, expected %v", i, side[i], boundViews[i])
		}
	}

	// TODO(herohde): check that we can bind data, if present

	// (2) Insert transformation

	edge := p.real.NewEdge(p.parent)
	edge.Op = graph.ParDo
	edge.DoFn = fn
	edge.Input = []*graph.Inbound{{From: col.n, T: bound}}
	applyOpts(opts, edge)

	// (3) Generate outputs.

	var ret []PCollection
	for _, t := range fn.Output() {
		real := t

		// If generic function, guess that the outputs have the same real
		// type as the main input. It's a bit of a fishing expedition ...

		// TODO(herohde): easier way to specify manually

		from := reflectx.ClassOf(col.n.T)
		to := reflectx.ClassOf(real)
		switch {
		case to.IsGeneric():
			real = col.n.T

		case to == reflectx.Composite:
			k, v, _ := reflectx.UnfoldComposite(real)
			classK := reflectx.ClassOf(k)
			classV := reflectx.ClassOf(v)

			if classK.IsGeneric() || classV.IsGeneric() {
				if classK.IsGeneric() {
					switch from {
					case reflectx.Composite:
						k, _, _ = reflectx.UnfoldComposite(col.n.T)
					default:
						k = col.n.T
					}
				}
				if classV.IsGeneric() {
					switch from {
					case reflectx.Composite:
						_, v, _ = reflectx.UnfoldComposite(col.n.T)
					default:
						v = col.n.T
					}
				}
				real, _ = reflectx.MakeComposite(real, k, v)
			}
		}

		n := p.real.NewNode(real)

		edge.Output = append(edge.Output, &graph.Outbound{To: n, T: t})
		ret = append(ret, PCollection{n})
	}

	log.Printf("Edge: %v", edge)
	return ret, nil
}

func ParDo0(p *Pipeline, dofn interface{}, col PCollection, opts ...Option) error {
	ret, err := ParDo(p, dofn, col, opts...)
	if err != nil {
		return err
	}
	if len(ret) != 0 {
		return fmt.Errorf("Expected 0 output. Found: %v", ret)
	}
	return nil
}

func ParDo1(p *Pipeline, dofn interface{}, col PCollection, opts ...Option) (PCollection, error) {
	ret, err := ParDo(p, dofn, col, opts...)
	if err != nil {
		return PCollection{}, err
	}
	if len(ret) != 1 {
		return PCollection{}, fmt.Errorf("Expected 1 output. Found: %v", ret)
	}
	return ret[0], nil
}

func ParDo2(p *Pipeline, dofn interface{}, col PCollection, opts ...Option) (PCollection, PCollection, error) {
	ret, err := ParDo(p, dofn, col, opts...)
	if err != nil {
		return PCollection{}, PCollection{}, err
	}
	if len(ret) != 2 {
		return PCollection{}, PCollection{}, fmt.Errorf("Expected 2 output. Found: %v", ret)
	}
	return ret[0], ret[1], nil
}
