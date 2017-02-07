package beam

import (
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/reflectx"
	"log"
)

// parDo inserts a ParDo transformation into the pipeline. It returns an
// explicit error for wrappers to include into the relevant PCollections.
func parDo(p *Pipeline, dofn interface{}, col PCollection, opts ...Option) ([]PCollection, error) {
	fn, err := graph.ReflectFn(dofn)
	if err != nil {
		return nil, Errorf(3, "Bad DoFn: %v", err)
	}
	side, _, err := parseOpts(opts)
	if err != nil {
		return nil, Errorf(3, "Bad options: %v", err)
	}

	if !col.IsValid() {
		return nil, Errorf(3, "Bad main pcollection")
	}
	for i, in := range side {
		if !in.Input.IsValid() {
			return nil, Errorf(3, "Bad side pcollection: index %v", i)
		}
	}

	log.Printf("ParDo: %v::%v -> %v", col, side, fn)

	// (1) Match and typecheck inputs.

	in, views := fn.Input()
	bound, boundViews, ok := graph.Bind(col.Type(), in, views)
	if !ok {
		return nil, Errorf(3, "ParDo: Cannot bind input (%v, %v) to fn: %v", col, side, fn)
	}

	if len(boundViews) != len(side) {
		return nil, Errorf(3, "ParDo: Mismatch number of side inputs: %v, expected %v", side, boundViews)
	}
	for i := 0; i < len(boundViews); i++ {
		if !graph.BindSide(boundViews[i], side[i].Input.Type()) {
			return nil, Errorf(3, "ParDo: Mismatch type for side input %v: %v, expected %v", i, side[i], boundViews[i])
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
		ret = append(ret, PCollection{n: n})
	}

	log.Printf("Edge: %v", edge)
	return ret, nil
}

// NOTE(herohde) 2/6/2017: A small winkle on errors in PCollections, is that for
// sinks, we have no good place to place them. We could push back to main incoming,
// but we use pipeline-level errors instead.

func ParDo0(p *Pipeline, dofn interface{}, col PCollection, opts ...Option) {
	ret, err := parDo(p, dofn, col, opts...)
	if err != nil {
		p.errs.Add(err)
		return
	}
	if len(ret) != 0 {
		p.errs.Add(Errorf(2, "ParDo: Expected 0 output. Found: %v", ret))
	}
}

func ParDo(p *Pipeline, dofn interface{}, col PCollection, opts ...Option) PCollection {
	ret, err := parDo(p, dofn, col, opts...)
	if err != nil {
		p.errs.Add(err)
		return PCollection{}
	}
	if len(ret) != 1 {
		p.errs.Add(Errorf(2, "ParDo: Expected 1 output. Found: %v", ret))
		return PCollection{}
	}
	return ret[0]
}

func ParDo2(p *Pipeline, dofn interface{}, col PCollection, opts ...Option) (PCollection, PCollection) {
	ret, err := parDo(p, dofn, col, opts...)
	if err != nil {
		p.errs.Add(err)
		return PCollection{}, PCollection{}
	}
	if len(ret) != 2 {
		p.errs.Add(Errorf(2, "ParDo: Expected 2 output. Found: %v", ret))
		return PCollection{}, PCollection{}
	}
	return ret[0], ret[1]
}
