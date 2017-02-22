package beam

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"log"
)

// TODO(herohde): more sophisticated source/sink model as per Fn API.

// TODO(herohde): how do a source cut bundles? Or does it even do that?

// source inserts a Source transform.
func source(p *Pipeline, dofn interface{}, opts ...Option) (PCollection, error) {
	fn, err := graph.ReflectFn(dofn)
	if err != nil {
		return PCollection{}, err
	}

	log.Printf("Source: %v", fn)

	// (1) Validate that it is indeed a "source" user fn

	if in, views := fn.Input(); len(in) > 0 || len(views) > 0 {
		return PCollection{}, fmt.Errorf("Source cannot have input: (%v, %v) ", in, views)
	}

	out := fn.Output()
	if len(out) != 1 {
		return PCollection{}, fmt.Errorf("Source must have exactly one output: %v", out)
	}
	for _, p := range fn.Ret {
		if p.Kind == graph.RetValue {
			return PCollection{}, fmt.Errorf("Source must use channel form for output: %v", p)
		}
	}

	// (2) Insert transformation

	n := p.real.NewNode(out[0])

	edge := p.real.NewEdge(p.parent)
	edge.Op = graph.Source
	edge.DoFn = fn
	edge.Output = []*graph.Outbound{{To: n, T: out[0]}}
	applyOpts(opts, edge)

	return PCollection{n}, nil
}

func Source(p *Pipeline, dofn interface{}, opts ...Option) PCollection {
	ret, err := source(p, dofn, opts...)
	if err != nil {
		panic(err)
	}
	return ret
}

func Sink(p *Pipeline, dofn interface{}, col PCollection, side ...Option) {
	ParDo0(p, dofn, col, side...)
}
