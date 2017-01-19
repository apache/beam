package beam

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"log"
)

// TODO(herohde): more sophisticated source/sink model as per Fn API.

// TODO(herohde): how do a source cut bundles? Or does it even do that?

// Source inserts a Source transform.
func Source(p *Pipeline, dofn interface{}) (PCollection, error) {
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
	edge.Output = []*graph.Outbound{{To: n}}

	return PCollection{n}, nil
}

func Sink(p *Pipeline, dofn interface{}, col PCollection, side ...PCollection) error {
	return ParDo0(p, dofn, col, side...)
}
