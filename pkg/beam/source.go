package beam

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/userfn"
)

// TODO(herohde): more sophisticated source/sink model as per Fn API.
// TODO(herohde): how do a source cut bundles? Or does it even do that?

func Source(p *Pipeline, dofn interface{}, opts ...Option) (PCollection, error) {
	side, data := parseOpts(opts)
	if len(side) > 0 {
		return PCollection{}, fmt.Errorf("sources cannot have side input: %v", side)
	}

	fn, err := userfn.New(dofn)
	if err != nil {
		return PCollection{}, fmt.Errorf("invalid DoFn: %v", err)
	}
	if err := applyData(fn, data); err != nil {
		return PCollection{}, err
	}

	edge, err := graph.NewSource(p.real, p.parent, fn)
	if err != nil {
		return PCollection{}, err
	}
	if len(edge.Output) != 1 {
		return PCollection{}, fmt.Errorf("Source must have exactly one output: %v", edge)
	}
	ret := PCollection{edge.Output[0].To}
	ret.SetCoder(NewCoder(ret.Type()))
	return ret, nil
}

func Sink(p *Pipeline, dofn interface{}, col PCollection, side ...Option) error {
	return ParDo0(p, dofn, col, side...)
}
