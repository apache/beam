package beam

import (
	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
)

// TODO(herohde): more sophisticated source/sink model as per Fn API.
// TODO(herohde): how do a source cut bundles? Or does it even do that?

// Source inserts a Source into the pipeline.
func Source(p *Pipeline, dofn interface{}, opts ...Option) PCollection {
	return Must(TrySource(p, dofn, opts...))
}

// TrySource inserts a Source into the pipeline.
func TrySource(p *Pipeline, dofn interface{}, opts ...Option) (PCollection, error) {
	side := parseOpts(opts)
	if len(side) > 0 {
		return PCollection{}, fmt.Errorf("sources cannot have side input: %v", side)
	}

	fn, err := graph.NewDoFn(dofn)
	if err != nil {
		return PCollection{}, fmt.Errorf("invalid DoFn: %v", err)
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

// Sink inserts a Sink into the pipeline.
func Sink(p *Pipeline, dofn interface{}, col PCollection, side ...Option) {
	ParDo0(p, dofn, col, side...)
}
