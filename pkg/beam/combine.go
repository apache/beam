package beam

import (
	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam/graph"
)

// TryCombine attempts to insert a Combine transform into the pipeline. It may fail
// for multiple reasons, notably that the combinefn is not valid or cannot be bound
// -- due to type mismatch, say -- to the incoming PCollections.
func TryCombine(p *Pipeline, combinefn interface{}, col PCollection, opts ...Option) (PCollection, error) {
	if !col.IsValid() {
		return PCollection{}, fmt.Errorf("invalid main pcollection")
	}
	side := parseOpts(opts)
	for i, in := range side {
		if !in.Input.IsValid() {
			return PCollection{}, fmt.Errorf("invalid side pcollection: index %v", i)
		}
	}

	fn, err := graph.NewCombineFn(combinefn)
	if err != nil {
		return PCollection{}, fmt.Errorf("invalid CombineFn: %v", err)
	}

	in := []*graph.Node{col.n}
	for _, s := range side {
		in = append(in, s.Input.n)
	}
	edge, err := graph.NewCombine(p.real, p.parent, fn, in)
	if err != nil {
		return PCollection{}, err
	}
	ret := PCollection{edge.Output[0].To}
	ret.SetCoder(NewCoder(ret.Type()))
	return ret, nil
}

// Combine inserts a Combine transform into the pipeline.
func Combine(p *Pipeline, combinefn interface{}, col PCollection, opts ...Option) PCollection {
	return Must(TryCombine(p, combinefn, col, opts...))
}
