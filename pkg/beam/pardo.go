package beam

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/userfn"
)

// TryParDo attempts to insert a ParDo transform into the pipeline. It may fail
// for multiple reasons, notably that the dofn is not valid or cannot be bound
// -- due to type mismatch, say -- to the incoming PCollections.
func TryParDo(p *Pipeline, dofn interface{}, col PCollection, opts ...Option) ([]PCollection, error) {
	if !col.IsValid() {
		return nil, fmt.Errorf("invalid main pcollection")
	}
	side, data := parseOpts(opts)
	for i, in := range side {
		if !in.Input.IsValid() {
			return nil, fmt.Errorf("invalid side pcollection: index %v", i)
		}
	}

	fn, err := userfn.New(dofn)
	if err != nil {
		return nil, fmt.Errorf("invalid DoFn: %v", err)
	}
	if err := applyData(fn, data); err != nil {
		return nil, err
	}

	in := []*graph.Node{col.n}
	for _, s := range side {
		in = append(in, s.Input.n)
	}
	edge, err := graph.NewParDo(p.real, p.parent, fn, in)
	if err != nil {
		return nil, err
	}

	var ret []PCollection
	for _, out := range edge.Output {
		c := PCollection{out.To}
		c.SetCoder(NewCoder(c.Type()))
		ret = append(ret, c)
	}
	return ret, nil
}

// ParDoN inserts a ParDo transform into the pipeline.
func ParDoN(p *Pipeline, dofn interface{}, col PCollection, opts ...Option) []PCollection {
	return MustN(TryParDo(p, dofn, col, opts...))
}

// ParDo0 inserts a ParDo transform into the pipeline.
func ParDo0(p *Pipeline, dofn interface{}, col PCollection, opts ...Option) {
	ret := MustN(TryParDo(p, dofn, col, opts...))
	if len(ret) != 0 {
		panic(fmt.Sprintf("expected 0 output. Found: %v", ret))
	}
}

// ParDo inserts a ParDo transform into the pipeline.
func ParDo(p *Pipeline, dofn interface{}, col PCollection, opts ...Option) PCollection {
	ret := MustN(TryParDo(p, dofn, col, opts...))
	if len(ret) != 1 {
		panic(fmt.Sprintf("expected 1 output. Found: %v", ret))
	}
	return ret[0]
}

// ParDo2 inserts a ParDo transform into the pipeline.
func ParDo2(p *Pipeline, dofn interface{}, col PCollection, opts ...Option) (PCollection, PCollection) {
	ret := MustN(TryParDo(p, dofn, col, opts...))
	if len(ret) != 2 {
		panic(fmt.Sprintf("expected 2 output. Found: %v", ret))
	}
	return ret[0], ret[1]
}
