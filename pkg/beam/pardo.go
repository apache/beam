package beam

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/userfn"
)

func ParDoN(p *Pipeline, dofn interface{}, col PCollection, opts ...Option) ([]PCollection, error) {
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

func ParDo0(p *Pipeline, dofn interface{}, col PCollection, opts ...Option) error {
	ret, err := ParDoN(p, dofn, col, opts...)
	if err != nil {
		return err
	}
	if len(ret) != 0 {
		return fmt.Errorf("expected 0 output. Found: %v", ret)
	}
	return nil
}

func ParDo(p *Pipeline, dofn interface{}, col PCollection, opts ...Option) (PCollection, error) {
	ret, err := ParDoN(p, dofn, col, opts...)
	if err != nil {
		return PCollection{}, err
	}
	if len(ret) != 1 {
		return PCollection{}, fmt.Errorf("expected 1 output. Found: %v", ret)
	}
	return ret[0], nil
}

func ParDo2(p *Pipeline, dofn interface{}, col PCollection, opts ...Option) (PCollection, PCollection, error) {
	ret, err := ParDoN(p, dofn, col, opts...)
	if err != nil {
		return PCollection{}, PCollection{}, err
	}
	if len(ret) != 2 {
		return PCollection{}, PCollection{}, fmt.Errorf("expected 2 output. Found: %v", ret)
	}
	return ret[0], ret[1], nil
}
