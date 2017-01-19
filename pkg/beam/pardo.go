package beam

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"log"
)

// ParDo inserts a ParDo transformation into the pipeline.
func ParDo(p *Pipeline, dofn interface{}, col PCollection, side ...PCollection) ([]PCollection, error) {
	fn, err := graph.ReflectFn(dofn)
	if err != nil {
		return nil, err
	}

	log.Printf("ParDo: %v::%v -> %v", col, side, fn)

	// (1) Match and typecheck inputs.

	in, views := fn.Input()
	if v, ok := graph.Bind(col.Type(), in, views); ok {
		views = v
	} else {
		return nil, fmt.Errorf("Cannot bind input (%v, %v) to fn: %v", col, side, fn)
	}

	if len(views) != len(side) {
		return nil, fmt.Errorf("Mismatch number of side inputs: %v, expected %v", side, views)
	}
	for i := 0; i < len(views); i++ {
		if !graph.BindSide(views[i], side[i].Type()) {
			return nil, fmt.Errorf("Mismatch type for side input %v: %v, expected %v", i, side[i], views[i])
		}
	}

	// (2) Insert transformation

	edge := p.real.NewEdge(p.parent)
	edge.Op = graph.ParDo
	edge.DoFn = fn
	edge.Input = []*graph.Inbound{{From: col.n}}
	for _, s := range side {
		edge.Input = append(edge.Input, &graph.Inbound{From: s.n})
	}

	// (3) Generate outputs.

	var ret []PCollection
	for _, t := range fn.Output() {
		n := p.real.NewNode(t)

		edge.Output = append(edge.Output, &graph.Outbound{To: n})
		ret = append(ret, PCollection{n})
	}

	return ret, nil
}

func ParDo0(p *Pipeline, dofn interface{}, col PCollection, side ...PCollection) error {
	ret, err := ParDo(p, dofn, col, side...)
	if err != nil {
		return err
	}
	if len(ret) != 0 {
		return fmt.Errorf("Expected 0 output. Found: %v", ret)
	}
	return nil
}

func ParDo1(p *Pipeline, dofn interface{}, col PCollection, side ...PCollection) (PCollection, error) {
	ret, err := ParDo(p, dofn, col, side...)
	if err != nil {
		return PCollection{}, err
	}
	if len(ret) != 1 {
		return PCollection{}, fmt.Errorf("Expected 1 output. Found: %v", ret)
	}
	return ret[0], nil
}

func ParDo2(p *Pipeline, dofn interface{}, col PCollection, side ...PCollection) (PCollection, PCollection, error) {
	ret, err := ParDo(p, dofn, col, side...)
	if err != nil {
		return PCollection{}, PCollection{}, err
	}
	if len(ret) != 2 {
		return PCollection{}, PCollection{}, fmt.Errorf("Expected 2 output. Found: %v", ret)
	}
	return ret[0], ret[1], nil
}
