package beam

import (
	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam/graph"
)

// TODO(herohde): make this metadata part of graph.Inbound?

type SideInputKind int

const (
	Singleton SideInputKind = iota
	Map
)

// Option is an optional value or context to a transformation, used at pipeline
// construction time.
type Option interface {
	apply(*graph.MultiEdge) error
}

type SideInput struct {
	Kind  SideInputKind
	Input PCollection

	// TODO(wcn): better types for these?
	// Motivating code snippet is
	// https://paste.googleplex.com/6748543817613312
	WindowFn interface{}
	ViewFn   interface{}
}

func (s SideInput) apply(edge *graph.MultiEdge) error {
	_, side := edge.DoFn.Input()
	index := len(edge.Input) - 1
	if index < 0 || index >= len(side) {
		return fmt.Errorf("No type for side input: %v", s.Input)
	}

	edge.Input = append(edge.Input, &graph.Inbound{From: s.Input.n, T: side[index]})
	return nil
}

type Data struct {
	Data interface{}
}

func (d Data) apply(edge *graph.MultiEdge) error {
	edge.Data = d.Data
	return nil
}

func parseOpts(opts []Option) ([]SideInput, []Data, error) {
	var side []SideInput
	var data []Data

	for _, opt := range opts {
		switch opt.(type) {
		case Data:
			data = append(data, opt.(Data))
		case SideInput:
			side = append(side, opt.(SideInput))
		default:
			return nil, nil, fmt.Errorf("Unexpected opt: %v", opt)
		}
	}
	return side, data, nil
}

func applyOpts(opts []Option, edge *graph.MultiEdge) error {
	for _, opt := range opts {
		if err := opt.apply(edge); err != nil {
			return err
		}
	}
	return nil
}
