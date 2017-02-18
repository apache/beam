package dataflow

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	df "google.golang.org/api/dataflow/v1b3"
)

func translate(edges []*graph.MultiEdge) ([]*df.Step, error) {
	// NOTE: Dataflow apparently assumes that the steps are in topological order.
	// Otherwise, it fails with "Output out for step  was not found." So we
	// preserve the creation order.

	nodes := translateNodes(edges)

	var steps []*df.Step
	for _, edge := range edges {
		kind, prop, err := translateEdge(edge)
		if err != nil {
			return nil, err
		}

		if len(edge.Input) > 0 {
			prop.ParallelInput = nodes[edge.Input[0].From.ID()]

			prop.NonParallelInputs = make(map[string]*outputReference)
			for i := 1; i < len(edge.Input); i++ {
				ref := nodes[edge.Input[i].From.ID()]
				prop.NonParallelInputs[ref.StepName] = ref
			}
		}

		for _, out := range edge.Output {
			ref := nodes[out.To.ID()]
			info := output{
				UserName:   ref.OutputName,
				OutputName: ref.OutputName,
				Encoding:   translateCoder(out.To),
			}
			prop.OutputInfo = append(prop.OutputInfo, info)
		}

		step := &df.Step{
			Name:       stepID(edge.ID()),
			Kind:       kind,
			Properties: newMsg(prop),
		}
		steps = append(steps, step)
	}

	return steps, nil
}

func translateNodes(edges []*graph.MultiEdge) map[int]*outputReference {
	nodes := make(map[int]*outputReference)
	for _, edge := range edges {
		for i, out := range edge.Output {
			name := "out"
			if i > 0 {
				name = fmt.Sprintf("side%v", i)
			}
			nodes[out.To.ID()] = newOutputReference(stepID(edge.ID()), name)
		}
	}
	return nodes
}

// TODO(herohde) 2/15/2017: user names encode composite names via "/"-separation.

func translateEdge(edge *graph.MultiEdge) (string, properties, error) {
	switch edge.Op {
	case graph.Source:
		return "ParallelRead", properties{
			CustomSourceInputStep: newCustomSourceInputStep(edge.DoFn.Name),
			UserName:              edge.DoFn.Name,
			Format:                "custom_source",
		}, nil

	case graph.ParDo:
		return "ParallelDo", properties{
			UserName:     edge.DoFn.Name,
			SerializedFn: edge.DoFn.Name,
		}, nil

	case graph.GBK:
		return "GroupByKey", properties{
			UserName: "group", // TODO: user-defined
		}, nil

	case graph.Sink:
		return "ParallelWrite", properties{
		// TODO
		}, nil

	case graph.Flatten:
		return "Flatten", properties{
			UserName: "flatten", // TODO: user-defined
		}, nil

	default:
		return "", properties{}, fmt.Errorf("Bad opcode: %v", edge)
	}
}

func translateCoder(ref *graph.Node) *encoding {
	// TODO

	return &encoding{
		Type: "string",
	}
}

func stepID(id int) string {
	return fmt.Sprintf("s%v", id)
}
