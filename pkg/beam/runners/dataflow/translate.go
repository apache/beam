package dataflow

import (
	"encoding/base64"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/reflectx"
	df "google.golang.org/api/dataflow/v1b3"
	"log"
	"path"
	"reflect"
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
				// Side input requires an additional conversion step,
				// which must be before the present one.

				ref := nodes[edge.Input[i].From.ID()]
				side := &df.Step{
					Name: fmt.Sprintf("view%v_%v", edge.ID(), i),
					Kind: "CollectionToSingleton",
					Properties: newMsg(properties{
						ParallelInput: ref,
						OutputInfo: []output{{
							UserName:   "out",
							OutputName: "out",
							Encoding: &encoding{
								Type: "kind:windowed_value",
								Components: []*encoding{
									translateCoder(edge.Input[i].From),
								},
							},
						}},
						UserName: buildName(edge.Parent, "AsView"),
					}),
				}
				steps = append(steps, side)

				prop.NonParallelInputs[side.Name] = newOutputReference(side.Name, "out")
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
		if len(prop.OutputInfo) == 0 {
			// NOTE: Dataflow seems to require at least one output.
			prop.OutputInfo = []output{{
				UserName:   "bogus",
				OutputName: "bogus",
				Encoding:   translateCoder(edge.Input[0].From),
			}}
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
		fn, err := serializeFn(edge)
		if err != nil {
			return "", properties{}, err
		}
		return "ParallelRead", properties{
			CustomSourceInputStep: newCustomSourceInputStep(fn),
			UserName:              buildName(edge.Parent, edge.DoFn.Name),
			Format:                "custom_source",
		}, nil

	case graph.ParDo:
		fn, err := serializeFn(edge)
		if err != nil {
			return "", properties{}, err
		}
		return "ParallelDo", properties{
			UserName:     buildName(edge.Parent, edge.DoFn.Name),
			SerializedFn: fn,
		}, nil

	case graph.GBK:
		return "GroupByKey", properties{
			UserName: buildName(edge.Parent, "group"), // TODO: user-defined
		}, nil

	case graph.Sink:
		return "ParallelWrite", properties{
		// TODO
		}, nil

	case graph.Flatten:
		return "Flatten", properties{
			UserName: buildName(edge.Parent, "flatten"), // TODO: user-defined
		}, nil

	default:
		return "", properties{}, fmt.Errorf("Bad opcode: %v", edge)
	}
}

func serializeFn(edge *graph.MultiEdge) (string, error) {
	// NOTE: Dataflow requires serialized functions to be base64 encoded.

	ref, err := graph.EncodeMultiEdge(edge)
	if err != nil {
		return "", fmt.Errorf("Failed to serialize %v: %v", edge, err)
	}

	log.Printf("SerializedFn: %v", proto.MarshalTextString(ref))

	data, err := proto.Marshal(ref)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(data), nil
}

// NOTE: Dataflow uses "/" to separate composite transforms, so we must remove
// them from the otherwise qualified package names of DoFns, etc.

func buildName(scope *graph.Scope, name string) string {
	if scope.Parent == nil {
		// Ignore "root" node in naming.
		return path.Base(name)
	}
	return buildScopeName(scope) + "/" + path.Base(name)
}

func buildScopeName(scope *graph.Scope) string {
	if scope.Parent.Parent == nil {
		return scope.Label
	}
	return buildScopeName(scope.Parent) + "/" + scope.Label
}

// TODO(herohde) 2/22/2017: for now, use structurally sound - but bogus - coders.

func translateCoder(ref *graph.Node) *encoding {
	return &encoding{
		Type: "kind:windowed_value",
		Components: []*encoding{
			translateType(ref.T),
			{Type: "kind:global_window"},
		},
		IsWrapper: true,
	}
}

func translateType(t reflect.Type) *encoding {
	if k, v, ok := reflectx.UnfoldComposite(t); ok {
		return &encoding{
			Type: "kind:pair",
			Components: []*encoding{
				translateType(k),
				translateType(v),
			},
			IsPairLike: true,
		}
	}

	return &encoding{
		Type: "json",
	}
}

func stepID(id int) string {
	return fmt.Sprintf("s%v", id)
}
