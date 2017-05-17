package dataflow

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/runtime/graphx"
	"github.com/apache/beam/sdks/go/pkg/beam/util/protox"
	df "google.golang.org/api/dataflow/v1b3"
	"log"
	"path"
)

// translate translates a Graph into a sequence of Dataflow steps. The step
// representation and its semantics are complex. In particular, the service
// optimizes the steps (step fusing, etc.) and may move steps around. Our
// decorations of the steps must thus be robust against such changes, so that
// they can be properly decoded in the harness. There are multiple quirks and
// requirements of specific semi-opaque formats, such as base64 encoded blobs.
//
// Moreover, the harness sees pieces of the translated steps only -- not the
// full graph. Special steps are also inserted around GBK, for example, which
// makes the placement of the decoration somewhat tricky. The harness will
// also never see steps that the service executes directly, notably GBK/CoGBK.
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

		if edge.Op == graph.Flatten {
			for _, in := range edge.Input {
				prop.Inputs = append(prop.Inputs, nodes[in.From.ID()])
			}
		} else if len(edge.Input) > 0 {
			prop.ParallelInput = nodes[edge.Input[0].From.ID()]

			prop.NonParallelInputs = make(map[string]*outputReference)
			for i := 1; i < len(edge.Input); i++ {
				// Side input requires an additional conversion step, which must
				// be before the present one.

				ref := nodes[edge.Input[i].From.ID()]
				c, err := graphx.EncodeCoder(edge.Input[i].From.Coder)
				if err != nil {
					return nil, err
				}

				side := &df.Step{
					Name: fmt.Sprintf("view%v_%v", edge.ID(), i),
					Kind: "CollectionToSingleton",
					Properties: newMsg(properties{
						ParallelInput: ref,
						OutputInfo: []output{{
							UserName:   "out",
							OutputName: "out",
							Encoding:   graphx.WrapExtraWindowedValue(c),
						}},
						UserName: buildName(edge.Scope(), "AsView"),
					}),
				}
				steps = append(steps, side)

				prop.NonParallelInputs[side.Name] = newOutputReference(side.Name, "out")
			}
		}

		for _, out := range edge.Output {
			ref := nodes[out.To.ID()]
			coder, err := graphx.EncodeCoder(out.To.Coder)
			if err != nil {
				return nil, err
			}
			info := output{
				UserName:   ref.OutputName,
				OutputName: ref.OutputName,
				Encoding:   coder,
			}
			prop.OutputInfo = append(prop.OutputInfo, info)
		}
		if len(prop.OutputInfo) == 0 {
			// Dataflow seems to require at least one output. We insert
			// a bogus one (named "bogus") and remove it in the harness.

			coder, err := graphx.EncodeCoder(edge.Input[0].From.Coder)
			if err != nil {
				return nil, err
			}

			prop.OutputInfo = []output{{
				UserName:   "bogus",
				OutputName: "bogus",
				Encoding:   coder,
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

// TODO(herohde) 5/16/2017: we might need to robustly encode the index into the
// name, so that we can infer the ordering from the names in the harness.

// translateNodes builds a map from nodeID to the Dataflow representation.
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
// We'll need to ensure that scopes are uniquely named.

// translateEdge translates part of a MultiEdge to the Dataflow kind and
// step-specific properties. We can't conveniently return a Step, because we
// need to add more properties and the (partly-encoded) Step form prevents such
// updates.
func translateEdge(edge *graph.MultiEdge) (string, properties, error) {
	switch edge.Op {
	case graph.Source:
		fn, err := serializeFn(edge)
		if err != nil {
			return "", properties{}, err
		}
		return "ParallelRead", properties{
			CustomSourceInputStep: newCustomSourceInputStep(fn),
			UserName:              buildName(edge.Scope(), edge.DoFn.Name),
			Format:                "custom_source",
		}, nil

	case graph.ParDo:
		fn, err := serializeFn(edge)
		if err != nil {
			return "", properties{}, err
		}
		return "ParallelDo", properties{
			UserName:     buildName(edge.Scope(), edge.DoFn.Name),
			SerializedFn: fn,
		}, nil

	case graph.GBK:
		return "GroupByKey", properties{
			UserName:                buildName(edge.Scope(), "group"), // TODO: user-defined
			DisallowCombinerLifting: true,
		}, nil

	case graph.Sink:
		return "ParallelWrite", properties{
		// TODO
		}, nil

	case graph.Flatten:
		return "Flatten", properties{
			UserName: buildName(edge.Scope(), "flatten"), // TODO: user-defined
		}, nil

	default:
		return "", properties{}, fmt.Errorf("Bad opcode: %v", edge)
	}
}

// serializeFn encodes and then base64-encodes a MultiEdge. Dataflow requires
// serialized functions to be base64 encoded.
func serializeFn(edge *graph.MultiEdge) (string, error) {
	ref, err := graphx.EncodeMultiEdge(edge)
	if err != nil {
		return "", fmt.Errorf("Failed to serialize %v: %v", edge, err)
	}

	log.Printf("SerializedFn: %v", proto.MarshalTextString(ref))
	return protox.EncodeBase64(ref)
}

// buildName computes a Dataflow composite name understood by the Dataflow UI,
// determined by the scope nesting. Dataflow simply uses "/" to separate
// composite transforms, so we must remove them from the otherwise qualified
// package names of DoFns, etc.
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

// stepID converts a MultiEdge ID, i, to a Step ID, "s"+i. The name has no
// semantic meaning.
func stepID(id int) string {
	return fmt.Sprintf("s%v", id)
}
