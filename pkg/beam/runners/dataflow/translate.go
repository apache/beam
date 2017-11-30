package dataflow

import (
	"bytes"
	"fmt"
	"log"
	"net/url"
	"path"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	rnapi_pb "github.com/apache/beam/sdks/go/pkg/beam/core/runtime/api/org_apache_beam_runner_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/protox"
	df "google.golang.org/api/dataflow/v1b3"
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
				c, err := graphx.EncodeCoderRef(edge.Input[i].From.Coder)
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
			coder, err := graphx.EncodeCoderRef(out.To.Coder)
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

			coder, err := graphx.EncodeCoderRef(edge.Input[0].From.Coder)
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
	case graph.Impulse:
		c := edge.Output[0].To.Coder

		// NOTE: The impulse []data value is encoded in a special way as a
		// URL Query-escaped windowed _unnested_ value. It is read back in
		// a nested context at runtime.
		var buf bytes.Buffer
		if err := exec.EncodeWindowedValueHeader(c, typex.EventTime(time.Time{}), &buf); err != nil {
			return "", properties{}, err
		}
		value := string(append(buf.Bytes(), edge.Value...))

		// log.Printf("Impulse data: %v", url.QueryEscape(value))

		return "CreateCollection", properties{
			UserName: buildName(edge.Scope(), "create"),
			Element:  []string{url.QueryEscape(value)},
		}, nil

	case graph.Source:
		fn, err := serializeFn(edge)
		if err != nil {
			return "", properties{}, err
		}
		return "ParallelRead", properties{
			CustomSourceInputStep: newCustomSourceInputStep(fn),
			UserName:              buildName(edge.Scope(), edge.DoFn.Name()),
			Format:                "custom_source",
		}, nil

	case graph.ParDo:
		fn, err := serializeFn(edge)
		if err != nil {
			return "", properties{}, err
		}
		return "ParallelDo", properties{
			UserName:     buildName(edge.Scope(), edge.DoFn.Name()),
			SerializedFn: fn,
		}, nil

	case graph.Combine:
		fn, err := serializeFn(edge)
		if err != nil {
			return "", properties{}, err
		}

		// TODO(herohde) 5/30/2017: we need the accumulator coder here instead.
		c, err := graphx.EncodeCoderRef(edge.Input[0].From.Coder)
		if err != nil {
			return "", properties{}, err
		}
		return "CombineValues", properties{
			UserName:     buildName(edge.Scope(), edge.CombineFn.Name()),
			Encoding:     c,
			SerializedFn: fn,
		}, nil

	case graph.GBK:
		w := edge.Input[0].From.Window()
		sfn, err := encodeSerializedFn(translateWindow(w))
		if err != nil {
			return "", properties{}, err
		}
		return "GroupByKey", properties{
			UserName:                buildName(edge.Scope(), "group"), // TODO: user-defined
			DisallowCombinerLifting: true,
			SerializedFn:            sfn,
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

func translateWindow(w *window.Window) proto.Message {
	// TODO: The only windowing strategy we support is the global window.
	if w.Kind() != window.GlobalWindow {
		panic(fmt.Sprintf("Unsupported window type supplied: %v", w))
	}
	// We compute the fixed content of this message for use in workflows.
	msg := rnapi_pb.MessageWithComponents{
		Components: &rnapi_pb.Components{
			Coders: map[string]*rnapi_pb.Coder{
				"Coder": &rnapi_pb.Coder{
					Spec: &rnapi_pb.SdkFunctionSpec{
						Spec: &rnapi_pb.FunctionSpec{
							Urn: "urn:beam:coders:global_window:0.1",
						},
					},
				},
			},
		},
		Root: &rnapi_pb.MessageWithComponents_WindowingStrategy{
			WindowingStrategy: &rnapi_pb.WindowingStrategy{
				WindowFn: &rnapi_pb.SdkFunctionSpec{
					Spec: &rnapi_pb.FunctionSpec{
						Urn: "beam:windowfn:global_windows:v0.1",
					},
				},
				MergeStatus:      rnapi_pb.MergeStatus_NON_MERGING,
				AccumulationMode: rnapi_pb.AccumulationMode_DISCARDING,
				WindowCoderId:    "Coder",
				Trigger: &rnapi_pb.Trigger{
					Trigger: &rnapi_pb.Trigger_Default_{
						Default: &rnapi_pb.Trigger_Default{},
					},
				},
				OutputTime:      rnapi_pb.OutputTime_END_OF_WINDOW,
				ClosingBehavior: rnapi_pb.ClosingBehavior_EMIT_IF_NONEMPTY,
				AllowedLateness: 0,
			},
		},
	}

	return &msg
}

func encodeSerializedFn(in proto.Message) (string, error) {
	// The Beam Runner API uses URL query escaping for serialized fn messages.
	return protox.EncodeQueryEscaped(in)
}
