// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dataflow

import (
	"bytes"
	"fmt"
	"net/url"
	"path"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx/v1"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/protox"
	pubsub_v1 "github.com/apache/beam/sdks/go/pkg/beam/io/pubsubio/v1"
	rnapi_pb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/golang/protobuf/proto"
	df "google.golang.org/api/dataflow/v1b3"
)

const (
	impulseKind    = "CreateCollection"
	parDoKind      = "ParallelDo"
	flattenKind    = "Flatten"
	gbkKind        = "GroupByKey"
	windowIntoKind = "Bucket"

	sideInputKind = "CollectionToSingleton"

	// Support for Dataflow native I/O, such as PubSub.
	readKind  = "ParallelRead"
	writeKind = "ParallelWrite"
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
		if edge.Op == graph.CoGBK && len(edge.Input) > 1 {
			expanded, err := expandCoGBK(nodes, edge)
			if err != nil {
				return nil, err
			}
			steps = append(steps, expanded...)
			continue
		}

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
				c, err := encodeCoderRef(edge.Input[i].From.Coder, edge.Input[i].From.WindowingStrategy().Fn.Coder())
				if err != nil {
					return nil, err
				}

				side := &df.Step{
					Name: fmt.Sprintf("view%v_%v", edge.ID(), i),
					Kind: sideInputKind,
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
			coder, err := encodeCoderRef(out.To.Coder, out.To.WindowingStrategy().Fn.Coder())
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

			coder, err := encodeCoderRef(edge.Input[0].From.Coder, edge.Input[0].From.WindowingStrategy().Fn.Coder())
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
		if prop.PubSubWithAttributes {
			// Hack to add a empty-value property for PubSub IO. This
			// will make PubSub send the entire message, not just
			// the payload.
			prop.PubSubWithAttributes = false
			step.Properties = newMsg(propertiesWithPubSubMessage{properties: prop})
		}
		steps = append(steps, step)
	}

	return steps, nil
}

func expandCoGBK(nodes map[int]*outputReference, edge *graph.MultiEdge) ([]*df.Step, error) {
	// TODO(BEAM-490): replace once CoGBK is a primitive. For now, we have to translate
	// CoGBK with multiple PCollections as described in graphx/cogbk.go.

	id := graphx.StableMultiEdgeID(edge)
	wc := edge.Output[0].To.WindowingStrategy().Fn.Coder()
	kvCoder, err := encodeCoderRef(graphx.MakeKVUnionCoder(edge), wc)
	if err != nil {
		return nil, err
	}
	gbkCoder, err := encodeCoderRef(graphx.MakeGBKUnionCoder(edge), wc)
	if err != nil {
		return nil, err
	}

	var steps []*df.Step

	var inputs []*outputReference
	for i, in := range edge.Input {
		// Inject(i)

		injectID := graphx.StableCoGBKInjectID(id, i)
		out := newOutputReference(injectID, "out")

		inject := &df.Step{
			Name: injectID,
			Kind: parDoKind,
			Properties: newMsg(properties{
				ParallelInput: nodes[in.From.ID()],
				SerializedFn: makeSerializedFnPayload(&v1.TransformPayload{
					Urn:    graphx.URNInject,
					Inject: &v1.InjectPayload{N: (int32)(i)},
				}),
				OutputInfo: []output{{
					UserName:   out.OutputName,
					OutputName: out.OutputName,
					Encoding:   kvCoder,
				}},
				UserName: buildName(edge.Scope(), injectID),
			}),
		}

		inputs = append(inputs, out)
		steps = append(steps, inject)
	}

	// Flatten

	flattenID := graphx.StableCoGBKFlattenID(id)
	out := newOutputReference(flattenID, "out")

	flatten := &df.Step{
		Name: flattenID,
		Kind: flattenKind,
		Properties: newMsg(properties{
			Inputs: inputs,
			OutputInfo: []output{{
				UserName:   out.OutputName,
				OutputName: out.OutputName,
				Encoding:   kvCoder,
			}},
			UserName: buildName(edge.Scope(), flattenID),
		}),
	}
	steps = append(steps, flatten)

	// GBK

	gbkID := graphx.StableCoGBKGBKID(id)
	gbkOut := newOutputReference(gbkID, "out")

	w := edge.Input[0].From.WindowingStrategy()
	sfn, err := encodeSerializedFn(translateWindowingStrategy(w))
	if err != nil {
		return nil, err
	}

	gbk := &df.Step{
		Name: gbkID,
		Kind: gbkKind,
		Properties: newMsg(properties{
			ParallelInput: out,
			OutputInfo: []output{{
				UserName:   gbkOut.OutputName,
				OutputName: gbkOut.OutputName,
				Encoding:   gbkCoder,
			}},
			UserName:                buildName(edge.Scope(), "group"),
			DisallowCombinerLifting: true,
			SerializedFn:            sfn,
		}),
	}
	steps = append(steps, gbk)

	// Expand

	ref := nodes[edge.Output[0].To.ID()]
	coder, err := encodeCoderRef(edge.Output[0].To.Coder, wc)
	if err != nil {
		return nil, err
	}

	expand := &df.Step{
		Name: stepID(edge.ID()), // the successor references this name.
		Kind: parDoKind,
		Properties: newMsg(properties{
			ParallelInput: gbkOut,
			OutputInfo: []output{{
				UserName:   ref.OutputName,
				OutputName: ref.OutputName,
				Encoding:   coder,
			}},
			UserName: buildName(edge.Scope(), "expand"),
			SerializedFn: makeSerializedFnPayload(&v1.TransformPayload{
				Urn: graphx.URNExpand,
			}),
		}),
	}
	steps = append(steps, expand)

	return steps, nil
}

// translateNodes builds a map from nodeID to the Dataflow representation.
func translateNodes(edges []*graph.MultiEdge) map[int]*outputReference {
	nodes := make(map[int]*outputReference)
	for _, edge := range edges {
		for i, out := range edge.Output {
			// Encode the ordering to match the scheme in exec/translate/unmarshalKeyedValues()
			name := fmt.Sprintf("i%v", i)
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
		// NOTE: The impulse []data value is encoded in a special way as a
		// URL Query-escaped windowed _unnested_ value. It is read back in
		// a nested context at runtime.
		var buf bytes.Buffer
		if err := exec.EncodeWindowedValueHeader(exec.MakeWindowEncoder(coder.NewGlobalWindow()), window.SingleGlobalWindow, mtime.ZeroTimestamp, &buf); err != nil {
			return "", properties{}, err
		}
		value := string(append(buf.Bytes(), edge.Value...))

		// log.Printf("Impulse data: %v", url.QueryEscape(value))

		return impulseKind, properties{
			UserName: buildName(edge.Scope(), "create"),
			Element:  []string{url.QueryEscape(value)},
		}, nil

	case graph.ParDo:
		return parDoKind, properties{
			UserName:     buildName(edge.Scope(), edge.DoFn.Name()),
			SerializedFn: graphx.StableMultiEdgeID(edge),
		}, nil

	case graph.Combine:
		// TODO(flaviocf) 8/08/2017: When combiners are supported, change "ParallelDo" to
		// "CombineValues", encode accumulator coder and pass it as a property "Encoding".
		return parDoKind, properties{
			UserName:     buildName(edge.Scope(), edge.CombineFn.Name()),
			SerializedFn: graphx.StableMultiEdgeID(edge),
		}, nil

	case graph.WindowInto:
		w := edge.Output[0].To.WindowingStrategy()
		sfn, err := encodeSerializedFn(translateWindowingStrategy(w))
		if err != nil {
			return "", properties{}, err
		}
		return windowIntoKind, properties{
			UserName:     buildName(edge.Scope(), "window"), // TODO: user-defined
			SerializedFn: sfn,
		}, nil

	case graph.CoGBK:
		w := edge.Input[0].From.WindowingStrategy()
		sfn, err := encodeSerializedFn(translateWindowingStrategy(w))
		if err != nil {
			return "", properties{}, err
		}
		return gbkKind, properties{
			UserName:                buildName(edge.Scope(), "group"), // TODO: user-defined
			DisallowCombinerLifting: true,
			SerializedFn:            sfn,
		}, nil

	case graph.Flatten:
		return flattenKind, properties{
			UserName: buildName(edge.Scope(), "flatten"), // TODO: user-defined
		}, nil

	case graph.External:
		switch edge.Payload.URN {
		case pubsub_v1.PubSubPayloadURN:
			// Translate to native handling of PubSub I/O.

			var msg pubsub_v1.PubSubPayload
			if err := proto.Unmarshal(edge.Payload.Data, &msg); err != nil {
				return "", properties{}, fmt.Errorf("bad pubsub payload: %v", err)
			}
			prop := properties{
				UserName:             buildName(edge.Scope(), fmt.Sprintf("%v", msg.Op)),
				Format:               "pubsub",
				PubSubTopic:          msg.GetTopic(),
				PubSubSubscription:   msg.GetSubscription(),
				PubSubIDLabel:        msg.GetIdAttribute(),
				PubSubTimestampLabel: msg.GetTimestampAttribute(),
				PubSubWithAttributes: msg.GetWithAttributes(),
			}
			if prop.PubSubSubscription != "" {
				prop.PubSubTopic = ""
			}

			switch msg.Op {
			case pubsub_v1.PubSubPayload_READ:
				return readKind, prop, nil

			case pubsub_v1.PubSubPayload_WRITE:
				c, err := encodeCoderRef(coder.NewBytes(), edge.Input[0].From.WindowingStrategy().Fn.Coder())
				if err != nil {
					return "", properties{}, fmt.Errorf("bad window coder: %v", err)
				}
				prop.Encoding = c
				return writeKind, prop, nil

			default:
				return "", properties{}, fmt.Errorf("bad pubsub op: %v", msg.Op)
			}

		default:
			return "", properties{}, fmt.Errorf("bad external urn: %v", edge.Payload.URN)
		}

	default:
		return "", properties{}, fmt.Errorf("bad opcode: %v", edge)
	}
}

func makeSerializedFnPayload(payload *v1.TransformPayload) string {
	return protox.MustEncodeBase64(payload)
}

func encodeCoderRef(c *coder.Coder, wc *coder.WindowCoder) (*graphx.CoderRef, error) {
	// TODO(herohde) 3/16/2018: ensure windowed values for Dataflow
	return graphx.EncodeCoderRef(coder.NewW(c, wc))
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

func translateWindowingStrategy(w *window.WindowingStrategy) proto.Message {
	c := graphx.NewCoderMarshaller()
	ws := graphx.MarshalWindowingStrategy(c, w)

	msg := &rnapi_pb.MessageWithComponents{
		Components: &rnapi_pb.Components{
			Coders: c.Build(),
		},
		Root: &rnapi_pb.MessageWithComponents_WindowingStrategy{
			WindowingStrategy: ws,
		},
	}
	return msg
}

func encodeSerializedFn(in proto.Message) (string, error) {
	// The Beam Runner API uses percent-encoding for serialized fn messages.
	// See: https://en.wikipedia.org/wiki/Percent-encoding

	data, err := proto.Marshal(in)
	if err != nil {
		return "", err
	}
	return url.PathEscape(string(data)), nil
}
