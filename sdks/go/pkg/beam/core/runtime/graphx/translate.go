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

package graphx

import (
	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	v1 "github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx/v1"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/pipelinex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/protox"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	pb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
)

// Model constants for interfacing with a Beam runner.
// TODO(lostluck): 2018/05/28 Extract these from their enum descriptors in the pipeline_v1 proto
const (
	URNImpulse       = "beam:transform:impulse:v1"
	URNParDo         = "beam:transform:pardo:v1"
	URNFlatten       = "beam:transform:flatten:v1"
	URNGBK           = "beam:transform:group_by_key:v1"
	URNCombinePerKey = "beam:transform:combine_per_key:v1"
	URNWindow        = "beam:transform:window:v1"

	// URNIterableSideInput = "beam:side_input:iterable:v1"
	URNMultimapSideInput = "beam:side_input:multimap:v1"

	URNGlobalWindowsWindowFn  = "beam:windowfn:global_windows:v0.1"
	URNFixedWindowsWindowFn   = "beam:windowfn:fixed_windows:v0.1"
	URNSlidingWindowsWindowFn = "beam:windowfn:sliding_windows:v0.1"
	URNSessionsWindowFn       = "beam:windowfn:session_windows:v0.1"

	// SDK constants

	// URNJavaDoFn is the legacy constant for marking a DoFn.
	// TODO: remove URNJavaDoFN when the Dataflow runner
	// uses the model pipeline and no longer falls back to Java.
	URNJavaDoFn = "beam:dofn:javasdk:0.1"
	URNDoFn     = "beam:go:transform:dofn:v1"

	URNIterableSideInputKey = "beam:go:transform:iterablesideinputkey:v1"
)

// TODO(herohde) 11/6/2017: move some of the configuration into the graph during construction.

// Options for marshalling a graph into a model pipeline.
type Options struct {
	// Environment used to run the user code.
	Environment pb.Environment
}

// Marshal converts a graph to a model pipeline.
func Marshal(edges []*graph.MultiEdge, opt *Options) (*pb.Pipeline, error) {
	tree := NewScopeTree(edges)

	m := newMarshaller(opt)
	for _, edge := range tree.Edges {
		m.addMultiEdge(edge)
	}
	for _, t := range tree.Children {
		m.addScopeTree(t)
	}

	p := &pb.Pipeline{
		Components: m.build(),
	}
	return pipelinex.Normalize(p)
}

type marshaller struct {
	opt *Options

	transforms   map[string]*pb.PTransform
	pcollections map[string]*pb.PCollection
	windowing    map[string]*pb.WindowingStrategy
	environments map[string]*pb.Environment

	coders *CoderMarshaller

	windowing2id map[string]string
}

func newMarshaller(opt *Options) *marshaller {
	return &marshaller{
		opt:          opt,
		transforms:   make(map[string]*pb.PTransform),
		pcollections: make(map[string]*pb.PCollection),
		windowing:    make(map[string]*pb.WindowingStrategy),
		environments: make(map[string]*pb.Environment),
		coders:       NewCoderMarshaller(),
		windowing2id: make(map[string]string),
	}
}

func (m *marshaller) build() *pb.Components {
	return &pb.Components{
		Transforms:          m.transforms,
		Pcollections:        m.pcollections,
		WindowingStrategies: m.windowing,
		Environments:        m.environments,
		Coders:              m.coders.Build(),
	}
}

func (m *marshaller) addScopeTree(s *ScopeTree) string {
	id := scopeID(s.Scope.Scope)
	if _, exists := m.transforms[id]; exists {
		return id
	}

	var subtransforms []string
	for _, edge := range s.Edges {
		subtransforms = append(subtransforms, m.addMultiEdge(edge)...)
	}
	for _, tree := range s.Children {
		subtransforms = append(subtransforms, m.addScopeTree(tree))
	}

	transform := &pb.PTransform{
		UniqueName:    s.Scope.Name,
		Subtransforms: subtransforms,
	}

	m.updateIfCombineComposite(s, transform)

	m.transforms[id] = transform
	return id
}

// updateIfCombineComposite examines the scope tree and sets the PTransform Spec
// to be a CombinePerKey with a CombinePayload if it's a liftable composite.
// Beam Portability requires that composites contain an implementation for runners
// that don't understand the URN and Payload, which this lightly checks for.
func (m *marshaller) updateIfCombineComposite(s *ScopeTree, transform *pb.PTransform) {
	if s.Scope.Name != graph.CombinePerKeyScope ||
		len(s.Edges) != 2 ||
		len(s.Edges[0].Edge.Input) != 1 ||
		len(s.Edges[1].Edge.Output) != 1 ||
		s.Edges[1].Edge.Op != graph.Combine {
		return
	}

	edge := s.Edges[1].Edge
	acID := m.coders.Add(edge.AccumCoder)
	payload := &pb.CombinePayload{
		CombineFn: &pb.FunctionSpec{
			Urn:     URNJavaDoFn,
			Payload: []byte(mustEncodeMultiEdgeBase64(edge)),
		},
		AccumulatorCoderId: acID,
	}
	transform.Spec = &pb.FunctionSpec{Urn: URNCombinePerKey, Payload: protox.MustEncode(payload)}
	transform.EnvironmentId = m.addDefaultEnv()
}

func (m *marshaller) addMultiEdge(edge NamedEdge) []string {
	id := edgeID(edge.Edge)
	if _, exists := m.transforms[id]; exists {
		return []string{id}
	}

	if edge.Edge.Op == graph.CoGBK && len(edge.Edge.Input) > 1 {
		return []string{m.expandCoGBK(edge)}
	}

	inputs := make(map[string]string)
	for i, in := range edge.Edge.Input {
		m.addNode(in.From)
		inputs[fmt.Sprintf("i%v", i)] = nodeID(in.From)
	}
	outputs := make(map[string]string)
	for i, out := range edge.Edge.Output {
		m.addNode(out.To)
		outputs[fmt.Sprintf("i%v", i)] = nodeID(out.To)
	}

	// allPIds tracks additional PTransformIDs generated for the pipeline
	var allPIds []string
	var spec *pb.FunctionSpec
	var transformEnvID = ""
	switch edge.Edge.Op {
	case graph.Impulse:
		// TODO(herohde) 7/18/2018: Encode data?
		spec = &pb.FunctionSpec{Urn: URNImpulse}

	case graph.ParDo:
		si := make(map[string]*pb.SideInput)
		for i, in := range edge.Edge.Input {
			switch in.Kind {
			case graph.Main:
				// ignore: not a side input

			case graph.Singleton, graph.Slice, graph.Iter, graph.ReIter:
				// The only supported form of side input is MultiMap, but we
				// want just iteration. So we must manually add a fixed key,
				// "", even if the input is already KV.

				out := fmt.Sprintf("%v_keyed%v_%v", nodeID(in.From), edgeID(edge.Edge), i)
				m.makeNode(out, m.coders.Add(makeBytesKeyedCoder(in.From.Coder)), in.From)

				payload := &pb.ParDoPayload{
					DoFn: &pb.FunctionSpec{
						Urn: URNIterableSideInputKey,
						Payload: []byte(protox.MustEncodeBase64(&v1.TransformPayload{
							Urn: URNIterableSideInputKey,
						})),
					},
				}

				keyedID := fmt.Sprintf("%v_keyed%v", edgeID(edge.Edge), i)
				keyed := &pb.PTransform{
					UniqueName: keyedID,
					Spec: &pb.FunctionSpec{
						Urn:     URNParDo,
						Payload: protox.MustEncode(payload),
					},
					Inputs:        map[string]string{"i0": nodeID(in.From)},
					Outputs:       map[string]string{"i0": out},
					EnvironmentId: m.addDefaultEnv(),
				}
				m.transforms[keyedID] = keyed
				allPIds = append(allPIds, keyedID)

				// Fixup input map
				inputs[fmt.Sprintf("i%v", i)] = out

				si[fmt.Sprintf("i%v", i)] = &pb.SideInput{
					AccessPattern: &pb.FunctionSpec{
						Urn: URNMultimapSideInput,
					},
					ViewFn: &pb.FunctionSpec{
						Urn: "foo",
					},
					WindowMappingFn: &pb.FunctionSpec{
						Urn: "bar",
					},
				}

			case graph.Map, graph.MultiMap:
				panic("NYI")

			default:
				panic(fmt.Sprintf("unexpected input kind: %v", edge))
			}
		}

		payload := &pb.ParDoPayload{
			DoFn: &pb.FunctionSpec{
				Urn:     URNJavaDoFn,
				Payload: []byte(mustEncodeMultiEdgeBase64(edge.Edge)),
			},
			SideInputs: si,
		}
		transformEnvID = m.addDefaultEnv()
		spec = &pb.FunctionSpec{Urn: URNParDo, Payload: protox.MustEncode(payload)}

	case graph.Combine:
		payload := &pb.ParDoPayload{
			DoFn: &pb.FunctionSpec{
				Urn:     URNJavaDoFn,
				Payload: []byte(mustEncodeMultiEdgeBase64(edge.Edge)),
			},
		}
		transformEnvID = m.addDefaultEnv()
		spec = &pb.FunctionSpec{Urn: URNParDo, Payload: protox.MustEncode(payload)}

	case graph.Flatten:
		spec = &pb.FunctionSpec{Urn: URNFlatten}

	case graph.CoGBK:
		spec = &pb.FunctionSpec{Urn: URNGBK}

	case graph.WindowInto:
		payload := &pb.WindowIntoPayload{
			WindowFn: makeWindowFn(edge.Edge.WindowFn),
		}
		spec = &pb.FunctionSpec{Urn: URNWindow, Payload: protox.MustEncode(payload)}

	case graph.External:
		spec = &pb.FunctionSpec{Urn: edge.Edge.Payload.URN, Payload: edge.Edge.Payload.Data}

	default:
		panic(fmt.Sprintf("Unexpected opcode: %v", edge.Edge.Op))
	}

	transform := &pb.PTransform{
		UniqueName:    edge.Name,
		Spec:          spec,
		Inputs:        inputs,
		Outputs:       outputs,
		EnvironmentId: transformEnvID,
	}
	m.transforms[id] = transform
	allPIds = append(allPIds, id)
	return allPIds
}

func (m *marshaller) expandCoGBK(edge NamedEdge) string {
	// TODO(BEAM-490): replace once CoGBK is a primitive. For now, we have to translate
	// CoGBK with multiple PCollections as described in cogbk.go.

	id := edgeID(edge.Edge)
	kvCoderID := m.coders.Add(MakeKVUnionCoder(edge.Edge))
	gbkCoderID := m.coders.Add(MakeGBKUnionCoder(edge.Edge))

	var subtransforms []string

	inputs := make(map[string]string)
	for i, in := range edge.Edge.Input {
		m.addNode(in.From)

		out := fmt.Sprintf("%v_%v_inject%v", nodeID(in.From), id, i)
		m.makeNode(out, kvCoderID, in.From)

		// Inject(i)

		injectID := fmt.Sprintf("%v_inject%v", id, i)
		payload := &pb.ParDoPayload{
			DoFn: &pb.FunctionSpec{
				Urn: URNInject,
				Payload: []byte(protox.MustEncodeBase64(&v1.TransformPayload{
					Urn:    URNInject,
					Inject: &v1.InjectPayload{N: (int32)(i)},
				})),
			},
		}
		inject := &pb.PTransform{
			UniqueName: injectID,
			Spec: &pb.FunctionSpec{
				Urn:     URNParDo,
				Payload: protox.MustEncode(payload),
			},
			Inputs:        map[string]string{"i0": nodeID(in.From)},
			Outputs:       map[string]string{"i0": out},
			EnvironmentId: m.addDefaultEnv(),
		}
		m.transforms[injectID] = inject
		subtransforms = append(subtransforms, injectID)

		inputs[fmt.Sprintf("i%v", i)] = out
	}

	outNode := edge.Edge.Output[0].To

	// Flatten

	out := fmt.Sprintf("%v_flatten", nodeID(outNode))
	m.makeNode(out, kvCoderID, outNode)

	flattenID := fmt.Sprintf("%v_flatten", id)
	flatten := &pb.PTransform{
		UniqueName: flattenID,
		Spec:       &pb.FunctionSpec{Urn: URNFlatten},
		Inputs:     inputs,
		Outputs:    map[string]string{"i0": out},
	}
	m.transforms[flattenID] = flatten
	subtransforms = append(subtransforms, flattenID)

	// CoGBK

	gbkOut := fmt.Sprintf("%v_out", nodeID(outNode))
	m.makeNode(gbkOut, gbkCoderID, outNode)

	gbkID := fmt.Sprintf("%v_gbk", id)
	gbk := &pb.PTransform{
		UniqueName: gbkID,
		Spec:       &pb.FunctionSpec{Urn: URNGBK},
		Inputs:     map[string]string{"i0": out},
		Outputs:    map[string]string{"i0": gbkOut},
	}
	m.transforms[gbkID] = gbk
	subtransforms = append(subtransforms, gbkID)

	// Expand

	m.addNode(outNode)

	expandID := fmt.Sprintf("%v_expand", id)
	payload := &pb.ParDoPayload{
		DoFn: &pb.FunctionSpec{
			Urn: URNExpand,
			Payload: []byte(protox.MustEncodeBase64(&v1.TransformPayload{
				Urn: URNExpand,
			})),
		},
	}
	expand := &pb.PTransform{
		UniqueName: expandID,
		Spec: &pb.FunctionSpec{
			Urn:     URNParDo,
			Payload: protox.MustEncode(payload),
		},
		Inputs:        map[string]string{"i0": gbkOut},
		Outputs:       map[string]string{"i0": nodeID(outNode)},
		EnvironmentId: m.addDefaultEnv(),
	}
	m.transforms[id] = expand
	subtransforms = append(subtransforms, id)

	// Add composite for visualization

	cogbkID := fmt.Sprintf("%v_cogbk", id)
	m.transforms[cogbkID] = &pb.PTransform{
		UniqueName:    edge.Name,
		Subtransforms: subtransforms,
	}
	return cogbkID
}

func (m *marshaller) addNode(n *graph.Node) string {
	id := nodeID(n)
	if _, exists := m.pcollections[id]; exists {
		return id
	}
	// TODO(herohde) 11/15/2017: expose UniqueName to user.
	return m.makeNode(id, m.coders.Add(n.Coder), n)
}

func (m *marshaller) makeNode(id, cid string, n *graph.Node) string {
	col := &pb.PCollection{
		UniqueName:          id,
		CoderId:             cid,
		IsBounded:           boolToBounded(n.Bounded()),
		WindowingStrategyId: m.addWindowingStrategy(n.WindowingStrategy()),
	}
	m.pcollections[id] = col
	return id
}

func boolToBounded(bounded bool) pb.IsBounded_Enum {
	if bounded {
		return pb.IsBounded_BOUNDED
	}
	return pb.IsBounded_UNBOUNDED
}

func (m *marshaller) addDefaultEnv() string {
	const id = "go"
	if _, exists := m.environments[id]; !exists {
		m.environments[id] = &m.opt.Environment
	}
	return id
}

func (m *marshaller) addWindowingStrategy(w *window.WindowingStrategy) string {
	ws := marshalWindowingStrategy(m.coders, w)
	ws.EnvironmentId = m.addDefaultEnv()
	return m.internWindowingStrategy(ws)
}

func (m *marshaller) internWindowingStrategy(w *pb.WindowingStrategy) string {
	key := proto.MarshalTextString(w)
	if id, exists := m.windowing2id[key]; exists {
		return id
	}

	id := fmt.Sprintf("w%v", len(m.windowing2id))
	m.windowing2id[key] = id
	m.windowing[id] = w
	return id
}

// marshalWindowingStrategy marshals the given windowing strategy in
// the given coder context.
func marshalWindowingStrategy(c *CoderMarshaller, w *window.WindowingStrategy) *pb.WindowingStrategy {
	ws := &pb.WindowingStrategy{
		WindowFn:         makeWindowFn(w.Fn),
		MergeStatus:      pb.MergeStatus_NON_MERGING,
		AccumulationMode: pb.AccumulationMode_DISCARDING,
		WindowCoderId:    c.AddWindowCoder(makeWindowCoder(w.Fn)),
		Trigger: &pb.Trigger{
			Trigger: &pb.Trigger_Default_{
				Default: &pb.Trigger_Default{},
			},
		},
		OutputTime:      pb.OutputTime_END_OF_WINDOW,
		ClosingBehavior: pb.ClosingBehavior_EMIT_IF_NONEMPTY,
		AllowedLateness: 0,
		OnTimeBehavior:  pb.OnTimeBehavior_FIRE_ALWAYS,
	}
	return ws
}

func makeWindowFn(w *window.Fn) *pb.FunctionSpec {
	switch w.Kind {
	case window.GlobalWindows:
		return &pb.FunctionSpec{
			Urn: URNGlobalWindowsWindowFn,
		}
	case window.FixedWindows:
		return &pb.FunctionSpec{
			Urn: URNFixedWindowsWindowFn,
			Payload: protox.MustEncode(
				&pb.FixedWindowsPayload{
					Size: ptypes.DurationProto(w.Size),
				},
			),
		}
	case window.SlidingWindows:
		return &pb.FunctionSpec{
			Urn: URNSlidingWindowsWindowFn,
			Payload: protox.MustEncode(
				&pb.SlidingWindowsPayload{
					Size:   ptypes.DurationProto(w.Size),
					Period: ptypes.DurationProto(w.Period),
				},
			),
		}
	case window.Sessions:
		return &pb.FunctionSpec{
			Urn: URNSessionsWindowFn,
			Payload: protox.MustEncode(
				&pb.SessionsPayload{
					GapSize: ptypes.DurationProto(w.Gap),
				},
			),
		}
	default:
		panic(fmt.Sprintf("Unexpected windowing strategy: %v", w))
	}
}

func makeWindowCoder(w *window.Fn) *coder.WindowCoder {
	switch w.Kind {
	case window.GlobalWindows:
		return coder.NewGlobalWindow()
	case window.FixedWindows, window.SlidingWindows, URNSlidingWindowsWindowFn:
		return coder.NewIntervalWindow()
	default:
		panic(fmt.Sprintf("Unexpected windowing strategy: %v", w))
	}
}

func mustEncodeMultiEdgeBase64(edge *graph.MultiEdge) string {
	ref, err := EncodeMultiEdge(edge)
	if err != nil {
		panic(errors.Wrapf(err, "Failed to serialize %v", edge))
	}
	return protox.MustEncodeBase64(&v1.TransformPayload{
		Urn:  URNDoFn,
		Edge: ref,
	})
}

// makeBytesKeyedCoder returns KV<[]byte,A,> for any coder,
// even if the coder is already a KV coder.
func makeBytesKeyedCoder(c *coder.Coder) *coder.Coder {
	return coder.NewKV([]*coder.Coder{coder.NewBytes(), c})
}

func edgeID(edge *graph.MultiEdge) string {
	return fmt.Sprintf("e%v", edge.ID())
}

func nodeID(n *graph.Node) string {
	return fmt.Sprintf("n%v", n.ID())
}

func scopeID(s *graph.Scope) string {
	return fmt.Sprintf("s%v", s.ID())
}
