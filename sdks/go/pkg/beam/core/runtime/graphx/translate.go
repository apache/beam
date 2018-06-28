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
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx/v1"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/protox"
	pb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
)

// Model constants for interfacing with a Beam runner.
// TODO(lostluck): 2018/05/28 Extract these from their enum descriptors in the pipeline_v1 proto
const (
	URNImpulse       = "beam:transform:impulse:v1"
	URNParDo         = "urn:beam:transform:pardo:v1"
	URNFlatten       = "beam:transform:flatten:v1"
	URNGBK           = "beam:transform:group_by_key:v1"
	URNCombinePerKey = "beam:transform:combine_per_key:v1"
	URNWindow        = "beam:transform:window:v1"

	URNGlobalWindowsWindowFn  = "beam:windowfn:global_windows:v0.1"
	URNFixedWindowsWindowFn   = "beam:windowfn:fixed_windows:v0.1"
	URNSlidingWindowsWindowFn = "beam:windowfn:sliding_windows:v0.1"
	URNSessionsWindowFn       = "beam:windowfn:session_windows:v0.1"

	// SDK constants

	// URNJavaDoFn is the legacy constant for marking a DoFn.
	// TODO: remove URNJavaDoFN when the Dataflow runner
	// uses the model pipeline and no longer falls back to Java.
	URNJavaDoFn = "urn:beam:dofn:javasdk:0.1"
	URNDoFn     = "beam:go:transform:dofn:v1"
)

// TODO(herohde) 11/6/2017: move some of the configuration into the graph during construction.

// Options for marshalling a graph into a model pipeline.
type Options struct {
	// ContainerImageURL is the default environment container image.
	ContainerImageURL string
}

// Marshal converts a graph to a model pipeline.
func Marshal(edges []*graph.MultiEdge, opt *Options) (*pb.Pipeline, error) {
	tree := NewScopeTree(edges)
	EnsureUniqueNames(tree)

	m := newMarshaller(opt)

	var roots []string
	for _, edge := range tree.Edges {
		roots = append(roots, m.addMultiEdge("", edge))
	}
	for _, t := range tree.Children {
		roots = append(roots, m.addScopeTree("", t))
	}

	p := &pb.Pipeline{
		Components:       m.build(),
		RootTransformIds: roots,
	}
	return p, nil
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

func (m *marshaller) addScopeTree(trunk string, s *ScopeTree) string {
	id := scopeID(s.Scope.Scope)
	if _, exists := m.transforms[id]; exists {
		return id
	}

	uniqueName := fmt.Sprintf("%v/%v", trunk, s.Scope.Name)

	var subtransforms []string
	for _, edge := range s.Edges {
		subtransforms = append(subtransforms, m.addMultiEdge(uniqueName, edge))
	}
	for _, tree := range s.Children {
		subtransforms = append(subtransforms, m.addScopeTree(uniqueName, tree))
	}

	// Compute the input/output for this scope:
	//    inputs  := U(subinputs)\U(suboutputs)
	//    outputs := U(suboutputs)\U(subinputs)
	// where U is set union and \ is set subtraction.

	in := make(map[string]bool)
	out := make(map[string]bool)
	for _, sid := range subtransforms {
		inout(m.transforms[sid], in, out)
	}

	transform := &pb.PTransform{
		UniqueName:    uniqueName,
		Subtransforms: subtransforms,
		Inputs:        diff(in, out),
		Outputs:       diff(out, in),
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
	if !tryAddingCoder(edge.AccumCoder) {
		return
	}
	acID := m.coders.Add(edge.AccumCoder)
	payload := &pb.CombinePayload{
		CombineFn: &pb.SdkFunctionSpec{
			Spec: &pb.FunctionSpec{
				Urn:     URNJavaDoFn,
				Payload: []byte(mustEncodeMultiEdgeBase64(edge)),
			},
			EnvironmentId: m.addDefaultEnv(),
		},
		AccumulatorCoderId: acID,
	}
	transform.Spec = &pb.FunctionSpec{Urn: URNCombinePerKey, Payload: protox.MustEncode(payload)}
}

// If the accumulator type is unencodable (eg. contains raw interface{})
// Try encoding the AccumCoder. If the marshaller doesn't panic, it's
// encodable.
func tryAddingCoder(c *coder.Coder) (ok bool) {
	defer func() {
		if p := recover(); p != nil {
			ok = false
			fmt.Printf("Unable to encode combiner for lifting: %v", p)
		}
	}()
	// Try in a new Marshaller to not corrupt state.
	NewCoderMarshaller().Add(c)
	return true
}

// diff computes A\B and returns its keys as an identity map.
func diff(a, b map[string]bool) map[string]string {
	ret := make(map[string]string)
	for key := range a {
		if !b[key] {
			ret[key] = key
		}
	}
	return ret
}

// inout adds the input and output pcollection ids to the accumulators.
func inout(transform *pb.PTransform, in, out map[string]bool) {
	for _, col := range transform.GetInputs() {
		in[col] = true
	}
	for _, col := range transform.GetOutputs() {
		out[col] = true
	}
}

func (m *marshaller) addMultiEdge(trunk string, edge NamedEdge) string {
	id := StableMultiEdgeID(edge.Edge)
	if _, exists := m.transforms[id]; exists {
		return id
	}

	if edge.Edge.Op == graph.CoGBK && len(edge.Edge.Input) > 1 {
		return m.expandCoGBK(edge)
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

	transform := &pb.PTransform{
		UniqueName: fmt.Sprintf("%v/%v", trunk, edge.Name),
		Spec:       m.makePayload(edge.Edge),
		Inputs:     inputs,
		Outputs:    outputs,
	}

	m.transforms[id] = transform
	return id
}

func (m *marshaller) expandCoGBK(edge NamedEdge) string {
	// TODO(BEAM-490): replace once CoGBK is a primitive. For now, we have to translate
	// CoGBK with multiple PCollections as described in cogbk.go.

	// TODO(herohde) 1/26/2018: we should make the expanded GBK a composite if we care
	// about correctly computing input/output in the enclosing Scope.

	id := StableMultiEdgeID(edge.Edge)
	kvCoderID := m.coders.Add(MakeKVUnionCoder(edge.Edge))
	gbkCoderID := m.coders.Add(MakeGBKUnionCoder(edge.Edge))

	inputs := make(map[string]string)
	for i, in := range edge.Edge.Input {
		m.addNode(in.From)

		out := fmt.Sprintf("%v_inject%v", nodeID(in.From), i)
		m.makeNode(out, kvCoderID, in.From)

		// Inject(i)

		injectID := StableCoGBKInjectID(id, i)
		payload := &pb.ParDoPayload{
			DoFn: &pb.SdkFunctionSpec{
				Spec: &pb.FunctionSpec{
					Urn: URNInject,
					Payload: protox.MustEncode(&v1.TransformPayload{
						Urn:    URNInject,
						Inject: &v1.InjectPayload{N: (int32)(i)},
					}),
				},
				EnvironmentId: m.addDefaultEnv(),
			},
		}
		inject := &pb.PTransform{
			UniqueName: injectID,
			Spec: &pb.FunctionSpec{
				Urn:     URNParDo,
				Payload: protox.MustEncode(payload),
			},
			Inputs:  map[string]string{"i0": nodeID(in.From)},
			Outputs: map[string]string{"i0": out},
		}
		m.transforms[injectID] = inject

		inputs[fmt.Sprintf("i%v", i)] = out
	}

	outNode := edge.Edge.Output[0].To

	// Flatten

	out := fmt.Sprintf("%v_flatten", nodeID(outNode))
	m.makeNode(out, kvCoderID, outNode)

	flattenID := StableCoGBKFlattenID(id)
	flatten := &pb.PTransform{
		UniqueName: flattenID,
		Spec:       &pb.FunctionSpec{Urn: URNFlatten},
		Inputs:     inputs,
		Outputs:    map[string]string{"i0": out},
	}
	m.transforms[flattenID] = flatten

	// CoGBK

	gbkOut := fmt.Sprintf("%v_out", nodeID(outNode))
	m.makeNode(gbkOut, gbkCoderID, outNode)

	gbkID := StableCoGBKGBKID(id)
	gbk := &pb.PTransform{
		UniqueName: edge.Name,
		Spec:       m.makePayload(edge.Edge),
		Inputs:     map[string]string{"i0": out},
		Outputs:    map[string]string{"i0": gbkOut},
	}
	m.transforms[gbkID] = gbk

	// Expand

	m.addNode(outNode)

	expand := &pb.PTransform{
		UniqueName: id,
		Spec: &pb.FunctionSpec{
			Urn:     URNExpand,
			Payload: protox.MustEncode(&v1.TransformPayload{Urn: URNExpand}),
		},
		Inputs:  map[string]string{"i0": out},
		Outputs: map[string]string{"i0": nodeID(outNode)},
	}
	m.transforms[id] = expand
	return id
}

func (m *marshaller) makePayload(edge *graph.MultiEdge) *pb.FunctionSpec {
	switch edge.Op {
	case graph.Impulse:
		return &pb.FunctionSpec{Urn: URNImpulse}

	case graph.ParDo, graph.Combine:
		payload := &pb.ParDoPayload{
			DoFn: &pb.SdkFunctionSpec{
				Spec: &pb.FunctionSpec{
					Urn:     URNJavaDoFn,
					Payload: []byte(mustEncodeMultiEdgeBase64(edge)),
				},
				EnvironmentId: m.addDefaultEnv(),
			},
		}
		return &pb.FunctionSpec{Urn: URNParDo, Payload: protox.MustEncode(payload)}

	case graph.Flatten:
		return &pb.FunctionSpec{Urn: URNFlatten}

	case graph.CoGBK:
		return &pb.FunctionSpec{Urn: URNGBK}

	case graph.WindowInto:
		payload := &pb.WindowIntoPayload{
			WindowFn: &pb.SdkFunctionSpec{
				Spec: makeWindowFn(edge.WindowFn),
			},
		}
		return &pb.FunctionSpec{Urn: URNWindow, Payload: protox.MustEncode(payload)}

	case graph.External:
		return &pb.FunctionSpec{Urn: edge.Payload.URN, Payload: edge.Payload.Data}

	default:
		panic(fmt.Sprintf("Unexpected opcode: %v", edge.Op))
	}
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
	} else {
		return pb.IsBounded_UNBOUNDED
	}
}

func (m *marshaller) addDefaultEnv() string {
	const id = "go"
	if _, exists := m.environments[id]; !exists {
		m.environments[id] = &pb.Environment{Url: m.opt.ContainerImageURL}
	}
	return id
}

func (m *marshaller) addWindowingStrategy(w *window.WindowingStrategy) string {
	ws := MarshalWindowingStrategy(m.coders, w)
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

// TODO(herohde) 4/14/2018: make below function private or refactor,
// once Dataflow doesn't need it anymore.

// MarshalWindowingStrategy marshals the given windowing strategy in
// the given coder context.
func MarshalWindowingStrategy(c *CoderMarshaller, w *window.WindowingStrategy) *pb.WindowingStrategy {
	ws := &pb.WindowingStrategy{
		WindowFn: &pb.SdkFunctionSpec{
			Spec: makeWindowFn(w.Fn),
		},
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
		panic(fmt.Sprintf("Failed to serialize %v: %v", edge, err))
	}
	return protox.MustEncodeBase64(&v1.TransformPayload{
		Urn:  URNDoFn,
		Edge: ref,
	})
}

func nodeID(n *graph.Node) string {
	return fmt.Sprintf("n%v", n.ID())
}

func scopeID(s *graph.Scope) string {
	return fmt.Sprintf("s%v", s.ID())
}

// TODO(herohde) 4/17/2018: StableXXXID returns deterministic transform ids
// for reference in the Dataflow runner. A better solution is to translate
// the proto pipeline to the Dataflow representation (or for Dataflow to
// support proto pipelines directly).

func StableMultiEdgeID(edge *graph.MultiEdge) string {
	return fmt.Sprintf("e%v", edge.ID())
}

func StableCoGBKInjectID(id string, i int) string {
	return fmt.Sprintf("%v_inject%v", id, i)
}

func StableCoGBKFlattenID(id string) string {
	return fmt.Sprintf("%v_flatten", id)
}

func StableCoGBKGBKID(id string) string {
	return fmt.Sprintf("%v_gbk", id)
}
