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
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx/v1"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/protox"
	pb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
)

const (
	// Model constants

	URNImpulse = "beam:transform:impulse:v1"
	URNParDo   = "urn:beam:transform:pardo:v1"
	URNFlatten = "beam:transform:flatten:v1"
	URNGBK     = "beam:transform:group_by_key:v1"
	URNCombine = "beam:transform:combine:v1"
	URNWindow  = "beam:transform:window:v1"

	URNGlobalWindowsWindowFn = "beam:windowfn:global_windows:v0.1"

	// SDK constants

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

	m := newMarshaller(opt.ContainerImageURL)

	var roots []string
	for _, edge := range tree.Edges {
		roots = append(roots, m.addMultiEdge(edge))
	}
	for _, t := range tree.Children {
		roots = append(roots, m.addScopeTree(t))
	}

	p := &pb.Pipeline{
		Components:       m.build(),
		RootTransformIds: roots,
	}
	return p, nil
}

type marshaller struct {
	imageURL string

	transforms   map[string]*pb.PTransform
	pcollections map[string]*pb.PCollection
	windowing    map[string]*pb.WindowingStrategy
	environments map[string]*pb.Environment

	coders *CoderMarshaller
}

func newMarshaller(imageURL string) *marshaller {
	return &marshaller{
		imageURL:     imageURL,
		transforms:   make(map[string]*pb.PTransform),
		pcollections: make(map[string]*pb.PCollection),
		windowing:    make(map[string]*pb.WindowingStrategy),
		environments: make(map[string]*pb.Environment),
		coders:       NewCoderMarshaller(),
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
		subtransforms = append(subtransforms, m.addMultiEdge(edge))
	}
	for _, tree := range s.Children {
		subtransforms = append(subtransforms, m.addScopeTree(tree))
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
		UniqueName:    s.Scope.Name,
		Subtransforms: subtransforms,
		Inputs:        diff(in, out),
		Outputs:       diff(out, in),
	}
	m.transforms[id] = transform
	return id
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

func (m *marshaller) addMultiEdge(edge NamedEdge) string {
	id := edgeID(edge.Edge)
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
		UniqueName: edge.Name,
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

	id := edgeID(edge.Edge)
	kvCoderID := m.coders.Add(MakeKVUnionCoder(edge.Edge))
	gbkCoderID := m.coders.Add(MakeGBKUnionCoder(edge.Edge))

	inputs := make(map[string]string)
	for i, in := range edge.Edge.Input {
		m.addNode(in.From)

		out := fmt.Sprintf("%v_inject%v", nodeID(in.From), i)
		m.addPCollection(out, kvCoderID)

		// Inject(i)

		injectID := fmt.Sprintf("%v_inject%v", id, i)
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

	// Flatten

	out := fmt.Sprintf("%v_flatten", nodeID(edge.Edge.Output[0].To))
	m.addPCollection(out, kvCoderID)

	flattenID := fmt.Sprintf("%v_flatten", id)
	flatten := &pb.PTransform{
		UniqueName: flattenID,
		Spec:       &pb.FunctionSpec{Urn: URNFlatten},
		Inputs:     inputs,
		Outputs:    map[string]string{"i0": out},
	}
	m.transforms[flattenID] = flatten

	// CoGBK

	gbkOut := fmt.Sprintf("%v_out", nodeID(edge.Edge.Output[0].To))
	m.addPCollection(gbkOut, gbkCoderID)

	gbk := &pb.PTransform{
		UniqueName: edge.Name,
		Spec:       m.makePayload(edge.Edge),
		Inputs:     map[string]string{"i0": out},
		Outputs:    map[string]string{"i0": gbkOut},
	}
	m.transforms[id] = gbk

	// Expand

	m.addNode(edge.Edge.Output[0].To)

	expandID := fmt.Sprintf("%v_expand", id)
	expand := &pb.PTransform{
		UniqueName: expandID,
		Spec: &pb.FunctionSpec{
			Urn:     URNExpand,
			Payload: protox.MustEncode(&v1.TransformPayload{Urn: URNExpand}),
		},
		Inputs:  map[string]string{"i0": out},
		Outputs: map[string]string{"i0": nodeID(edge.Edge.Output[0].To)},
	}
	m.transforms[expandID] = expand
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
	// TODO(herohde) 11/15/2017: expose UniqueName to user. Handle unbounded and windowing.
	return m.addPCollection(id, m.coders.Add(n.Coder))
}

func (m *marshaller) addPCollection(id, cid string) string {
	col := &pb.PCollection{
		UniqueName:          id,
		CoderId:             cid,
		IsBounded:           pb.IsBounded_BOUNDED,
		WindowingStrategyId: m.addWindowingStrategy(window.NewGlobalWindow()),
	}
	m.pcollections[id] = col
	return id
}

func (m *marshaller) addDefaultEnv() string {
	const id = "go"
	if _, exists := m.environments[id]; !exists {
		m.environments[id] = &pb.Environment{Url: m.imageURL}
	}
	return id
}

func (m *marshaller) addWindowingStrategy(w *window.Window) string {
	if w.Kind() != window.GlobalWindow {
		panic(fmt.Sprintf("Unsupported window type supplied: %v", w))
	}

	const id = "global"
	if _, exists := m.windowing[id]; !exists {
		wcid := m.coders.AddWindow(w)

		ws := &pb.WindowingStrategy{
			WindowFn: &pb.SdkFunctionSpec{
				Spec: &pb.FunctionSpec{
					Urn: URNGlobalWindowsWindowFn,
				},
			},
			MergeStatus:      pb.MergeStatus_NON_MERGING,
			AccumulationMode: pb.AccumulationMode_DISCARDING,
			WindowCoderId:    wcid,
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
		m.windowing[id] = ws
	}
	return id
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

func edgeID(e *graph.MultiEdge) string {
	return fmt.Sprintf("e%v", e.ID())
}
