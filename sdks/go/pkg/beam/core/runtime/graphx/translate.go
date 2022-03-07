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
	"context"
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window/trigger"
	v1pb "github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/pipelinex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/protox"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
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
	URNReshuffle     = "beam:transform:reshuffle:v1"
	URNCombinePerKey = "beam:transform:combine_per_key:v1"
	URNWindow        = "beam:transform:window_into:v1"

	URNIterableSideInput = "beam:side_input:iterable:v1"
	URNMultimapSideInput = "beam:side_input:multimap:v1"

	URNGlobalWindowsWindowFn  = "beam:window_fn:global_windows:v1"
	URNFixedWindowsWindowFn   = "beam:window_fn:fixed_windows:v1"
	URNSlidingWindowsWindowFn = "beam:window_fn:sliding_windows:v1"
	URNSessionsWindowFn       = "beam:window_fn:session_windows:v1"

	// SDK constants
	URNDoFn = "beam:go:transform:dofn:v1"

	URNIterableSideInputKey = "beam:go:transform:iterablesideinputkey:v1"
	URNReshuffleInput       = "beam:go:transform:reshuffleinput:v1"
	URNReshuffleOutput      = "beam:go:transform:reshuffleoutput:v1"

	URNWindowMappingGlobal  = "beam:go:windowmapping:global:v1"
	URNWindowMappingFixed   = "beam:go:windowmapping:fixed:v1"
	URNWindowMappingSliding = "beam:go:windowmapping:sliding:v1"

	URNLegacyProgressReporting = "beam:protocol:progress_reporting:v0"
	URNMultiCore               = "beam:protocol:multi_core_bundle_processing:v1"

	URNRequiresSplittableDoFn = "beam:requirement:pardo:splittable_dofn:v1"

	// Deprecated: Determine worker binary based on GoWorkerBinary Role instead.
	URNArtifactGoWorker = "beam:artifact:type:go_worker_binary:v1"

	URNArtifactFileType     = "beam:artifact:type:file:v1"
	URNArtifactURLType      = "beam:artifact:type:url:v1"
	URNArtifactGoWorkerRole = "beam:artifact:role:go_worker_binary:v1"

	// Environment Urns.
	URNEnvProcess  = "beam:env:process:v1"
	URNEnvExternal = "beam:env:external:v1"
	URNEnvDocker   = "beam:env:docker:v1"
)

func goCapabilities() []string {
	capabilities := []string{
		URNLegacyProgressReporting,
		URNMultiCore,
		// TOOD(BEAM-9614): Make this versioned.
		"beam:version:sdk_base:go",
	}
	return append(capabilities, knownStandardCoders()...)
}

// CreateEnvironment produces the appropriate payload for the type of environment.
func CreateEnvironment(ctx context.Context, urn string, extractEnvironmentConfig func(context.Context) string) (*pipepb.Environment, error) {
	var serializedPayload []byte
	switch urn {
	case URNEnvProcess:
		// TODO Support process based SDK Harness.
		return nil, errors.Errorf("unsupported environment %v", urn)
	case URNEnvExternal:
		config := extractEnvironmentConfig(ctx)
		payload := &pipepb.ExternalPayload{Endpoint: &pipepb.ApiServiceDescriptor{Url: config}}
		serializedPayload = protox.MustEncode(payload)
	case URNEnvDocker:
		fallthrough
	default:
		config := extractEnvironmentConfig(ctx)
		payload := &pipepb.DockerPayload{ContainerImage: config}
		serializedPayload = protox.MustEncode(payload)
	}
	return &pipepb.Environment{
		Urn:          urn,
		Payload:      serializedPayload,
		Capabilities: goCapabilities(),
		Dependencies: []*pipepb.ArtifactInformation{
			{
				TypeUrn:     URNArtifactFileType,
				TypePayload: protox.MustEncode(&pipepb.ArtifactFilePayload{}),
				RoleUrn:     URNArtifactGoWorkerRole,
			},
		},
	}, nil
}

// TODO(herohde) 11/6/2017: move some of the configuration into the graph during construction.

// Options for marshalling a graph into a model pipeline.
type Options struct {
	// Environment used to run the user code.
	Environment *pipepb.Environment
}

// Marshal converts a graph to a model pipeline.
func Marshal(edges []*graph.MultiEdge, opt *Options) (*pipepb.Pipeline, error) {
	if len(edges) == 0 {
		return nil, errors.New("empty graph")
	}

	tree := NewScopeTree(edges)

	m := newMarshaller(opt)
	for _, edge := range tree.Edges {
		_, err := m.addMultiEdge(edge)
		if err != nil {
			return nil, err
		}
	}
	for _, t := range tree.Children {
		_, err := m.addScopeTree(t)
		if err != nil {
			return nil, err
		}
	}

	p := &pipepb.Pipeline{
		Components:   m.build(),
		Requirements: m.getRequirements(),
	}

	p, err := pipelinex.Normalize(p)
	if err != nil {
		return nil, err
	}

	// If there are external transforms that need expanding, do it now.
	if m.needsExpansion {
		// Merge the expanded components into the existing pipeline
		mergeExpandedWithPipeline(edges, p)

		// Remap outputs of expanded external transforms to be the inputs for all downstream consumers
		// Must happen after merging, so that the inputs in the expanded transforms are also updated.
		purgeOutputInput(edges, p)
	}

	return p, nil
}

type marshaller struct {
	opt *Options

	transforms   map[string]*pipepb.PTransform
	pcollections map[string]*pipepb.PCollection
	windowing    map[string]*pipepb.WindowingStrategy
	environments map[string]*pipepb.Environment
	requirements map[string]bool

	coders *CoderMarshaller

	windowing2id map[string]string

	needsExpansion bool // Indicates external transforms need to be expanded.
}

func newMarshaller(opt *Options) *marshaller {
	return &marshaller{
		opt:          opt,
		transforms:   make(map[string]*pipepb.PTransform),
		pcollections: make(map[string]*pipepb.PCollection),
		windowing:    make(map[string]*pipepb.WindowingStrategy),
		environments: make(map[string]*pipepb.Environment),
		requirements: make(map[string]bool),
		coders:       NewCoderMarshaller(),
		windowing2id: make(map[string]string),
	}
}

func (m *marshaller) build() *pipepb.Components {
	return &pipepb.Components{
		Transforms:          m.transforms,
		Pcollections:        m.pcollections,
		WindowingStrategies: m.windowing,
		Environments:        m.environments,
		Coders:              m.coders.Build(),
	}
}

func (m *marshaller) getRequirements() []string {
	var reqs []string
	for req, ok := range m.requirements {
		if ok {
			reqs = append(reqs, req)
		}
	}
	return reqs
}

func (m *marshaller) addScopeTree(s *ScopeTree) (string, error) {
	id := scopeID(s.Scope.Scope)
	if _, exists := m.transforms[id]; exists {
		return id, nil
	}

	var subtransforms []string
	for _, edge := range s.Edges {
		ids, err := m.addMultiEdge(edge)
		if err != nil {
			return "", errors.Wrapf(err, "failed to add scope tree: %v", s)
		}
		subtransforms = append(subtransforms, ids...)
	}
	for _, tree := range s.Children {
		id, err := m.addScopeTree(tree)
		if err != nil {
			return "", errors.Wrapf(err, "failed to add scope tree: %v", s)
		}
		subtransforms = append(subtransforms, id)
	}

	transform := &pipepb.PTransform{
		UniqueName:    s.Scope.Name,
		Subtransforms: subtransforms,
		EnvironmentId: m.addDefaultEnv(),
	}

	if err := m.updateIfCombineComposite(s, transform); err != nil {
		return "", errors.Wrapf(err, "failed to add scope tree: %v", s)
	}

	m.transforms[id] = transform
	return id, nil
}

// updateIfCombineComposite examines the scope tree and sets the PTransform Spec
// to be a CombinePerKey with a CombinePayload if it's a liftable composite.
// Beam Portability requires that composites contain an implementation for runners
// that don't understand the URN and Payload, which this lightly checks for.
func (m *marshaller) updateIfCombineComposite(s *ScopeTree, transform *pipepb.PTransform) error {
	if s.Scope.Name != graph.CombinePerKeyScope ||
		len(s.Edges) != 2 ||
		len(s.Edges[0].Edge.Input) != 1 ||
		len(s.Edges[1].Edge.Output) != 1 ||
		s.Edges[1].Edge.Op != graph.Combine {
		return nil
	}

	edge := s.Edges[1].Edge
	acID, err := m.coders.Add(edge.AccumCoder)
	if err != nil {
		return errors.Wrapf(err, "failed to update PTransform spec: %v", transform)
	}
	mustEncodeMultiEdge, err := mustEncodeMultiEdgeBase64(edge)
	if err != nil {
		return errors.Wrapf(err, "failed to update PTransform spec: %v", transform)
	}
	payload := &pipepb.CombinePayload{
		CombineFn: &pipepb.FunctionSpec{
			Urn:     URNDoFn,
			Payload: []byte(mustEncodeMultiEdge),
		},
		AccumulatorCoderId: acID,
	}
	transform.Spec = &pipepb.FunctionSpec{Urn: URNCombinePerKey, Payload: protox.MustEncode(payload)}
	return nil
}

func getSideWindowMappingUrn(winFn *window.Fn) string {
	var mappingUrn string
	switch winFn.Kind {
	case window.GlobalWindows:
		mappingUrn = URNWindowMappingGlobal
	case window.FixedWindows:
		mappingUrn = URNWindowMappingFixed
	case window.SlidingWindows:
		mappingUrn = URNWindowMappingSliding
	case window.Sessions:
		panic("session windowing is not supported for side inputs")
	}
	return mappingUrn
}

func (m *marshaller) addMultiEdge(edge NamedEdge) ([]string, error) {
	handleErr := func(err error) ([]string, error) {
		return nil, errors.Wrapf(err, "failed to add input kind: %v", edge)
	}
	id := edgeID(edge.Edge)
	if _, exists := m.transforms[id]; exists {
		return []string{id}, nil
	}

	switch {
	case edge.Edge.Op == graph.CoGBK && len(edge.Edge.Input) > 1:
		cogbkID, err := m.expandCoGBK(edge)
		if err != nil {
			return handleErr(err)
		}
		return []string{cogbkID}, nil
	case edge.Edge.Op == graph.Reshuffle:
		reshuffleID, err := m.expandReshuffle(edge)
		if err != nil {
			return handleErr(err)
		}
		return []string{reshuffleID}, nil
	case edge.Edge.Op == graph.External:
		if edge.Edge.External != nil {
			if edge.Edge.External.Expanded != nil {
				m.needsExpansion = true
			}
		}
		if edge.Edge.Payload == nil {
			edgeID, err := m.expandCrossLanguage(edge)
			if err != nil {
				return handleErr(err)
			}
			return []string{edgeID}, nil
		}
	}

	inputs := make(map[string]string)
	for i, in := range edge.Edge.Input {
		if _, err := m.addNode(in.From); err != nil {
			return handleErr(err)
		}
		inputs[fmt.Sprintf("i%v", i)] = nodeID(in.From)
	}
	outputs := make(map[string]string)
	for i, out := range edge.Edge.Output {
		if _, err := m.addNode(out.To); err != nil {
			return handleErr(err)
		}
		outputs[fmt.Sprintf("i%v", i)] = nodeID(out.To)
	}
	var annotations map[string][]byte

	// allPIds tracks additional PTransformIDs generated for the pipeline
	var allPIds []string
	var spec *pipepb.FunctionSpec
	switch edge.Edge.Op {
	case graph.Impulse:
		spec = &pipepb.FunctionSpec{Urn: URNImpulse}

	case graph.ParDo:
		si := make(map[string]*pipepb.SideInput)
		for i, in := range edge.Edge.Input {
			switch in.Kind {
			case graph.Main:
				// ignore: not a side input

			case graph.Singleton, graph.Slice, graph.Iter, graph.ReIter:
				siWfn := in.From.WindowingStrategy().Fn
				mappingUrn := getSideWindowMappingUrn(siWfn)

				siWSpec, err := makeWindowFn(siWfn)
				if err != nil {
					return nil, err
				}

				si[fmt.Sprintf("i%v", i)] = &pipepb.SideInput{
					AccessPattern: &pipepb.FunctionSpec{
						Urn: URNIterableSideInput,
					},
					ViewFn: &pipepb.FunctionSpec{
						Urn: "foo",
					},
					WindowMappingFn: &pipepb.FunctionSpec{
						Urn:     mappingUrn,
						Payload: siWSpec.Payload,
					},
				}

			case graph.Map, graph.MultiMap:
				// Already in a MultiMap form, don't need to add a fixed key.
				// Get window mapping, arrange proto field.
				siWfn := in.From.WindowingStrategy().Fn
				mappingUrn := getSideWindowMappingUrn(siWfn)

				siWSpec, err := makeWindowFn(siWfn)
				if err != nil {
					return nil, err
				}

				si[fmt.Sprintf("i%v", i)] = &pipepb.SideInput{
					AccessPattern: &pipepb.FunctionSpec{
						Urn: URNMultimapSideInput,
					},
					ViewFn: &pipepb.FunctionSpec{
						Urn: "foo",
					},
					WindowMappingFn: &pipepb.FunctionSpec{
						Urn:     mappingUrn,
						Payload: siWSpec.Payload,
					},
				}
			default:
				return nil, errors.Errorf("unexpected input kind: %v", edge)
			}
		}

		mustEncodeMultiEdge, err := mustEncodeMultiEdgeBase64(edge.Edge)
		if err != nil {
			return handleErr(err)
		}

		payload := &pipepb.ParDoPayload{
			DoFn: &pipepb.FunctionSpec{
				Urn:     URNDoFn,
				Payload: []byte(mustEncodeMultiEdge),
			},
			SideInputs: si,
		}
		if edge.Edge.DoFn.IsSplittable() {
			coderId, err := m.coders.Add(edge.Edge.RestrictionCoder)
			if err != nil {
				return handleErr(err)
			}
			payload.RestrictionCoderId = coderId
			m.requirements[URNRequiresSplittableDoFn] = true
		}
		spec = &pipepb.FunctionSpec{Urn: URNParDo, Payload: protox.MustEncode(payload)}
		annotations = edge.Edge.DoFn.Annotations()

	case graph.Combine:
		mustEncodeMultiEdge, err := mustEncodeMultiEdgeBase64(edge.Edge)
		if err != nil {
			return handleErr(err)
		}
		payload := &pipepb.ParDoPayload{
			DoFn: &pipepb.FunctionSpec{
				Urn:     URNDoFn,
				Payload: []byte(mustEncodeMultiEdge),
			},
		}
		spec = &pipepb.FunctionSpec{Urn: URNParDo, Payload: protox.MustEncode(payload)}

	case graph.Flatten:
		spec = &pipepb.FunctionSpec{Urn: URNFlatten}

	case graph.CoGBK:
		spec = &pipepb.FunctionSpec{Urn: URNGBK}

	case graph.WindowInto:
		windowFn, err := makeWindowFn(edge.Edge.WindowFn)
		if err != nil {
			return handleErr(err)
		}
		payload := &pipepb.WindowIntoPayload{
			WindowFn: windowFn,
		}
		spec = &pipepb.FunctionSpec{Urn: URNWindow, Payload: protox.MustEncode(payload)}

	case graph.External:
		pyld := edge.Edge.Payload
		spec = &pipepb.FunctionSpec{Urn: pyld.URN, Payload: pyld.Data}

		if len(pyld.InputsMap) != 0 {
			if got, want := len(pyld.InputsMap), len(edge.Edge.Input); got != want {
				return handleErr(errors.Errorf("mismatch'd counts between External tags (%v) and inputs (%v)", got, want))
			}
			inputs = make(map[string]string)
			for tag, in := range InboundTagToNode(pyld.InputsMap, edge.Edge.Input) {
				if _, err := m.addNode(in); err != nil {
					return handleErr(err)
				}
				inputs[tag] = nodeID(in)
			}
		}

		if len(pyld.OutputsMap) != 0 {
			if got, want := len(pyld.OutputsMap), len(edge.Edge.Output); got != want {
				return handleErr(errors.Errorf("mismatch'd counts between External tags (%v) and outputs (%v)", got, want))
			}
			outputs = make(map[string]string)
			for tag, out := range OutboundTagToNode(pyld.OutputsMap, edge.Edge.Output) {
				if _, err := m.addNode(out); err != nil {
					return handleErr(err)
				}
				outputs[tag] = nodeID(out)
			}
		}

	default:
		err := errors.Errorf("unexpected opcode: %v", edge.Edge.Op)
		return handleErr(err)
	}

	var transformEnvID = ""
	if !(spec.Urn == URNGBK || spec.Urn == URNImpulse) {
		transformEnvID = m.addDefaultEnv()
	}

	transform := &pipepb.PTransform{
		UniqueName:    edge.Name,
		Spec:          spec,
		Inputs:        inputs,
		Outputs:       outputs,
		EnvironmentId: transformEnvID,
		Annotations:   annotations,
	}
	m.transforms[id] = transform
	allPIds = append(allPIds, id)
	return allPIds, nil
}

func (m *marshaller) expandCrossLanguage(namedEdge NamedEdge) (string, error) {
	edge := namedEdge.Edge
	id := edgeID(edge)

	inputs := make(map[string]string)

	for tag, n := range ExternalInputs(edge) {
		if _, err := m.addNode(n); err != nil {
			return "", errors.Wrapf(err, "failed to expand cross language transform for edge: %v", namedEdge)
		}
		// Ignore tag if it is a dummy UnnamedInputTag
		if tag == graph.UnnamedInputTag {
			tag = fmt.Sprintf("i%v", edge.External.InputsMap[tag])
		}
		inputs[tag] = nodeID(n)
	}

	spec := &pipepb.FunctionSpec{
		Urn:     edge.External.Urn,
		Payload: edge.External.Payload,
	}

	transform := &pipepb.PTransform{
		UniqueName:    namedEdge.Name,
		Spec:          spec,
		Inputs:        inputs,
		EnvironmentId: m.addDefaultEnv(),
	}

	if edge.External.Expanded != nil {
		// Outputs need to temporarily match format of unnamed Go SDK Nodes.
		// After the initial pipeline is constructed, these will be used to correctly
		// map consumers of these outputs to the expanded transform's outputs.
		outputs := make(map[string]string)
		for i, out := range edge.Output {
			if _, err := m.addNode(out.To); err != nil {
				return "", errors.Wrapf(err, "failed to expand cross language transform for edge: %v", namedEdge)
			}
			outputs[fmt.Sprintf("i%v", i)] = nodeID(out.To)
		}
		transform.Outputs = outputs
		environment, err := ExpandedTransform(edge.External.Expanded)
		if err != nil {
			return "", errors.Wrapf(err, "failed to expand cross language transform for edge: %v", namedEdge)
		}
		transform.EnvironmentId = environment.EnvironmentId
	}

	m.transforms[id] = transform
	return id, nil
}

func (m *marshaller) expandCoGBK(edge NamedEdge) (string, error) {
	// TODO(BEAM-490): replace once CoGBK is a primitive. For now, we have to translate
	// CoGBK with multiple PCollections as described in cogbk.go.
	handleErr := func(err error) (string, error) {
		return "", errors.Wrapf(err, "failed to expand CoGBK transform for edge: %v", edge)
	}

	id := edgeID(edge.Edge)
	kvCoder, err := MakeKVUnionCoder(edge.Edge)
	if err != nil {
		return handleErr(err)
	}
	kvCoderID, err := m.coders.Add(kvCoder)
	if err != nil {
		return handleErr(err)
	}
	gbkCoder, err := MakeGBKUnionCoder(edge.Edge)
	if err != nil {
		return handleErr(err)
	}
	gbkCoderID, err := m.coders.Add(gbkCoder)
	if err != nil {
		return handleErr(err)
	}

	var subtransforms []string

	inputs := make(map[string]string)
	for i, in := range edge.Edge.Input {

		if _, err := m.addNode(in.From); err != nil {
			return handleErr(err)
		}

		out := fmt.Sprintf("%v_%v_inject%v", nodeID(in.From), id, i)
		if _, err := m.makeNode(out, kvCoderID, in.From); err != nil {
			return handleErr(err)
		}

		// Inject(i)

		injectID := fmt.Sprintf("%v_inject%v", id, i)
		payload := &pipepb.ParDoPayload{
			DoFn: &pipepb.FunctionSpec{
				Urn: URNInject,
				Payload: []byte(protox.MustEncodeBase64(&v1pb.TransformPayload{
					Urn:    URNInject,
					Inject: &v1pb.InjectPayload{N: (int32)(i)},
				})),
			},
		}
		inject := &pipepb.PTransform{
			UniqueName: injectID,
			Spec: &pipepb.FunctionSpec{
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
	if _, err := m.makeNode(out, kvCoderID, outNode); err != nil {
		return handleErr(err)
	}

	flattenID := fmt.Sprintf("%v_flatten", id)
	flatten := &pipepb.PTransform{
		UniqueName:    flattenID,
		Spec:          &pipepb.FunctionSpec{Urn: URNFlatten},
		Inputs:        inputs,
		Outputs:       map[string]string{"i0": out},
		EnvironmentId: m.addDefaultEnv(),
	}
	m.transforms[flattenID] = flatten
	subtransforms = append(subtransforms, flattenID)

	// CoGBK

	gbkOut := fmt.Sprintf("%v_out", nodeID(outNode))
	if _, err := m.makeNode(gbkOut, gbkCoderID, outNode); err != nil {
		return handleErr(err)
	}

	gbkID := fmt.Sprintf("%v_gbk", id)
	gbk := &pipepb.PTransform{
		UniqueName: gbkID,
		Spec:       &pipepb.FunctionSpec{Urn: URNGBK},
		Inputs:     map[string]string{"i0": out},
		Outputs:    map[string]string{"i0": gbkOut},
	}
	m.transforms[gbkID] = gbk
	subtransforms = append(subtransforms, gbkID)

	// Expand

	if _, err := m.addNode(outNode); err != nil {
		return handleErr(err)
	}

	expandID := fmt.Sprintf("%v_expand", id)
	payload := &pipepb.ParDoPayload{
		DoFn: &pipepb.FunctionSpec{
			Urn: URNExpand,
			Payload: []byte(protox.MustEncodeBase64(&v1pb.TransformPayload{
				Urn: URNExpand,
			})),
		},
	}
	expand := &pipepb.PTransform{
		UniqueName: expandID,
		Spec: &pipepb.FunctionSpec{
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
	m.transforms[cogbkID] = &pipepb.PTransform{
		UniqueName:    edge.Name,
		Subtransforms: subtransforms,
		EnvironmentId: m.addDefaultEnv(),
	}
	return cogbkID, nil
}

// expandReshuffle translates resharding to a composite reshuffle
// transform.
//
// With proper runner support, the SDK doesn't need to do anything.
// However, we still need to provide a backup plan in terms of other
// PTransforms in the event the runner doesn't have a native implementation.
//
// In particular, the "backup plan" needs to:
//
//  * Encode the windowed element, preserving timestamps.
//  * Add random keys to the encoded windowed element []bytes
//  * GroupByKey (in the global window).
//  * Explode the resulting elements list.
//  * Decode the windowed element []bytes.
//
// While a simple reshard can be written in user terms, (timestamps and windows
// are accessible to user functions) there are some framework internal
// optimizations that can be done if the framework is aware of the reshard, though
// ideally this is handled on the runner side.
//
// User code is able to write reshards, but it's easier to access
// the window coders framework side, which is critical for the reshard
// to function with unbounded inputs.
func (m *marshaller) expandReshuffle(edge NamedEdge) (string, error) {
	handleErr := func(err error) (string, error) {
		return "", errors.Wrapf(err, "failed to expand Reshuffle transform for edge: %v", edge)
	}
	id := edgeID(edge.Edge)
	kvCoder, err := makeUnionCoder()
	if err != nil {
		return handleErr(err)
	}
	kvCoderID, err := m.coders.Add(kvCoder)
	if err != nil {
		return handleErr(err)
	}
	gbkCoderID, err := m.coders.Add(coder.NewCoGBK(kvCoder.Components))
	if err != nil {
		return handleErr(err)
	}

	var subtransforms []string

	in := edge.Edge.Input[0]

	origInput, err := m.addNode(in.From)
	if err != nil {
		return handleErr(err)
	}
	// We need to preserve the old windowing/triggering here
	// for re-instatement after the GBK.
	preservedWSId := m.pcollections[origInput].GetWindowingStrategyId()

	// Get the windowing strategy from before:
	postReify := fmt.Sprintf("%v_%v_reifyts", nodeID(in.From), id)
	if _, err := m.makeNode(postReify, kvCoderID, in.From); err != nil {
		return handleErr(err)
	}

	// We need to replace postReify's windowing strategy with one appropriate
	// for reshuffles.
	{
		wfn := window.NewGlobalWindows()
		windowFn, err := makeWindowFn(wfn)
		if err != nil {
			return handleErr(err)
		}
		coderId, err := makeWindowCoder(wfn)
		if err != nil {
			return handleErr(err)
		}
		windowCoderId, err := m.coders.AddWindowCoder(coderId)
		if err != nil {
			return handleErr(err)
		}
		m.pcollections[postReify].WindowingStrategyId =
			m.internWindowingStrategy(&pipepb.WindowingStrategy{
				// Not segregated by time...
				WindowFn: windowFn,
				// ...output after every element is received...
				Trigger: &pipepb.Trigger{
					Trigger: &pipepb.Trigger_Always_{
						Always: &pipepb.Trigger_Always{},
					},
				},
				// ...and after outputing, discard the output elements...
				AccumulationMode: pipepb.AccumulationMode_DISCARDING,
				// ...and since every pane should have 1 element,
				// try to preserve the timestamp.
				OutputTime: pipepb.OutputTime_EARLIEST_IN_PANE,
				// Defaults copied from MarshalWindowingStrategy.
				// TODO(BEAM-3304): migrate to user side operations once trigger support is in.
				EnvironmentId:   m.addDefaultEnv(),
				MergeStatus:     pipepb.MergeStatus_NON_MERGING,
				WindowCoderId:   windowCoderId,
				ClosingBehavior: pipepb.ClosingBehavior_EMIT_IF_NONEMPTY,
				AllowedLateness: 0,
				OnTimeBehavior:  pipepb.OnTimeBehavior_FIRE_ALWAYS,
			})
	}

	// Inputs (i)

	inputID := fmt.Sprintf("%v_reifyts", id)
	payload := &pipepb.ParDoPayload{
		DoFn: &pipepb.FunctionSpec{
			Urn: URNReshuffleInput,
			Payload: []byte(protox.MustEncodeBase64(&v1pb.TransformPayload{
				Urn: URNReshuffleInput,
			})),
		},
	}
	input := &pipepb.PTransform{
		UniqueName: inputID,
		Spec: &pipepb.FunctionSpec{
			Urn:     URNParDo,
			Payload: protox.MustEncode(payload),
		},
		Inputs:        map[string]string{"i0": nodeID(in.From)},
		Outputs:       map[string]string{"i0": postReify},
		EnvironmentId: m.addDefaultEnv(),
	}
	m.transforms[inputID] = input
	subtransforms = append(subtransforms, inputID)

	outNode := edge.Edge.Output[0].To

	// GBK

	gbkOut := fmt.Sprintf("%v_out", nodeID(outNode))
	if _, err := m.makeNode(gbkOut, gbkCoderID, outNode); err != nil {
		return handleErr(err)
	}

	gbkID := fmt.Sprintf("%v_gbk", id)
	gbk := &pipepb.PTransform{
		UniqueName: gbkID,
		Spec:       &pipepb.FunctionSpec{Urn: URNGBK},
		Inputs:     map[string]string{"i0": postReify},
		Outputs:    map[string]string{"i0": gbkOut},
	}
	m.transforms[gbkID] = gbk
	subtransforms = append(subtransforms, gbkID)

	// Expand

	outPCol, err := m.addNode(outNode)
	if err != nil {
		return handleErr(err)
	}
	m.pcollections[outPCol].WindowingStrategyId = preservedWSId

	outputID := fmt.Sprintf("%v_unreify", id)
	outputPayload := &pipepb.ParDoPayload{
		DoFn: &pipepb.FunctionSpec{
			Urn: URNReshuffleOutput,
			Payload: []byte(protox.MustEncodeBase64(&v1pb.TransformPayload{
				Urn: URNReshuffleOutput,
			})),
		},
	}
	output := &pipepb.PTransform{
		UniqueName: outputID,
		Spec: &pipepb.FunctionSpec{
			Urn:     URNParDo,
			Payload: protox.MustEncode(outputPayload),
		},
		Inputs:        map[string]string{"i0": gbkOut},
		Outputs:       map[string]string{"i0": nodeID(outNode)},
		EnvironmentId: m.addDefaultEnv(),
	}
	m.transforms[id] = output
	subtransforms = append(subtransforms, id)

	// Add composite for visualization, or runner optimization
	reshuffleID := fmt.Sprintf("%v_reshuffle", id)
	m.transforms[reshuffleID] = &pipepb.PTransform{
		UniqueName:    edge.Name,
		Subtransforms: subtransforms,
		Spec: &pipepb.FunctionSpec{
			Urn: URNReshuffle,
		},
		EnvironmentId: m.addDefaultEnv(),
	}
	return reshuffleID, nil
}

func (m *marshaller) addNode(n *graph.Node) (string, error) {
	id := nodeID(n)
	if _, exists := m.pcollections[id]; exists {
		return id, nil
	}
	// TODO(herohde) 11/15/2017: expose UniqueName to user.
	cid, err := m.coders.Add(n.Coder)
	if err != nil {
		return "", err
	}
	return m.makeNode(id, cid, n)
}

func (m *marshaller) makeNode(id, cid string, n *graph.Node) (string, error) {
	windowingStrategyId, err := m.addWindowingStrategy(n.WindowingStrategy())

	if err != nil {
		return "", errors.Wrapf(err, "failed to make node %v with node id %v", n, id)
	}

	col := &pipepb.PCollection{
		UniqueName:          id,
		CoderId:             cid,
		IsBounded:           boolToBounded(n.Bounded()),
		WindowingStrategyId: windowingStrategyId,
	}
	m.pcollections[id] = col
	return id, nil
}

func boolToBounded(bounded bool) pipepb.IsBounded_Enum {
	if bounded {
		return pipepb.IsBounded_BOUNDED
	}
	return pipepb.IsBounded_UNBOUNDED
}

// defaultEnvId is the environment ID used for Go Pipeline Environments.
const defaultEnvId = "go"

func (m *marshaller) addDefaultEnv() string {
	if _, exists := m.environments[defaultEnvId]; !exists {
		m.environments[defaultEnvId] = m.opt.Environment
	}
	return defaultEnvId
}

func (m *marshaller) addWindowingStrategy(w *window.WindowingStrategy) (string, error) {
	ws, err := MarshalWindowingStrategy(m.coders, w)
	if err != nil {
		return "", errors.Wrapf(err, "failed to add window strategy %v", w)
	}
	ws.EnvironmentId = m.addDefaultEnv()
	return m.internWindowingStrategy(ws), nil
}

func (m *marshaller) internWindowingStrategy(w *pipepb.WindowingStrategy) string {
	key := proto.MarshalTextString(w)
	if id, exists := m.windowing2id[key]; exists {
		return id
	}

	id := fmt.Sprintf("w%v", len(m.windowing2id))
	m.windowing2id[key] = id
	m.windowing[id] = w
	return id
}

// MarshalWindowingStrategy marshals the given windowing strategy in
// the given coder context.
func MarshalWindowingStrategy(c *CoderMarshaller, w *window.WindowingStrategy) (*pipepb.WindowingStrategy, error) {
	windowFn, err := makeWindowFn(w.Fn)
	if err != nil {
		return nil, err
	}
	coderId, err := makeWindowCoder(w.Fn)
	if err != nil {
		return nil, err
	}
	windowCoderId, err := c.AddWindowCoder(coderId)
	if err != nil {
		return nil, err
	}
	var mergeStat pipepb.MergeStatus_Enum
	if w.Fn.Kind == window.Sessions {
		mergeStat = pipepb.MergeStatus_NEEDS_MERGE
	} else {
		mergeStat = pipepb.MergeStatus_NON_MERGING
	}

	ws := &pipepb.WindowingStrategy{
		WindowFn:         windowFn,
		MergeStatus:      mergeStat,
		WindowCoderId:    windowCoderId,
		Trigger:          makeTrigger(w.Trigger),
		AccumulationMode: makeAccumulationMode(w.AccumulationMode),
		OutputTime:       pipepb.OutputTime_END_OF_WINDOW,
		ClosingBehavior:  pipepb.ClosingBehavior_EMIT_IF_NONEMPTY,
		AllowedLateness:  0,
		OnTimeBehavior:   pipepb.OnTimeBehavior_FIRE_IF_NONEMPTY,
	}
	return ws, nil
}

func makeAccumulationMode(m window.AccumulationMode) pipepb.AccumulationMode_Enum {
	switch m {
	case window.Accumulating:
		return pipepb.AccumulationMode_ACCUMULATING
	case window.Discarding:
		return pipepb.AccumulationMode_DISCARDING
	case window.Unspecified:
		return pipepb.AccumulationMode_UNSPECIFIED
	case window.Retracting:
		return pipepb.AccumulationMode_RETRACTING
	default:
		return pipepb.AccumulationMode_DISCARDING
	}
}

func makeTrigger(t trigger.Trigger) *pipepb.Trigger {
	switch t := t.(type) {
	case *trigger.DefaultTrigger:
		return &pipepb.Trigger{
			Trigger: &pipepb.Trigger_Default_{
				Default: &pipepb.Trigger_Default{},
			},
		}
	case *trigger.AlwaysTrigger:
		return &pipepb.Trigger{
			Trigger: &pipepb.Trigger_Always_{
				Always: &pipepb.Trigger_Always{},
			},
		}
	case *trigger.AfterAnyTrigger:
		return &pipepb.Trigger{
			Trigger: &pipepb.Trigger_AfterAny_{
				AfterAny: &pipepb.Trigger_AfterAny{
					Subtriggers: extractSubtriggers(t.SubTriggers()),
				},
			},
		}
	case *trigger.AfterAllTrigger:
		return &pipepb.Trigger{
			Trigger: &pipepb.Trigger_AfterAll_{
				AfterAll: &pipepb.Trigger_AfterAll{
					Subtriggers: extractSubtriggers(t.SubTriggers()),
				},
			},
		}
	case *trigger.AfterProcessingTimeTrigger:
		if len(t.TimestampTransforms()) == 0 {
			panic("AfterProcessingTime trigger set without a delay or alignment.")
		}
		tts := []*pipepb.TimestampTransform{}
		for _, tt := range t.TimestampTransforms() {
			var ttp *pipepb.TimestampTransform
			switch tt := tt.(type) {
			case trigger.DelayTransform:
				ttp = &pipepb.TimestampTransform{
					TimestampTransform: &pipepb.TimestampTransform_Delay_{
						Delay: &pipepb.TimestampTransform_Delay{DelayMillis: tt.Delay},
					}}
			case trigger.AlignToTransform:
				ttp = &pipepb.TimestampTransform{
					TimestampTransform: &pipepb.TimestampTransform_AlignTo_{
						AlignTo: &pipepb.TimestampTransform_AlignTo{
							Period: tt.Period,
							Offset: tt.Offset,
						},
					}}
			}
			tts = append(tts, ttp)
		}
		return &pipepb.Trigger{
			Trigger: &pipepb.Trigger_AfterProcessingTime_{
				AfterProcessingTime: &pipepb.Trigger_AfterProcessingTime{
					TimestampTransforms: tts,
				},
			},
		}
	case *trigger.AfterCountTrigger:
		return &pipepb.Trigger{
			Trigger: &pipepb.Trigger_ElementCount_{
				ElementCount: &pipepb.Trigger_ElementCount{ElementCount: t.ElementCount()},
			},
		}
	case *trigger.AfterEndOfWindowTrigger:
		var lateTrigger *pipepb.Trigger
		if t.Late() != nil {
			lateTrigger = makeTrigger(t.Late())
		}
		return &pipepb.Trigger{
			Trigger: &pipepb.Trigger_AfterEndOfWindow_{
				AfterEndOfWindow: &pipepb.Trigger_AfterEndOfWindow{
					EarlyFirings: makeTrigger(t.Early()),
					LateFirings:  lateTrigger,
				},
			},
		}
	case *trigger.RepeatTrigger:
		return &pipepb.Trigger{
			Trigger: &pipepb.Trigger_Repeat_{
				Repeat: &pipepb.Trigger_Repeat{Subtrigger: makeTrigger(t.SubTrigger())},
			},
		}
	case *trigger.NeverTrigger:
		return &pipepb.Trigger{
			Trigger: &pipepb.Trigger_Never_{
				Never: &pipepb.Trigger_Never{},
			},
		}
	case *trigger.AfterSynchronizedProcessingTimeTrigger:
		return &pipepb.Trigger{
			Trigger: &pipepb.Trigger_AfterSynchronizedProcessingTime_{
				AfterSynchronizedProcessingTime: &pipepb.Trigger_AfterSynchronizedProcessingTime{},
			},
		}
	default:
		return &pipepb.Trigger{
			Trigger: &pipepb.Trigger_Default_{
				Default: &pipepb.Trigger_Default{},
			},
		}
	}
}

func extractSubtriggers(t []trigger.Trigger) []*pipepb.Trigger {
	if len(t) <= 0 {
		panic("At least one subtrigger required for composite triggers.")
	}

	var result []*pipepb.Trigger
	for _, tr := range t {
		result = append(result, makeTrigger(tr))
	}
	return result
}

func makeWindowFn(w *window.Fn) (*pipepb.FunctionSpec, error) {
	switch w.Kind {
	case window.GlobalWindows:
		return &pipepb.FunctionSpec{
			Urn: URNGlobalWindowsWindowFn,
		}, nil
	case window.FixedWindows:
		return &pipepb.FunctionSpec{
			Urn: URNFixedWindowsWindowFn,
			Payload: protox.MustEncode(
				&pipepb.FixedWindowsPayload{
					Size: ptypes.DurationProto(w.Size),
				},
			),
		}, nil
	case window.SlidingWindows:
		return &pipepb.FunctionSpec{
			Urn: URNSlidingWindowsWindowFn,
			Payload: protox.MustEncode(
				&pipepb.SlidingWindowsPayload{
					Size:   ptypes.DurationProto(w.Size),
					Period: ptypes.DurationProto(w.Period),
				},
			),
		}, nil
	case window.Sessions:
		return &pipepb.FunctionSpec{
			Urn: URNSessionsWindowFn,
			Payload: protox.MustEncode(
				&pipepb.SessionWindowsPayload{
					GapSize: ptypes.DurationProto(w.Gap),
				},
			),
		}, nil
	default:
		return nil, errors.Errorf("unexpected windowing strategy: %v", w)
	}
}

func makeWindowCoder(w *window.Fn) (*coder.WindowCoder, error) {
	switch w.Kind {
	case window.GlobalWindows:
		return coder.NewGlobalWindow(), nil
	case window.FixedWindows, window.SlidingWindows, window.Sessions, URNSlidingWindowsWindowFn:
		return coder.NewIntervalWindow(), nil
	default:
		return nil, errors.Errorf("unexpected windowing strategy for coder: %v", w)
	}
}

func mustEncodeMultiEdgeBase64(edge *graph.MultiEdge) (string, error) {
	ref, err := EncodeMultiEdge(edge)
	if err != nil {
		return "", errors.Wrapf(err, "failed to serialize %v", edge)
	}
	return protox.MustEncodeBase64(&v1pb.TransformPayload{
		Urn:  URNDoFn,
		Edge: ref,
	}), nil
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

// UpdateDefaultEnvWorkerType is so runners can update the pipeline's default environment
// with the correct artifact type and payload for the Go worker binary.
func UpdateDefaultEnvWorkerType(typeUrn string, pyld []byte, p *pipepb.Pipeline) error {
	// Get the Go environment out.
	envs := p.GetComponents().GetEnvironments()
	env, ok := envs[defaultEnvId]
	if !ok {
		return errors.Errorf("unable to find default Go environment with ID %q", defaultEnvId)
	}
	for _, dep := range env.GetDependencies() {
		if dep.RoleUrn != URNArtifactGoWorkerRole {
			continue
		}
		dep.TypeUrn = typeUrn
		dep.TypePayload = pyld
		return nil
	}
	return errors.Errorf("unable to find dependency with %q role in environment with ID %q,", URNArtifactGoWorkerRole, defaultEnvId)
}
