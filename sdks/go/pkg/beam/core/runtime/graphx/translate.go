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

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	v1pb "github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx/v1"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/pipelinex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/protox"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
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
	URNWindow        = "beam:transform:window:v1"

	// URNIterableSideInput = "beam:side_input:iterable:v1"
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

	URNLegacyProgressReporting = "beam:protocol:progress_reporting:v0"
	URNMultiCore               = "beam:protocol:multi_core_bundle_processing:v1"

	URNRequiresSplittableDoFn = "beam:requirement:pardo:splittable_dofn:v1"

	URNArtifactGoWorker  = "beam:artifact:type:go_worker_binary:v1"
	URNArtifactStagingTo = "beam:artifact:role:staging_to:v1"
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
func CreateEnvironment(ctx context.Context, urn string, extractEnvironmentConfig func(context.Context) string) *pipepb.Environment {
	var serializedPayload []byte
	switch urn {
	case "beam:env:process:v1":
		// TODO Support process based SDK Harness.
		panic(fmt.Sprintf("Unsupported environment %v", urn))
	case "beam:env:external:v1":
		config := extractEnvironmentConfig(ctx)
		payload := &pipepb.ExternalPayload{Endpoint: &pipepb.ApiServiceDescriptor{Url: config}}
		serializedPayload = protox.MustEncode(payload)
	case "beam:env:docker:v1":
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
			&pipepb.ArtifactInformation{
				TypeUrn: URNArtifactGoWorker,
				RoleUrn: URNArtifactStagingTo,
				RolePayload: protox.MustEncode(&pipepb.ArtifactStagingToRolePayload{
					StagedName: "worker",
				}),
			},
		},
	}
}

// TODO(herohde) 11/6/2017: move some of the configuration into the graph during construction.

// Options for marshalling a graph into a model pipeline.
type Options struct {
	// Environment used to run the user code.
	Environment *pipepb.Environment
}

// Marshal converts a graph to a model pipeline.
func Marshal(edges []*graph.MultiEdge, opt *Options) (*pipepb.Pipeline, error) {
	tree := NewScopeTree(edges)

	m := newMarshaller(opt)
	for _, edge := range tree.Edges {
		m.addMultiEdge(edge)
	}
	for _, t := range tree.Children {
		m.addScopeTree(t)
	}

	p := &pipepb.Pipeline{
		Components:   m.build(),
		Requirements: m.getRequirements(),
	}
	return pipelinex.Normalize(p)
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

	transform := &pipepb.PTransform{
		UniqueName:    s.Scope.Name,
		Subtransforms: subtransforms,
		EnvironmentId: m.addDefaultEnv(),
	}

	m.updateIfCombineComposite(s, transform)

	m.transforms[id] = transform
	return id
}

// updateIfCombineComposite examines the scope tree and sets the PTransform Spec
// to be a CombinePerKey with a CombinePayload if it's a liftable composite.
// Beam Portability requires that composites contain an implementation for runners
// that don't understand the URN and Payload, which this lightly checks for.
func (m *marshaller) updateIfCombineComposite(s *ScopeTree, transform *pipepb.PTransform) {
	if s.Scope.Name != graph.CombinePerKeyScope ||
		len(s.Edges) != 2 ||
		len(s.Edges[0].Edge.Input) != 1 ||
		len(s.Edges[1].Edge.Output) != 1 ||
		s.Edges[1].Edge.Op != graph.Combine {
		return
	}

	edge := s.Edges[1].Edge
	acID := m.coders.Add(edge.AccumCoder)
	payload := &pipepb.CombinePayload{
		CombineFn: &pipepb.FunctionSpec{
			Urn:     URNDoFn,
			Payload: []byte(mustEncodeMultiEdgeBase64(edge)),
		},
		AccumulatorCoderId: acID,
	}
	transform.Spec = &pipepb.FunctionSpec{Urn: URNCombinePerKey, Payload: protox.MustEncode(payload)}
}

func (m *marshaller) addMultiEdge(edge NamedEdge) []string {
	id := edgeID(edge.Edge)
	if _, exists := m.transforms[id]; exists {
		return []string{id}
	}

	switch {
	case edge.Edge.Op == graph.CoGBK && len(edge.Edge.Input) > 1:
		return []string{m.expandCoGBK(edge)}
	case edge.Edge.Op == graph.Reshuffle:
		return []string{m.expandReshuffle(edge)}
	case edge.Edge.Op == graph.External && edge.Edge.Payload == nil:
		return []string{m.expandCrossLanguage(edge)}
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
				// The only supported form of side input is MultiMap, but we
				// want just iteration. So we must manually add a fixed key,
				// "", even if the input is already KV.

				out := fmt.Sprintf("%v_keyed%v_%v", nodeID(in.From), edgeID(edge.Edge), i)
				m.makeNode(out, m.coders.Add(makeBytesKeyedCoder(in.From.Coder)), in.From)

				payload := &pipepb.ParDoPayload{
					DoFn: &pipepb.FunctionSpec{
						Urn: URNIterableSideInputKey,
						Payload: []byte(protox.MustEncodeBase64(&v1pb.TransformPayload{
							Urn: URNIterableSideInputKey,
						})),
					},
				}

				keyedID := fmt.Sprintf("%v_keyed%v", edgeID(edge.Edge), i)
				keyed := &pipepb.PTransform{
					UniqueName: keyedID,
					Spec: &pipepb.FunctionSpec{
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

				si[fmt.Sprintf("i%v", i)] = &pipepb.SideInput{
					AccessPattern: &pipepb.FunctionSpec{
						Urn: URNMultimapSideInput,
					},
					ViewFn: &pipepb.FunctionSpec{
						Urn: "foo",
					},
					WindowMappingFn: &pipepb.FunctionSpec{
						Urn: "bar",
					},
				}

			case graph.Map, graph.MultiMap:
				panic("NYI")

			default:
				panic(fmt.Sprintf("unexpected input kind: %v", edge))
			}
		}

		payload := &pipepb.ParDoPayload{
			DoFn: &pipepb.FunctionSpec{
				Urn:     URNDoFn,
				Payload: []byte(mustEncodeMultiEdgeBase64(edge.Edge)),
			},
			SideInputs: si,
		}
		if edge.Edge.DoFn.IsSplittable() {
			payload.RestrictionCoderId = m.coders.Add(edge.Edge.RestrictionCoder)
			m.requirements[URNRequiresSplittableDoFn] = true
		}
		spec = &pipepb.FunctionSpec{Urn: URNParDo, Payload: protox.MustEncode(payload)}

	case graph.Combine:
		payload := &pipepb.ParDoPayload{
			DoFn: &pipepb.FunctionSpec{
				Urn:     URNDoFn,
				Payload: []byte(mustEncodeMultiEdgeBase64(edge.Edge)),
			},
		}
		spec = &pipepb.FunctionSpec{Urn: URNParDo, Payload: protox.MustEncode(payload)}

	case graph.Flatten:
		spec = &pipepb.FunctionSpec{Urn: URNFlatten}

	case graph.CoGBK:
		spec = &pipepb.FunctionSpec{Urn: URNGBK}

	case graph.WindowInto:
		payload := &pipepb.WindowIntoPayload{
			WindowFn: makeWindowFn(edge.Edge.WindowFn),
		}
		spec = &pipepb.FunctionSpec{Urn: URNWindow, Payload: protox.MustEncode(payload)}

	case graph.External:
		spec = &pipepb.FunctionSpec{Urn: edge.Edge.Payload.URN, Payload: edge.Edge.Payload.Data}

	default:
		panic(fmt.Sprintf("Unexpected opcode: %v", edge.Edge.Op))
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
	}
	m.transforms[id] = transform
	allPIds = append(allPIds, id)
	return allPIds
}

func (m *marshaller) expandCrossLanguage(edge NamedEdge) string {
	e := edge.Edge
	id := edgeID(e)

	inputs := make(map[string]string)

	for tag, n := range e.External.Inputs() {
		m.addNode(n)
		inputs[tag] = nodeID(n)
	}

	spec := &pipepb.FunctionSpec{
		Urn:     e.External.Urn,
		Payload: e.External.Payload,
	}

	transform := &pipepb.PTransform{
		UniqueName: edge.Name,
		Spec:       spec,
		Inputs:     inputs,
	}

	if e.External.IsExpanded() {
		// Outputs need to temporarily match format of unnamed Go SDK Nodes.
		// After the initial pipeline is constructed, these will be used to correctly
		// map consumers of these outputs to the expanded transform's outputs.
		outputs := make(map[string]string)
		for i, out := range edge.Edge.Output {
			m.addNode(out.To)
			outputs[fmt.Sprintf("i%v", i)] = nodeID(out.To)
		}
		transform.Outputs = outputs
	}

	m.transforms[id] = transform
	return id
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
	m.makeNode(out, kvCoderID, outNode)

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
	m.makeNode(gbkOut, gbkCoderID, outNode)

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

	m.addNode(outNode)

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
	return cogbkID
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
func (m *marshaller) expandReshuffle(edge NamedEdge) string {
	id := edgeID(edge.Edge)
	var kvCoderID, gbkCoderID string
	{
		kv := makeUnionCoder()
		kvCoderID = m.coders.Add(kv)
		gbkCoderID = m.coders.Add(coder.NewCoGBK(kv.Components))
	}

	var subtransforms []string

	in := edge.Edge.Input[0]

	origInput := m.addNode(in.From)
	// We need to preserve the old windowing/triggering here
	// for re-instatement after the GBK.
	preservedWSId := m.pcollections[origInput].GetWindowingStrategyId()

	// Get the windowing strategy from before:
	postReify := fmt.Sprintf("%v_%v_reifyts", nodeID(in.From), id)
	m.makeNode(postReify, kvCoderID, in.From)

	// We need to replace postReify's windowing strategy with one appropriate
	// for reshuffles.
	{
		wfn := window.NewGlobalWindows()
		m.pcollections[postReify].WindowingStrategyId =
			m.internWindowingStrategy(&pipepb.WindowingStrategy{
				// Not segregated by time...
				WindowFn: makeWindowFn(wfn),
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
				// Defaults copied from marshalWindowingStrategy.
				// TODO(BEAM-3304): migrate to user side operations once trigger support is in.
				EnvironmentId:   m.addDefaultEnv(),
				MergeStatus:     pipepb.MergeStatus_NON_MERGING,
				WindowCoderId:   m.coders.AddWindowCoder(makeWindowCoder(wfn)),
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
	m.makeNode(gbkOut, gbkCoderID, outNode)

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

	outPCol := m.addNode(outNode)
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
	return reshuffleID
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
	col := &pipepb.PCollection{
		UniqueName:          id,
		CoderId:             cid,
		IsBounded:           boolToBounded(n.Bounded()),
		WindowingStrategyId: m.addWindowingStrategy(n.WindowingStrategy()),
	}
	m.pcollections[id] = col
	return id
}

func boolToBounded(bounded bool) pipepb.IsBounded_Enum {
	if bounded {
		return pipepb.IsBounded_BOUNDED
	}
	return pipepb.IsBounded_UNBOUNDED
}

func (m *marshaller) addDefaultEnv() string {
	const id = "go"
	if _, exists := m.environments[id]; !exists {
		m.environments[id] = m.opt.Environment
	}
	return id
}

func (m *marshaller) addWindowingStrategy(w *window.WindowingStrategy) string {
	ws := marshalWindowingStrategy(m.coders, w)
	ws.EnvironmentId = m.addDefaultEnv()
	return m.internWindowingStrategy(ws)
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

// marshalWindowingStrategy marshals the given windowing strategy in
// the given coder context.
func marshalWindowingStrategy(c *CoderMarshaller, w *window.WindowingStrategy) *pipepb.WindowingStrategy {
	ws := &pipepb.WindowingStrategy{
		WindowFn:         makeWindowFn(w.Fn),
		MergeStatus:      pipepb.MergeStatus_NON_MERGING,
		AccumulationMode: pipepb.AccumulationMode_DISCARDING,
		WindowCoderId:    c.AddWindowCoder(makeWindowCoder(w.Fn)),
		Trigger: &pipepb.Trigger{
			Trigger: &pipepb.Trigger_Default_{
				Default: &pipepb.Trigger_Default{},
			},
		},
		OutputTime:      pipepb.OutputTime_END_OF_WINDOW,
		ClosingBehavior: pipepb.ClosingBehavior_EMIT_IF_NONEMPTY,
		AllowedLateness: 0,
		OnTimeBehavior:  pipepb.OnTimeBehavior_FIRE_ALWAYS,
	}
	return ws
}

func makeWindowFn(w *window.Fn) *pipepb.FunctionSpec {
	switch w.Kind {
	case window.GlobalWindows:
		return &pipepb.FunctionSpec{
			Urn: URNGlobalWindowsWindowFn,
		}
	case window.FixedWindows:
		return &pipepb.FunctionSpec{
			Urn: URNFixedWindowsWindowFn,
			Payload: protox.MustEncode(
				&pipepb.FixedWindowsPayload{
					Size: ptypes.DurationProto(w.Size),
				},
			),
		}
	case window.SlidingWindows:
		return &pipepb.FunctionSpec{
			Urn: URNSlidingWindowsWindowFn,
			Payload: protox.MustEncode(
				&pipepb.SlidingWindowsPayload{
					Size:   ptypes.DurationProto(w.Size),
					Period: ptypes.DurationProto(w.Period),
				},
			),
		}
	case window.Sessions:
		return &pipepb.FunctionSpec{
			Urn: URNSessionsWindowFn,
			Payload: protox.MustEncode(
				&pipepb.SessionWindowsPayload{
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
	return protox.MustEncodeBase64(&v1pb.TransformPayload{
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
