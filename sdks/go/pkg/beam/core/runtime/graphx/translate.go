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
	"sort"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window/trigger"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/contextreg"
	v1pb "github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/pipelinex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/state"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/protox"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/resource"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Model constants for interfacing with a Beam runner.
const (
	URNImpulse       = "beam:transform:impulse:v1"
	URNParDo         = "beam:transform:pardo:v1"
	URNFlatten       = "beam:transform:flatten:v1"
	URNGBK           = "beam:transform:group_by_key:v1"
	URNReshuffle     = "beam:transform:reshuffle:v1"
	URNCombinePerKey = "beam:transform:combine_per_key:v1"
	URNWindow        = "beam:transform:window_into:v1"
	URNMapWindows    = "beam:transform:map_windows:v1"
	URNToString      = "beam:transform:to_string:v1"

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

	URNProgressReporting        = "beam:protocol:progress_reporting:v1"
	URNMultiCore                = "beam:protocol:multi_core_bundle_processing:v1"
	URNWorkerStatus             = "beam:protocol:worker_status:v1"
	URNMonitoringInfoShortID    = "beam:protocol:monitoring_info_short_ids:v1"
	URNDataSampling             = "beam:protocol:data_sampling:v1"
	URNSDKConsumingReceivedData = "beam:protocol:sdk_consuming_received_data:v1"

	URNRequiresSplittableDoFn     = "beam:requirement:pardo:splittable_dofn:v1"
	URNRequiresBundleFinalization = "beam:requirement:pardo:finalization:v1"
	URNRequiresStatefulProcessing = "beam:requirement:pardo:stateful:v1"
	URNTruncate                   = "beam:transform:sdf_truncate_sized_restrictions:v1"

	// Deprecated: Determine worker binary based on GoWorkerBinary Role instead.
	URNArtifactGoWorker = "beam:artifact:type:go_worker_binary:v1"

	URNArtifactFileType     = "beam:artifact:type:file:v1"
	URNArtifactURLType      = "beam:artifact:type:url:v1"
	URNArtifactGoWorkerRole = "beam:artifact:role:go_worker_binary:v1"

	// Environment URNs.
	URNEnvProcess  = "beam:env:process:v1"
	URNEnvExternal = "beam:env:external:v1"
	URNEnvDocker   = "beam:env:docker:v1"

	// Userstate URNs.
	URNBagUserState      = "beam:user_state:bag:v1"
	URNMultiMapUserState = "beam:user_state:multimap:v1"

	// Base version URNs are to allow runners to make distinctions between different releases
	// in a way that won't change based on actual releases, in particular for FnAPI behaviors.
	URNBaseVersionGo = "beam:version:sdk_base:go:" + core.DefaultDockerImage
)

func goCapabilities() []string {
	capabilities := []string{
		URNProgressReporting,
		URNMultiCore,
		URNTruncate,
		URNWorkerStatus,
		URNMonitoringInfoShortID,
		URNBaseVersionGo,
		URNToString,
		URNDataSampling,
		URNSDKConsumingReceivedData,
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
		payload := pipepb.ExternalPayload_builder{Endpoint: pipepb.ApiServiceDescriptor_builder{Url: config}.Build()}.Build()
		serializedPayload = protox.MustEncode(payload)
	case URNEnvDocker:
		fallthrough
	default:
		config := extractEnvironmentConfig(ctx)
		payload := pipepb.DockerPayload_builder{ContainerImage: config}.Build()
		serializedPayload = protox.MustEncode(payload)
	}
	return pipepb.Environment_builder{
		Urn:          urn,
		Payload:      serializedPayload,
		Capabilities: goCapabilities(),
		Dependencies: []*pipepb.ArtifactInformation{
			pipepb.ArtifactInformation_builder{
				TypeUrn:     URNArtifactFileType,
				TypePayload: protox.MustEncode(&pipepb.ArtifactFilePayload{}),
				RoleUrn:     URNArtifactGoWorkerRole,
			}.Build(),
		},
	}.Build(), nil
}

// TODO(https://github.com/apache/beam/issues/23893): Along with scoped resource hints,
// move some of the configuration into the graph during construction.

// Options for marshalling a graph into a model pipeline.
type Options struct {
	// Environment used to run the user code.
	Environment *pipepb.Environment

	// PipelineResourceHints for setting defaults across the whole pipeline.
	PipelineResourceHints resource.Hints

	// ContextReg is an override for the context extractor registry for testing.
	ContextReg *contextreg.Registry
}

// GetContextReg returns the default context registry if the option is
// unset, and the field version otherwise.
func (opts *Options) GetContextReg() *contextreg.Registry {
	if opts.ContextReg == nil {
		return contextreg.Default()
	}
	return opts.ContextReg
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

	p := pipepb.Pipeline_builder{
		Components:   m.build(),
		Requirements: m.getRequirements(),
	}.Build()

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
	return pipepb.Components_builder{
		Transforms:          m.transforms,
		Pcollections:        m.pcollections,
		WindowingStrategies: m.windowing,
		Environments:        m.environments,
		Coders:              m.coders.Build(),
	}.Build()
}

func (m *marshaller) getRequirements() []string {
	var reqs []string
	for req, ok := range m.requirements {
		if ok {
			reqs = append(reqs, req)
		}
	}
	sort.Strings(reqs)
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

	metadata := m.opt.GetContextReg().ExtractTransformMetadata(s.Scope.Scope.Context)

	transform := pipepb.PTransform_builder{
		UniqueName:    s.Scope.Name,
		Subtransforms: subtransforms,
		EnvironmentId: m.addDefaultEnv(),
		Annotations:   metadata.Annotations,
		// DisplayData: metadata.DisplayData,
	}.Build()

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
	payload := pipepb.CombinePayload_builder{
		CombineFn: pipepb.FunctionSpec_builder{
			Urn:     URNDoFn,
			Payload: []byte(mustEncodeMultiEdge),
		}.Build(),
		AccumulatorCoderId: acID,
	}.Build()
	transform.SetSpec(pipepb.FunctionSpec_builder{Urn: URNCombinePerKey, Payload: protox.MustEncode(payload)}.Build())
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
		spec = pipepb.FunctionSpec_builder{Urn: URNImpulse}.Build()

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

				si[fmt.Sprintf("i%v", i)] = pipepb.SideInput_builder{
					AccessPattern: pipepb.FunctionSpec_builder{
						Urn: URNIterableSideInput,
					}.Build(),
					ViewFn: pipepb.FunctionSpec_builder{
						Urn: "foo",
					}.Build(),
					WindowMappingFn: pipepb.FunctionSpec_builder{
						Urn:     mappingUrn,
						Payload: siWSpec.GetPayload(),
					}.Build(),
				}.Build()

			case graph.Map, graph.MultiMap:
				// Already in a MultiMap form, don't need to add a fixed key.
				// Get window mapping, arrange proto field.
				siWfn := in.From.WindowingStrategy().Fn
				mappingUrn := getSideWindowMappingUrn(siWfn)

				siWSpec, err := makeWindowFn(siWfn)
				if err != nil {
					return nil, err
				}

				si[fmt.Sprintf("i%v", i)] = pipepb.SideInput_builder{
					AccessPattern: pipepb.FunctionSpec_builder{
						Urn: URNMultimapSideInput,
					}.Build(),
					ViewFn: pipepb.FunctionSpec_builder{
						Urn: "foo",
					}.Build(),
					WindowMappingFn: pipepb.FunctionSpec_builder{
						Urn:     mappingUrn,
						Payload: siWSpec.GetPayload(),
					}.Build(),
				}.Build()
			default:
				return nil, errors.Errorf("unexpected input kind: %v", edge)
			}
		}

		mustEncodeMultiEdge, err := mustEncodeMultiEdgeBase64(edge.Edge)
		if err != nil {
			return handleErr(err)
		}

		payload := pipepb.ParDoPayload_builder{
			DoFn: pipepb.FunctionSpec_builder{
				Urn:     URNDoFn,
				Payload: []byte(mustEncodeMultiEdge),
			}.Build(),
			SideInputs: si,
		}.Build()
		if edge.Edge.DoFn.IsSplittable() {
			coderID, err := m.coders.Add(edge.Edge.RestrictionCoder)
			if err != nil {
				return handleErr(err)
			}
			payload.SetRestrictionCoderId(coderID)
			m.requirements[URNRequiresSplittableDoFn] = true
		}
		if _, ok := edge.Edge.DoFn.ProcessElementFn().BundleFinalization(); ok {
			payload.SetRequestsFinalization(true)
			m.requirements[URNRequiresBundleFinalization] = true
		}
		if _, ok := edge.Edge.DoFn.ProcessElementFn().StateProvider(); ok {
			m.requirements[URNRequiresStatefulProcessing] = true
			stateSpecs := make(map[string]*pipepb.StateSpec)
			for _, ps := range edge.Edge.DoFn.PipelineState() {
				coderID := ""
				c, ok := edge.Edge.StateCoders[UserStateCoderID(ps)]
				if ok {
					coderID, err = m.coders.Add(c)
					if err != nil {
						return handleErr(err)
					}
				}
				keyCoderID := ""
				if c, ok := edge.Edge.StateCoders[UserStateKeyCoderID(ps)]; ok {
					keyCoderID, err = m.coders.Add(c)
					if err != nil {
						return handleErr(err)
					}
				} else if ps.StateType() == state.TypeMap || ps.StateType() == state.TypeSet {
					return nil, errors.Errorf("set or map state type %v must have a key coder type, none detected", ps)
				}
				switch ps.StateType() {
				case state.TypeValue:
					stateSpecs[ps.StateKey()] = pipepb.StateSpec_builder{
						ReadModifyWriteSpec: pipepb.ReadModifyWriteStateSpec_builder{
							CoderId: coderID,
						}.Build(),
						Protocol: pipepb.FunctionSpec_builder{
							Urn: URNBagUserState,
						}.Build(),
					}.Build()
				case state.TypeBag:
					stateSpecs[ps.StateKey()] = pipepb.StateSpec_builder{
						BagSpec: pipepb.BagStateSpec_builder{
							ElementCoderId: coderID,
						}.Build(),
						Protocol: pipepb.FunctionSpec_builder{
							Urn: URNBagUserState,
						}.Build(),
					}.Build()
				case state.TypeCombining:
					cps := ps.(state.CombiningPipelineState).GetCombineFn()
					f, err := graph.NewFn(cps)
					if err != nil {
						return handleErr(err)
					}
					cf, err := graph.AsCombineFn(f)
					if err != nil {
						return handleErr(err)
					}
					me := graph.MultiEdge{
						Op:        graph.Combine,
						CombineFn: cf,
					}
					mustEncodeMultiEdge, err := mustEncodeMultiEdgeBase64(&me)
					if err != nil {
						return handleErr(err)
					}
					stateSpecs[ps.StateKey()] = pipepb.StateSpec_builder{
						CombiningSpec: pipepb.CombiningStateSpec_builder{
							AccumulatorCoderId: coderID,
							CombineFn: pipepb.FunctionSpec_builder{
								Urn:     "beam:combinefn:gosdk:v1",
								Payload: []byte(mustEncodeMultiEdge),
							}.Build(),
						}.Build(),
						Protocol: pipepb.FunctionSpec_builder{
							Urn: URNBagUserState,
						}.Build(),
					}.Build()
				case state.TypeMap:
					stateSpecs[ps.StateKey()] = pipepb.StateSpec_builder{
						MapSpec: pipepb.MapStateSpec_builder{
							KeyCoderId:   keyCoderID,
							ValueCoderId: coderID,
						}.Build(),
						Protocol: pipepb.FunctionSpec_builder{
							Urn: URNMultiMapUserState,
						}.Build(),
					}.Build()
				case state.TypeSet:
					stateSpecs[ps.StateKey()] = pipepb.StateSpec_builder{
						SetSpec: pipepb.SetStateSpec_builder{
							ElementCoderId: keyCoderID,
						}.Build(),
						Protocol: pipepb.FunctionSpec_builder{
							Urn: URNMultiMapUserState,
						}.Build(),
					}.Build()
				default:
					return nil, errors.Errorf("State type %v not recognized for state %v", ps.StateKey(), ps)
				}
			}
			payload.SetStateSpecs(stateSpecs)
		}
		if _, ok := edge.Edge.DoFn.ProcessElementFn().TimerProvider(); ok {
			m.requirements[URNRequiresStatefulProcessing] = true
			timerSpecs := make(map[string]*pipepb.TimerFamilySpec)
			pipelineTimers, _ := edge.Edge.DoFn.PipelineTimers()

			// All timers for a single DoFn have the same key and window coders, that match the input PCollection.
			mainInputID := inputs["i0"]
			pCol := m.pcollections[mainInputID]
			kvCoder := m.coders.coders[pCol.GetCoderId()]
			if kvCoder.GetSpec().GetUrn() != urnKVCoder {
				return nil, errors.Errorf("timer using DoFn %v doesn't use a KV as PCollection input. Unable to extract key coder for timers, got %v", edge.Name, kvCoder.GetSpec().GetUrn())
			}
			keyCoderID := kvCoder.GetComponentCoderIds()[0]

			wsID := pCol.GetWindowingStrategyId()
			ws := m.windowing[wsID]
			windowCoderID := ws.GetWindowCoderId()

			timerCoderID := m.coders.internBuiltInCoder(urnTimerCoder, keyCoderID, windowCoderID)

			for _, pt := range pipelineTimers {
				for timerFamilyID, timeDomain := range pt.Timers() {
					timerSpecs[timerFamilyID] = pipepb.TimerFamilySpec_builder{
						TimeDomain:         pipepb.TimeDomain_Enum(timeDomain),
						TimerFamilyCoderId: timerCoderID,
					}.Build()
				}
			}
			payload.SetTimerFamilySpecs(timerSpecs)
		}
		spec = pipepb.FunctionSpec_builder{Urn: URNParDo, Payload: protox.MustEncode(payload)}.Build()
		annotations = edge.Edge.DoFn.Annotations()

	case graph.Combine:
		mustEncodeMultiEdge, err := mustEncodeMultiEdgeBase64(edge.Edge)
		if err != nil {
			return handleErr(err)
		}
		payload := pipepb.ParDoPayload_builder{
			DoFn: pipepb.FunctionSpec_builder{
				Urn:     URNDoFn,
				Payload: []byte(mustEncodeMultiEdge),
			}.Build(),
		}.Build()
		spec = pipepb.FunctionSpec_builder{Urn: URNParDo, Payload: protox.MustEncode(payload)}.Build()

	case graph.Flatten:
		spec = pipepb.FunctionSpec_builder{Urn: URNFlatten}.Build()

	case graph.CoGBK:
		spec = pipepb.FunctionSpec_builder{Urn: URNGBK}.Build()

	case graph.WindowInto:
		windowFn, err := makeWindowFn(edge.Edge.WindowFn)
		if err != nil {
			return handleErr(err)
		}
		payload := pipepb.WindowIntoPayload_builder{
			WindowFn: windowFn,
		}.Build()
		spec = pipepb.FunctionSpec_builder{Urn: URNWindow, Payload: protox.MustEncode(payload)}.Build()

	case graph.External:
		pyld := edge.Edge.Payload
		spec = pipepb.FunctionSpec_builder{Urn: pyld.URN, Payload: pyld.Data}.Build()

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
	if !(spec.GetUrn() == URNGBK || spec.GetUrn() == URNImpulse) {
		transformEnvID = m.addDefaultEnv()
	}

	transform := pipepb.PTransform_builder{
		UniqueName:    edge.Name,
		Spec:          spec,
		Inputs:        inputs,
		Outputs:       outputs,
		EnvironmentId: transformEnvID,
		Annotations:   annotations,
	}.Build()
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

	spec := pipepb.FunctionSpec_builder{
		Urn:     edge.External.Urn,
		Payload: edge.External.Payload,
	}.Build()

	transform := pipepb.PTransform_builder{
		UniqueName:    namedEdge.Name,
		Spec:          spec,
		Inputs:        inputs,
		EnvironmentId: m.addDefaultEnv(),
	}.Build()

	// Add the coders for output in the marshaller even if expanded is nil
	// for output coder field in expansion request.
	// We need this specifically for Python External Transforms.
	names := strings.Split(spec.GetUrn(), ":")
	if len(names) > 2 && names[2] == "python" {
		for _, out := range edge.Output {
			id, err := m.coders.Add(out.To.Coder)
			if err != nil {
				return "", errors.Wrapf(err, "failed to add output coder to coder registry: %v", m.coders)
			}
			out.To.Coder.ID = id
		}
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
		transform.SetOutputs(outputs)
		environment, err := ExpandedTransform(edge.External.Expanded)
		if err != nil {
			return "", errors.Wrapf(err, "failed to expand cross language transform for edge: %v", namedEdge)
		}
		transform.SetEnvironmentId(environment.GetEnvironmentId())
	}

	m.transforms[id] = transform
	return id, nil
}

func (m *marshaller) expandCoGBK(edge NamedEdge) (string, error) {
	// TODO(https://github.com/apache/beam/issues/18032): replace once CoGBK is a primitive. For now, we have to translate
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
		payload := pipepb.ParDoPayload_builder{
			DoFn: pipepb.FunctionSpec_builder{
				Urn: URNInject,
				Payload: []byte(protox.MustEncodeBase64(&v1pb.TransformPayload{
					Urn:    URNInject,
					Inject: &v1pb.InjectPayload{N: (int32)(i)},
				})),
			}.Build(),
		}.Build()
		inject := pipepb.PTransform_builder{
			UniqueName: injectID,
			Spec: pipepb.FunctionSpec_builder{
				Urn:     URNParDo,
				Payload: protox.MustEncode(payload),
			}.Build(),
			Inputs:        map[string]string{"i0": nodeID(in.From)},
			Outputs:       map[string]string{"i0": out},
			EnvironmentId: m.addDefaultEnv(),
		}.Build()
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
	flatten := pipepb.PTransform_builder{
		UniqueName:    flattenID,
		Spec:          pipepb.FunctionSpec_builder{Urn: URNFlatten}.Build(),
		Inputs:        inputs,
		Outputs:       map[string]string{"i0": out},
		EnvironmentId: m.addDefaultEnv(),
	}.Build()
	m.transforms[flattenID] = flatten
	subtransforms = append(subtransforms, flattenID)

	// CoGBK

	gbkOut := fmt.Sprintf("%v_out", nodeID(outNode))
	if _, err := m.makeNode(gbkOut, gbkCoderID, outNode); err != nil {
		return handleErr(err)
	}

	gbkID := fmt.Sprintf("%v_gbk", id)
	gbk := pipepb.PTransform_builder{
		UniqueName: gbkID,
		Spec:       pipepb.FunctionSpec_builder{Urn: URNGBK}.Build(),
		Inputs:     map[string]string{"i0": out},
		Outputs:    map[string]string{"i0": gbkOut},
	}.Build()
	m.transforms[gbkID] = gbk
	subtransforms = append(subtransforms, gbkID)

	// Expand

	if _, err := m.addNode(outNode); err != nil {
		return handleErr(err)
	}

	expandID := fmt.Sprintf("%v_expand", id)
	payload := pipepb.ParDoPayload_builder{
		DoFn: pipepb.FunctionSpec_builder{
			Urn: URNExpand,
			Payload: []byte(protox.MustEncodeBase64(&v1pb.TransformPayload{
				Urn: URNExpand,
			})),
		}.Build(),
	}.Build()
	expand := pipepb.PTransform_builder{
		UniqueName: expandID,
		Spec: pipepb.FunctionSpec_builder{
			Urn:     URNParDo,
			Payload: protox.MustEncode(payload),
		}.Build(),
		Inputs:        map[string]string{"i0": gbkOut},
		Outputs:       map[string]string{"i0": nodeID(outNode)},
		EnvironmentId: m.addDefaultEnv(),
	}.Build()
	m.transforms[id] = expand
	subtransforms = append(subtransforms, id)

	// Add composite for visualization

	cogbkID := fmt.Sprintf("%v_cogbk", id)
	m.transforms[cogbkID] = pipepb.PTransform_builder{
		UniqueName:    edge.Name,
		Subtransforms: subtransforms,
		EnvironmentId: m.addDefaultEnv(),
	}.Build()
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
//   - Encode the windowed element, preserving timestamps.
//   - Add random keys to the encoded windowed element []bytes
//   - GroupByKey (in the global window).
//   - Explode the resulting elements list.
//   - Decode the windowed element []bytes.
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
	preservedWSID := m.pcollections[origInput].GetWindowingStrategyId()
	preservedCoderID, coderPayloads := m.marshalReshuffleCodersForPCollection(origInput)

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
		windowCoder, err := makeWindowCoder(wfn)
		if err != nil {
			return handleErr(err)
		}
		windowCoderID, err := m.coders.AddWindowCoder(windowCoder)
		if err != nil {
			return handleErr(err)
		}
		m.pcollections[postReify].SetWindowingStrategyId(
			m.internWindowingStrategy(pipepb.WindowingStrategy_builder{
				// Not segregated by time...
				WindowFn: windowFn,
				// ...output after every element is received...
				Trigger: pipepb.Trigger_builder{
					Always: &pipepb.Trigger_Always{},
				}.Build(),
				// ...and after outputing, discard the output elements...
				AccumulationMode: pipepb.AccumulationMode_DISCARDING,
				// ...and since every pane should have 1 element,
				// try to preserve the timestamp.
				OutputTime: pipepb.OutputTime_EARLIEST_IN_PANE,
				// Defaults copied from MarshalWindowingStrategy.
				// TODO(BEAM-3304): migrate to user side operations once trigger support is in.
				EnvironmentId:   m.addDefaultEnv(),
				MergeStatus:     pipepb.MergeStatus_NON_MERGING,
				WindowCoderId:   windowCoderID,
				ClosingBehavior: pipepb.ClosingBehavior_EMIT_IF_NONEMPTY,
				AllowedLateness: int64(in.From.WindowingStrategy().AllowedLateness),
				OnTimeBehavior:  pipepb.OnTimeBehavior_FIRE_ALWAYS,
			}.Build()))
	}

	// Inputs (i)

	inputID := fmt.Sprintf("%v_reifyts", id)
	payload := pipepb.ParDoPayload_builder{
		DoFn: pipepb.FunctionSpec_builder{
			Urn: URNReshuffleInput,
			Payload: []byte(protox.MustEncodeBase64(&v1pb.TransformPayload{
				Urn: URNReshuffleInput,
				Reshuffle: &v1pb.ReshufflePayload{
					CoderId:       preservedCoderID,
					CoderPayloads: coderPayloads,
				},
			})),
		}.Build(),
	}.Build()
	input := pipepb.PTransform_builder{
		UniqueName: inputID,
		Spec: pipepb.FunctionSpec_builder{
			Urn:     URNParDo,
			Payload: protox.MustEncode(payload),
		}.Build(),
		Inputs:        map[string]string{"i0": nodeID(in.From)},
		Outputs:       map[string]string{"i0": postReify},
		EnvironmentId: m.addDefaultEnv(),
	}.Build()
	m.transforms[inputID] = input
	subtransforms = append(subtransforms, inputID)

	outNode := edge.Edge.Output[0].To

	// GBK

	gbkOut := fmt.Sprintf("%v_out", nodeID(outNode))
	if _, err := m.makeNode(gbkOut, gbkCoderID, outNode); err != nil {
		return handleErr(err)
	}

	gbkID := fmt.Sprintf("%v_gbk", id)
	gbk := pipepb.PTransform_builder{
		UniqueName: gbkID,
		Spec:       pipepb.FunctionSpec_builder{Urn: URNGBK}.Build(),
		Inputs:     map[string]string{"i0": postReify},
		Outputs:    map[string]string{"i0": gbkOut},
	}.Build()
	m.transforms[gbkID] = gbk
	subtransforms = append(subtransforms, gbkID)

	// Expand

	outPCol, err := m.addNode(outNode)
	if err != nil {
		return handleErr(err)
	}
	m.pcollections[outPCol].SetWindowingStrategyId(preservedWSID)

	outputID := fmt.Sprintf("%v_unreify", id)
	outputPayload := pipepb.ParDoPayload_builder{
		DoFn: pipepb.FunctionSpec_builder{
			Urn: URNReshuffleOutput,
			Payload: []byte(protox.MustEncodeBase64(&v1pb.TransformPayload{
				Urn: URNReshuffleOutput,
				Reshuffle: &v1pb.ReshufflePayload{
					CoderId:       preservedCoderID,
					CoderPayloads: coderPayloads,
				},
			})),
		}.Build(),
	}.Build()
	output := pipepb.PTransform_builder{
		UniqueName: outputID,
		Spec: pipepb.FunctionSpec_builder{
			Urn:     URNParDo,
			Payload: protox.MustEncode(outputPayload),
		}.Build(),
		Inputs:        map[string]string{"i0": gbkOut},
		Outputs:       map[string]string{"i0": nodeID(outNode)},
		EnvironmentId: m.addDefaultEnv(),
	}.Build()
	m.transforms[id] = output
	subtransforms = append(subtransforms, id)

	// Add composite for visualization, or runner optimization
	reshuffleID := fmt.Sprintf("%v_reshuffle", id)
	m.transforms[reshuffleID] = pipepb.PTransform_builder{
		UniqueName:    edge.Name,
		Subtransforms: subtransforms,
		Spec: pipepb.FunctionSpec_builder{
			Urn: URNReshuffle,
		}.Build(),
		EnvironmentId: m.addDefaultEnv(),
	}.Build()
	return reshuffleID, nil
}

func (m *marshaller) marshalReshuffleCodersForPCollection(pcolID string) (string, map[string][]byte) {
	preservedCoderId := m.pcollections[pcolID].GetCoderId()

	cps := map[string][]byte{}
	cs := []string{preservedCoderId}
	for cs != nil {
		var next []string
		for _, cid := range cs {
			c := m.coders.coders[cid]
			cps[cid] = protox.MustEncode(c)
			for _, newCid := range c.GetComponentCoderIds() {
				if _, ok := cps[newCid]; !ok {
					next = append(next, c.GetComponentCoderIds()...)
				}
			}
		}
		cs = next
	}

	return preservedCoderId, cps
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

	col := pipepb.PCollection_builder{
		UniqueName:          id,
		CoderId:             cid,
		IsBounded:           boolToBounded(n.Bounded()),
		WindowingStrategyId: windowingStrategyId,
	}.Build()
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
		env := proto.Clone(m.opt.Environment).(*pipepb.Environment)
		// If there's no environment set, we need to ignore
		if env == nil {
			return defaultEnvId
		}
		// Add the pipeline level resource hints here for now.
		// TODO(https://github.com/apache/beam/issues/23893) move to a better place for
		// scoped hints in next pass, which affect number of environments set by Go pipelines.
		env.SetResourceHints(m.opt.PipelineResourceHints.Payloads())
		m.environments[defaultEnvId] = env
	}
	return defaultEnvId
}

func (m *marshaller) addWindowingStrategy(w *window.WindowingStrategy) (string, error) {
	ws, err := MarshalWindowingStrategy(m.coders, w)
	if err != nil {
		return "", errors.Wrapf(err, "failed to add window strategy %v", w)
	}
	ws.SetEnvironmentId(m.addDefaultEnv())
	return m.internWindowingStrategy(ws), nil
}

func (m *marshaller) internWindowingStrategy(w *pipepb.WindowingStrategy) string {
	key := w.String()
	if id, exists := m.windowing2id[(key)]; exists {
		return id
	}

	id := fmt.Sprintf("w%v", len(m.windowing2id))
	m.windowing2id[string(key)] = id
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
	coderID, err := makeWindowCoder(w.Fn)
	if err != nil {
		return nil, err
	}
	windowCoderID, err := c.AddWindowCoder(coderID)
	if err != nil {
		return nil, err
	}
	var mergeStat pipepb.MergeStatus_Enum
	if w.Fn.Kind == window.Sessions {
		mergeStat = pipepb.MergeStatus_NEEDS_MERGE
	} else {
		mergeStat = pipepb.MergeStatus_NON_MERGING
	}

	ws := pipepb.WindowingStrategy_builder{
		WindowFn:         windowFn,
		MergeStatus:      mergeStat,
		WindowCoderId:    windowCoderID,
		Trigger:          makeTrigger(w.Trigger),
		AccumulationMode: makeAccumulationMode(w.AccumulationMode),
		OutputTime:       pipepb.OutputTime_END_OF_WINDOW,
		ClosingBehavior:  pipepb.ClosingBehavior_EMIT_IF_NONEMPTY,
		AllowedLateness:  int64(w.AllowedLateness),
		OnTimeBehavior:   pipepb.OnTimeBehavior_FIRE_IF_NONEMPTY,
	}.Build()
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
		return pipepb.Trigger_builder{
			Default: &pipepb.Trigger_Default{},
		}.Build()
	case *trigger.AlwaysTrigger:
		return pipepb.Trigger_builder{
			Always: &pipepb.Trigger_Always{},
		}.Build()
	case *trigger.AfterAnyTrigger:
		return pipepb.Trigger_builder{
			AfterAny: pipepb.Trigger_AfterAny_builder{
				Subtriggers: extractSubtriggers(t.SubTriggers()),
			}.Build(),
		}.Build()
	case *trigger.AfterAllTrigger:
		return pipepb.Trigger_builder{
			AfterAll: pipepb.Trigger_AfterAll_builder{
				Subtriggers: extractSubtriggers(t.SubTriggers()),
			}.Build(),
		}.Build()
	case *trigger.AfterProcessingTimeTrigger:
		if len(t.TimestampTransforms()) == 0 {
			panic("AfterProcessingTime trigger set without a delay or alignment.")
		}
		tts := []*pipepb.TimestampTransform{}
		for _, tt := range t.TimestampTransforms() {
			var ttp *pipepb.TimestampTransform
			switch tt := tt.(type) {
			case trigger.DelayTransform:
				ttp = pipepb.TimestampTransform_builder{
					Delay: pipepb.TimestampTransform_Delay_builder{DelayMillis: tt.Delay}.Build()}.Build()
			case trigger.AlignToTransform:
				ttp = pipepb.TimestampTransform_builder{
					AlignTo: pipepb.TimestampTransform_AlignTo_builder{
						Period: tt.Period,
						Offset: tt.Offset,
					}.Build()}.Build()
			}
			tts = append(tts, ttp)
		}
		return pipepb.Trigger_builder{
			AfterProcessingTime: pipepb.Trigger_AfterProcessingTime_builder{
				TimestampTransforms: tts,
			}.Build(),
		}.Build()
	case *trigger.AfterCountTrigger:
		return pipepb.Trigger_builder{
			ElementCount: pipepb.Trigger_ElementCount_builder{ElementCount: t.ElementCount()}.Build(),
		}.Build()
	case *trigger.AfterEndOfWindowTrigger:
		var lateTrigger *pipepb.Trigger
		if t.Late() != nil {
			lateTrigger = makeTrigger(t.Late())
		}
		return pipepb.Trigger_builder{
			AfterEndOfWindow: pipepb.Trigger_AfterEndOfWindow_builder{
				EarlyFirings: makeTrigger(t.Early()),
				LateFirings:  lateTrigger,
			}.Build(),
		}.Build()
	case *trigger.RepeatTrigger:
		return pipepb.Trigger_builder{
			Repeat: pipepb.Trigger_Repeat_builder{Subtrigger: makeTrigger(t.SubTrigger())}.Build(),
		}.Build()
	case *trigger.NeverTrigger:
		return pipepb.Trigger_builder{
			Never: &pipepb.Trigger_Never{},
		}.Build()
	case *trigger.AfterSynchronizedProcessingTimeTrigger:
		return pipepb.Trigger_builder{
			AfterSynchronizedProcessingTime: &pipepb.Trigger_AfterSynchronizedProcessingTime{},
		}.Build()
	case *trigger.OrFinallyTrigger:
		return pipepb.Trigger_builder{
			OrFinally: pipepb.Trigger_OrFinally_builder{
				Main:    makeTrigger(t.Main()),
				Finally: makeTrigger(t.Finally()),
			}.Build(),
		}.Build()
	case *trigger.AfterEachTrigger:
		return pipepb.Trigger_builder{
			AfterEach: pipepb.Trigger_AfterEach_builder{
				Subtriggers: extractSubtriggers(t.Subtriggers()),
			}.Build(),
		}.Build()
	default:
		return pipepb.Trigger_builder{
			Default: &pipepb.Trigger_Default{},
		}.Build()
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
		return pipepb.FunctionSpec_builder{
			Urn: URNGlobalWindowsWindowFn,
		}.Build(), nil
	case window.FixedWindows:
		return pipepb.FunctionSpec_builder{
			Urn: URNFixedWindowsWindowFn,
			Payload: protox.MustEncode(
				pipepb.FixedWindowsPayload_builder{
					Size: durationpb.New(w.Size),
				}.Build(),
			),
		}.Build(), nil
	case window.SlidingWindows:
		return pipepb.FunctionSpec_builder{
			Urn: URNSlidingWindowsWindowFn,
			Payload: protox.MustEncode(
				pipepb.SlidingWindowsPayload_builder{
					Size:   durationpb.New(w.Size),
					Period: durationpb.New(w.Period),
				}.Build(),
			),
		}.Build(), nil
	case window.Sessions:
		return pipepb.FunctionSpec_builder{
			Urn: URNSessionsWindowFn,
			Payload: protox.MustEncode(
				pipepb.SessionWindowsPayload_builder{
					GapSize: durationpb.New(w.Gap),
				}.Build(),
			),
		}.Build(), nil
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
		if dep.GetRoleUrn() != URNArtifactGoWorkerRole {
			continue
		}
		dep.SetTypeUrn(typeUrn)
		dep.SetTypePayload(pyld)
		return nil
	}
	return errors.Errorf("unable to find dependency with %q role in environment with ID %q,", URNArtifactGoWorkerRole, defaultEnvId)
}

// UserStateCoderID returns the coder id of a user state
func UserStateCoderID(ps state.PipelineState) string {
	return fmt.Sprintf("val_%v", ps.StateKey())
}

// UserStateKeyCoderID returns the key coder id of a user state
func UserStateKeyCoderID(ps state.PipelineState) string {
	return fmt.Sprintf("key_%v", ps.StateKey())
}
