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

	"strconv"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx/v1"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/protox"
	fnpb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
	pb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/golang/protobuf/proto"
)

const (
	// Model constants

	urnImpulse = "urn:beam:transform:impulse:v1"
	urnParDo   = "urn:beam:transform:pardo:v1"
	urnFlatten = "urn:beam:transform:flatten:v1"
	urnGBK     = "urn:beam:transform:groupbykey:v1"
	urnCombine = "urn:beam:transform:combine:v1"
	urnWindow  = "urn:beam:transform:window:v1"

	urnDataSource = "urn:org.apache.beam:source:runner:0.1"
	urnDataSink   = "urn:org.apache.beam:sink:runner:0.1"

	urnGlobalWindowsWindowFn = "beam:windowfn:global_windows:v0.1"

	// SDK constants

	// TODO: use "urn:beam:go:transform:dofn:v1" when the Dataflow runner
	// uses the model pipeline and no longer falls back to Java.
	urnDoFn = "urn:beam:dofn:javasdk:0.1"
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
	// TODO(herohde) 12/1/2017: set target for DataSource/DataSink

	m.transforms[id] = transform
	return id
}

func (m *marshaller) makePayload(edge *graph.MultiEdge) *pb.FunctionSpec {
	switch edge.Op {
	case graph.Impulse:
		return &pb.FunctionSpec{Urn: urnImpulse}

	case graph.DataSource:
		payload := &fnpb.RemoteGrpcPort{
			ApiServiceDescriptor: &pb.ApiServiceDescriptor{
				Url: edge.Port.URL,
			},
		}
		return &pb.FunctionSpec{Urn: urnDataSource, Payload: protox.MustEncode(payload)}

	case graph.DataSink:
		payload := &fnpb.RemoteGrpcPort{
			ApiServiceDescriptor: &pb.ApiServiceDescriptor{
				Url: edge.Port.URL,
			},
		}
		return &pb.FunctionSpec{Urn: urnDataSink, Payload: protox.MustEncode(payload)}

	case graph.ParDo, graph.Combine:
		payload := &pb.ParDoPayload{
			DoFn: &pb.SdkFunctionSpec{
				Spec: &pb.FunctionSpec{
					Urn:     urnDoFn,
					Payload: []byte(mustEncodeMultiEdgeBase64(edge)),
				},
				EnvironmentId: m.addDefaultEnv(),
			},
		}
		return &pb.FunctionSpec{Urn: urnParDo, Payload: protox.MustEncode(payload)}

	case graph.Flatten:
		return &pb.FunctionSpec{Urn: urnFlatten}

	case graph.GBK:
		return &pb.FunctionSpec{Urn: urnGBK}

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

	col := &pb.PCollection{
		UniqueName:          id,
		CoderId:             m.coders.Add(n.Coder),
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
					Urn: urnGlobalWindowsWindowFn,
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
	return protox.MustEncodeBase64(ref)
}

// Unmarshal converts a model pipeline into a graph.
func Unmarshal(p *pb.Pipeline) (*graph.Graph, error) {
	u, s := newUnmarshaller(p.GetComponents())
	for _, id := range p.GetRootTransformIds() {
		if err := u.unmarshalTransform(s, id); err != nil {
			return nil, err
		}
	}
	return u.build(), nil
}

type unmarshaller struct {
	g *graph.Graph

	nodes     map[string]*graph.Node
	windowing map[string]*window.Window
	coders    *CoderUnmarshaller

	comp *pb.Components
}

func newUnmarshaller(comp *pb.Components) (*unmarshaller, *graph.Scope) {
	u := &unmarshaller{
		g:         graph.New(),
		nodes:     make(map[string]*graph.Node),
		windowing: make(map[string]*window.Window),
		coders:    NewCoderUnmarshaller(comp.GetCoders()),
		comp:      comp,
	}
	return u, u.g.Root()
}

func (u *unmarshaller) build() *graph.Graph {
	return u.g
}

func (u *unmarshaller) unmarshalWindow(id string) (*window.Window, error) {
	if w, exists := u.windowing[id]; exists {
		return w, nil
	}

	ws, ok := u.comp.GetWindowingStrategies()[id]
	if !ok {
		return nil, fmt.Errorf("windowing strategy %v not found", id)
	}
	if urn := ws.GetWindowFn().GetSpec().GetUrn(); urn != urnGlobalWindowsWindowFn {
		return nil, fmt.Errorf("unsupported window type: %v", urn)
	}

	w := window.NewGlobalWindow()
	u.windowing[id] = w
	return w, nil
}

func (u *unmarshaller) unmarshalNode(id string) (*graph.Node, error) {
	if n, exists := u.nodes[id]; exists {
		return n, nil
	}

	col, ok := u.comp.GetPcollections()[id]
	if !ok {
		return nil, fmt.Errorf("windowing strategy %v not found", id)
	}
	c, err := u.coders.Coder(col.CoderId)
	if err != nil {
		return nil, err
	}

	w := window.NewGlobalWindow()
	if col.WindowingStrategyId != "" {
		// TODO(herohde) 12/4/2017: seems to be optional or just not present through legacy Dataflow.

		w, err = u.unmarshalWindow(col.WindowingStrategyId)
		if err != nil {
			return nil, err
		}
	}

	n := u.g.NewNode(c.T, w)
	n.Coder = c

	u.nodes[id] = n
	return n, nil
}

func (u *unmarshaller) unmarshalTransform(scope *graph.Scope, id string) error {
	transform, ok := u.comp.GetTransforms()[id]
	if !ok {
		return fmt.Errorf("transform %v not found", id)
	}

	if len(transform.GetSubtransforms()) > 0 {
		// Composite transform. Ignore in/out.

		scope = u.g.NewScope(scope, transform.GetUniqueName())
		for _, cid := range transform.GetSubtransforms() {
			if err := u.unmarshalTransform(scope, cid); err != nil {
				return err
			}
		}
		return nil
	}

	urn := transform.GetSpec().GetUrn()
	payload := transform.GetSpec().GetPayload()

	edge := u.g.NewEdge(scope)

	switch urn {
	case urnImpulse:
		edge.Op = graph.Impulse
		edge.Output = makeEmptyOutbound(1)

	case urnDataSource:
		port, err := unmarshalPort(payload)
		if err != nil {
			return err
		}
		edge.Op = graph.DataSource
		edge.Port = port
		edge.Output = makeEmptyOutbound(1)

		for key := range transform.GetOutputs() {
			edge.Target = &graph.Target{ID: id, Name: key}
		}

	case urnDataSink:
		port, err := unmarshalPort(payload)
		if err != nil {
			return err
		}

		edge.Op = graph.DataSink
		edge.Port = port
		edge.Input = makeEmptyInbound(1)

		for key := range transform.GetInputs() {
			edge.Target = &graph.Target{ID: id, Name: key}
		}

	case urnParDo:
		var pardo pb.ParDoPayload
		if err := proto.Unmarshal(payload, &pardo); err != nil {
			return err
		}
		var me v1.MultiEdge
		if err := protox.DecodeBase64(string(pardo.GetDoFn().GetSpec().GetPayload()), &me); err != nil {
			return err
		}
		op, fn, in, out, err := DecodeMultiEdge(&me)
		if err != nil {
			return err
		}

		edge.Op = op
		edge.Input = in
		edge.Output = out

		switch edge.Op {
		case graph.ParDo:
			edge.DoFn, err = graph.AsDoFn(fn)
			if err != nil {
				return err
			}
		case graph.Combine:
			edge.CombineFn, err = graph.AsCombineFn(fn)
			if err != nil {
				return err
			}
		default:
			panic(fmt.Sprintf("Opcode should be one of ParDo or Combine, but it is: %v", edge.Op))
		}

	case urnDoFn:
		// TODO(herohde) 12/4/2017: we see DoFns directly with Dataflow. Handle that
		// case here, for now, so that the harness can use this logic.

		var me v1.MultiEdge
		if err := protox.DecodeBase64(string(payload), &me); err != nil {
			return err
		}
		op, fn, in, out, err := DecodeMultiEdge(&me)
		if err != nil {
			return err
		}

		edge.Op = op
		edge.Input = in
		edge.Output = out

		switch edge.Op {
		case graph.ParDo:
			edge.DoFn, err = graph.AsDoFn(fn)
			if err != nil {
				return err
			}
		case graph.Combine:
			edge.CombineFn, err = graph.AsCombineFn(fn)
			if err != nil {
				return err
			}
		default:
			panic(fmt.Sprintf("Opcode should be one of ParDo or Combine, but it is: %v", edge.Op))
		}

	case urnFlatten:
		edge.Op = graph.Flatten
		edge.Input = makeEmptyInbound(len(transform.GetInputs()))
		edge.Output = makeEmptyOutbound(1)

	case urnGBK:
		edge.Op = graph.GBK
		edge.Input = makeEmptyInbound(1)
		edge.Output = makeEmptyOutbound(1)

	default:
		panic(fmt.Sprintf("Unexpected transform URN: %v", urn))
	}

	if err := u.linkInbound(transform.GetInputs(), edge.Input); err != nil {
		return err
	}
	return u.linkOutbound(transform.GetOutputs(), edge.Output)
}

func (u *unmarshaller) linkInbound(m map[string]string, in []*graph.Inbound) error {
	nodes, err := u.unmarshalKeyedNodes(m)
	if err != nil {
		return err
	}
	if len(nodes) != len(in) {
		return fmt.Errorf("unexpected number of inputs: %v, want %v", len(nodes), len(in))
	}
	for i := 0; i < len(in); i++ {
		in[i].From = nodes[i]
		if in[i].Type == nil {
			in[i].Type = nodes[i].Type()
		}
	}
	return nil
}

func (u *unmarshaller) linkOutbound(m map[string]string, out []*graph.Outbound) error {
	nodes, err := u.unmarshalKeyedNodes(m)
	if err != nil {
		return err
	}
	if len(nodes) != len(out) {
		return fmt.Errorf("unexpected number of outputs: %v, want %v", len(nodes), len(out))
	}
	for i := 0; i < len(out); i++ {
		out[i].To = nodes[i]
		if out[i].Type == nil {
			out[i].Type = nodes[i].Type()
		}
	}
	return nil
}

func (u *unmarshaller) unmarshalKeyedNodes(m map[string]string) ([]*graph.Node, error) {
	if len(m) == 0 {
		return nil, nil
	}

	// (1) Compute index. If generate by the marshaller above, we have
	// a "iN" name that directly indicates the position.

	index := make(map[string]int)
	nodes := make(map[string]*graph.Node)
	complete := true

	for key, value := range m {
		if i, err := strconv.Atoi(strings.TrimPrefix(key, "i")); !strings.HasPrefix(key, "i") || err != nil {
			complete = false
			if key == "bogus" {
				continue // Ignore special bogus node for legacy Dataflow.
			}
		} else {
			index[key] = i
		}
		n, err := u.unmarshalNode(value)
		if err != nil {
			return nil, err
		}
		nodes[key] = n
	}

	// (2) Impose order, if present, on nodes.

	if !complete {
		// Inserted node or fallback. Assume any order is ok.
		var ret []*graph.Node
		for _, n := range nodes {
			ret = append(ret, n)
		}
		return ret, nil
	}

	ret := make([]*graph.Node, len(m), len(m))
	for key, n := range nodes {
		ret[index[key]] = n
	}
	return ret, nil

}

func unmarshalPort(data []byte) (*graph.Port, error) {
	var port fnpb.RemoteGrpcPort
	if err := proto.Unmarshal(data, &port); err != nil {
		return nil, err
	}
	return &graph.Port{
		URL: port.GetApiServiceDescriptor().GetUrl(),
	}, nil
}

func makeEmptyInbound(n int) []*graph.Inbound {
	var ret []*graph.Inbound
	for i := 0; i < n; i++ {
		ret = append(ret, &graph.Inbound{Kind: graph.Main})
	}
	return ret
}

func makeEmptyOutbound(n int) []*graph.Outbound {
	var ret []*graph.Outbound
	for i := 0; i < n; i++ {
		ret = append(ret, &graph.Outbound{})
	}
	return ret
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
