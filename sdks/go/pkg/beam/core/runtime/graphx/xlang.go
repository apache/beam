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

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
)

// mergeExpandedWithPipeline adds expanded components of all ExternalTransforms to the existing pipeline
func mergeExpandedWithPipeline(edges []*graph.MultiEdge, p *pipepb.Pipeline) {
	// Adding Expanded transforms to their counterparts in the Pipeline

	for _, e := range edges {
		if e.Op != graph.External || e.External == nil || e.External.Expanded == nil {
			continue
		}
		exp := e.External.Expanded
		id := fmt.Sprintf("e%v", e.ID())

		p.Requirements = append(p.Requirements, exp.Requirements...)

		// Adding components of the Expanded Transforms to the current Pipeline
		components, err := ExpandedComponents(exp)
		if err != nil {
			panic(err)
		}
		for k, v := range components.GetTransforms() {
			p.Components.Transforms[k] = v
		}
		for k, v := range components.GetPcollections() {
			p.Components.Pcollections[k] = v
		}
		for k, v := range components.GetWindowingStrategies() {
			p.Components.WindowingStrategies[k] = v
		}
		for k, v := range components.GetCoders() {
			p.Components.Coders[k] = v
		}
		for k, v := range components.GetEnvironments() {
			if k == defaultEnvId {
				// This case is not an anomaly. It is expected to be always
				// present. Any initial ExpansionRequest will have a
				// component which requires the default environment. Scoping
				// using unique namespace prevents collision.
				continue
			}
			p.Components.Environments[k] = v
		}

		transform, err := ExpandedTransform(exp)
		if err != nil {
			panic(err)
		}
		p.Components.Transforms[id] = transform
	}
}

// PurgeOutputInput remaps outputs from edge corresponding to an
// ExternalTransform with the correct expanded outputs. All consumers of the
// previous outputs are updated with new inputs.
func purgeOutputInput(edges []*graph.MultiEdge, p *pipepb.Pipeline) {
	idxMap := make(map[string]string)
	components := p.GetComponents()

	// Generating map (oldID -> newID) of outputs to be purged
	for _, e := range edges {
		if e.Op != graph.External || e.External == nil || e.External.Expanded == nil {
			continue
		}
		for tag, n := range ExternalOutputs(e) {
			nodeID := fmt.Sprintf("n%v", n.ID())

			transform, err := ExpandedTransform(e.External.Expanded)
			if err != nil {
				panic(err)
			}
			expandedOutputs := transform.GetOutputs()
			var pcolID string
			if tag == graph.UnnamedOutputTag {
				for _, pcolID = range expandedOutputs {
					// easiest way to access map with one entry (key,value)
				}
			} else {
				pcolID = expandedOutputs[tag]
			}

			idxMap[nodeID] = pcolID
			delete(components.Pcollections, nodeID)
		}
	}

	// Updating all input and output ids to reflect the correct PCollections
	for _, t := range components.GetTransforms() {
		inputs := t.GetInputs()
		for tag, nodeID := range inputs {
			if pcolID, exists := idxMap[nodeID]; exists {
				inputs[tag] = pcolID
			}
		}
		outputs := t.GetOutputs()
		for tag, nodeID := range outputs {
			if pcolID, exists := idxMap[nodeID]; exists {
				outputs[tag] = pcolID
			}
		}
	}
}

// VerifyNamedOutputs ensures the expanded outputs correspond to the correct and expected named outputs
func VerifyNamedOutputs(ext *graph.ExternalTransform) {
	transform, err := ExpandedTransform(ext.Expanded)
	if err != nil {
		panic(err)
	}
	expandedOutputs := transform.GetOutputs()

	if len(expandedOutputs) != len(ext.OutputsMap) {
		panic(errors.Errorf("mismatched number of named outputs:\nreceived - %v\nexpected - %v", len(expandedOutputs), len(ext.OutputsMap)))
	}

	for tag := range ext.OutputsMap {
		_, exists := expandedOutputs[tag]
		if tag != graph.UnnamedOutputTag && !exists {
			panic(errors.Errorf("missing named output in expanded transform: %v is expected in %v", tag, expandedOutputs))
		}
		if tag == graph.UnnamedOutputTag && len(expandedOutputs) > 1 {
			panic(errors.Errorf("mismatched number of unnamed outputs:\nreceived - %v\nexpected - 1", len(expandedOutputs)))
		}
	}
}

// ResolveOutputIsBounded updates each Output node with respect to the received
// expanded components to reflect if it is bounded or not
func ResolveOutputIsBounded(e *graph.MultiEdge, isBoundedUpdater func(*graph.Node, bool)) {
	ext := e.External
	exp := ext.Expanded
	components, err := ExpandedComponents(exp)
	if err != nil {
		panic(err)
	}
	expandedPCollections := components.GetPcollections()
	transform, err := ExpandedTransform(exp)
	if err != nil {
		panic(err)
	}
	expandedOutputs := transform.GetOutputs()

	for tag, node := range ExternalOutputs(e) {
		var id string
		isBounded := true

		switch tag {
		case graph.UnnamedOutputTag:
			for _, id = range expandedOutputs {
				// easiest way to access map with one entry (key,value)
			}
		default:
			id = expandedOutputs[tag]
		}

		if pcol, exists := expandedPCollections[id]; exists {
			if pcol.GetIsBounded() == pipepb.IsBounded_UNBOUNDED {
				isBounded = false
			}
			isBoundedUpdater(node, isBounded)
		} else {
			panic(errors.Errorf("missing corresponding pcollection of named output: %v is expected in %v", id, expandedPCollections))
		}

	}
}

// AddFakeImpulses adds an impulse transform as the producer for each input to
// the root transform. Inputs need producers to form a correct pipeline.
func AddFakeImpulses(p *pipepb.Pipeline) {
	// For a pipeline consisting of only the external node edge, there will be
	// single root transform which will be the external transform.
	// Adding fake impulses per input to this external transform
	transforms := p.GetComponents().GetTransforms()
	ext := transforms[p.GetRootTransformIds()[0]]

	for tag, id := range ext.GetInputs() {
		key := fmt.Sprintf("%s_%s", "impulse", tag)
		output := map[string]string{"out": id}

		impulse := &pipepb.PTransform{
			UniqueName: key,
			Spec: &pipepb.FunctionSpec{
				Urn: URNImpulse,
			},
			Outputs: output,
		}

		transforms[key] = impulse
	}

}

// RemoveFakeImpulses removes each fake impulse per input to the transform.
// Multiple producers for one Input cannot be present.
func RemoveFakeImpulses(c *pipepb.Components, ext *pipepb.PTransform) {
	transforms := c.GetTransforms()
	var impulseIDs []string

	for tag := range ext.GetInputs() {
		id := fmt.Sprintf("%s_%s", "impulse", tag)
		impulseIDs = append(impulseIDs, id)
	}

	for _, id := range impulseIDs {
		t := transforms[id]
		if t.GetSpec().GetUrn() == URNImpulse {
			delete(transforms, id)
		}
	}
}

// ExpandedComponents type asserts the Components field with any type
// and returns its pipeline component proto representation
func ExpandedComponents(exp *graph.ExpandedTransform) (*pipepb.Components, error) {
	if c, ok := exp.Components.(*pipepb.Components); ok {
		return c, nil
	}
	return nil, errors.Errorf("malformed components; %v lacks a conforming pipeline component", exp)
}

// ExpandedTransform type asserts the Transform field with any type
// and returns its pipeline ptransform proto representation
func ExpandedTransform(exp *graph.ExpandedTransform) (*pipepb.PTransform, error) {
	if t, ok := exp.Transform.(*pipepb.PTransform); ok {
		return t, nil
	}
	return nil, errors.Errorf("malformed transform; %v lacks a conforming pipeline ptransform", exp)
}

// ExternalInputs returns the map (tag -> graph node representing the
// pcollection) of input nodes with respect to the map (tag -> index of Inbound
// in MultiEdge.Input) of named inputs
func ExternalInputs(e *graph.MultiEdge) map[string]*graph.Node {
	return InboundTagToNode(e.External.InputsMap, e.Input)
}

// InboundTagToNode relates the tags from inbound links to their respective nodes.
func InboundTagToNode(inputsMap map[string]int, inbound []*graph.Inbound) map[string]*graph.Node {
	inputs := make(map[string]*graph.Node)
	for tag, id := range inputsMap {
		inputs[tag] = inbound[id].From
	}
	return inputs
}

// ExternalOutputs returns the map (tag -> graph node representing the
// pcollection) of output nodes with respect to the map (tag -> index of
// Outbound in MultiEdge.Output) of named outputs
func ExternalOutputs(e *graph.MultiEdge) map[string]*graph.Node {
	return OutboundTagToNode(e.External.OutputsMap, e.Output)
}

// OutboundTagToNode relates the tags from outbound links to their respective nodes.
func OutboundTagToNode(outputsMap map[string]int, outbound []*graph.Outbound) map[string]*graph.Node {
	outputs := make(map[string]*graph.Node)
	for tag, id := range outputsMap {
		outputs[tag] = outbound[id].To
	}
	return outputs
}
