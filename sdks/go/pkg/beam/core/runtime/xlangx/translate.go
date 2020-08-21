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

package xlangx

import (
	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
)

// TODO(BEAM-9919): add documentation to all helper methods

func MergeExpandedWithPipeline(edges []*graph.MultiEdge, p *pipepb.Pipeline) {
	// Adding Expanded transforms to their counterparts in the Pipeline

	for _, e := range edges {
		if e.Op == graph.External {
			id := fmt.Sprintf("e%v", e.ID())
			exp := e.External.Expanded

			p.Requirements = append(p.Requirements, exp.Requirements...)

			// Adding components of the Expanded Transforms to the current Pipeline
			components := graphx.ExpandedComponents(exp)
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
				if k == "go" {
					continue
				}
				p.Components.Environments[k] = v
			}

			p.Components.Transforms[id] = graphx.ExpandedTransform(exp)

		}
	}
}

func PurgeOutputInput(edges []*graph.MultiEdge, p *pipepb.Pipeline) {

	idxMap := make(map[string]string)
	components := p.GetComponents()

	for _, e := range edges {
		if e.Op == graph.External {
			for tag, n := range graphx.ExternalOutputs(e) {
				nodeID := fmt.Sprintf("n%v", n.ID())

				expandedOutputs := graphx.ExpandedTransform(e.External.Expanded).GetOutputs()
				var pcolID string
				if tag == graph.SinkOutputTag {
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
	}

	for _, t := range components.GetTransforms() {
		inputs := t.GetInputs()
		for tag, nodeID := range inputs {
			if pcolID, exists := idxMap[nodeID]; exists {
				inputs[tag] = pcolID
			}
		}
	}

}

func VerifyNamedOutputs(ext *graph.ExternalTransform) {
	expandedOutputs := graphx.ExpandedTransform(ext.Expanded).GetOutputs()

	if len(expandedOutputs) != len(ext.OutputsMap) {
		panic(errors.Errorf("mismatched number of named outputs:\nreceived - %v\nexpected - %v", len(expandedOutputs), len(ext.OutputsMap)))
	}

	for tag := range ext.OutputsMap {
		_, exists := expandedOutputs[tag]
		if tag != graph.SinkOutputTag && !exists {
			panic(errors.Errorf("missing named output in expanded transform: %v is expected in %v", tag, expandedOutputs))
		}
		if tag == graph.SinkOutputTag && len(expandedOutputs) > 1 {
			panic(errors.Errorf("mismatched number of unnamed outputs:\nreceived - %v\nexpected - 1", len(expandedOutputs)))
		}
	}
}

func ResolveOutputIsBounded(e *graph.MultiEdge, isBoundedUpdater func(*graph.Node, bool)) {
	ext := e.External
	exp := ext.Expanded
	expandedPCollections := graphx.ExpandedComponents(exp).GetPcollections()
	expandedOutputs := graphx.ExpandedTransform(exp).GetOutputs()

	for tag, node := range graphx.ExternalOutputs(e) {
		var id string
		isBounded := true

		switch tag {
		case graph.SinkOutputTag:
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
			panic(errors.Errorf("missing corresponsing pcollection of named output: %v is expected in %v", id, expandedPCollections))
		}

	}
}

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
				Urn: graphx.URNImpulse,
			},
			Outputs: output,
		}

		transforms[key] = impulse
	}

}

func RemoveFakeImpulses(c *pipepb.Components, ext *pipepb.PTransform) {
	transforms := c.GetTransforms()
	var impulseIDs []string

	for tag, _ := range ext.GetInputs() {
		id := fmt.Sprintf("%s_%s", "impulse", tag)
		impulseIDs = append(impulseIDs, id)
	}

	for _, id := range impulseIDs {
		t := transforms[id]
		if t.GetSpec().GetUrn() == string(graphx.URNImpulse) {
			delete(transforms, id)

		}
	}
}
