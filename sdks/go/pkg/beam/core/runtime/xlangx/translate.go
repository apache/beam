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

func MergeExpandedWithPipeline(edges []*graph.MultiEdge, p *pipepb.Pipeline) {
	// Adding Expanded transforms to their counterparts in the Pipeline

	for _, e := range edges {
		if e.Op == graph.External {
			id := fmt.Sprintf("e%v", e.ID())
			exp := e.External.Expanded()

			p.Requirements = append(p.Requirements, exp.Requirements()...)

			// Adding components of the Expanded Transforms to the current Pipeline
			for k, v := range exp.Components().GetTransforms() {
				p.Components.Transforms[k] = v
			}
			for k, v := range exp.Components().GetPcollections() {
				p.Components.Pcollections[k] = v
			}
			for k, v := range exp.Components().GetWindowingStrategies() {
				p.Components.WindowingStrategies[k] = v
			}
			for k, v := range exp.Components().GetCoders() {
				p.Components.Coders[k] = v
			}
			for k, v := range exp.Components().GetEnvironments() {
				// TODO(pskevin): Resolve temporary hack to enable LOOPBACK mode
				if k == "go" {
					continue
				}
				p.Components.Environments[k] = v
			}

			p.Components.Transforms[id] = exp.Transform()

		}
	}
}

func PurgeOutputInput(edges []*graph.MultiEdge, p *pipepb.Pipeline) {

	idxMap := make(map[string]string)
	components := p.GetComponents()

	for _, e := range edges {
		if e.Op == graph.External {
			for tag, n := range e.External.Outputs {
				nodeID := fmt.Sprintf("n%v", n.ID())
				pcolID := e.External.Expanded_.Transform().GetOutputs()[tag]
				idxMap[nodeID] = pcolID
				components.GetPcollections()[nodeID] = nil // Will get purged while using pipelinex.Update
			}
		}
	}

	for _, t := range p.GetComponents().GetTransforms() {
		inputs := t.GetInputs()
		for tag, nodeID := range inputs {
			if pcolID, exists := idxMap[nodeID]; exists {
				inputs[tag] = pcolID
			}
		}
	}
}

func VerifyNamedOutputs(ext *graph.ExternalTransform) {
	expandedTransform := ext.Expanded().Transform()
	expandedOutputs := expandedTransform.GetOutputs()

	if len(expandedOutputs) != len(ext.OutputTypes()) {
		panic(errors.Errorf("mismatched number of outputs:\nreceived - %v\nexpected - %v", len(expandedOutputs), len(ext.OutputTypes())))
	}

	for tag := range ext.OutputTypes() {
		if _, exists := expandedOutputs[tag]; tag != "sinkOutput" && !exists {
			panic(errors.Errorf("missing named output in expanded transform: %v is expected in %v", tag, expandedOutputs))
		}
	}
}

func ResolveOutputIsBounded(ext *graph.ExternalTransform) {
	exp := ext.Expanded()
	expandedPCollections := exp.Components().GetPcollections()
	expandedOutputs := exp.Transform().GetOutputs()

	for tag := range ext.OutputTypes() {
		var id string
		isBounded := true

		switch tag {
		case "sinkOutput":
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
			exp.BoundedOutputs_[tag] = isBounded
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
	// ext := transforms[p.GetRootTransformIds()[0]] // Will there be only one root transform after expansion? Should be.
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
