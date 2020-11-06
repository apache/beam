// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to You under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package beam

import (
	"context"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/pipelinex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/xlangx"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

// xlang exposes an API to execute cross-language transforms within the Go SDK.
// It is experimental and likely to change. It exposes convenient wrappers
// around the core functions to pass in any combination of named/unnamed
// inputs/outputs.

// UnnamedInput is a helper function for passing single unnamed inputs to
// `beam.CrossLanguage`.
//
// Example:
//    beam.CrossLanguage(s, urn, payload, addr, UnnamedInput(input), outputs);
func UnnamedInput(col PCollection) map[string]PCollection {
	return map[string]PCollection{graph.UnnamedInputTag: col}
}

// UnnamedOutput is a helper function for passing single unnamed output types to
// `beam.CrossLanguage`.
//
// Example:
//    beam.CrossLanguage(s, urn, payload, addr, inputs, UnnamedOutput(output));
func UnnamedOutput(t FullType) map[string]FullType {
	return map[string]FullType{graph.UnnamedOutputTag: t}
}

// CrossLanguage executes a cross-language transform that uses named inputs and
// returns named outputs.
func CrossLanguage(
	s Scope,
	urn string,
	payload []byte,
	expansionAddr string,
	namedInputs map[string]PCollection,
	namedOutputTypes map[string]FullType,
) map[string]PCollection {
	if !s.IsValid() {
		panic(errors.New("invalid scope"))
	}

	inputsMap, inboundLinks := graph.NamedInboundLinks(mapPCollectionToNode(namedInputs))
	outputsMap, outboundLinks := graph.NamedOutboundLinks(s.real, namedOutputTypes)

	ext := graph.ExternalTransform{
		Urn:           urn,
		Payload:       payload,
		ExpansionAddr: expansionAddr,
	}.WithNamedInputs(inputsMap).WithNamedOutputs(outputsMap)

	namedOutputs, err := TryCrossLanguage(s, &ext, inboundLinks, outboundLinks)
	if err != nil {
		panic(errors.WithContextf(err, "tried cross-language and failed"))
	}
	return mapNodeToPCollection(namedOutputs)
}

// TryCrossLanguage coordinates the core functions required to execute the cross-language transform
func TryCrossLanguage(s Scope, ext *graph.ExternalTransform, ins []*graph.Inbound, outs []*graph.Outbound) (map[string]*graph.Node, error) {
	// Adding an edge in the graph corresponding to the ExternalTransform
	edge, isBoundedUpdater := graph.NewCrossLanguage(s.real, s.scope, ext, ins, outs)

	// Once the appropriate input and output nodes are added to the edge, a
	// unique namespace can be requested.
	ext.Namespace = graph.NewNamespace()

	// Build the ExpansionRequest

	// Obtaining the components and transform proto representing this transform
	// TODO(BEAM-11188): Move proto handling code into xlangx or graphx package.
	p, err := graphx.Marshal([]*graph.MultiEdge{edge}, &graphx.Options{})
	if err != nil {
		return nil, errors.Wrapf(err, "unable to generate proto representation of %v", ext)
	}

	transforms := p.GetComponents().GetTransforms()

	// Transforms consist of only External transform and composites. Composites
	// should be removed from proto before submitting expansion request.
	extTransformID := p.GetRootTransformIds()[0]
	extTransform := transforms[extTransformID]
	for extTransform.UniqueName != "External" {
		delete(transforms, extTransformID)
		p, err := pipelinex.Normalize(p)
		if err != nil {
			return nil, err
		}
		extTransformID = p.GetRootTransformIds()[0]
		extTransform = transforms[extTransformID]
	}

	// Scoping the ExternalTransform with respect to it's unique namespace, thus
	// avoiding future collisions
	xlangx.AddNamespace(extTransform, p.GetComponents(), ext.Namespace)

	xlangx.AddFakeImpulses(p) // Inputs need to have sources
	delete(transforms, extTransformID)

	// Querying the expansion service
	res, err := xlangx.Expand(context.Background(), p.GetComponents(), extTransform, ext.Namespace, ext.ExpansionAddr)
	if err != nil {
		return nil, err
	}

	// Handling ExpansionResponse

	// Previously added fake impulses need to be removed to avoid having
	// multiple sources to the same pcollection in the graph
	xlangx.RemoveFakeImpulses(res.GetComponents(), res.GetTransform())

	exp := &graph.ExpandedTransform{
		Components:   res.GetComponents(),
		Transform:    res.GetTransform(),
		Requirements: res.GetRequirements(),
	}
	ext.Expanded = exp

	// Ensures the expected named outputs are present
	xlangx.VerifyNamedOutputs(ext)
	// Using the expanded outputs, the graph's counterpart outputs are updated with bounded values
	xlangx.ResolveOutputIsBounded(edge, isBoundedUpdater)

	return graphx.ExternalOutputs(edge), nil
}

// Wrapper functions to handle beam <-> graph boundaries

func pCollectionToNode(p PCollection) *graph.Node {
	if !p.IsValid() {
		panic("tried converting invalid PCollection")
	}
	return p.n
}

func nodeToPCollection(n *graph.Node) PCollection {
	if n == nil {
		panic("tried converting invalid Node")
	}
	c := PCollection{n}
	c.SetCoder(NewCoder(c.Type()))
	return c
}

func mapPCollectionToNode(pMap map[string]PCollection) map[string]*graph.Node {
	if pMap == nil {
		return nil
	}

	nMap := make(map[string]*graph.Node)
	for k, p := range pMap {
		nMap[k] = pCollectionToNode(p)
	}
	return nMap
}

func mapNodeToPCollection(nMap map[string]*graph.Node) map[string]PCollection {
	if nMap == nil {
		return nil
	}

	pMap := make(map[string]PCollection)
	for k, n := range nMap {
		pMap[k] = nodeToPCollection(n)
	}
	return pMap
}
