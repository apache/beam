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

package beam

import (
	"context"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/xlangx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	jobpb "github.com/apache/beam/sdks/go/pkg/beam/model/jobmanagement_v1"
)

// This is an experimetnal API and subject to change
func CrossLanguage(s Scope, urn string, payload []byte, expansionAddr string, namedInputs map[string]PCollection, namedOutputTypes map[string]FullType) map[string]PCollection {
	if !s.IsValid() {
		panic(errors.New("invalid scope"))
	}

	inputsMap, inboundLinks := graph.NewNamedInboundLinks(mapPCollectionToNode(namedInputs))
	outputsMap, outboundLinks := graph.NewNamedOutboundLinks(s.real, namedOutputTypes)

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

func CrossLanguageWithSingleInputOutput(s Scope, urn string, payload []byte, expansionAddr string, input PCollection, outputType FullType) PCollection {
	if !s.IsValid() {
		panic(errors.New("invalid scope"))
	}

	namedInput := mapPCollectionToNode(map[string]PCollection{graph.SourceInputTag: input})
	namedOutputType := map[string]typex.FullType{graph.SinkOutputTag: outputType}

	inputsMap, inboundLinks := graph.NewNamedInboundLinks(namedInput)
	outputsMap, outboundLinks := graph.NewNamedOutboundLinks(s.real, namedOutputType)

	ext := graph.ExternalTransform{
		Urn:           urn,
		Payload:       payload,
		ExpansionAddr: expansionAddr,
	}.WithNamedInputs(inputsMap).WithNamedOutputs(outputsMap)

	namedOutput, err := TryCrossLanguage(s, &ext, inboundLinks, outboundLinks)
	if err != nil {
		panic(errors.WithContextf(err, "tried cross-language and failed"))
	}
	return nodeToPCollection(namedOutput[graph.SinkOutputTag])
}

func CrossLanguageWithSink(s Scope, urn string, payload []byte, expansionAddr string, namedInputs map[string]PCollection, outputType FullType) PCollection {
	if !s.IsValid() {
		panic(errors.New("invalid scope"))
	}

	namedOutputType := map[string]typex.FullType{graph.SinkOutputTag: outputType}

	inputsMap, inboundLinks := graph.NewNamedInboundLinks(mapPCollectionToNode(namedInputs))
	outputsMap, outboundLinks := graph.NewNamedOutboundLinks(s.real, namedOutputType)

	ext := graph.ExternalTransform{
		Urn:           urn,
		Payload:       payload,
		ExpansionAddr: expansionAddr,
	}.WithNamedInputs(inputsMap).WithNamedOutputs(outputsMap)

	namedOutput, err := TryCrossLanguage(s, &ext, inboundLinks, outboundLinks)
	if err != nil {
		panic(errors.WithContextf(err, "tried cross-language and failed"))
	}
	return nodeToPCollection(namedOutput[graph.SinkOutputTag])
}

func CrossLanguageWithSource(s Scope, urn string, payload []byte, expansionAddr string, input PCollection, namedOutputTypes map[string]FullType) map[string]PCollection {
	if !s.IsValid() {
		panic(errors.New("invalid scope"))
	}

	namedInput := mapPCollectionToNode(map[string]PCollection{graph.SourceInputTag: input})

	inputsMap, inboundLinks := graph.NewNamedInboundLinks(namedInput)
	outputsMap, outboundLinks := graph.NewNamedOutboundLinks(s.real, namedOutputTypes)

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

func TryCrossLanguage(s Scope, ext *graph.ExternalTransform, ins []*graph.Inbound, outs []*graph.Outbound) (map[string]*graph.Node, error) {
	// Add ExternalTransform to the Graph

	// Using existing MultiEdge format to represent ExternalTransform (already backwards compatible)
	edge, isBoundedUpdater := graph.NewCrossLanguage(s.real, s.scope, ext, ins, outs)

	// Once the edge with the appropriate input and output nodes are added, a unique namespace can be requested.
	ext.Namespace = graph.NewNamespace()

	// Build the ExpansionRequest

	// Obtaining the components and transform proto representing this transform
	p, err := graphx.Marshal([]*graph.MultiEdge{edge}, &graphx.Options{})
	if err != nil {
		return nil, errors.Wrapf(err, "unable to generate proto representation of %v", ext)
	}

	// Assembling ExpansionRequest proto
	transforms := p.GetComponents().GetTransforms()
	rootTransformID := p.GetRootTransformIds()[0] // External transform is the only root transform
	rootTransform := transforms[rootTransformID]

	xlangx.AddNamespace(rootTransform, p.GetComponents(), ext.Namespace)

	xlangx.AddFakeImpulses(p)

	delete(transforms, rootTransformID)

	req := &jobpb.ExpansionRequest{
		Components: p.GetComponents(),
		Transform:  rootTransform,
		Namespace:  ext.Namespace,
	}

	res, err := xlangx.Expand(context.Background(), req, ext.ExpansionAddr)
	if err != nil {
		return nil, errors.WithContextf(err, "failed to expand external transform with error [%v] for ExpansionRequest: %v", res.GetError(), req)
	}

	// fmt.Println(prototext.Format(proto.MessageV2(res.Components)))

	xlangx.RemoveFakeImpulses(res.GetComponents(), res.GetTransform())

	exp := &graph.ExpandedTransform{
		Components:   res.GetComponents(),
		Transform:    res.GetTransform(),
		Requirements: res.GetRequirements(),
	}
	ext.Expanded = exp

	xlangx.VerifyNamedOutputs(ext)

	xlangx.ResolveOutputIsBounded(edge, isBoundedUpdater)

	return graphx.ExternalOutputs(edge), nil
}

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
