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
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
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

// CrossLanguagePayload encodes a native Go struct into a payload for
// cross-language transforms. To find the expected structure of a payload,
// consult the documentation in the SDK performing the expansion.
func CrossLanguagePayload(pl interface{}) []byte {
	bytes, err := xlangx.EncodeStructPayload(pl)
	if err != nil {
		panic(err)
	}
	return bytes
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

	// Expand the transform into ext.Expanded.
	if err := xlangx.Expand(edge, ext); err != nil {
		return nil, errors.WithContext(err, "expanding external transform")
	}

	// Ensures the expected named outputs are present
	graphx.VerifyNamedOutputs(ext)
	// Using the expanded outputs, the graph's counterpart outputs are updated with bounded values
	graphx.ResolveOutputIsBounded(edge, isBoundedUpdater)

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
