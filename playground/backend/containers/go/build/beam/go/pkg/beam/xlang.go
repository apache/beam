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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/xlangx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

// UnnamedInput is a helper function for passing single unnamed inputs to
// beam.CrossLanguage.
//
// Example:
//
//	beam.CrossLanguage(s, urn, payload, addr, UnnamedInput(input), outputs)
func UnnamedInput(col PCollection) map[string]PCollection {
	return map[string]PCollection{graph.UnnamedInputTag: col}
}

// UnnamedOutput is a helper function for passing single unnamed output types to
// beam.CrossLanguage. The associated output can be accessed with beam.UnnamedOutputTag.
//
// Example:
//
//	resultMap := beam.CrossLanguage(s, urn, payload, addr, inputs, UnnamedOutput(output));
//	result := resultMap[beam.UnnamedOutputTag()]
func UnnamedOutput(t FullType) map[string]FullType {
	return map[string]FullType{graph.UnnamedOutputTag: t}
}

// UnnamedOutputTag provides the output tag used for an output passed to beam.UnnamedOutput.
// Needed to retrieve the unnamed output PCollection from the result of beam.CrossLanguage.
func UnnamedOutputTag() string {
	return graph.UnnamedOutputTag
}

// CrossLanguagePayload encodes a native Go struct into a payload for cross-language transforms.
// payloads are []byte encoded ExternalConfigurationPayload protobufs. In order to fill the
// contents of the protobuf, the provided struct will be used to converted to a row encoded
// representation with an accompanying schema, so the input struct must be compatible with schemas.
//
// See https://beam.apache.org/documentation/programming-guide/#schemas for basic information on
// schemas, and pkg/beam/core/runtime/graphx/schema for details on schemas in the Go SDK.
//
// Example:
//
//	type stringPayload struct {
//	    Data string
//	}
//	encodedPl := beam.CrossLanguagePayload(stringPayload{Data: "foo"})
func CrossLanguagePayload(pl any) []byte {
	bytes, err := xlangx.EncodeStructPayload(pl)
	if err != nil {
		panic(err)
	}
	return bytes
}

// CrossLanguage is a low-level transform for executing cross-language transforms written in other
// SDKs. Because this is low-level, it is recommended to use one of the higher-level IO-specific
// wrappers where available. These can be found in the pkg/beam/io/xlang subdirectory.
// CrossLanguage is useful for executing cross-language transforms which do not have any existing
// IO wrappers.
//
// Usage requires an address for an expansion service accessible during pipeline construction, a
// URN identifying the desired transform, an optional payload with configuration information, and
// input and output names. It outputs a map of named output PCollections.
//
// For more information on expansion services and other aspects of cross-language transforms in
// general, refer to the Beam programming guide: https://beam.apache.org/documentation/programming-guide/#multi-language-pipelines
//
// # Payload
//
// Payloads are configuration data that some cross-language transforms require for expansion.
// Consult the documentation of the transform in the source SDK to find out what payload data it
// requires. If no payload is required, pass in nil.
//
// CrossLanguage accepts payloads as a []byte containing an encoded ExternalConfigurationPayload
// protobuf. The helper function beam.CrossLanguagePayload is the recommended way to easily encode
// a standard Go struct for use as a payload.
//
// # Inputs and Outputs
//
// Like most transforms, any input PCollections must be provided. Unlike most transforms, output
// types must be provided because Go cannot infer output types from external transforms.
//
// Inputs and outputs to a cross-language transform may be either named or unnamed. Named
// inputs/outputs are used when there are more than one input/output, and are provided as maps with
// names as keys. Unnamed inputs/outputs are used when there is only one, and a map can be quickly
// constructed with the UnnamedInput and UnnamedOutput methods.
//
// An example of defining named inputs and outputs:
//
//	namedInputs := map[string]beam.PCollection{"pcol1": pcol1, "pcol2": pcol2}
//	namedOutputTypes := map[string]typex.FullType{
//	    "main": typex.New(reflectx.String),
//	    "side": typex.New(reflectx.Int64),
//	}
//
// CrossLanguage outputs a map of PCollections with associated names. These names will match those
// from provided named outputs. If the beam.UnnamedOutput method was used, the PCollection can be
// retrieved with beam.UnnamedOutputTag().
//
// An example of retrieving named outputs from a call to CrossLanguage:
//
//	outputs := beam.CrossLanguage(...)
//	mainPcol := outputs["main"]
//	sidePcol := outputs["side"]
//
// # Example
//
// This example shows using CrossLanguage to execute the Prefix cross-language transform using an
// expansion service running on localhost:8099. Prefix requires a payload containing a prefix to
// prepend to every input string.
//
//	type prefixPayload struct {
//	    Data string
//	}
//	encodedPl := beam.CrossLanguagePayload(prefixPayload{Data: "foo"})
//	urn := "beam:transforms:xlang:test:prefix"
//	expansionAddr := "localhost:8099"
//	outputType := beam.UnnamedOutput(typex.New(reflectx.String))
//	input := beam.UnnamedInput(inputPcol)
//	outs := beam.CrossLanguage(s, urn, encodedPl, expansionAddr, input, outputType)
//	outPcol := outputs[beam.UnnamedOutputTag()]
//
// # Alternative Expansion Handlers
//
// The xlangx.RegisterHandler function can be used to register alternative expansion
// handlers to a namespace, for use with this function. This allows for custom handling
// of expansion addresses or starting up expansion services automatically beneath the
// CrossLanguage call.
//
// In addition, urns can be bound to specific expansion addresses, using
// xlangx.RegisterOverrideForUrn. This allows for testing specific overrides, or other
// custom implementations to be used instead.
//
// To ignore overrides regardless of URN, wrapping the expansion address in
// a call to xlangx.Require, will force expansion using the given address.
func CrossLanguage(
	s Scope,
	urn string,
	payload []byte,
	expansionAddr string,
	namedInputs map[string]PCollection,
	namedOutputTypes map[string]FullType,
) map[string]PCollection {
	namedOutputs, err := TryCrossLanguage(s, urn, payload, expansionAddr, namedInputs, namedOutputTypes)
	if err != nil {
		panic(errors.WithContextf(err, "tried cross-language for %v against %v and failed", urn, expansionAddr))
	}
	return namedOutputs
}

// TryCrossLanguage coordinates the core functions required to execute the cross-language transform.
// See CrossLanguage for user documentation.
func TryCrossLanguage(
	s Scope,
	urn string,
	payload []byte,
	expansionAddr string,
	namedInputs map[string]PCollection,
	namedOutputTypes map[string]FullType,
) (map[string]PCollection, error) {
	if !s.IsValid() {
		panic(errors.New("invalid scope"))
	}

	inputsMap, inboundLinks := graph.NamedInboundLinks(mapPCollectionToNode(namedInputs))
	outputsMap, outboundLinks := graph.NamedOutboundLinks(s.real, namedOutputTypes)

	// Set the coder for outbound links for downstream validation.
	for n, i := range outputsMap {
		c := NewCoder(namedOutputTypes[n])
		outboundLinks[i].To.Coder = c.coder
	}

	ext := graph.ExternalTransform{
		Urn:           urn,
		Payload:       payload,
		ExpansionAddr: expansionAddr,
	}.WithNamedInputs(inputsMap).WithNamedOutputs(outputsMap)

	// Adding an edge in the graph corresponding to the ExternalTransform
	edge, isBoundedUpdater := graph.NewCrossLanguage(s.real, s.scope, &ext, inboundLinks, outboundLinks)

	// Once the appropriate input and output nodes are added to the edge, a
	// unique namespace can be requested.
	ext.Namespace = graph.NewNamespace()

	// Expand the transform into ext.Expanded.
	if err := xlangx.Expand(edge, &ext); err != nil {
		return nil, errors.WithContext(err, "expanding external transform")
	}

	// Ensures the expected named outputs are present
	graphx.VerifyNamedOutputs(&ext)
	// Using the expanded outputs, the graph's counterpart outputs are updated with bounded values
	graphx.ResolveOutputIsBounded(edge, isBoundedUpdater)

	return mapNodeToPCollection(graphx.ExternalOutputs(edge)), nil
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
