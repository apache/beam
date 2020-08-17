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
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	jobpb "github.com/apache/beam/sdks/go/pkg/beam/model/jobmanagement_v1"
)

func CrossLanguage(s Scope, urn string, payload []byte, expansionAddr string, inputs map[string]PCollection, outputTypes map[string]FullType) map[string]PCollection {
	inputNodes := mapPCollectionToNode(inputs)

	ext := graph.ExternalTransform{
		Urn:           urn,
		Payload:       payload,
		ExpansionAddr: expansionAddr,
	}.WithNamedInputs(inputNodes).WithNamedOutputs(outputTypes)

	outputNodes, err := TryCrossLanguage(s, &ext)
	if err != nil {
		panic(errors.WithContextf(err, "tried cross-language and failed"))
	}
	return mapNodeToPCollection(outputNodes)
}

/*
func CrossLanguageWithSink(s Scope, urn string, payload []byte, expansionAddr string, inputs map[string]PCollection, output FullType) PCollection {
	return MustN(
		TryCrossLanguage(
			&ExternalTransform{
				Urn:           urn,
				Payload:       payload,
				ExpansionAddr: expansionAddr,
			}.
				withNamedInputs(inputs).
				withSink(output)))
}

func CrossLanguageWithSource(s Scope, urn string, payload []byte, expansionAddr string, input PCollection, outputs map[string]FullType) map[string]PCollection {
	return MustN(
		TryCrossLanguage(
			&ExternalTransform{
				Urn:           urn,
				Payload:       payload,
				ExpansionAddr: expansionAddr,
			}.
				withSource(input).
				withNamedOutputs(outputs)))
}

func CrossLanguageWithSingleInputOutput(s Scope, urn string, payload []byte, expansionAddr string, input PCollection, output FullType) PCollection {
	return MustN(TryCrossLanguage(
		&ExternalTransform{
			Urn:           urn,
			Payload:       payload,
			ExpansionAddr: expansionAddr,
		}.
			withSource(input).
			withSink(output)))
}
*/

func TryCrossLanguage(s Scope, ext *graph.ExternalTransform) (map[string]*graph.Node, error) {
	// Add ExternalTransform to the Graph

	// Validating scope
	if !s.IsValid() {
		return nil, errors.New("invalid scope")
	}

	// Using existing MultiEdge format to represent ExternalTransform (already backwards compatible)
	edge := graph.NewCrossLanguage(s.real, s.scope, ext)

	// Build the ExpansionRequest

	// Obtaining the components and transform proto representing this transform
	p, err := graphx.Marshal([]*graph.MultiEdge{edge}, &graphx.Options{})
	if err != nil {
		return nil, errors.Wrapf(err, "unable to generate proto representation of %v", ext)
	}

	xlangx.AddFakeImpulses(p)

	// Assembling ExpansionRequest proto
	transforms := p.GetComponents().GetTransforms()
	rootTransformID := p.GetRootTransformIds()[0] // External transform is the only root transform
	rootTransform := transforms[rootTransformID]

	delete(transforms, rootTransformID)

	req := &jobpb.ExpansionRequest{
		Components: p.GetComponents(),
		Transform:  rootTransform,
		Namespace:  s.String(), //TODO(pskevin): Need to be unique per transform (along with the UniqueName which is just string(Opcode) for now)
	}

	res, err := xlangx.Expand(context.Background(), req, ext.ExpansionAddr)
	if err != nil {
		return nil, errors.WithContextf(err, "failed to expand external transform with error [%v] for ExpansionRequest: %v", res.GetError(), req)
	}

	xlangx.RemoveFakeImpulses(res.GetComponents(), res.GetTransform())

	exp := &graph.ExpandedTransform{
		Components_:     res.GetComponents(),
		Transform_:      res.GetTransform(),
		Requirements_:   res.GetRequirements(),
		BoundedOutputs_: make(map[string]bool),
	}
	ext.Expanded_ = exp

	xlangx.VerifyNamedOutputs(ext)

	xlangx.ResolveOutputIsBounded(ext)

	// Using information about the output types and bounded nature inferred or explicitly passed by the user
	graph.AddOutboundLinks(s.real, edge)

	return ext.Outputs, nil
}
