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
	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	jobpb "github.com/apache/beam/sdks/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"google.golang.org/grpc"
)

// ExternalTransform represents the cross-language transform in and out of the Pipeline as a MultiEdge and Expanded proto respectively
type ExternalTransform struct {
	id                int
	Urn               string
	Payload           []byte
	In                []PCollection
	Out               []FullType
	Bounded           bool
	ExpansionAddr     string
	Components        *pipepb.Components
	ExpandedTransform *pipepb.PTransform
	Requirements      []string
}

// CrossLanguage is the temporary API to execute external transforms
// TODO(pskevin): Handle errors using the TryN and Must strategies instead one function handling multiple points of failure
func CrossLanguage(s Scope, p *Pipeline, e *ExternalTransform) []PCollection {
	if e.ExpansionAddr == "" { // TODO(pskevin): Better way to check if the value was ever set
		// return Legacy External API
	}

	/*
		Add ExternalTranform to the Graph
	*/
	// Validating scope and inputs
	if !s.IsValid() {
		// return nil, errors.New("invalid scope")
		fmt.Println("invalid scope")
	}
	for i, col := range e.In {
		if !col.IsValid() {
			// return nil, errors.Errorf("invalid pcollection to external: index %v", i)
			fmt.Printf("\ninvalid pcollection to external: index %v", i)

		}
	}

	// Using exisiting MultiEdge format to represent ExternalTransform (already backwards compatible)
	payload := &graph.Payload{
		URN:  e.Urn,
		Data: e.Payload,
	}
	var ins []*graph.Node
	for _, col := range e.In {
		ins = append(ins, col.n)
	}
	edge := graph.NewCrossLanguage(s.real, s.scope, ins, payload)

	// TODO(pskevin): There needs to be a better way of associating this ExternalTransform to the pipeline
	// Adding ExternalTransform to pipeline referenced by MultiEdge ID
	if p.ExpandedTransforms == nil {
		p.ExpandedTransforms = make(map[string]*ExternalTransform)
	}
	p.ExpandedTransforms[fmt.Sprintf("e%v", edge.ID())] = e

	/*
		Build the ExpansionRequest
	*/
	// Obtaining the components and transform proto representing this transform
	pipeline, err := graphx.Marshal([]*graph.MultiEdge{edge}, &graphx.Options{})
	if err != nil {
		panic(err)
	}

	// Adding fake impulses to each input as required for correct expansion
	// TODO(pskevin): Remove these fake impulses from final Pipeline since multiple producers of the same PCollections is logically wrong
	transforms := pipeline.Components.Transforms
	rootTransformID := pipeline.RootTransformIds[0]
	for tag, id := range transforms[rootTransformID].Inputs {
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

	// Assembling ExpansionRequest proto
	req := &jobpb.ExpansionRequest{
		Components: pipeline.Components,
		Transform:  transforms[rootTransformID],
		Namespace:  s.String(),
	}

	/*
		Querying Expansion Service
	*/
	// Setting grpc client
	conn, err := grpc.Dial(e.ExpansionAddr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	client := jobpb.NewExpansionServiceClient(conn)

	// Handling ExpansionResponse
	res, err := client.Expand(context.Background(), req)
	if err != nil {
		panic(err)
	}
	e.Components = res.GetComponents()
	e.ExpandedTransform = res.GetTransform()
	e.Requirements = res.GetRequirements()

	/*
		Associating output PCollections of the expanded transform with correct internal outbound links and nodes
	*/
	// No information about the output types and bounded nature has been explicitly passed by the user
	if len(e.Out) == 0 || cap(e.Out) == 0 {
		// Infer output types from ExpansionResponse and update e.Out
		if e.Out == nil {
			// Use reverse schema encoding
		} else {
			// Use the coders list and map from coder id to internal FullType?
		}
	}

	// Using information about the output types and bounded nature inferred or explicitly passed by the user
	graph.AddOutboundLinks(s.real, edge, ins, e.Out, e.Bounded)
	var ret []PCollection
	for _, out := range edge.Output {
		c := PCollection{out.To}
		c.SetCoder(NewCoder(c.Type()))
		ret = append(ret, c)
	}
	return ret
}

// External defines a Beam external transform. The interpretation of this primitive is runner
// specific. The runner is responsible for parsing the payload based on the
// spec provided to implement the behavior of the operation. Transform
// libraries should expose an API that captures the user's intent and serialize
// the payload as a byte slice that the runner will deserialize.
func External(s Scope, spec string, payload []byte, in []PCollection, out []FullType, bounded bool) []PCollection {
	return MustN(TryExternal(s, spec, payload, in, out, bounded))
}

// TryExternal attempts to perform the work of External, returning an error indicating why the operation
// failed.
func TryExternal(s Scope, spec string, payload []byte, in []PCollection, out []FullType, bounded bool) ([]PCollection, error) {
	if !s.IsValid() {
		return nil, errors.New("invalid scope")
	}
	for i, col := range in {
		if !col.IsValid() {
			return nil, errors.Errorf("invalid pcollection to external: index %v", i)
		}
	}

	var ins []*graph.Node
	for _, col := range in {
		ins = append(ins, col.n)
	}
	edge := graph.NewExternal(s.real, s.scope, &graph.Payload{URN: spec, Data: payload}, ins, out, bounded)

	var ret []PCollection
	for _, out := range edge.Output {
		c := PCollection{out.To}
		c.SetCoder(NewCoder(c.Type()))
		ret = append(ret, c)
	}
	return ret, nil
}
