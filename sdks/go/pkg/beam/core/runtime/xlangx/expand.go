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

// Package xlangx contains various low-level utilities needed for adding
// cross-language transforms to the pipeline.
package xlangx

import (
	"context"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/pipelinex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"google.golang.org/grpc"
)

// Expand expands an unexpanded graph.ExternalTransform as a
// graph.ExpandedTransform and assigns it to the ExternalTransform's Expanded
// field. This requires querying an expansion service based on the configuration
// details within the ExternalTransform.
//
// For framework use only. Users should call beam.CrossLanguage to access foreign transforms
// rather than calling this function directly.
func Expand(edge *graph.MultiEdge, ext *graph.ExternalTransform) error {
	// Build the ExpansionRequest

	// Obtaining the components and transform proto representing this transform
	p, err := graphx.Marshal([]*graph.MultiEdge{edge}, &graphx.Options{})
	if err != nil {
		return errors.Wrapf(err, "unable to generate proto representation of %v", ext)
	}

	transforms := p.GetComponents().GetTransforms()

	// Transforms consist of only External transform and composites. Composites
	// should be removed from proto before submitting expansion request.
	extTransformID := p.GetRootTransformIds()[0]
	extTransform := transforms[extTransformID]
	for extTransform.UniqueName != "External" {
		delete(transforms, extTransformID)
		p, err = pipelinex.Normalize(p) // Update root transform IDs.
		if err != nil {
			return err
		}
		transforms = p.GetComponents().GetTransforms()
		extTransformID = p.GetRootTransformIds()[0]
		extTransform = transforms[extTransformID]
	}

	// Scoping the ExternalTransform with respect to it's unique namespace, thus
	// avoiding future collisions
	addNamespace(extTransform, p.GetComponents(), ext.Namespace)
	delete(transforms, extTransformID)

	// Querying the expansion service
	res, err := expand(context.Background(), p.GetComponents(), extTransform, edge, ext)
	if err != nil {
		return err
	}

	exp := &graph.ExpandedTransform{
		Components:   res.GetComponents(),
		Transform:    res.GetTransform(),
		Requirements: res.GetRequirements(),
	}
	ext.Expanded = exp
	return nil
}

func expand(
	ctx context.Context,
	comps *pipepb.Components,
	transform *pipepb.PTransform,
	edge *graph.MultiEdge,
	ext *graph.ExternalTransform) (*jobpb.ExpansionResponse, error) {

	h, config := defaultReg.getHandlerFunc(transform.GetSpec().GetUrn(), ext.ExpansionAddr)
	// Overwrite expansion address if changed due to override for service or URN.
	if config != ext.ExpansionAddr {
		ext.ExpansionAddr = config
	}
	return h(ctx, &HandlerParams{
		Config: config,
		Req: &jobpb.ExpansionRequest{
			Components: comps,
			Transform:  transform,
			Namespace:  ext.Namespace,
		},
		edge: edge,
		ext:  ext,
	})
}

// QueryExpansionService submits an external transform to be expanded by the
// expansion service. The given transform should be the external transform, and
// the components are any additional components necessary for the pipeline
// snippet.
//
// The address to be queried is determined by the Config field of HandlerParams.
//
// This HandlerFunc is exported to simplify building custom handler functions
// that do end up calling a Beam ExpansionService, either as a fallback or
// as part of normal flow.
func QueryExpansionService(ctx context.Context, p *HandlerParams) (*jobpb.ExpansionResponse, error) {
	req := p.Req
	// Setting grpc client
	conn, err := grpc.Dial(p.Config, grpc.WithInsecure())
	if err != nil {
		err = errors.Wrapf(err, "unable to connect to expansion service at %v", p.Config)
		return nil, errors.WithContextf(err, "expanding transform with ExpansionRequest: %v", req)
	}
	defer conn.Close()
	client := jobpb.NewExpansionServiceClient(conn)

	// Handling ExpansionResponse
	res, err := client.Expand(ctx, req)
	if err != nil {
		err = errors.Wrapf(err, "expansion failed")
		return nil, errors.WithContextf(err, "expanding transform with ExpansionRequest: %v", req)
	}
	if len(res.GetError()) != 0 { // ExpansionResponse includes an error.
		err := errors.New(res.GetError())
		err = errors.Wrapf(err, "expansion failed")
		return nil, errors.WithContextf(err, "expanding transform with ExpansionRequest: %v", req)
	}
	return res, nil
}
