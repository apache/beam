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
	"context"

	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	jobpb "github.com/apache/beam/sdks/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"google.golang.org/grpc"
)

// Expand submits an external transform to be expanded by the expansion service.
// The given transform should be the external transform, and the components are
// any additional components necessary for the pipeline snippet.
//
// Users should generally call beam.CrossLanguage to access foreign transforms
// rather than calling this function directly.
func Expand(
	ctx context.Context,
	comps *pipepb.Components,
	transform *pipepb.PTransform,
	namespace string,
	expansionAddr string) (*jobpb.ExpansionResponse, error) {
	// Querying Expansion Service

	// Build expansion request proto.
	req := &jobpb.ExpansionRequest{
		Components: comps,
		Transform:  transform,
		Namespace:  namespace,
	}

	// Setting grpc client
	conn, err := grpc.Dial(expansionAddr, grpc.WithInsecure())
	if err != nil {
		err = errors.Wrapf(err, "unable to connect to expansion service at %v", expansionAddr)
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
	return res, nil
}
