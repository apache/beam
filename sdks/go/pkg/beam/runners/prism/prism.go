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

// Package prism contains a local runner for running
// pipelines in the current process. Useful for testing.
package prism

import (
	"context"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/jobopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/jobservices"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/web"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/universal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func init() {
	beam.RegisterRunner("prism", Execute)
	beam.RegisterRunner("PrismRunner", Execute)
}

// Execute runs the given pipeline on prism. If no endpoint is set, then an in process instance
// is started, and the job run against that.
//
// At present, loopback mode is forced, though this will change once prism is able to
// use SDK containers.
func Execute(ctx context.Context, p *beam.Pipeline) (beam.PipelineResult, error) {
	if *jobopts.Endpoint == "" {
		// One hasn't been selected, so lets start one up and set the address.
		// Conveniently, this means that if multiple pipelines are executed against
		// the local runner, they will all use the same server.
		s := jobservices.NewServer(0, internal.RunPipeline)
		*jobopts.Endpoint = s.Endpoint()
		go s.Serve()
		if !jobopts.IsLoopback() {
			*jobopts.EnvironmentType = "loopback"
		}
	}
	return universal.Execute(ctx, p)
}

// Options for in process server creation.
type Options struct {
	Port int
}

// CreateJobServer returns a Beam JobServicesClient connected to an in memory JobServer.
// This call is non-blocking.
func CreateJobServer(ctx context.Context, opts Options) (jobpb.JobServiceClient, error) {
	s := jobservices.NewServer(opts.Port, internal.RunPipeline)
	go s.Serve()
	clientConn, err := grpc.DialContext(ctx, s.Endpoint(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	return jobpb.NewJobServiceClient(clientConn), nil
}

// CreateWebServer initialises the web UI for prism against the given JobsServiceClient.
// This call is blocking.
func CreateWebServer(ctx context.Context, cli jobpb.JobServiceClient, opts Options) error {
	return web.Initialize(ctx, opts.Port, cli)
}
