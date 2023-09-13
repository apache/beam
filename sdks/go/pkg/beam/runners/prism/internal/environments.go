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

package internal

import (
	"context"
	"fmt"

	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/jobservices"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/urns"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/worker"
	"golang.org/x/exp/slog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

// TODO move environment handling to the worker package.

func runEnvironment(ctx context.Context, j *jobservices.Job, env string, wk *worker.W) error {
	// TODO fix broken abstraction.
	// We're starting a worker pool here, because that's the loopback environment.
	// It's sort of a mess, largely because of loopback, which has
	// a different flow from a provisioned docker container.
	e := j.Pipeline.GetComponents().GetEnvironments()[env]
	switch e.GetUrn() {
	case urns.EnvExternal:
		ep := &pipepb.ExternalPayload{}
		if err := (proto.UnmarshalOptions{}).Unmarshal(e.GetPayload(), ep); err != nil {
			slog.Error("unmarshing external environment payload", err, slog.String("envID", wk.Env))
		}
		go func() {
			externalEnvironment(ctx, ep, wk)
			slog.Debug("environment stopped", slog.String("envID", wk.String()), slog.String("job", j.String()))
		}()
		return nil
	case urns.EnvDocker:
		dp := &pipepb.DockerPayload{}
		if err := (proto.UnmarshalOptions{}).Unmarshal(e.GetPayload(), dp); err != nil {
			slog.Error("unmarshing docker environment payload", err, slog.String("envID", wk.Env))
		}

		// TODO: Check if the image exists.
		// TODO: Download if the image doesn't exist.
		// TODO: Ensure artifact server downloads to a consistent cache, and doesn't re-download artifacts.
		// Ensure Go worker rebuilds are consistent?
		// TODO: Fail sensibly when the image can't be downloaded or started without process crash.
		return fmt.Errorf("environment %v with urn %v unimplemented:e\n%v payload\n%v ", env, e.GetUrn(), prototext.Format(e), prototext.Format(dp))
	default:
		return fmt.Errorf("environment %v with urn %v unimplemented", env, e.GetUrn())
	}
}

func externalEnvironment(ctx context.Context, ep *pipepb.ExternalPayload, wk *worker.W) {
	conn, err := grpc.Dial(ep.GetEndpoint().GetUrl(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(fmt.Sprintf("unable to dial sdk worker %v: %v", ep.GetEndpoint().GetUrl(), err))
	}
	defer conn.Close()
	pool := fnpb.NewBeamFnExternalWorkerPoolClient(conn)

	endpoint := &pipepb.ApiServiceDescriptor{
		Url: wk.Endpoint(),
	}
	pool.StartWorker(ctx, &fnpb.StartWorkerRequest{
		WorkerId:          wk.ID,
		ControlEndpoint:   endpoint,
		LoggingEndpoint:   endpoint,
		ArtifactEndpoint:  endpoint,
		ProvisionEndpoint: endpoint,
		Params:            nil,
	})

	// Job processing happens here, but orchestrated by other goroutines
	// This goroutine blocks until the context is cancelled, signalling
	// that the pool runner should stop the worker.
	<-ctx.Done()

	// Previous context cancelled so we need a new one
	// for this request.
	pool.StopWorker(context.Background(), &fnpb.StopWorkerRequest{
		WorkerId: wk.ID,
	})
}
