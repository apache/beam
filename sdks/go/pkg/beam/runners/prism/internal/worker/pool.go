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

package worker

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"

	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/grpcx"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

var (
	mu sync.Mutex

	// Pool stores *W.
	Pool = make(MapW)
)

// MapW manages the creation and querying of *W.
type MapW map[string]*W

func (m MapW) workerFromMetadataCtx(ctx context.Context) (*W, error) {
	id, err := grpcx.ReadWorkerID(ctx)
	if err != nil {
		return nil, err
	}
	if id == "" {
		return nil, fmt.Errorf("worker id in ctx metadata is an empty string")
	}
	mu.Lock()
	defer mu.Unlock()
	if w, ok := m[id]; ok {
		return w, nil
	}
	return nil, fmt.Errorf("worker id: '%s' read from ctx but not registered in worker pool", id)
}

// NewWorker instantiates and registers new *W instances.
func (m MapW) NewWorker(id, env string) *W {
	wk := &W{
		ID:  id,
		Env: env,

		InstReqs:    make(chan *fnpb.InstructionRequest, 10),
		DataReqs:    make(chan *fnpb.Elements, 10),
		StoppedChan: make(chan struct{}),

		activeInstructions: make(map[string]controlResponder),
		Descriptors:        make(map[string]*fnpb.ProcessBundleDescriptor),
	}
	mu.Lock()
	defer mu.Unlock()
	m[wk.ID] = wk
	return wk
}

// NewMultiplexW instantiates a grpc.Server for multiplexing worker FnAPI requests.
func NewMultiplexW(opts ...grpc.ServerOption) *grpc.Server {
	opts = append(opts, grpc.MaxSendMsgSize(math.MaxInt32))

	g := grpc.NewServer(opts...)
	wk := &MultiplexW{
		logger: slog.Default(),
	}

	fnpb.RegisterBeamFnControlServer(g, wk)
	fnpb.RegisterBeamFnDataServer(g, wk)
	fnpb.RegisterBeamFnLoggingServer(g, wk)
	fnpb.RegisterBeamFnStateServer(g, wk)
	fnpb.RegisterProvisionServiceServer(g, wk)
	healthpb.RegisterHealthServer(g, wk)

	return g
}

// MultiplexW multiplexes FnAPI gRPC requests to *W stored in the Pool.
type MultiplexW struct {
	fnpb.UnimplementedBeamFnControlServer
	fnpb.UnimplementedBeamFnDataServer
	fnpb.UnimplementedBeamFnStateServer
	fnpb.UnimplementedBeamFnLoggingServer
	fnpb.UnimplementedProvisionServiceServer
	healthpb.UnimplementedHealthServer

	logger *slog.Logger
}

func (mw *MultiplexW) Check(_ context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func (mw *MultiplexW) GetProvisionInfo(ctx context.Context, req *fnpb.GetProvisionInfoRequest) (*fnpb.GetProvisionInfoResponse, error) {
	w, err := Pool.workerFromMetadataCtx(ctx)
	if err != nil {
		return nil, err
	}
	return w.GetProvisionInfo(ctx, req)
}

func (mw *MultiplexW) Logging(stream fnpb.BeamFnLogging_LoggingServer) error {
	w, err := Pool.workerFromMetadataCtx(stream.Context())
	if err != nil {
		return err
	}
	return w.Logging(stream)
}

func (mw *MultiplexW) GetProcessBundleDescriptor(ctx context.Context, req *fnpb.GetProcessBundleDescriptorRequest) (*fnpb.ProcessBundleDescriptor, error) {
	w, err := Pool.workerFromMetadataCtx(ctx)
	if err != nil {
		return nil, err
	}
	return w.GetProcessBundleDescriptor(ctx, req)
}

func (mw *MultiplexW) Control(ctrl fnpb.BeamFnControl_ControlServer) error {
	w, err := Pool.workerFromMetadataCtx(ctrl.Context())
	if err != nil {
		return err
	}
	return w.Control(ctrl)
}

func (mw *MultiplexW) Data(data fnpb.BeamFnData_DataServer) error {
	w, err := Pool.workerFromMetadataCtx(data.Context())
	if err != nil {
		return err
	}
	return w.Data(data)
}

func (mw *MultiplexW) State(state fnpb.BeamFnState_StateServer) error {
	w, err := Pool.workerFromMetadataCtx(state.Context())
	if err != nil {
		return err
	}
	return w.State(state)
}

func (mw *MultiplexW) MonitoringMetadata(ctx context.Context, unknownIDs []string) *fnpb.MonitoringInfosMetadataResponse {
	w, err := Pool.workerFromMetadataCtx(ctx)
	if err != nil {
		mw.logger.Error(err.Error())
		return nil
	}
	return w.MonitoringMetadata(ctx, unknownIDs)
}
