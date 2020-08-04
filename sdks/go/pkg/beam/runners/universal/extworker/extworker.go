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

// Package extworker provides an external worker service and related utilities.
package extworker

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/harness"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	fnpb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/util/grpcx"
	"google.golang.org/grpc"
)

// StartLoopback initializes a Loopback ExternalWorkerService, at the given port.
func StartLoopback(ctx context.Context, port int) (*Loopback, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return nil, err
	}

	log.Infof(ctx, "starting Loopback server at %v", lis.Addr())
	grpcServer := grpc.NewServer()
	root, cancel := context.WithCancel(ctx)
	s := &Loopback{lis: lis, root: root, rootCancel: cancel, workers: map[string]context.CancelFunc{},
		grpcServer: grpcServer}
	fnpb.RegisterBeamFnExternalWorkerPoolServer(grpcServer, s)
	go grpcServer.Serve(lis)
	return s, nil
}

// Loopback implements fnpb.BeamFnExternalWorkerPoolServer
type Loopback struct {
	lis        net.Listener
	root       context.Context
	rootCancel context.CancelFunc

	mu      sync.Mutex
	workers map[string]context.CancelFunc

	grpcServer *grpc.Server
}

// StartWorker initializes a new worker harness, implementing BeamFnExternalWorkerPoolServer.StartWorker.
func (s *Loopback) StartWorker(ctx context.Context, req *fnpb.StartWorkerRequest) (*fnpb.StartWorkerResponse, error) {
	log.Infof(ctx, "starting worker %v", req.GetWorkerId())
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.workers[req.GetWorkerId()]; ok {
		return &fnpb.StartWorkerResponse{
			Error: fmt.Sprintf("worker with ID %q already exists", req.GetWorkerId()),
		}, nil
	}
	if req.GetLoggingEndpoint() == nil {
		return &fnpb.StartWorkerResponse{Error: fmt.Sprintf("Missing logging endpoint for worker %v", req.GetWorkerId())}, nil
	}
	if req.GetControlEndpoint() == nil {
		return &fnpb.StartWorkerResponse{Error: fmt.Sprintf("Missing control endpoint for worker %v", req.GetWorkerId())}, nil
	}
	if req.GetLoggingEndpoint().Authentication != nil || req.GetControlEndpoint().Authentication != nil {
		return &fnpb.StartWorkerResponse{Error: "[BEAM-10610] Secure endpoints not supported."}, nil
	}

	ctx = grpcx.WriteWorkerID(s.root, req.GetWorkerId())
	ctx, s.workers[req.GetWorkerId()] = context.WithCancel(ctx)

	go harness.Main(ctx, req.GetLoggingEndpoint().GetUrl(), req.GetControlEndpoint().GetUrl())
	return &fnpb.StartWorkerResponse{}, nil
}

// StopWorker terminates a worker harness, implementing BeamFnExternalWorkerPoolServer.StopWorker.
func (s *Loopback) StopWorker(ctx context.Context, req *fnpb.StopWorkerRequest) (*fnpb.StopWorkerResponse, error) {
	log.Infof(ctx, "stopping worker %v", req.GetWorkerId())
	s.mu.Lock()
	defer s.mu.Unlock()
	if cancelfn, ok := s.workers[req.GetWorkerId()]; ok {
		cancelfn()
		delete(s.workers, req.GetWorkerId())
		return &fnpb.StopWorkerResponse{}, nil
	}
	return &fnpb.StopWorkerResponse{
		Error: fmt.Sprintf("no worker with id %q running", req.GetWorkerId()),
	}, nil

}

// Stop terminates the service and stops all workers.
func (s *Loopback) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Infof(ctx, "stopping Loopback, and %d workers", len(s.workers))
	s.workers = map[string]context.CancelFunc{}
	s.lis.Close()
	s.rootCancel()
	s.grpcServer.GracefulStop()
	return nil
}

// EnvironmentConfig returns the environment config for this service instance.
func (s *Loopback) EnvironmentConfig(context.Context) string {
	return fmt.Sprintf("localhost:%d", s.lis.Addr().(*net.TCPAddr).Port)
}
