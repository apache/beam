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

// Package pool facilitates a external worker service, as an alternate mode for
// the standard Beam container.
//
// This is predeominantly to serve as a process spawner within a given container
// VM for an arbitrary number of jobs, instead of for a single worker instance.
//
// Workers will be spawned as executed OS processes.
package pool

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"sync"

	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/grpcx"
	"google.golang.org/grpc"
)

// New initializes a process based ExternalWorkerService, at the given
// port.
func New(ctx context.Context, port int, containerExecutable string) (*Process, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}
	slog.Info("starting Process server", "addr", lis.Addr())
	grpcServer := grpc.NewServer()
	root, cancel := context.WithCancel(ctx)
	s := &Process{lis: lis, root: root, rootCancel: cancel, workers: map[string]context.CancelFunc{},
		grpcServer: grpcServer, containerExecutable: containerExecutable}
	fnpb.RegisterBeamFnExternalWorkerPoolServer(grpcServer, s)
	return s, nil
}

// ServeAndWait starts the ExternalWorkerService and blocks until exit.
func (s *Process) ServeAndWait() error {
	return s.grpcServer.Serve(s.lis)
}

// Process implements fnpb.BeamFnExternalWorkerPoolServer, by starting external
// processes.
type Process struct {
	fnpb.UnimplementedBeamFnExternalWorkerPoolServer

	containerExecutable string // The host for the container executable.

	lis        net.Listener
	root       context.Context
	rootCancel context.CancelFunc

	mu      sync.Mutex
	workers map[string]context.CancelFunc

	grpcServer *grpc.Server
}

// StartWorker initializes a new worker harness, implementing BeamFnExternalWorkerPoolServer.StartWorker.
func (s *Process) StartWorker(_ context.Context, req *fnpb.StartWorkerRequest) (*fnpb.StartWorkerResponse, error) {
	slog.Info("starting worker", "id", req.GetWorkerId())
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.workers == nil {
		return &fnpb.StartWorkerResponse{
			Error: "worker pool shutting down",
		}, nil
	}

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

	ctx := grpcx.WriteWorkerID(s.root, req.GetWorkerId())
	ctx, s.workers[req.GetWorkerId()] = context.WithCancel(ctx)

	args := []string{
		"--id=" + req.GetWorkerId(),
		"--control_endpoint=" + req.GetControlEndpoint().GetUrl(),
		"--artifact_endpoint=" + req.GetArtifactEndpoint().GetUrl(),
		"--provision_endpoint=" + req.GetProvisionEndpoint().GetUrl(),
		"--logging_endpoint=" + req.GetLoggingEndpoint().GetUrl(),
	}

	cmd := exec.CommandContext(ctx, s.containerExecutable, args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = nil // Use the current environment.

	if err := cmd.Start(); err != nil {
		return &fnpb.StartWorkerResponse{Error: fmt.Sprintf("Unable to start boot for worker %v: %v", req.GetWorkerId(), err)}, nil
	}
	return &fnpb.StartWorkerResponse{}, nil
}

// StopWorker terminates a worker harness, implementing BeamFnExternalWorkerPoolServer.StopWorker.
func (s *Process) StopWorker(_ context.Context, req *fnpb.StopWorkerRequest) (*fnpb.StopWorkerResponse, error) {
	slog.Info("stopping worker", "id", req.GetWorkerId())
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.workers == nil {
		// Worker pool is already shutting down, so no action is needed.
		return &fnpb.StopWorkerResponse{}, nil
	}
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
func (s *Process) Stop(ctx context.Context) error {
	s.mu.Lock()

	slog.Debug("stopping Process", "worker_count", len(s.workers))
	s.workers = nil
	s.rootCancel()

	// There can be a deadlock between the StopWorker RPC and GracefulStop
	// which waits for all RPCs to finish, so it must be outside the critical section.
	s.mu.Unlock()

	s.grpcServer.GracefulStop()
	return nil
}
