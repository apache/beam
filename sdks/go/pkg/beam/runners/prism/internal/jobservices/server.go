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

package jobservices

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	"google.golang.org/grpc"
)

type Server struct {
	jobpb.UnimplementedJobServiceServer
	jobpb.UnimplementedArtifactStagingServiceServer
	jobpb.UnimplementedArtifactRetrievalServiceServer
	fnpb.UnimplementedProvisionServiceServer

	// Server management
	lis    net.Listener
	server *grpc.Server

	// Job Management
	mu    sync.Mutex
	index uint32 // Use with atomics.
	jobs  map[string]*Job

	// IdleShutdown management. Needs to use atomics, since they
	// may be both while already holding the lock, or when not
	// (eg via job state).
	idleTimer          atomic.Pointer[time.Timer]
	terminatedJobCount uint32 // Use with atomics.
	idleTimeout        time.Duration
	cancelFn           context.CancelCauseFunc
	logger             *slog.Logger

	// execute defines how a job is executed.
	execute func(*Job)

	// Artifact hack
	artifacts          map[string][]byte
	WorkerPoolEndpoint string
}

// NewServer acquires the indicated port.
func NewServer(port int, execute func(*Job)) *Server {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	}
	s := &Server{
		lis:     lis,
		jobs:    make(map[string]*Job),
		execute: execute,
		logger:  slog.Default(), // TODO substitute with a configured logger.
	}
	s.logger.Info("Serving JobManagement", slog.String("endpoint", s.Endpoint()))
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(math.MaxInt32),
	}
	s.server = grpc.NewServer(opts...)
	jobpb.RegisterJobServiceServer(s.server, s)
	jobpb.RegisterArtifactStagingServiceServer(s.server, s)
	jobpb.RegisterArtifactRetrievalServiceServer(s.server, s)
	return s
}

func (s *Server) getJob(id string) *Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.jobs[id]
}

func (s *Server) Endpoint() string {
	_, port, _ := net.SplitHostPort(s.lis.Addr().String())
	return fmt.Sprintf("localhost:%v", port)
}

// Serve serves on the started listener. Blocks.
func (s *Server) Serve() {
	s.server.Serve(s.lis)
}

// Stop the GRPC server.
func (s *Server) Stop() {
	s.server.GracefulStop()
}

// IdleShutdown allows the server to call the cancelFn if there have been no active jobs
// for at least the given timeout.
func (s *Server) IdleShutdown(timeout time.Duration, cancelFn context.CancelCauseFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.idleTimeout = timeout
	s.cancelFn = cancelFn

	// Stop gap to kill the process less gracefully.
	if s.cancelFn == nil {
		s.cancelFn = func(cause error) {
			os.Exit(1)
		}
	}

	s.idleTimer.Store(time.AfterFunc(timeout, s.idleShutdownCallback))
}

// idleShutdownCallback is called by the AfterFunc timer for idle shutdown.
func (s *Server) idleShutdownCallback() {
	index := atomic.LoadUint32(&s.index)
	terminated := atomic.LoadUint32(&s.terminatedJobCount)
	if index == terminated {
		slog.Info("shutting down after being idle", "idleTimeout", s.idleTimeout)
		s.cancelFn(nil)
	}
}

// jobTerminated marks that the job has been terminated, and if there are no active jobs, starts the idle timer.
func (s *Server) jobTerminated() {
	if s.idleTimer.Load() != nil {
		terminated := atomic.AddUint32(&s.terminatedJobCount, 1)
		total := atomic.LoadUint32(&s.index)
		if total == terminated {
			s.idleTimer.Store(time.AfterFunc(s.idleTimeout, s.idleShutdownCallback))
		}
	}
}
