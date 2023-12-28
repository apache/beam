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
	"fmt"
	"math"
	"net"
	"sync"

	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	"golang.org/x/exp/slog"
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
	index uint32
	jobs  map[string]*Job

	// execute defines how a job is executed.
	execute func(*Job)

	// Artifact hack
	artifacts map[string][]byte
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
	}
	slog.Info("Serving JobManagement", slog.String("endpoint", s.Endpoint()))
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
