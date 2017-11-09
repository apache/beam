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

package artifact

import (
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/apache/beam/sdks/go/pkg/beam/model/jobmanagement_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/util/grpcx"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// startServer starts an in-memory staging and retrieval artifact server
// and returns a gRPC connection to it.
func startServer(t *testing.T) *grpc.ClientConn {
	// If port is zero this will bind an unused port.
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to find unused port: %v", err)
	}
	endpoint := listener.Addr().String()

	real := &server{m: make(map[string]*manifest)}

	gs := grpc.NewServer()
	pb.RegisterArtifactStagingServiceServer(gs, real)
	pb.RegisterArtifactRetrievalServiceServer(gs, real)
	go gs.Serve(listener)

	t.Logf("server listening on %v", endpoint)

	cc, err := grpcx.Dial(context.Background(), endpoint, time.Minute)
	if err != nil {
		t.Fatalf("failed to dial fake server at %v: %v", endpoint, err)
	}
	return cc
}

type data struct {
	md     *pb.ArtifactMetadata
	chunks [][]byte
}

type manifest struct {
	md *pb.Manifest
	m  map[string]*data // key -> data
	mu sync.Mutex
}

// server is a in-memory staging and retrieval artifact server for testing.
type server struct {
	m  map[string]*manifest // token -> manifest
	mu sync.Mutex
}

func (s *server) PutArtifact(ps pb.ArtifactStagingService_PutArtifactServer) error {
	id, err := grpcx.ReadWorkerID(ps.Context())
	if err != nil {
		return fmt.Errorf("expected worker id: %v", err)
	}

	// Read header

	header, err := ps.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive header: %v", err)
	}
	if header.GetMetadata() == nil {
		return fmt.Errorf("expected header as first message: %v", header)
	}
	key := header.GetMetadata().Name

	// Read chunks

	var chunks [][]byte
	for {
		msg, err := ps.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if msg.GetData() == nil {
			return fmt.Errorf("expected data: %v", msg)
		}
		if len(msg.GetData().GetData()) == 0 {
			return fmt.Errorf("expected non-empty data: %v", msg)
		}
		chunks = append(chunks, msg.GetData().GetData())
	}

	// Updated staged artifact. This test implementation will allow updates to artifacts
	// that are already committed, but real implementations should manage artifacts in a
	// way that makes that impossible.

	m := s.getManifest(id, true)
	m.mu.Lock()
	m.m[key] = &data{chunks: chunks}
	m.mu.Unlock()

	return ps.SendAndClose(&pb.PutArtifactResponse{})
}

func (s *server) CommitManifest(ctx context.Context, req *pb.CommitManifestRequest) (*pb.CommitManifestResponse, error) {
	id, err := grpcx.ReadWorkerID(ctx)
	if err != nil {
		return nil, fmt.Errorf("expected worker id: %v", err)
	}

	m := s.getManifest(id, true)
	m.mu.Lock()
	defer m.mu.Unlock()

	// Verify that all artifacts are properly staged. Fail if not.

	artifacts := req.GetManifest().GetArtifact()
	for _, md := range artifacts {
		if _, ok := m.m[md.Name]; !ok {
			return nil, fmt.Errorf("artifact %v not staged", md.Name)
		}
	}

	// Update commit. Only one manifest can exist for each staging id.

	for _, md := range artifacts {
		m.m[md.Name].md = md
	}
	m.md = req.GetManifest()

	return &pb.CommitManifestResponse{StagingToken: id}, nil
}

func (s *server) GetManifest(ctx context.Context, req *pb.GetManifestRequest) (*pb.GetManifestResponse, error) {
	id, err := grpcx.ReadWorkerID(ctx)
	if err != nil {
		return nil, fmt.Errorf("expected worker id: %v", err)
	}

	m := s.getManifest(id, false)
	if m == nil || m.md == nil {
		return nil, fmt.Errorf("manifest for %v not found", id)
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	return &pb.GetManifestResponse{Manifest: m.md}, nil
}

func (s *server) GetArtifact(req *pb.GetArtifactRequest, stream pb.ArtifactRetrievalService_GetArtifactServer) error {
	id, err := grpcx.ReadWorkerID(stream.Context())
	if err != nil {
		return fmt.Errorf("expected worker id: %v", err)
	}

	m := s.getManifest(id, false)
	if m == nil || m.md == nil {
		return fmt.Errorf("manifest for %v not found", id)
	}

	// Validate artifact and grab chunks so that we can stream them without
	// holding the lock.

	m.mu.Lock()
	elm, ok := m.m[req.GetName()]
	if !ok || elm.md == nil {
		m.mu.Unlock()
		return fmt.Errorf("manifest for %v does not contain artifact %v", id, req.GetName())
	}
	chunks := elm.chunks
	m.mu.Unlock()

	// Send chunks exactly as we received them.

	for _, chunk := range chunks {
		if err := stream.Send(&pb.ArtifactChunk{Data: chunk}); err != nil {
			return err
		}
	}
	return nil
}

func (s *server) getManifest(id string, create bool) *manifest {
	s.mu.Lock()
	defer s.mu.Unlock()

	ret, ok := s.m[id]
	if !ok && create {
		ret = &manifest{m: make(map[string]*data)}
		s.m[id] = ret
	}
	return ret
}
