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
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	jobpb "github.com/apache/beam/sdks/go/pkg/beam/model/jobmanagement_v1"
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
	jobpb.RegisterLegacyArtifactStagingServiceServer(gs, real)
	jobpb.RegisterLegacyArtifactRetrievalServiceServer(gs, real)
	go gs.Serve(listener)

	t.Logf("server listening on %v", endpoint)

	cc, err := grpcx.Dial(context.Background(), endpoint, time.Minute)
	if err != nil {
		t.Fatalf("failed to dial fake server at %v: %v", endpoint, err)
	}
	return cc
}

type data struct {
	md     *jobpb.ArtifactMetadata
	chunks [][]byte
}

type manifest struct {
	md *jobpb.Manifest
	m  map[string]*data // key -> data
	mu sync.Mutex
}

// server is a in-memory staging and retrieval artifact server for testing.
type server struct {
	m  map[string]*manifest // token -> manifest
	mu sync.Mutex
}

func (s *server) PutArtifact(ps jobpb.LegacyArtifactStagingService_PutArtifactServer) error {
	// Read header

	header, err := ps.Recv()
	if err != nil {
		return errors.Wrap(err, "failed to receive header")
	}
	if header.GetMetadata() == nil {
		return errors.Errorf("expected header as first message: %v", header)
	}
	key := header.GetMetadata().GetMetadata().Name
	if header.GetMetadata().GetStagingSessionToken() == "" {
		return errors.New("missing staging session token")
	}
	token := header.GetMetadata().GetStagingSessionToken()

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
			return errors.Errorf("expected data: %v", msg)
		}
		if len(msg.GetData().GetData()) == 0 {
			return errors.Errorf("expected non-empty data: %v", msg)
		}
		chunks = append(chunks, msg.GetData().GetData())
	}

	// Updated staged artifact. This test implementation will allow updates to artifacts
	// that are already committed, but real implementations should manage artifacts in a
	// way that makes that impossible.

	m := s.getManifest(token, true)
	m.mu.Lock()
	m.m[key] = &data{chunks: chunks}
	m.mu.Unlock()

	return ps.SendAndClose(&jobpb.PutArtifactResponse{})
}

func (s *server) CommitManifest(ctx context.Context, req *jobpb.CommitManifestRequest) (*jobpb.CommitManifestResponse, error) {
	token := req.GetStagingSessionToken()
	if token == "" {
		return nil, errors.New("missing staging session token")
	}

	m := s.getManifest(token, true)
	m.mu.Lock()
	defer m.mu.Unlock()

	// Verify that all artifacts are properly staged. Fail if not.

	artifacts := req.GetManifest().GetArtifact()
	for _, md := range artifacts {
		if _, ok := m.m[md.Name]; !ok {
			return nil, errors.Errorf("artifact %v not staged", md.Name)
		}
	}

	// Update commit. Only one manifest can exist for each staging id.

	for _, md := range artifacts {
		m.m[md.Name].md = md
	}
	m.md = req.GetManifest()

	return &jobpb.CommitManifestResponse{RetrievalToken: token}, nil
}

func (s *server) GetManifest(ctx context.Context, req *jobpb.GetManifestRequest) (*jobpb.GetManifestResponse, error) {
	token := req.GetRetrievalToken()
	if token == "" {
		return nil, errors.New("missing retrieval token")
	}

	m := s.getManifest(token, false)
	if m == nil || m.md == nil {
		return nil, errors.Errorf("manifest for %v not found", token)
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	return &jobpb.GetManifestResponse{Manifest: m.md}, nil
}

func (s *server) GetArtifact(req *jobpb.LegacyGetArtifactRequest, stream jobpb.LegacyArtifactRetrievalService_GetArtifactServer) error {
	token := req.GetRetrievalToken()
	if token == "" {
		return errors.New("missing retrieval token")
	}

	m := s.getManifest(token, false)
	if m == nil || m.md == nil {
		return errors.Errorf("manifest for %v not found", token)
	}

	// Validate artifact and grab chunks so that we can stream them without
	// holding the lock.

	m.mu.Lock()
	elm, ok := m.m[req.GetName()]
	if !ok || elm.md == nil {
		m.mu.Unlock()
		return errors.Errorf("manifest for %v does not contain artifact %v", token, req.GetName())
	}
	chunks := elm.chunks
	m.mu.Unlock()

	// Send chunks exactly as we received them.

	for _, chunk := range chunks {
		if err := stream.Send(&jobpb.ArtifactChunk{Data: chunk}); err != nil {
			return err
		}
	}
	return nil
}

func (s *server) getManifest(token string, create bool) *manifest {
	s.mu.Lock()
	defer s.mu.Unlock()

	ret, ok := s.m[token]
	if !ok && create {
		ret = &manifest{m: make(map[string]*data)}
		s.m[token] = ret
	}
	return ret
}
