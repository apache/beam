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

package runnerlib

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/protox"
	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/testing/protocmp"
)

func initDummyServer(ctx context.Context, t *testing.T) (*grpc.ClientConn, *dummyArtifactServer) {
	t.Helper()
	const buffsize = 1024 * 1024
	l := bufconn.Listen(buffsize)

	server := grpc.NewServer()
	das := &dummyArtifactServer{
		t: t,
	}
	jobpb.RegisterArtifactStagingServiceServer(server, das)
	jobpb.RegisterLegacyArtifactStagingServiceServer(server, das)

	go func() {
		server.Serve(l)
	}()

	t.Cleanup(func() {
		server.Stop()
		l.Close()
	})

	clientConn, err := grpc.DialContext(ctx, "", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return l.DialContext(ctx)
	}), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatal("couldn't create bufconn grpc connection:", err)
	}
	return clientConn, das
}

func TestPortableArtifactStaging(t *testing.T) {
	ctx, cancelFn := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancelFn)

	cc, das := initDummyServer(ctx, t)

	das.wantToken = "token"
	das.reqFile = "stage.go"

	err := StageViaPortableAPI(ctx, cc, "reqFile", das.wantToken)
	if err != nil {
		t.Fatal(err)
	}
}

type dummyArtifactServer struct {
	jobpb.UnimplementedArtifactStagingServiceServer
	jobpb.UnimplementedLegacyArtifactStagingServiceServer

	t *testing.T

	wantToken, reqFile string
}

func (das *dummyArtifactServer) ReverseArtifactRetrievalService(stream jobpb.ArtifactStagingService_ReverseArtifactRetrievalServiceServer) error {
	msg, err := stream.Recv()
	if err == io.EOF {
		das.t.Error("ReverseArtifactRetrievalService.Recv: unexpected EOF")
		return nil
	}
	if err != nil {
		das.t.Errorf("ReverseArtifactRetrievalService.Recv: unexpected error %v", err)
		return err
	}
	if got, want := msg.GetStagingToken(), das.wantToken; got != want {
		das.t.Errorf("ReverseArtifactRetrievalService.Recv: unexpected token %v, want %v", got, want)
	}

	// Check artifact Resolve Requests.
	wantArts := []*pipepb.ArtifactInformation{
		{
			TypeUrn:     "dummy",
			TypePayload: []byte("dummy"),
			RoleUrn:     "dummy",
			RolePayload: []byte("dummy"),
		},
	}

	if err := stream.Send(&jobpb.ArtifactRequestWrapper{
		Request: &jobpb.ArtifactRequestWrapper_ResolveArtifact{
			ResolveArtifact: &jobpb.ResolveArtifactsRequest{
				Artifacts: wantArts,
			},
		},
	}); err != nil {
		das.t.Fatalf("unexpected err on artifact resolve request Send: %v", err)
	}

	msg, err = stream.Recv()
	if err != nil {
		das.t.Fatalf("unexpected err on artifact resolve response Recv: %v", err)
	}
	resolve := msg.GetResolveArtifactResponse()
	if resolve == nil {
		das.t.Fatalf("unexpected err on artifact resolve response Resp, got %T", msg.GetResponse())
	}
	gotArts := resolve.GetReplacements()
	if d := cmp.Diff(wantArts, gotArts, protocmp.Transform()); d != "" {
		das.t.Errorf("diff on artifact resolve response (-want, +got):\n%v", d)
	}

	typePl := protox.MustEncode(&pipepb.ArtifactFilePayload{
		Path: das.reqFile,
	})

	// Check file upload requests
	if err := stream.Send(&jobpb.ArtifactRequestWrapper{
		Request: &jobpb.ArtifactRequestWrapper_GetArtifact{
			GetArtifact: &jobpb.GetArtifactRequest{
				Artifact: &pipepb.ArtifactInformation{
					TypeUrn:     graphx.URNArtifactFileType,
					TypePayload: typePl,
				},
			},
		},
	}); err != nil {
		das.t.Fatalf("unexpected err on get artifact request Send: %v", err)
	}

	msg, err = stream.Recv()
	if err != nil {
		das.t.Fatalf("unexpected err on get artifact response Recv: %v", err)
	}

	if msg.GetIsLast() {
		das.t.Fatalf("unexpected IsLast on get artifact response Recv: %v", prototext.Format(msg))
	}
	msg, err = stream.Recv()
	if err != nil {
		das.t.Fatalf("unexpected err on artifact resolve response Recv: %v", err)
	}
	if !msg.GetIsLast() {
		das.t.Fatalf("want IsLast on get artifact response Recv: %v", prototext.Format(msg))
	}

	return nil
}
