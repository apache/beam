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
	"context"
	"io/ioutil"
	"os"
	"testing"

	pb "github.com/apache/beam/sdks/go/pkg/beam/model/jobmanagement_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/util/grpcx"
	"google.golang.org/grpc"
)

// TestStage verifies that local files can be staged correctly.
func TestStage(t *testing.T) {
	cc := startServer(t)
	defer cc.Close()
	client := pb.NewArtifactStagingServiceClient(cc)

	ctx := grpcx.WriteWorkerID(context.Background(), "idA")
	keys := []string{"foo", "bar", "baz/baz/baz"}

	src := makeTempDir(t)
	defer os.RemoveAll(src)
	md5s := makeTempFiles(t, src, keys, 300)

	var artifacts []*pb.ArtifactMetadata
	for _, key := range keys {
		a, err := Stage(ctx, client, key, makeFilename(src, key))
		if err != nil {
			t.Errorf("failed to stage %v: %v", key, err)
		}
		artifacts = append(artifacts, a)
	}
	if _, err := Commit(ctx, client, artifacts); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	validate(ctx, cc, t, keys, md5s)
}

// TestStageDir validates that local files can be staged concurrently.
func TestStageDir(t *testing.T) {
	cc := startServer(t)
	defer cc.Close()
	client := pb.NewArtifactStagingServiceClient(cc)

	ctx := grpcx.WriteWorkerID(context.Background(), "idB")
	keys := []string{"1", "2", "3", "4", "a/5", "a/6", "a/7", "a/8", "a/a/9", "a/a/10", "a/b/11", "a/b/12"}

	src := makeTempDir(t)
	defer os.RemoveAll(src)
	md5s := makeTempFiles(t, src, keys, 300)

	artifacts, err := StageDir(ctx, client, src)
	if err != nil {
		t.Errorf("failed to stage dir %v: %v", src, err)
	}
	if _, err := Commit(ctx, client, artifacts); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	validate(ctx, cc, t, keys, md5s)
}

func validate(ctx context.Context, cc *grpc.ClientConn, t *testing.T, keys, md5s []string) {
	rcl := pb.NewArtifactRetrievalServiceClient(cc)

	for i, key := range keys {
		stream, err := rcl.GetArtifact(ctx, &pb.GetArtifactRequest{Name: key})
		if err != nil {
			t.Fatalf("failed to get artifact for %v: %v", key, err)
		}

		hash, err := retrieveChunks(stream, ioutil.Discard)
		if err != nil {
			t.Fatalf("failed to get chunks for %v: %v", key, err)
		}
		if hash != md5s[i] {
			t.Errorf("incorrect MD5: %v, want %v", hash, md5s[i])
		}
	}
}
