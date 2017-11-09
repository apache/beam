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
	"crypto/md5"
	"encoding/base64"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	pb "github.com/apache/beam/sdks/go/pkg/beam/model/jobmanagement_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/util/grpcx"
	"google.golang.org/grpc"
)

// TestRetrieve tests that we can successfully retrieve fresh files.
func TestRetrieve(t *testing.T) {
	cc := startServer(t)
	defer cc.Close()

	ctx := grpcx.WriteWorkerID(context.Background(), "idA")
	keys := []string{"foo", "bar", "baz/baz/baz"}
	artifacts := populate(ctx, cc, t, keys, 300)

	dst := makeTempDir(t)
	defer os.RemoveAll(dst)

	client := pb.NewArtifactRetrievalServiceClient(cc)
	for _, a := range artifacts {
		filename := makeFilename(dst, a.Name)
		if err := Retrieve(ctx, client, a, dst); err != nil {
			t.Errorf("failed to retrieve %v: %v", a.Name, err)
			continue
		}
		verifyMD5(t, filename, a.Md5)
	}
}

// TestMultiRetrieve tests that we can successfully retrieve fresh files
// concurrently.
func TestMultiRetrieve(t *testing.T) {
	cc := startServer(t)
	defer cc.Close()

	ctx := grpcx.WriteWorkerID(context.Background(), "idB")
	keys := []string{"1", "2", "3", "4", "a/5", "a/6", "a/7", "a/8", "a/a/9", "a/a/10", "a/b/11", "a/b/12"}
	artifacts := populate(ctx, cc, t, keys, 300)

	dst := makeTempDir(t)
	defer os.RemoveAll(dst)

	client := pb.NewArtifactRetrievalServiceClient(cc)
	if err := MultiRetrieve(ctx, client, 10, artifacts, dst); err != nil {
		t.Errorf("failed to retrieve: %v", err)
	}

	for _, a := range artifacts {
		verifyMD5(t, makeFilename(dst, a.Name), a.Md5)
	}
}

// TestDirtyRetrieve tests that we can successfully retrieve files in a
// dirty setup with correct and incorrect pre-existing files.
func TestDirtyRetrieve(t *testing.T) {
	cc := startServer(t)
	defer cc.Close()

	ctx := grpcx.WriteWorkerID(context.Background(), "idC")
	scl := pb.NewArtifactStagingServiceClient(cc)

	list := []*pb.ArtifactMetadata{
		stage(ctx, scl, t, "good", 500, 100),
		stage(ctx, scl, t, "bad", 500, 100),
	}
	if _, err := Commit(ctx, scl, list); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Kill good file in server by re-staging conflicting content. That ensures
	// we don't retrieve it.
	stage(ctx, scl, t, "good", 100, 100)

	dst := makeTempDir(t)
	defer os.RemoveAll(dst)

	good := filepath.Join(dst, "good")
	bad := filepath.Join(dst, "bad")

	makeTempFile(t, good, 500) // correct content. Do nothing.
	makeTempFile(t, bad, 367)  // invalid content. Delete and retrieve.

	rcl := pb.NewArtifactRetrievalServiceClient(cc)
	if err := MultiRetrieve(ctx, rcl, 2, list, dst); err != nil {
		t.Fatalf("failed to get retrieve: %v", err)
	}

	verifyMD5(t, good, list[0].Md5)
	verifyMD5(t, bad, list[1].Md5)
}

// populate stages a set of artifacts with the given keys, each with
// slightly different sizes and chucksizes.
func populate(ctx context.Context, cc *grpc.ClientConn, t *testing.T, keys []string, size int) []*pb.ArtifactMetadata {
	scl := pb.NewArtifactStagingServiceClient(cc)

	var artifacts []*pb.ArtifactMetadata
	for i, key := range keys {
		a := stage(ctx, scl, t, key, size+7*i, 97+i)
		artifacts = append(artifacts, a)
	}
	if _, err := Commit(ctx, scl, artifacts); err != nil {
		t.Fatalf("failed to commit manifest: %v", err)
	}
	return artifacts
}

// stage stages an artifact with the given key, size and chuck size. The content is
// always 'z's.
func stage(ctx context.Context, scl pb.ArtifactStagingServiceClient, t *testing.T, key string, size, chunkSize int) *pb.ArtifactMetadata {
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = 'z'
	}

	md5W := md5.New()
	md5W.Write(data)
	hash := base64.StdEncoding.EncodeToString(md5W.Sum(nil))
	md := makeArtifact(key, hash)

	stream, err := scl.PutArtifact(ctx)
	if err != nil {
		t.Fatalf("put failed: %v", err)
	}
	header := &pb.PutArtifactRequest{
		Content: &pb.PutArtifactRequest_Metadata{
			Metadata: md,
		},
	}
	if err := stream.Send(header); err != nil {
		t.Fatalf("send header failed: %v", err)
	}

	for i := 0; i < size; i += chunkSize {
		end := i + chunkSize
		if size < end {
			end = size
		}

		chunk := &pb.PutArtifactRequest{
			Content: &pb.PutArtifactRequest_Data{
				Data: &pb.ArtifactChunk{
					Data: data[i:end],
				},
			},
		}
		if err := stream.Send(chunk); err != nil {
			t.Fatalf("send chunk[%v:%v] failed: %v", i, end, err)
		}
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	return md
}

func verifyMD5(t *testing.T, filename, hash string) {
	actual, err := computeMD5(filename)
	if err != nil {
		t.Errorf("failed to compute hash for %v: %v", filename, err)
		return
	}
	if actual != hash {
		t.Errorf("file %v has bad MD5: %v, want %v", filename, actual, hash)
	}
}

func makeTempDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "artifact_test_")
	if err != nil {
		t.Errorf("Test failure: cannot create temporary directory: %+v", err)
	}
	return dir
}

func makeTempFiles(t *testing.T, dir string, keys []string, size int) []string {
	var md5s []string
	for i, key := range keys {
		hash := makeTempFile(t, makeFilename(dir, key), size+i)
		md5s = append(md5s, hash)
	}
	return md5s
}

func makeTempFile(t *testing.T, filename string, size int) string {
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = 'z'
	}

	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		t.Fatalf("cannot create directory for %s: %v", filename, err)
	}
	if err := ioutil.WriteFile(filename, data, 0644); err != nil {
		t.Fatalf("cannot create file %s: %v", filename, err)
	}

	md5W := md5.New()
	md5W.Write(data)
	return base64.StdEncoding.EncodeToString(md5W.Sum(nil))
}

func makeArtifact(key, hash string) *pb.ArtifactMetadata {
	return &pb.ArtifactMetadata{
		Name:        key,
		Md5:         hash,
		Permissions: 0644,
	}
}

func makeFilename(dir, key string) string {
	return filepath.Join(dir, filepath.FromSlash(key))
}
