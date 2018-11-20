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
	"crypto/sha256"
	"encoding/hex"
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
	st := "whatever"
	rt, artifacts := populate(ctx, cc, t, keys, 300, st)

	dst := makeTempDir(t)
	defer os.RemoveAll(dst)

	client := pb.NewArtifactRetrievalServiceClient(cc)
	for _, a := range artifacts {
		filename := makeFilename(dst, a.Name)
		if err := Retrieve(ctx, client, a, rt, dst); err != nil {
			t.Errorf("failed to retrieve %v: %v", a.Name, err)
			continue
		}
		verifySHA256(t, filename, a.Sha256)
	}
}

// TestMultiRetrieve tests that we can successfully retrieve fresh files
// concurrently.
func TestMultiRetrieve(t *testing.T) {
	cc := startServer(t)
	defer cc.Close()

	ctx := grpcx.WriteWorkerID(context.Background(), "idB")
	keys := []string{"1", "2", "3", "4", "a/5", "a/6", "a/7", "a/8", "a/a/9", "a/a/10", "a/b/11", "a/b/12"}
	st := "whatever"
	rt, artifacts := populate(ctx, cc, t, keys, 300, st)

	dst := makeTempDir(t)
	defer os.RemoveAll(dst)

	client := pb.NewArtifactRetrievalServiceClient(cc)
	if err := MultiRetrieve(ctx, client, 10, artifacts, rt, dst); err != nil {
		t.Errorf("failed to retrieve: %v", err)
	}

	for _, a := range artifacts {
		verifySHA256(t, makeFilename(dst, a.Name), a.Sha256)
	}
}

// populate stages a set of artifacts with the given keys, each with
// slightly different sizes and chucksizes.
func populate(ctx context.Context, cc *grpc.ClientConn, t *testing.T, keys []string, size int, st string) (string, []*pb.ArtifactMetadata) {
	scl := pb.NewArtifactStagingServiceClient(cc)

	var artifacts []*pb.ArtifactMetadata
	for i, key := range keys {
		a := stage(ctx, scl, t, key, size+7*i, 97+i, st)
		artifacts = append(artifacts, a)
	}
	token, err := Commit(ctx, scl, artifacts, st)
	if err != nil {
		t.Fatalf("failed to commit manifest: %v", err)
		return "", nil
	}
	return token, artifacts
}

// stage stages an artifact with the given key, size and chuck size. The content is
// always 'z's.
func stage(ctx context.Context, scl pb.ArtifactStagingServiceClient, t *testing.T, key string, size, chunkSize int, st string) *pb.ArtifactMetadata {
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = 'z'
	}

	sha256W := sha256.New()
	sha256W.Write(data)
	hash := hex.EncodeToString(sha256W.Sum(nil))
	md := makeArtifact(key, hash)
	pmd := &pb.PutArtifactMetadata{
		Metadata:            md,
		StagingSessionToken: st,
	}

	stream, err := scl.PutArtifact(ctx)
	if err != nil {
		t.Fatalf("put failed: %v", err)
	}
	header := &pb.PutArtifactRequest{
		Content: &pb.PutArtifactRequest_Metadata{
			Metadata: pmd,
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

func verifySHA256(t *testing.T, filename, hash string) {
	actual, err := computeSHA256(filename)
	if err != nil {
		t.Errorf("failed to compute hash for %v: %v", filename, err)
		return
	}
	if actual != hash {
		t.Errorf("file %v has bad SHA256: %v, want %v", filename, actual, hash)
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
	var sha256s []string
	for i, key := range keys {
		hash := makeTempFile(t, makeFilename(dir, key), size+i)
		sha256s = append(sha256s, hash)
	}
	return sha256s
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

	sha256W := sha256.New()
	sha256W.Write(data)
	return hex.EncodeToString(sha256W.Sum(nil))
}

func makeArtifact(key, hash string) *pb.ArtifactMetadata {
	return &pb.ArtifactMetadata{
		Name:        key,
		Sha256:      hash,
		Permissions: 0644,
	}
}

func makeFilename(dir, key string) string {
	return filepath.Join(dir, filepath.FromSlash(key))
}
