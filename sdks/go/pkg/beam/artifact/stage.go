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
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	pb "github.com/apache/beam/sdks/go/pkg/beam/model/jobmanagement_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/util/errorx"
)

// Commit commits a manifest with the given staged artifacts. It returns the
// staging token, if successful.
func Commit(ctx context.Context, client pb.ArtifactStagingServiceClient, artifacts []*pb.ArtifactMetadata, st string) (string, error) {
	req := &pb.CommitManifestRequest{
		Manifest: &pb.Manifest{
			Artifact: artifacts,
		},
		StagingSessionToken: st,
	}
	resp, err := client.CommitManifest(ctx, req)
	if err != nil {
		return "", err
	}
	return resp.GetRetrievalToken(), nil
}

// StageDir stages a local directory with relative path keys. Convenience wrapper.
func StageDir(ctx context.Context, client pb.ArtifactStagingServiceClient, src string, st string) ([]*pb.ArtifactMetadata, error) {
	list, err := scan(src)
	if err != nil || len(list) == 0 {
		return nil, err
	}
	return MultiStage(ctx, client, 10, list, st)
}

// MultiStage stages a set of local files with the given keys. It returns
// the full artifact metadate.  It retries each artifact a few times.
// Convenience wrapper.
func MultiStage(ctx context.Context, client pb.ArtifactStagingServiceClient, cpus int, list []KeyedFile, st string) ([]*pb.ArtifactMetadata, error) {
	if cpus < 1 {
		cpus = 1
	}
	if len(list) < cpus {
		cpus = len(list)
	}

	q := make(chan KeyedFile, len(list))
	for _, f := range list {
		q <- f
	}
	close(q)
	var permErr errorx.GuardedError

	ret := make(chan *pb.ArtifactMetadata, len(list))

	var wg sync.WaitGroup
	for i := 0; i < cpus; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f := range q {
				if permErr.Error() != nil {
					continue
				}

				const attempts = 3

				var failures []string
				for {
					a, err := Stage(ctx, client, f.Key, f.Filename, st)
					if err == nil {
						ret <- a
						break
					}
					if permErr.Error() != nil {
						break // give up
					}
					failures = append(failures, err.Error())
					if len(failures) > attempts {
						permErr.TrySetError(fmt.Errorf("failed to stage %v in %v attempts: %v", f.Filename, attempts, strings.Join(failures, "; ")))
						break // give up
					}
					time.Sleep(time.Duration(rand.Intn(5)+1) * time.Second)
				}
			}
		}()
	}
	wg.Wait()
	close(ret)

	return queue2slice(ret), permErr.Error()
}

// Stage stages a local file as an artifact with the given key. It computes
// the SHA256 and returns the full artifact metadata.
func Stage(ctx context.Context, client pb.ArtifactStagingServiceClient, key, filename, st string) (*pb.ArtifactMetadata, error) {
	stat, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}
	hash, err := computeSHA256(filename)
	if err != nil {
		return nil, err
	}
	md := &pb.ArtifactMetadata{
		Name:        key,
		Permissions: uint32(stat.Mode()),
		Sha256:      hash,
	}
	pmd := &pb.PutArtifactMetadata{
		Metadata:            md,
		StagingSessionToken: st,
	}

	fd, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	stream, err := client.PutArtifact(ctx)
	if err != nil {
		return nil, err
	}

	header := &pb.PutArtifactRequest{
		Content: &pb.PutArtifactRequest_Metadata{
			Metadata: pmd,
		},
	}
	if err := stream.Send(header); err != nil {
		stream.CloseAndRecv() // ignore error
		return nil, fmt.Errorf("failed to send header for %v: %v", filename, err)
	}
	stagedHash, err := stageChunks(stream, fd)
	if err != nil {
		stream.CloseAndRecv() // ignore error
		return nil, fmt.Errorf("failed to send chunks for %v: %v", filename, err)
	}
	if _, err := stream.CloseAndRecv(); err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to close stream for %v: %v", filename, err)
	}
	if hash != stagedHash {
		return nil, fmt.Errorf("unexpected SHA256 for sent chunks for %v: %v, want %v", filename, stagedHash, hash)
	}
	return md, nil
}

func stageChunks(stream pb.ArtifactStagingService_PutArtifactClient, r io.Reader) (string, error) {
	sha256W := sha256.New()
	data := make([]byte, 1<<20)
	for {
		n, err := r.Read(data)
		if n > 0 {
			if _, err := sha256W.Write(data[:n]); err != nil {
				panic(err) // cannot fail
			}

			chunk := &pb.PutArtifactRequest{
				Content: &pb.PutArtifactRequest_Data{
					Data: &pb.ArtifactChunk{
						Data: data[:n],
					},
				},
			}
			if err := stream.Send(chunk); err != nil {
				return "", fmt.Errorf("chunk send failed: %v", err)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(sha256W.Sum(nil)), nil
}

// KeyedFile is a key and filename pair.
type KeyedFile struct {
	Key, Filename string
}

func scan(dir string) ([]KeyedFile, error) {
	var ret []KeyedFile
	if err := walk(dir, "", &ret); err != nil {
		return nil, fmt.Errorf("failed to scan %v for artifacts to stage: %v", dir, err)
	}
	return ret, nil
}

func walk(dir, key string, accum *[]KeyedFile) error {
	list, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, elm := range list {
		k := makeKey(key, elm.Name())
		f := filepath.Join(dir, elm.Name())

		if elm.IsDir() {
			walk(f, k, accum)
			continue
		}
		*accum = append(*accum, KeyedFile{k, f})
	}
	return nil
}

func makeKey(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return path.Join(prefix, name)
}
