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

// Package artifact contains utilities for staging and retrieving artifacts.
package artifact

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	pb "github.com/apache/beam/sdks/go/pkg/beam/model/jobmanagement_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/util/errorx"
	"github.com/apache/beam/sdks/go/pkg/beam/util/grpcx"
)

// Materialize is a convenience helper for ensuring that all artifacts are
// present and uncorrupted. It interprets each artifact name as a relative
// path under the dest directory. It does not retrieve valid artifacts already
// present.
func Materialize(ctx context.Context, endpoint string, dest string) ([]*pb.ArtifactMetadata, error) {
	cc, err := grpcx.Dial(ctx, endpoint, 2*time.Minute)
	if err != nil {
		return nil, err
	}
	defer cc.Close()

	client := pb.NewArtifactRetrievalServiceClient(cc)

	m, err := client.GetManifest(ctx, &pb.GetManifestRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to get manifest: %v", err)
	}
	md := m.GetManifest().GetArtifact()
	return md, MultiRetrieve(ctx, client, 10, md, dest)
}

// MultiRetrieve retrieves multiple artifacts concurrently, using at most 'cpus'
// goroutines. It retries each artifact a few times. Convenience wrapper.
func MultiRetrieve(ctx context.Context, client pb.ArtifactRetrievalServiceClient, cpus int, list []*pb.ArtifactMetadata, dest string) error {
	if len(list) == 0 {
		return nil
	}
	if cpus < 1 {
		cpus = 1
	}
	if len(list) < cpus {
		cpus = len(list)
	}

	q := slice2queue(list)
	var permErr errorx.GuardedError

	var wg sync.WaitGroup
	for i := 0; i < cpus; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for a := range q {
				if permErr.Error() != nil {
					continue
				}

				const attempts = 3

				var failures []string
				for {
					err := Retrieve(ctx, client, a, dest)
					if err == nil || permErr.Error() != nil {
						break // done or give up
					}
					failures = append(failures, err.Error())
					if len(failures) > attempts {
						permErr.TrySetError(fmt.Errorf("failed to retrieve %v in %v attempts: %v", a.Name, attempts, strings.Join(failures, "; ")))
						break // give up
					}
					time.Sleep(time.Duration(rand.Intn(5)+1) * time.Second)
				}
			}
		}()
	}
	wg.Wait()

	return permErr.Error()
}

// Retrieve checks whether the given artifact is already successfully
// retrieved. If not, it retrieves into the dest directory. It overwrites any
// previous retrieval attempt and may leave a corrupt/partial local file on
// failure.
func Retrieve(ctx context.Context, client pb.ArtifactRetrievalServiceClient, a *pb.ArtifactMetadata, dest string) error {
	filename := filepath.Join(dest, filepath.FromSlash(a.Name))

	_, err := os.Stat(filename)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to stat %v: %v", filename, err)
	}
	if err == nil {
		// File already exists. Validate or delete.

		hash, err := computeMD5(filename)
		if err == nil && a.Md5 == hash {
			// NOTE(herohde) 10/5/2017: We ignore permissions here, because
			// they may differ from the requested permissions due to umask
			// settings on unix systems (which we in turn want to respect).
			// We have no good way to know what to expect and thus assume
			// any permissions are fine.
			return nil
		}

		if err2 := os.Remove(filename); err2 != nil {
			return fmt.Errorf("failed to both validate %v and delete: %v (remove: %v)", filename, err, err2)
		} // else: successfully deleted bad file.
	} // else: file does not exist.

	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return err
	}
	return retrieve(ctx, client, a, filename)
}

// retrieve retrieves the given artifact and stores it as the given filename.
// It validates that the given MD5 matches the content and fails otherwise.
// It expects the file to not exist, but does not clean up on failure and
// may leave a corrupt file.
func retrieve(ctx context.Context, client pb.ArtifactRetrievalServiceClient, a *pb.ArtifactMetadata, filename string) error {
	stream, err := client.GetArtifact(ctx, &pb.GetArtifactRequest{Name: a.Name})
	if err != nil {
		return err
	}

	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(a.Permissions))
	if err != nil {
		return err
	}
	w := bufio.NewWriter(fd)

	hash, err := retrieveChunks(stream, w)
	if err != nil {
		fd.Close() // drop any buffered content
		return fmt.Errorf("failed to retrieve chunk for %v: %v", filename, err)
	}
	if err := w.Flush(); err != nil {
		fd.Close()
		return fmt.Errorf("failed to flush chunks for %v: %v", filename, err)
	}
	if err := fd.Close(); err != nil {
		return err
	}

	if hash != a.Md5 {
		return fmt.Errorf("bad MD5 for %v: %v, want %v", filename, hash, a.Md5)
	}
	return nil
}

func retrieveChunks(stream pb.ArtifactRetrievalService_GetArtifactClient, w io.Writer) (string, error) {
	md5W := md5.New()
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}

		if _, err := md5W.Write(chunk.Data); err != nil {
			panic(err) // cannot fail
		}
		if _, err := w.Write(chunk.Data); err != nil {
			return "", fmt.Errorf("chunk write failed: %v", err)
		}
	}
	return base64.StdEncoding.EncodeToString(md5W.Sum(nil)), nil
}

func computeMD5(filename string) (string, error) {
	fd, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer fd.Close()

	md5W := md5.New()
	data := make([]byte, 1<<20)
	for {
		n, err := fd.Read(data)
		if n > 0 {
			if _, err := md5W.Write(data[:n]); err != nil {
				panic(err) // cannot fail
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
	}
	return base64.StdEncoding.EncodeToString(md5W.Sum(nil)), nil
}

func slice2queue(list []*pb.ArtifactMetadata) chan *pb.ArtifactMetadata {
	q := make(chan *pb.ArtifactMetadata, len(list))
	for _, elm := range list {
		q <- elm
	}
	close(q)
	return q
}

func queue2slice(q chan *pb.ArtifactMetadata) []*pb.ArtifactMetadata {
	var ret []*pb.ArtifactMetadata
	for elm := range q {
		ret = append(ret, elm)
	}
	return ret
}
