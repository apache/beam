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
	"crypto/sha256"
	"encoding/hex"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	jobpb "github.com/apache/beam/sdks/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/util/errorx"
	"github.com/apache/beam/sdks/go/pkg/beam/util/grpcx"
	"github.com/golang/protobuf/proto"
)

// TODO(lostluck): 2018/05/28 Extract these from their enum descriptors in the pipeline_v1 proto
const (
	URNStagingTo      = "beam:artifact:role:staging_to:v1"
	NoArtifactsStaged = "__no_artifacts_staged__"
)

// Materialize is a convenience helper for ensuring that all artifacts are
// present and uncorrupted. It interprets each artifact name as a relative
// path under the dest directory. It does not retrieve valid artifacts already
// present.
// TODO(BEAM-9577): Return a mapping of filename to dependency, rather than []*jobpb.ArtifactMetadata.
// TODO(BEAM-9577): Leverage richness of roles rather than magic names to understand artifacts.
func Materialize(ctx context.Context, endpoint string, dependencies []*pipepb.ArtifactInformation, rt string, dest string) ([]*jobpb.ArtifactMetadata, error) {
	if len(dependencies) > 0 {
		return newMaterialize(ctx, endpoint, dependencies, dest)
	} else if rt == "" || rt == NoArtifactsStaged {
		return []*jobpb.ArtifactMetadata{}, nil
	} else {
		return legacyMaterialize(ctx, endpoint, rt, dest)
	}
}

func newMaterialize(ctx context.Context, endpoint string, dependencies []*pipepb.ArtifactInformation, dest string) ([]*jobpb.ArtifactMetadata, error) {
	cc, err := grpcx.Dial(ctx, endpoint, 2*time.Minute)
	if err != nil {
		return nil, err
	}
	defer cc.Close()

	return newMaterializeWithClient(ctx, jobpb.NewArtifactRetrievalServiceClient(cc), dependencies, dest)
}

func newMaterializeWithClient(ctx context.Context, client jobpb.ArtifactRetrievalServiceClient, dependencies []*pipepb.ArtifactInformation, dest string) ([]*jobpb.ArtifactMetadata, error) {
	resolution, err := client.ResolveArtifacts(ctx, &jobpb.ResolveArtifactsRequest{Artifacts: dependencies})
	if err != nil {
		return nil, err
	}

	var md []*jobpb.ArtifactMetadata
	var list []retrievable
	for _, dep := range resolution.Replacements {
		path, err := extractStagingToPath(dep)
		if err != nil {
			return nil, err
		}
		md = append(md, &jobpb.ArtifactMetadata{
			Name: path,
		})

		list = append(list, &artifact{
			client: client,
			dep:    dep,
		})
	}

	return md, MultiRetrieve(ctx, 10, list, dest)
}

func extractStagingToPath(artifact *pipepb.ArtifactInformation) (string, error) {
	if artifact.RoleUrn != URNStagingTo {
		return "", errors.Errorf("Unsupported artifact role %s", artifact.RoleUrn)
	}
	role := pipepb.ArtifactStagingToRolePayload{}
	if err := proto.Unmarshal(artifact.RolePayload, &role); err != nil {
		return "", err
	}
	return role.StagedName, nil
}

type artifact struct {
	client jobpb.ArtifactRetrievalServiceClient
	dep    *pipepb.ArtifactInformation
}

func (a artifact) retrieve(ctx context.Context, dest string) error {
	path, err := extractStagingToPath(a.dep)
	if err != nil {
		return err
	}

	filename := filepath.Join(dest, filepath.FromSlash(path))

	_, err = os.Stat(filename)
	if err == nil {
		if err = os.Remove(filename); err != nil {
			return errors.Errorf("failed to delete: %v (remove: %v)", filename, err)
		}
	} else if !os.IsNotExist(err) {
		return errors.Wrapf(err, "failed to stat %v", filename)
	}

	if err := os.MkdirAll(filepath.Dir(filename), os.ModePerm); err != nil {
		return err
	}

	stream, err := a.client.GetArtifact(ctx, &jobpb.GetArtifactRequest{Artifact: a.dep})
	if err != nil {
		return err
	}

	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(fd)

	err = writeChunks(stream, w)
	if err != nil {
		fd.Close() // drop any buffered content
		return errors.Wrapf(err, "failed to retrieve chunk for %v", filename)
	}
	if err := w.Flush(); err != nil {
		fd.Close()
		return errors.Wrapf(err, "failed to flush chunks for %v", filename)
	}
	return fd.Close()
}

func writeChunks(stream jobpb.ArtifactRetrievalService_GetArtifactClient, w io.Writer) error {
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if _, err := w.Write(chunk.Data); err != nil {
			return errors.Wrapf(err, "chunk write failed")
		}
	}
	return nil
}

func legacyMaterialize(ctx context.Context, endpoint string, rt string, dest string) ([]*jobpb.ArtifactMetadata, error) {
	cc, err := grpcx.Dial(ctx, endpoint, 2*time.Minute)
	if err != nil {
		return nil, err
	}
	defer cc.Close()

	client := jobpb.NewLegacyArtifactRetrievalServiceClient(cc)

	m, err := client.GetManifest(ctx, &jobpb.GetManifestRequest{RetrievalToken: rt})
	if err != nil {
		return nil, errors.Wrap(err, "failed to get manifest")
	}
	mds := m.GetManifest().GetArtifact()

	var list []retrievable
	for _, md := range mds {
		list = append(list, &legacyArtifact{
			client: client,
			rt:     rt,
			md:     md,
		})
	}

	return mds, MultiRetrieve(ctx, 10, list, dest)
}

// MultiRetrieve retrieves multiple artifacts concurrently, using at most 'cpus'
// goroutines. It retries each artifact a few times. Convenience wrapper.
func MultiRetrieve(ctx context.Context, cpus int, list []retrievable, dest string) error {
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
					err := a.retrieve(ctx, dest)
					if err == nil || permErr.Error() != nil {
						break // done or give up
					}
					failures = append(failures, err.Error())
					if len(failures) > attempts {
						permErr.TrySetError(errors.Errorf("failed to retrieve %v in %v attempts: %v", dest, attempts, strings.Join(failures, "; ")))
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

type retrievable interface {
	retrieve(ctx context.Context, dest string) error
}

// LegacyMultiRetrieve is exported for testing.
func LegacyMultiRetrieve(ctx context.Context, client jobpb.LegacyArtifactRetrievalServiceClient, cpus int, list []*jobpb.ArtifactMetadata, rt string, dest string) error {
	var rlist []retrievable
	for _, md := range list {
		rlist = append(rlist, &legacyArtifact{
			client: client,
			rt:     rt,
			md:     md,
		})
	}
	return MultiRetrieve(ctx, cpus, rlist, dest)
}

type legacyArtifact struct {
	client jobpb.LegacyArtifactRetrievalServiceClient
	rt     string
	md     *jobpb.ArtifactMetadata
}

func (a legacyArtifact) retrieve(ctx context.Context, dest string) error {
	return Retrieve(ctx, a.client, a.md, a.rt, dest)
}

// Retrieve checks whether the given artifact is already successfully
// retrieved. If not, it retrieves into the dest directory. It overwrites any
// previous retrieval attempt and may leave a corrupt/partial local file on
// failure.
func Retrieve(ctx context.Context, client jobpb.LegacyArtifactRetrievalServiceClient, a *jobpb.ArtifactMetadata, rt string, dest string) error {
	filename := filepath.Join(dest, filepath.FromSlash(a.Name))

	_, err := os.Stat(filename)
	if err != nil && !os.IsNotExist(err) {
		return errors.Wrapf(err, "failed to stat %v", filename)
	}
	if err == nil {
		// File already exists. Validate or delete.

		hash, err := computeSHA256(filename)
		if err == nil && a.Sha256 == hash {
			// NOTE(herohde) 10/5/2017: We ignore permissions here, because
			// they may differ from the requested permissions due to umask
			// settings on unix systems (which we in turn want to respect).
			// We have no good way to know what to expect and thus assume
			// any permissions are fine.
			return nil
		}

		if err2 := os.Remove(filename); err2 != nil {
			return errors.Errorf("failed to both validate %v and delete: %v (remove: %v)", filename, err, err2)
		} // else: successfully deleted bad file.
	} // else: file does not exist.

	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return err
	}
	return retrieve(ctx, client, a, rt, filename)
}

// retrieve retrieves the given artifact and stores it as the given filename.
// It validates that the given SHA256 matches the content and fails otherwise.
// It expects the file to not exist, but does not clean up on failure and
// may leave a corrupt file.
func retrieve(ctx context.Context, client jobpb.LegacyArtifactRetrievalServiceClient, a *jobpb.ArtifactMetadata, rt string, filename string) error {
	stream, err := client.GetArtifact(ctx, &jobpb.LegacyGetArtifactRequest{Name: a.Name, RetrievalToken: rt})
	if err != nil {
		return err
	}

	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(a.Permissions))
	if err != nil {
		return err
	}
	w := bufio.NewWriter(fd)

	sha256Hash, err := retrieveChunks(stream, w)
	if err != nil {
		fd.Close() // drop any buffered content
		return errors.Wrapf(err, "failed to retrieve chunk for %v", filename)
	}
	if err := w.Flush(); err != nil {
		fd.Close()
		return errors.Wrapf(err, "failed to flush chunks for %v", filename)
	}
	if err := fd.Close(); err != nil {
		return err
	}

	// Artifact Sha256 hash is an optional field in metadata so we should only validate when its present.
	if a.Sha256 != "" && sha256Hash != a.Sha256 {
		return errors.Errorf("bad SHA256 for %v: %v, want %v", filename, sha256Hash, a.Sha256)
	}
	return nil
}

func retrieveChunks(stream jobpb.LegacyArtifactRetrievalService_GetArtifactClient, w io.Writer) (string, error) {
	sha256W := sha256.New()
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}

		if _, err := sha256W.Write(chunk.Data); err != nil {
			panic(err) // cannot fail
		}
		if _, err := w.Write(chunk.Data); err != nil {
			return "", errors.Wrapf(err, "chunk write failed")
		}
	}
	return hex.EncodeToString(sha256W.Sum(nil)), nil
}

func computeSHA256(filename string) (string, error) {
	fd, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer fd.Close()

	sha256W := sha256.New()
	data := make([]byte, 1<<20)
	for {
		n, err := fd.Read(data)
		if n > 0 {
			if _, err := sha256W.Write(data[:n]); err != nil {
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
	return hex.EncodeToString(sha256W.Sum(nil)), nil
}

func slice2queue(list []retrievable) chan retrievable {
	q := make(chan retrievable, len(list))
	for _, elm := range list {
		q <- elm
	}
	close(q)
	return q
}

func queue2slice(q chan *jobpb.ArtifactMetadata) []*jobpb.ArtifactMetadata {
	var ret []*jobpb.ArtifactMetadata
	for elm := range q {
		ret = append(ret, elm)
	}
	return ret
}
