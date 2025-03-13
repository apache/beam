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

// Package gcsproxy contains artifact staging and retrieval servers backed by GCS.
package gcsproxy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"path"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/gcsx"
	"golang.org/x/net/context"
	"google.golang.org/protobuf/proto"
)

// StagingServer is a artifact staging server backed by Google Cloud Storage
// (GCS). It commits a single manifest and ignores the staging id.
type StagingServer struct {
	manifest     string
	bucket, root string
	blobs        map[string]staged // guarded by mu
	mu           sync.Mutex
}

type staged struct {
	object, hash string
}

// NewStagingServer creates a artifact staging server for the given manifest.
// It requires that the manifest is in GCS and will stage the supplied
// artifacts next to it.
func NewStagingServer(manifest string) (*StagingServer, error) {
	bucket, object, err := gcsx.ParseObject(manifest)
	if err != nil {
		return nil, errors.Wrap(err, "invalid manifest location")
	}
	root := path.Join(path.Dir(object), "blobs")

	return &StagingServer{
		manifest: object,
		bucket:   bucket,
		root:     root,
		blobs:    make(map[string]staged),
	}, nil
}

// CommitManifest commits the given artifact manifest to GCS.
func (s *StagingServer) CommitManifest(ctx context.Context, req *jobpb.CommitManifestRequest) (*jobpb.CommitManifestResponse, error) {
	manifest := req.GetManifest()

	s.mu.Lock()
	loc, err := matchLocations(manifest.GetArtifact(), s.blobs)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	s.mu.Unlock()

	data, err := proto.Marshal(&jobpb.ProxyManifest{Manifest: manifest, Location: loc})
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal proxy manifest")
	}

	cl, err := gcsx.NewClient(ctx, storage.ScopeReadWrite)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create GCS client")
	}
	if err := gcsx.WriteObject(ctx, cl, s.bucket, s.manifest, bytes.NewReader(data)); err != nil {
		return nil, errors.Wrap(err, "failed to write manifest")
	}

	// Commit returns the location of the manifest as the token, which can
	// then be used to configure the retrieval proxy. It is redundant right
	// now, but would be needed for a staging server that serves multiple
	// jobs. Such a server would also use the ID sent with each request.

	return &jobpb.CommitManifestResponse{RetrievalToken: gcsx.MakeObject(s.bucket, s.manifest)}, nil
}

// matchLocations ensures that all artifacts have been staged and have valid
// content. It is fine for staged artifacts to not appear in the manifest.
func matchLocations(artifacts []*jobpb.ArtifactMetadata, blobs map[string]staged) ([]*jobpb.ProxyManifest_Location, error) {
	var loc []*jobpb.ProxyManifest_Location
	for _, a := range artifacts {
		info, ok := blobs[a.Name]
		if !ok {
			return nil, errors.Errorf("artifact %v not staged", a.Name)
		}
		if a.Sha256 == "" {
			a.Sha256 = info.hash
		}
		if info.hash != a.Sha256 {
			return nil, errors.Errorf("staged artifact for %v has invalid SHA256: %v, want %v", a.Name, info.hash, a.Sha256)
		}

		loc = append(loc, &jobpb.ProxyManifest_Location{Name: a.Name, Uri: info.object})
	}
	return loc, nil
}

// PutArtifact stores the given artifact in GCS.
func (s *StagingServer) PutArtifact(ps jobpb.LegacyArtifactStagingService_PutArtifactServer) error {
	// Read header

	header, err := ps.Recv()
	if err != nil {
		return errors.Wrap(err, "failed to receive header")
	}
	md := header.GetMetadata().GetMetadata()
	if md == nil {
		return errors.Errorf("expected header as first message: %v", header)
	}
	object := path.Join(s.root, md.Name)

	// Stream content to GCS. We don't have to worry about partial
	// or abandoned writes, because object writes are atomic.

	ctx := ps.Context()
	cl, err := gcsx.NewClient(ctx, storage.ScopeReadWrite)
	if err != nil {
		return errors.Wrap(err, "failed to create GCS client")
	}

	r := &reader{sha256W: sha256.New(), stream: ps}
	if err := gcsx.WriteObject(ctx, cl, s.bucket, object, r); err != nil {
		return errors.Wrapf(err, "failed to stage artifact %v", md.Name)
	}
	hash := r.SHA256()
	if md.Sha256 != "" && md.Sha256 != hash {
		return errors.Errorf("invalid SHA256 for artifact %v: %v want %v", md.Name, hash, md.Sha256)
	}

	s.mu.Lock()
	s.blobs[md.Name] = staged{object: gcsx.MakeObject(s.bucket, object), hash: hash}
	s.mu.Unlock()

	return ps.SendAndClose(&jobpb.PutArtifactResponse{})
}

// reader is an adapter between the artifact stream and the GCS stream reader.
// It also computes the SHA256 of the content.
type reader struct {
	sha256W hash.Hash
	buf     []byte
	stream  jobpb.LegacyArtifactStagingService_PutArtifactServer
}

func (r *reader) Read(buf []byte) (int, error) {
	if len(r.buf) == 0 {
		// Buffer empty. Read from upload stream.

		msg, err := r.stream.Recv()
		if err != nil {
			return 0, err // EOF or real error
		}

		r.buf = msg.GetData().GetData()
		if len(r.buf) == 0 {
			return 0, errors.New("empty chunk")
		}
	}

	// Copy out bytes from non-empty buffer.

	n := len(r.buf)
	if n > len(buf) {
		n = len(buf)
	}
	for i := 0; i < n; i++ {
		buf[i] = r.buf[i]
	}
	if _, err := r.sha256W.Write(r.buf[:n]); err != nil {
		panic(err) // cannot fail
	}
	r.buf = r.buf[n:]
	return n, nil
}

func (r *reader) SHA256() string {
	return hex.EncodeToString(r.sha256W.Sum(nil))
}
