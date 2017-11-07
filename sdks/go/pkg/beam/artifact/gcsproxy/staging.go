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
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"hash"
	"path"
	"sync"

	pb "github.com/apache/beam/sdks/go/pkg/beam/model/jobmanagement_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/util/gcsx"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"google.golang.org/api/storage/v1"
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
		return nil, fmt.Errorf("invalid manifest location: %v", err)
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
func (s *StagingServer) CommitManifest(ctx context.Context, req *pb.CommitManifestRequest) (*pb.CommitManifestResponse, error) {
	manifest := req.GetManifest()

	s.mu.Lock()
	loc, err := matchLocations(manifest.GetArtifact(), s.blobs)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	s.mu.Unlock()

	data, err := proto.Marshal(&pb.ProxyManifest{Manifest: manifest, Location: loc})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal proxy manifest: %v", err)
	}

	cl, err := gcsx.NewClient(ctx, storage.DevstorageReadWriteScope)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %v", err)
	}
	if err := gcsx.WriteObject(cl, s.bucket, s.manifest, bytes.NewReader(data)); err != nil {
		return nil, fmt.Errorf("failed to write manifest: %v", err)
	}

	// Commit returns the location of the manifest as the token, which can
	// then be used to configure the retrieval proxy. It is redundant right
	// now, but would be needed for a staging server that serves multiple
	// jobs. Such a server would also use the ID sent with each request.

	return &pb.CommitManifestResponse{StagingToken: gcsx.MakeObject(s.bucket, s.manifest)}, nil
}

// matchLocations ensures that all artifacts have been staged and have valid
// content. It is fine for staged artifacts to not appear in the manifest.
func matchLocations(artifacts []*pb.ArtifactMetadata, blobs map[string]staged) ([]*pb.ProxyManifest_Location, error) {
	var loc []*pb.ProxyManifest_Location
	for _, a := range artifacts {
		info, ok := blobs[a.Name]
		if !ok {
			return nil, fmt.Errorf("artifact %v not staged", a.Name)
		}
		if a.Md5 == "" {
			a.Md5 = info.hash
		}
		if info.hash != a.Md5 {
			return nil, fmt.Errorf("staged artifact for %v has invalid MD5: %v, want %v", a.Name, info.hash, a.Md5)
		}

		loc = append(loc, &pb.ProxyManifest_Location{Name: a.Name, Uri: info.object})
	}
	return loc, nil
}

// PutArtifact stores the given artifact in GCS.
func (s *StagingServer) PutArtifact(ps pb.ArtifactStagingService_PutArtifactServer) error {
	// Read header

	header, err := ps.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive header: %v", err)
	}
	md := header.GetMetadata()
	if md == nil {
		return fmt.Errorf("expected header as first message: %v", header)
	}
	object := path.Join(s.root, md.Name)

	// Stream content to GCS. We don't have to worry about partial
	// or abandoned writes, because object writes are atomic.

	cl, err := gcsx.NewClient(ps.Context(), storage.DevstorageReadWriteScope)
	if err != nil {
		return fmt.Errorf("failed to create GCS client: %v", err)
	}

	r := &reader{md5W: md5.New(), stream: ps}
	if err := gcsx.WriteObject(cl, s.bucket, object, r); err != nil {
		return fmt.Errorf("failed to stage artifact %v: %v", md.Name, err)
	}
	hash := r.MD5()
	if md.Md5 != "" && md.Md5 != hash {
		return fmt.Errorf("invalid MD5 for artifact %v: %v want %v", md.Name, hash, md.Md5)
	}

	s.mu.Lock()
	s.blobs[md.Name] = staged{object: gcsx.MakeObject(s.bucket, object), hash: hash}
	s.mu.Unlock()

	return ps.SendAndClose(&pb.PutArtifactResponse{})
}

// reader is an adapter between the artifact stream and the GCS stream reader.
// It also computes the MD5 of the content.
type reader struct {
	md5W   hash.Hash
	buf    []byte
	stream pb.ArtifactStagingService_PutArtifactServer
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
	if _, err := r.md5W.Write(r.buf[:n]); err != nil {
		panic(err) // cannot fail
	}
	r.buf = r.buf[n:]
	return n, nil
}

func (r *reader) MD5() string {
	return base64.StdEncoding.EncodeToString(r.md5W.Sum(nil))
}
