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

package gcsproxy

import (
	"io"

	"cloud.google.com/go/storage"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/gcsx"
	"golang.org/x/net/context"
	"google.golang.org/protobuf/proto"
)

// RetrievalServer is a artifact retrieval server backed by Google
// Cloud Storage (GCS). It serves a single manifest and ignores
// the worker id. The server performs no caching or pre-fetching.
type RetrievalServer struct {
	md    *jobpb.Manifest
	blobs map[string]string
}

// ReadProxyManifest reads and parses the proxy manifest from GCS.
func ReadProxyManifest(ctx context.Context, object string) (*jobpb.ProxyManifest, error) {
	bucket, obj, err := gcsx.ParseObject(object)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid manifest object %v", object)
	}

	cl, err := gcsx.NewClient(ctx, storage.ScopeReadOnly)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create GCS client")
	}
	content, err := gcsx.ReadObject(ctx, cl, bucket, obj)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read manifest %v", object)
	}
	var md jobpb.ProxyManifest
	if err := proto.Unmarshal(content, &md); err != nil {
		return nil, errors.Wrapf(err, "invalid manifest %v", object)
	}
	return &md, nil
}

// NewRetrievalServer creates a artifact retrieval server for the
// given manifest. It requires that the locations are in GCS.
func NewRetrievalServer(md *jobpb.ProxyManifest) (*RetrievalServer, error) {
	if err := validate(md); err != nil {
		return nil, err
	}

	blobs := make(map[string]string)
	for _, l := range md.GetLocation() {
		if _, _, err := gcsx.ParseObject(l.GetUri()); err != nil {
			return nil, errors.Wrapf(err, "location %v is not a GCS object", l.GetUri())
		}
		blobs[l.GetName()] = l.GetUri()
	}
	return &RetrievalServer{md: md.GetManifest(), blobs: blobs}, nil
}

// GetManifest returns the manifest for all artifacts.
func (s *RetrievalServer) GetManifest(ctx context.Context, req *jobpb.GetManifestRequest) (*jobpb.GetManifestResponse, error) {
	return &jobpb.GetManifestResponse{Manifest: s.md}, nil
}

// GetArtifact returns a given artifact.
func (s *RetrievalServer) GetArtifact(req *jobpb.LegacyGetArtifactRequest, stream jobpb.LegacyArtifactRetrievalService_GetArtifactServer) error {
	key := req.GetName()
	blob, ok := s.blobs[key]
	if !ok {
		return errors.Errorf("artifact %v not found", key)
	}

	bucket, object := parseObject(blob)

	ctx := stream.Context()
	client, err := gcsx.NewClient(ctx, storage.ScopeReadOnly)
	if err != nil {
		return errors.Wrapf(err, "Failed to create client for %v", key)
	}

	// Stream artifact in up to 1MB chunks.
	r, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return errors.Wrapf(err, "Failed to read object for %v", key)
	}
	defer r.Close()

	data := make([]byte, 1<<20)
	for {
		n, err := r.Read(data)
		if n > 0 {
			if err := stream.Send(&jobpb.ArtifactChunk{Data: data[:n]}); err != nil {
				return errors.Wrap(err, "chunk send failed")
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.Wrapf(err, "failed to read from %v", blob)
		}
	}
	return nil
}

func validate(md *jobpb.ProxyManifest) error {
	keys := make(map[string]bool)
	for _, a := range md.GetManifest().GetArtifact() {
		if _, seen := keys[a.Name]; seen {
			return errors.Errorf("multiple artifact with name %v", a.Name)
		}
		keys[a.Name] = true
	}
	for _, l := range md.GetLocation() {
		fresh, seen := keys[l.Name]
		if !seen {
			return errors.Errorf("no artifact named %v for location %v", l.Name, l.Uri)
		}
		if !fresh {
			return errors.Errorf("multiple locations for %v:%v", l.Name, l.Uri)
		}
		keys[l.Name] = false
	}

	for key, fresh := range keys {
		if fresh {
			return errors.Errorf("no location for %v", key)
		}
	}
	return nil
}

func parseObject(blob string) (string, string) {
	bucket, object, err := gcsx.ParseObject(blob)
	if err != nil {
		panic(err)
	}
	return bucket, object
}
