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
	"fmt"
	"io"

	pb "github.com/apache/beam/sdks/go/pkg/beam/model/jobmanagement_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/util/gcsx"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"google.golang.org/api/storage/v1"
)

// RetrievalServer is a artifact retrieval server backed by Google
// Cloud Storage (GCS). It serves a single manifest and ignores
// the worker id. The server performs no caching or pre-fetching.
type RetrievalServer struct {
	md    *pb.Manifest
	blobs map[string]string
}

// ReadProxyManifest reads and parses the proxy manifest from GCS.
func ReadProxyManifest(ctx context.Context, object string) (*pb.ProxyManifest, error) {
	bucket, obj, err := gcsx.ParseObject(object)
	if err != nil {
		return nil, fmt.Errorf("invalid manifest object %v: %v", object, err)
	}

	cl, err := gcsx.NewClient(ctx, storage.DevstorageReadOnlyScope)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %v", err)
	}
	content, err := gcsx.ReadObject(cl, bucket, obj)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest %v: %v", object, err)
	}
	var md pb.ProxyManifest
	if err := proto.Unmarshal(content, &md); err != nil {
		return nil, fmt.Errorf("invalid manifest %v: %v", object, err)
	}
	return &md, nil
}

// NewRetrievalServer creates a artifact retrieval server for the
// given manifest. It requires that the locations are in GCS.
func NewRetrievalServer(md *pb.ProxyManifest) (*RetrievalServer, error) {
	if err := validate(md); err != nil {
		return nil, err
	}

	blobs := make(map[string]string)
	for _, l := range md.GetLocation() {
		if _, _, err := gcsx.ParseObject(l.GetUri()); err != nil {
			return nil, fmt.Errorf("location %v is not a GCS object: %v", l.GetUri(), err)
		}
		blobs[l.GetName()] = l.GetUri()
	}
	return &RetrievalServer{md: md.GetManifest(), blobs: blobs}, nil
}

// GetManifest returns the manifest for all artifacts.
func (s *RetrievalServer) GetManifest(ctx context.Context, req *pb.GetManifestRequest) (*pb.GetManifestResponse, error) {
	return &pb.GetManifestResponse{Manifest: s.md}, nil
}

// GetArtifact returns a given artifact.
func (s *RetrievalServer) GetArtifact(req *pb.GetArtifactRequest, stream pb.ArtifactRetrievalService_GetArtifactServer) error {
	key := req.GetName()
	blob, ok := s.blobs[key]
	if !ok {
		return fmt.Errorf("artifact %v not found", key)
	}

	bucket, object := parseObject(blob)

	client, err := gcsx.NewClient(stream.Context(), storage.DevstorageReadOnlyScope)
	if err != nil {
		return fmt.Errorf("Failed to create client for %v: %v", key, err)
	}

	// Stream artifact in up to 1MB chunks.

	resp, err := client.Objects.Get(bucket, object).Download()
	if err != nil {
		return fmt.Errorf("Failed to read object for %v: %v", key, err)
	}
	defer resp.Body.Close()

	data := make([]byte, 1<<20)
	for {
		n, err := resp.Body.Read(data)
		if n > 0 {
			if err := stream.Send(&pb.ArtifactChunk{Data: data[:n]}); err != nil {
				return fmt.Errorf("chunk send failed: %v", err)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read from %v: %v", blob, err)
		}
	}
	return nil
}

func validate(md *pb.ProxyManifest) error {
	keys := make(map[string]bool)
	for _, a := range md.GetManifest().GetArtifact() {
		if _, seen := keys[a.Name]; seen {
			return fmt.Errorf("multiple artifact with name %v", a.Name)
		}
		keys[a.Name] = true
	}
	for _, l := range md.GetLocation() {
		fresh, seen := keys[l.Name]
		if !seen {
			return fmt.Errorf("no artifact named %v for location %v", l.Name, l.Uri)
		}
		if !fresh {
			return fmt.Errorf("multiple locations for %v:%v", l.Name, l.Uri)
		}
		keys[l.Name] = false
	}

	for key, fresh := range keys {
		if fresh {
			return fmt.Errorf("no location for %v", key)
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
