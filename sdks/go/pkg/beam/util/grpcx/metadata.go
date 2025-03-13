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

// Package grpcx contains utilities for working with gRPC.
package grpcx

import (
	"context"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"google.golang.org/grpc/metadata"
)

const idKey = "worker_id"

// ReadWorkerID reads the worker ID from an incoming gRPC request context.
func ReadWorkerID(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", errors.New("failed to read metadata from context")
	}
	id, ok := md[idKey]
	if !ok || len(id) < 1 {
		return "", errors.Errorf("failed to find worker id in metadata %v", md)
	}
	if len(id) > 1 {
		return "", errors.Errorf("multiple worker ids in metadata: %v", id)
	}
	return id[0], nil
}

// WriteWorkerID write the worker ID to an outgoing gRPC request context. It
// merges the information with any existing gRPC metadata.
func WriteWorkerID(ctx context.Context, id string) context.Context {
	md := metadata.New(map[string]string{
		idKey: id,
	})
	if old, ok := metadata.FromOutgoingContext(ctx); ok {
		md = metadata.Join(md, old)
	}
	return metadata.NewOutgoingContext(ctx, md)
}
