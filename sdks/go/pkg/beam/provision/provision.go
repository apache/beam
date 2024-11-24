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

// Package provision contains utilities for obtaining runtime provision,
// information -- such as pipeline options.
//
// Deprecated: Use github.com/apache/beam/sdks/v2/go/container/tools package instead.
package provision

import (
	"context"

	"github.com/apache/beam/sdks/v2/go/container/tools"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	google_pb "google.golang.org/protobuf/types/known/structpb"
)

// Info returns the runtime provisioning info for the worker.
//
// Deprecated: Use github.com/apache/beam/sdks/v2/go/container/tools instead.
func Info(ctx context.Context, endpoint string) (*fnpb.ProvisionInfo, error) {
	return tools.ProvisionInfo(ctx, endpoint)
}

// OptionsToProto converts pipeline options to a proto struct via JSON.
//
// Deprecated: Use github.com/apache/beam/sdks/v2/go/container/tools.OptionsToProto instead.
func OptionsToProto(v any) (*google_pb.Struct, error) {
	return tools.OptionsToProto(v)
}

// JSONToProto converts JSON-encoded pipeline options to a proto struct.
//
// Deprecated: Use github.com/apache/beam/sdks/v2/go/container/tools.JSONToProto instead.
func JSONToProto(data string) (*google_pb.Struct, error) {
	return tools.JSONToProto(data)
}

// ProtoToOptions converts pipeline options from a proto struct via JSON.
//
// Deprecated: Use github.com/apache/beam/sdks/v2/go/container/tools.ProtoToOptions instead.
func ProtoToOptions(opt *google_pb.Struct, v any) error {
	return tools.ProtoToOptions(opt, v)
}

// ProtoToJSON converts pipeline options from a proto struct to JSON.
//
// Deprecated: Use github.com/apache/beam/sdks/v2/go/container/tools.ProtoToJSON instead.
func ProtoToJSON(opt *google_pb.Struct) (string, error) {
	return tools.ProtoToJSON(opt)
}
