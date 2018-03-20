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
package provision

import (
	"context"
	"encoding/json"
	"fmt"

	"time"

	pb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/util/grpcx"
	"github.com/golang/protobuf/jsonpb"
	google_protobuf "github.com/golang/protobuf/ptypes/struct"
)

// Info returns the runtime provisioning info for the worker.
func Info(ctx context.Context, endpoint string) (*pb.ProvisionInfo, error) {
	cc, err := grpcx.Dial(ctx, endpoint, 2*time.Minute)
	if err != nil {
		return nil, err
	}
	defer cc.Close()

	client := pb.NewProvisionServiceClient(cc)

	resp, err := client.GetProvisionInfo(ctx, &pb.GetProvisionInfoRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to get manifest: %v", err)
	}
	if resp.GetInfo() == nil {
		return nil, fmt.Errorf("empty manifest")
	}
	return resp.GetInfo(), nil
}

// OptionsToProto converts pipeline options to a proto struct via JSON.
func OptionsToProto(v interface{}) (*google_protobuf.Struct, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return JSONToProto(string(data))
}

// JSONToProto converts JSON-encoded pipeline options to a proto struct.
func JSONToProto(data string) (*google_protobuf.Struct, error) {
	var out google_protobuf.Struct
	if err := jsonpb.UnmarshalString(string(data), &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ProtoToOptions converts pipeline options from a proto struct via JSON.
func ProtoToOptions(opt *google_protobuf.Struct, v interface{}) error {
	data, err := ProtoToJSON(opt)
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(data), v)
}

// ProtoToJSON converts pipeline options from a proto struct to JSON.
func ProtoToJSON(opt *google_protobuf.Struct) (string, error) {
	if opt == nil {
		return "{}", nil
	}
	return (&jsonpb.Marshaler{}).MarshalToString(opt)
}
