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

// Package tools contains utilities for Beam bootloader containers, such as
// for obtaining runtime provision information -- such as pipeline options.
// or for logging to the log service.
//
// For Beam Internal use.
package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/grpcx"
	"google.golang.org/protobuf/encoding/protojson"
	google_pb "google.golang.org/protobuf/types/known/structpb"
)

// ProvisionInfo returns the runtime provisioning info for the worker.
func ProvisionInfo(ctx context.Context, endpoint string) (*fnpb.ProvisionInfo, error) {
	cc, err := grpcx.Dial(ctx, endpoint, 2*time.Minute)
	if err != nil {
		return nil, err
	}
	defer cc.Close()

	client := fnpb.NewProvisionServiceClient(cc)

	resp, err := client.GetProvisionInfo(ctx, &fnpb.GetProvisionInfoRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to get manifest: %w", err)
	}
	if resp.GetInfo() == nil {
		return nil, errors.New("empty manifest")
	}
	return resp.GetInfo(), nil
}

// OptionsToProto converts pipeline options to a proto struct via JSON.
func OptionsToProto(v any) (*google_pb.Struct, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return JSONToProto(string(data))
}

// JSONToProto converts JSON-encoded pipeline options to a proto struct.
func JSONToProto(data string) (*google_pb.Struct, error) {
	var out google_pb.Struct

	if err := protojson.Unmarshal([]byte(data), &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ProtoToOptions converts pipeline options from a proto struct via JSON.
func ProtoToOptions(opt *google_pb.Struct, v any) error {
	data, err := ProtoToJSON(opt)
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(data), v)
}

// ProtoToJSON converts pipeline options from a proto struct to JSON.
func ProtoToJSON(opt *google_pb.Struct) (string, error) {
	if opt == nil {
		return "{}", nil
	}
	bytes, err := protojson.Marshal(opt)
	if err != nil {
		return "", err
	}
	return string(bytes), err
}
