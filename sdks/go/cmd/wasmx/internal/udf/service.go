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

package udf

import (
	"bytes"
	"context"
	"fmt"
	udf_v1 "github.com/apache/beam/sdks/v2/go/cmd/wasmx/internal/proto/udf/v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io"
)

func Service(g *grpc.Server, registry Registry) error {
	if registry == nil {
		return fmt.Errorf("registry is nil")
	}
	udf_v1.RegisterUDFServiceServer(g, &service{
		registry: registry,
	})
	return nil
}

type service struct {
	registry Registry
	udf_v1.UnimplementedUDFServiceServer
}

func (s *service) Describe(ctx context.Context, request *udf_v1.DescribeRequest) (*udf_v1.DescribeResponse, error) {
	fn, err := s.registry.Get(ctx, request.Urn)
	if err != nil {
		return nil, err
	}
	return &udf_v1.DescribeResponse{
		Udf: fn,
	}, nil
}

func (s *service) Create(ctx context.Context, request *udf_v1.CreateRequest) (*udf_v1.CreateResponse, error) {
	var wasmSrc WasmSource = request.Source
	meta, err := Parse(wasmSrc)
	if err != nil {
		return nil, err
	}
	if err = meta.isValidErr(); err != nil {
		return nil, err
	}
	bldr := goBuilder()
	defer bldr.Teardown(ctx)
	if err = bldr.Setup(ctx); err != nil {
		return nil, err
	}
	dst := &bytes.Buffer{}
	var src io.Reader = bytes.NewBuffer(request.Source)
	if err = bldr.Build(ctx, dst, src); err != nil {
		return nil, err
	}

	now := timestamppb.Now()

	udf := &udf_v1.UserDefinedFunction{
		UniqueName: request.Urn,
		Urn:        request.Urn,
		Bytes:      dst.Bytes(),
		Language:   udf_v1.UserDefinedFunction_Language_Go,
		Created:    now,
		Updated:    now,
		InputUrn:   meta.BeamInput,
		OutputUrn:  meta.BeamOutput,
	}

	if err = s.registry.Set(ctx, udf.Urn, udf); err != nil {
		return nil, err
	}

	return &udf_v1.CreateResponse{
		Urn: udf.Urn,
		Udf: udf,
	}, nil
}

func (s *service) Update(ctx context.Context, request *udf_v1.UpdateRequest) (*udf_v1.UpdateResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (s *service) Delete(ctx context.Context, request *udf_v1.DeleteRequest) (*udf_v1.DeleteResponse, error) {
	udf, err := s.registry.Get(ctx, request.Urn)
	if err != nil {
		return nil, err
	}
	if err = s.registry.Delete(ctx, request.Urn); err != nil {
		return nil, err
	}
	return &udf_v1.DeleteResponse{
		Urn: request.Urn,
		Udf: udf,
	}, nil
}

func NewExpansionResponse(fn *udf_v1.UserDefinedFunction) *pipeline_v1.PTransform {
	var wasmFn WasmFn = fn.Bytes
	return &pipeline_v1.PTransform{
		UniqueName:    fn.UniqueName,
		Spec:          wasmFn.FunctionSpec(),
		Inputs:        map[string]string{"i0": "n1"},
		Outputs:       map[string]string{"i0": "n2"},
		EnvironmentId: WasmEnvironmentId,
	}
}
