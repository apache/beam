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

// Package echo simulates a simple echo service that holds knowledge of a quota.
package echo

import (
	context "context"
	"fmt"

	"github.com/apache/beam/test-infra/pipelines/src/main/go/internal/cache"
	"github.com/apache/beam/test-infra/pipelines/src/main/go/internal/logging"
	echov1 "github.com/apache/beam/test-infra/pipelines/src/main/go/internal/proto/echo/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RegisterService to a grpcServer.
func RegisterService(ctx context.Context, server *grpc.Server, quotaCache cache.Decrementer) error {
	if quotaCache == nil {
		return fmt.Errorf("quotaCache is nil")
	}

	svc := &echoService{
		logger: logging.New(
			ctx,
			"github.com/apache/beam/.test-infra/pipelines/src/main/go/internal/echo",
			logging.LevelVariable),
		quotaCache: quotaCache,
	}

	echov1.RegisterEchoServiceServer(server, svc)
	svc.logger.Debug(ctx, "registered server")
	return nil
}

type echoService struct {
	echov1.UnimplementedEchoServiceServer
	quotaCache cache.Decrementer
	logger     *logging.Logger
}

// Echo a EchoRequest with an EchoResponse.
// Decrements quota identified by the request's id and responds with an error
// if quota exceeded.
func (e *echoService) Echo(ctx context.Context, request *echov1.EchoRequest) (*echov1.EchoResponse, error) {
	e.logger.Debug(ctx, "received request", logging.Any("request", request))
	if request == nil {
		return nil, status.Errorf(codes.InvalidArgument, "received nil request")
	}
	if request.Id == "" {
		return nil, status.Errorf(codes.InvalidArgument, "invalid request: Id is required but empty")
	}
	if err := e.quotaCache.Decrement(ctx, request.Id); err != nil {
		return nil, status.Error(codes.ResourceExhausted, err.Error())
	}
	return &echov1.EchoResponse{
		Id:      request.Id,
		Payload: request.Payload,
	}, nil
}
