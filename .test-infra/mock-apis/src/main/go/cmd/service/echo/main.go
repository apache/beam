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

// echo is an executable that runs the echov1.EchoService.
package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"

	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/cache"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/common"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/environment"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/logging"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/service/echo"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

var (
	logger = logging.New("github.com/apache/beam/test-infra/mock-apis/src/main/go/cmd/echo")
	env    = []environment.Variable{
		common.CacheHost,
		common.GrpcPort,
		common.HttpPort,
	}
	loggingFields []logging.Field
	decrementer   cache.Decrementer
	grpcAddress   string

	httpAddress string
)

func init() {
	ctx := context.Background()
	for _, v := range env {
		loggingFields = append(loggingFields, logging.Field{
			Key:   v.Key(),
			Value: v.Value(),
		})
	}
	if err := initE(ctx); err != nil {
		logger.Fatal(ctx, err, loggingFields...)
	}
}

func initE(ctx context.Context) error {
	if err := environment.Missing(env...); err != nil {
		return err
	}

	grpcPort, err := common.GrpcPort.Int()
	if err != nil {
		logger.Fatal(ctx, err, loggingFields...)
		return err
	}
	grpcAddress = fmt.Sprintf(":%v", grpcPort)

	httpPort, err := common.HttpPort.Int()
	if err != nil {
		logger.Fatal(ctx, err, loggingFields...)
		return err
	}
	httpAddress = fmt.Sprintf(":%v", httpPort)

	r := redis.NewClient(&redis.Options{
		Addr: common.CacheHost.Value(),
	})
	rc := (*cache.RedisCache)(r)
	decrementer = rc

	return decrementer.Alive(ctx)
}

func main() {
	ctx := context.Background()
	if err := run(ctx); err != nil {
		logger.Error(ctx, err, loggingFields...)
	}
}

func run(ctx context.Context) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()
	logger.Info(ctx, "starting service", loggingFields...)
	s, handler, err := setup(ctx)
	if err != nil {
		return err
	}
	defer s.GracefulStop()

	lis, err := net.Listen("tcp", grpcAddress)
	if err != nil {
		logger.Error(ctx, err, loggingFields...)
		return err
	}

	errChan := make(chan error)
	go func() {
		if err := s.Serve(lis); err != nil {
			logger.Error(ctx, err, loggingFields...)
			errChan <- err
		}
	}()

	go func() {
		if err := http.ListenAndServe(httpAddress, handler); err != nil {
			errChan <- err
		}
	}()

	for {
		select {
		case err := <-errChan:
			return err
		case <-ctx.Done():
			logger.Info(ctx, "shutting down", loggingFields...)
			return nil
		}
	}
}

func setup(ctx context.Context) (*grpc.Server, http.Handler, error) {
	s := grpc.NewServer()
	handler, err := echo.Register(s, decrementer)
	if err != nil {
		logger.Error(ctx, err, loggingFields...)
		return nil, nil, err
	}

	return s, handler, nil
}
