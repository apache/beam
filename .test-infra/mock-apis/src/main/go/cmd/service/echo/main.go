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
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"

	gcplogging "cloud.google.com/go/logging"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/cache"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/environment"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/logging"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/service/echo"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

var (
	env = []environment.Variable{
		environment.CacheHost,
		environment.GrpcPort,
		environment.HttpPort,
	}

	logger   *slog.Logger
	logAttrs []slog.Attr
	opts     = &logging.Options{
		Name: "echo",
	}
)

func init() {
	for _, v := range env {
		logAttrs = append(logAttrs, slog.Attr{
			Key:   v.Key(),
			Value: slog.StringValue(v.Value()),
		})
	}
}

func main() {
	ctx := context.Background()

	if !environment.ProjectId.Missing() {
		client, err := gcplogging.NewClient(ctx, environment.ProjectId.Value())
		if err != nil {
			slog.LogAttrs(ctx, slog.LevelError, err.Error(), logAttrs...)
			os.Exit(1)
		}

		opts.Client = client
	}

	logger = logging.New(opts)

	if err := run(ctx); err != nil {
		logger.LogAttrs(ctx, slog.LevelError, err.Error(), logAttrs...)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	if err := environment.Missing(env...); err != nil {
		return err
	}

	grpcPort, err := environment.GrpcPort.Int()
	if err != nil {
		return err
	}
	grpcAddress := fmt.Sprintf(":%v", grpcPort)

	httpPort, err := environment.HttpPort.Int()
	if err != nil {
		return err
	}
	httpAddress := fmt.Sprintf(":%v", httpPort)

	s := grpc.NewServer()
	defer s.GracefulStop()

	r := redis.NewClient(&redis.Options{
		Addr: environment.CacheHost.Value(),
	})

	echoOpts := &echo.Options{
		Decrementer:  (*cache.RedisCache)(r),
		LoggingAttrs: logAttrs,
		Logger:       logger,
		// TODO(damondouglas): add GCP metrics client
		// 	MetricsWriter:
	}

	handler, err := echo.Register(s, echoOpts)
	if err != nil {
		return err
	}

	logger.LogAttrs(ctx, slog.LevelInfo, "starting service", logAttrs...)

	lis, err := net.Listen("tcp", grpcAddress)
	if err != nil {
		return err
	}

	errChan := make(chan error)
	go func() {
		if err := s.Serve(lis); err != nil {
			errChan <- err
		}
	}()

	go func() {
		if err := http.ListenAndServe(httpAddress, handler); err != nil {
			errChan <- err
		}
	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		logger.LogAttrs(ctx, slog.LevelInfo, "shutting down", logAttrs...)
		return nil
	}
}
