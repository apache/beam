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

package main

import (
	"context"
	"fmt"
	"net"

	"github.com/apache/beam/test-infra/pipelines/src/main/go/internal/cache"
	"github.com/apache/beam/test-infra/pipelines/src/main/go/internal/echo"
	"github.com/apache/beam/test-infra/pipelines/src/main/go/internal/environment"
	"github.com/apache/beam/test-infra/pipelines/src/main/go/internal/logging"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
)

var (
	port environment.Variable = "PORT"

	address    = fmt.Sprintf(":%s", port.Value())
	logger     = logging.MustLogger(context.Background(), "echo")
	cacheQuota cache.Decrementer

	required = []environment.Variable{
		port,
		cache.Host,
	}

	env = environment.Map(required...)
)

func init() {
	ctx := context.Background()

	if err := environment.Missing(required...); err != nil {
		logger.Fatal(ctx, err.Error(), logging.Any("env", env))
	}

	if err := vars(ctx); err != nil {
		logger.Fatal(ctx, err.Error(), logging.Any("env", env))
	}

}

func vars(ctx context.Context) error {
	redisClient := redis.NewClient(&redis.Options{
		Addr: cache.Host.Value(),
	})

	cacheQuota = (*cache.RedisCache)(redisClient)

	if err := redisClient.Ping(ctx).Err(); err != nil {
		return err
	}
	logger.Info(ctx, "pinged cache host ok",
		logging.String("host", cache.Host.Value()))
	return nil
}

func main() {
	ctx, cancel := context.WithCancel(signals.SetupSignalHandler())
	defer cancel()

	lis, err := net.Listen("tcp", address)
	if err != nil {
		err = fmt.Errorf("error listening on address: %s, %w", address, err)
		logger.Error(ctx, err.Error(), logging.Any("env", env))
		return
	}

	svc := grpc.NewServer()

	if err := echo.RegisterService(ctx, svc, cacheQuota); err != nil {
		err = fmt.Errorf("error registering echo service: %w", err)
		logger.Error(ctx, err.Error())
		return
	}

	go func() {
		if err := svc.Serve(lis); err != nil {
			logger.Error(ctx, err.Error())
			return
		}
	}()

	logger.Info(ctx, "started echo service", logging.Any("env", env))

	for {
		select {
		case <-ctx.Done():
			if svc != nil {
				svc.GracefulStop()
			}
			return
		}
	}
}
