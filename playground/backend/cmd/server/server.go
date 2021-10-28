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
	"beam.apache.org/playground/backend/internal/cache"
	"beam.apache.org/playground/backend/internal/cache/local"
	"context"
	"log"
	"os"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/environment"
	"github.com/improbable-eng/grpc-web/go/grpcweb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

// runServer is starting http server wrapped on grpc
func runServer() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	envService, err := setupEnvironment()
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()

	cacheService, err := setupCache(ctx, envService.ApplicationEnvs)
	if err != nil {
		return err
	}
	pb.RegisterPlaygroundServiceServer(grpcServer, &playgroundController{
		env:          envService,
		cacheService: cacheService,
	})

	grpclog.SetLoggerV2(grpclog.NewLoggerV2(os.Stdout, os.Stdout, os.Stderr))
	handler := Wrap(grpcServer, getGrpcWebOptions())
	errChan := make(chan error)

	go listenHttp(ctx, errChan, envService.NetworkEnvs, handler)

	for {
		select {
		case err := <-errChan:
			return err
		case <-ctx.Done():
			log.Println("interrupt signal received; stopping...")
			return nil
		}
	}
}

func setupEnvironment() (*environment.Environment, error) {
	networkEnvs, err := environment.GetNetworkEnvsFromOsEnvs()
	if err != nil {
		return nil, err
	}
	appEnvs, err := environment.GetApplicationEnvsFromOsEnvs()
	if err != nil {
		return nil, err
	}
	beamEnvs, err := environment.ConfigureBeamEnvs(appEnvs.WorkingDir())
	if err != nil {
		return nil, err
	}
	return environment.NewEnvironment(*networkEnvs, *beamEnvs, *appEnvs), nil
}

// getGrpcWebOptions returns grpcweb options needed to configure wrapper
func getGrpcWebOptions() []grpcweb.Option {
	return []grpcweb.Option{
		grpcweb.WithCorsForRegisteredEndpointsOnly(false),
		grpcweb.WithAllowNonRootResource(true),
		grpcweb.WithOriginFunc(func(origin string) bool {
			return true
		}),
	}

}

// setupCache constructs required cache by application environment
func setupCache(ctx context.Context, appEnv environment.ApplicationEnvs) (cache.Cache, error) {
	switch appEnv.CacheEnvs().CacheType() {
	default:
		return local.New(ctx), nil
	}
}

func main() {
	err := runServer()
	if err != nil {
		panic(err)
	}
}
