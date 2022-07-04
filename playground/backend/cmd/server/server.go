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
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/cache"
	"beam.apache.org/playground/backend/internal/cache/local"
	"beam.apache.org/playground/backend/internal/cache/redis"
	"beam.apache.org/playground/backend/internal/cloud_bucket"
	"beam.apache.org/playground/backend/internal/db"
	"beam.apache.org/playground/backend/internal/db/datastore"
	"beam.apache.org/playground/backend/internal/db/mapper"
	"beam.apache.org/playground/backend/internal/db/schema"
	"beam.apache.org/playground/backend/internal/db/schema/migration"
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/logger"
	"beam.apache.org/playground/backend/internal/utils"
	"context"
	"fmt"
	"github.com/improbable-eng/grpc-web/go/grpcweb"
	"google.golang.org/grpc"
)

// runServer is starting http server wrapped on grpc
func runServer() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	envService, err := setupEnvironment()
	if err != nil {
		return err
	}

	props, err := environment.NewProperties(envService.ApplicationEnvs.PropertyPath())
	if err != nil {
		return err
	}

	logger.SetupLogger(ctx, envService.ApplicationEnvs.LaunchSite(), envService.ApplicationEnvs.GoogleProjectId())

	grpcServer := grpc.NewServer()

	cacheService, err := setupCache(ctx, envService.ApplicationEnvs)
	if err != nil {
		return err
	}

	var dbClient db.Database
	var entityMapper mapper.EntityMapper

	// Examples catalog should be retrieved and saved to cache only if the server doesn't suppose to run code, i.e. SDK is unspecified
	// Database setup only if the server doesn't suppose to run code, i.e. SDK is unspecified
	if envService.BeamSdkEnvs.ApacheBeamSdk == pb.Sdk_SDK_UNSPECIFIED {
		err = setupExamplesCatalog(ctx, cacheService, envService.ApplicationEnvs.BucketName())
		if err != nil {
			return err
		}

		dbClient, err = datastore.New(ctx, envService.ApplicationEnvs.GoogleProjectId())
		if err != nil {
			return err
		}

		if err = setupDBStructure(ctx, dbClient, &envService.ApplicationEnvs, props); err != nil {
			return err
		}

		entityMapper = mapper.New(&envService.ApplicationEnvs, props)
	}

	pb.RegisterPlaygroundServiceServer(grpcServer, &playgroundController{
		env:          envService,
		cacheService: cacheService,
		db:           dbClient,
		props:        props,
		entityMapper: entityMapper,
	})

	errChan := make(chan error)

	switch envService.NetworkEnvs.Protocol() {
	case "TCP":
		go listenTcp(ctx, errChan, envService.NetworkEnvs, grpcServer)
	case "HTTP":
		handler := Wrap(grpcServer, getGrpcWebOptions())
		go listenHttp(ctx, errChan, envService, handler)
	}

	for {
		select {
		case err := <-errChan:
			return err
		case <-ctx.Done():
			logger.Info("interrupt signal received; stopping...")
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
	case "remote":
		return redis.New(ctx, appEnv.CacheEnvs().Address())
	default:
		return local.New(ctx), nil
	}
}

// setupExamplesCatalog saves precompiled objects catalog from storage to cache
func setupExamplesCatalog(ctx context.Context, cacheService cache.Cache, bucketName string) error {
	catalog, err := utils.GetCatalogFromStorage(ctx, bucketName)
	if err != nil {
		return err
	}
	if err = cacheService.SetCatalog(ctx, catalog); err != nil {
		logger.Errorf("GetPrecompiledObjects(): cache error: %s", err.Error())
	}

	bucket := cloud_bucket.New()
	defaultPrecompiledObjects, err := bucket.GetDefaultPrecompiledObjects(ctx, bucketName)
	if err != nil {
		return err
	}
	for sdk, precompiledObject := range defaultPrecompiledObjects {
		if err := cacheService.SetDefaultPrecompiledObject(ctx, sdk, precompiledObject); err != nil {
			logger.Errorf("GetPrecompiledObjects(): cache error: %s", err.Error())
			return err
		}
	}
	return nil
}

// setupDBStructure initializes the data structure
func setupDBStructure(ctx context.Context, db db.Database, appEnv *environment.ApplicationEnvs, props *environment.Properties) error {
	versions := []schema.Version{new(migration.InitialStructure)}
	dbSchema := schema.New(ctx, db, appEnv, props, versions)
	actualSchemaVersion, err := dbSchema.InitiateData()
	if err != nil {
		return err
	}
	if actualSchemaVersion == "" {
		errMsg := "schema version must not be empty"
		logger.Error(errMsg)
		return fmt.Errorf(errMsg)
	}
	appEnv.SetSchemaVersion(actualSchemaVersion)
	return nil
}

func main() {
	err := runServer()
	if err != nil {
		panic(err)
	}
}
