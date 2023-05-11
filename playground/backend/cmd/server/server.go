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
	"beam.apache.org/playground/backend/internal/external_functions"
	"context"
	"fmt"
	"github.com/improbable-eng/grpc-web/go/grpcweb"
	"google.golang.org/grpc"
	"os"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/cache"
	"beam.apache.org/playground/backend/internal/cache/local"
	"beam.apache.org/playground/backend/internal/cache/redis"
	"beam.apache.org/playground/backend/internal/components"
	"beam.apache.org/playground/backend/internal/db"
	"beam.apache.org/playground/backend/internal/db/datastore"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/db/mapper"
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/logger"
	"beam.apache.org/playground/backend/internal/tasks"
	"beam.apache.org/playground/backend/internal/tests/test_data"
)

// runServer is starting http server wrapped on grpc
func runServer() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	envService, err := setupEnvironment()
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
	var props *environment.Properties
	var cacheComponent *components.CacheComponent

	// Examples catalog should be retrieved and saved to cache only if the server doesn't suppose to run code, i.e. SDK is unspecified
	// Database setup only if the server doesn't suppose to run code, i.e. SDK is unspecified
	if envService.BeamSdkEnvs.ApacheBeamSdk == pb.Sdk_SDK_UNSPECIFIED {
		externalFunctions := external_functions.NewExternalFunctionsComponent(envService.ApplicationEnvs)

		props, err = environment.NewProperties(envService.ApplicationEnvs.PropertyPath())
		if err != nil {
			return err
		}

		dbClient, err = datastore.New(ctx, mapper.NewPrecompiledObjectMapper(), externalFunctions, envService.ApplicationEnvs.GoogleProjectId())
		if err != nil {
			return err
		}

		downloadCatalogsToDatastoreEmulator(ctx)

		migrationVersion, err := dbClient.GetCurrentDbMigrationVersion(ctx)
		if err != nil {
			return err
		}

		envService.ApplicationEnvs.SetSchemaVersion(migrationVersion)

		sdks, err := setupSdkCatalog(ctx, cacheService, dbClient)
		if err != nil {
			return err
		}

		if err = setupExamplesCatalogFromDatastore(ctx, cacheService, dbClient, sdks); err != nil {
			return err
		}

		entityMapper = mapper.NewDatastoreMapper(ctx, &envService.ApplicationEnvs, props)
		cacheComponent = components.NewService(cacheService, dbClient)

		// Since only router server has the scheduled task, the task creation is here
		scheduledTasks := tasks.New(ctx)
		if err = scheduledTasks.StartRemovingExtraSnippets(props.RemovingUnusedSnptsCron, externalFunctions); err != nil {
			return err
		}
	}

	pb.RegisterPlaygroundServiceServer(grpcServer, &playgroundController{
		env:            envService,
		cacheService:   cacheService,
		db:             dbClient,
		props:          props,
		entityMapper:   entityMapper,
		cacheComponent: cacheComponent,
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

func downloadCatalogsToDatastoreEmulator(ctx context.Context) {
	if _, ok := os.LookupEnv("DATASTORE_EMULATOR_HOST"); ok {
		test_data.DownloadCatalogsWithMockData(ctx)
	}
}

// setupSdkCatalog saves the sdk catalog from the cloud datastore to the cache
func setupSdkCatalog(ctx context.Context, cacheService cache.Cache, db db.Database) ([]*entity.SDKEntity, error) {
	sdks, err := db.GetSDKs(ctx)
	if err != nil {
		logger.Errorf("setupSdkCatalog() error during getting the sdk catalog, err: %s", err.Error())
		return nil, err
	}
	sdkNames := pb.Sdk_value
	delete(sdkNames, pb.Sdk_SDK_UNSPECIFIED.String())
	if len(sdks) != len(sdkNames) {
		errMsg := "setupSdkCatalog() database doesn't have all sdks"
		logger.Error(errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	if err = cacheService.SetSdkCatalog(ctx, sdks); err != nil {
		logger.Errorf("setupSdkCatalog() error during setting sdk catalog to the cache, err: %s", err.Error())
		return nil, err
	}
	return sdks, nil
}

// setupEnvironment constructs the environment required by the app
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

// setupExamplesCatalogFromDatastore saves precompiled objects catalog from the cloud datastore to the cache
func setupExamplesCatalogFromDatastore(ctx context.Context, cacheService cache.Cache, db db.Database, sdks []*entity.SDKEntity) error {
	catalog, err := db.GetCatalog(ctx, sdks)
	if len(catalog) == 0 {
		logger.Warn("example catalog is empty")
		return nil
	}
	if err != nil {
		return err
	}
	if err = cacheService.SetCatalog(ctx, catalog); err != nil {
		logger.Errorf("GetPrecompiledObjects(): cache error: %s", err.Error())
	}
	defaultExamples, err := db.GetDefaultExamples(ctx, sdks)
	if err != nil {
		return err
	}
	for sdk, precompiledObject := range defaultExamples {
		if err := cacheService.SetDefaultPrecompiledObject(ctx, sdk, precompiledObject); err != nil {
			logger.Errorf("GetPrecompiledObjects(): cache error: %s", err.Error())
			return err
		}
	}
	return nil
}

func main() {
	err := runServer()
	if err != nil {
		panic(err)
	}
}
