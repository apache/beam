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

package components

import (
	"context"
	"fmt"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/cache"
	"beam.apache.org/playground/backend/internal/db"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/logger"
)

type CacheComponent struct {
	cache cache.Cache
	db    db.Database
}

func NewService(cache cache.Cache, db db.Database) *CacheComponent {
	return &CacheComponent{cache: cache, db: db}
}

// GetSdkCatalogFromCacheOrDatastore returns the sdk catalog from the cache
// - If there is no sdk catalog in the cache, gets the sdk catalog from the Cloud Datastore and saves it to the cache
func (cp *CacheComponent) GetSdkCatalogFromCacheOrDatastore(ctx context.Context) ([]*entity.SDKEntity, error) {
	sdks, err := cp.cache.GetSdkCatalog(ctx)
	if err != nil {
		logger.Errorf("error during getting the sdk catalog from the cache, err: %s", err.Error())
		sdks, err = cp.db.GetSDKs(ctx)
		if err != nil {
			logger.Errorf("error during getting the sdk catalog from the cloud datastore, err: %s", err.Error())
			return nil, err
		}
		if err = cp.cache.SetSdkCatalog(ctx, sdks); err != nil {
			logger.Errorf("error during setting the sdk catalog to the cache, err: %s", err.Error())
			return nil, err
		}
	}
	return sdks, nil
}

// GetCatalogFromCacheOrDatastore returns the example catalog from cache
// - If there is no catalog in the cache, gets the catalog from the Cloud Datastore and saves it to the cache
func (cp *CacheComponent) GetCatalogFromCacheOrDatastore(ctx context.Context) ([]*pb.Categories, error) {
	catalog, err := cp.cache.GetCatalog(ctx)
	if err != nil {
		logger.Errorf("error during getting the catalog from the cache, err: %s", err.Error())
		sdkCatalog, err := cp.GetSdkCatalogFromCacheOrDatastore(ctx)
		if err != nil {
			logger.Errorf("error during getting the sdk catalog from the cache or datastore, err: %s", err.Error())
			return nil, err
		}
		catalog, err = cp.db.GetCatalog(ctx, sdkCatalog)
		if err != nil {
			return nil, err
		}
		if len(catalog) == 0 {
			logger.Warn("example catalog is empty")
			return catalog, nil
		}
		if err = cp.cache.SetCatalog(ctx, catalog); err != nil {
			logger.Errorf("SetCatalog(): cache error: %s", err.Error())
			return nil, err
		}
	}
	return catalog, nil
}

// GetDefaultPrecompiledObjectFromCacheOrDatastore returns the default example from cache by sdk
// - If there is no a default example in the cache, gets the default example from the Cloud Datastore and saves it to the cache
func (cp *CacheComponent) GetDefaultPrecompiledObjectFromCacheOrDatastore(ctx context.Context, sdk pb.Sdk) (*pb.PrecompiledObject, error) {
	defaultExample, err := cp.cache.GetDefaultPrecompiledObject(ctx, sdk)
	if err != nil {
		logger.Errorf("error during getting a default precompiled object, err: %s", err.Error())
		sdks, err := cp.GetSdkCatalogFromCacheOrDatastore(ctx)
		if err != nil {
			logger.Errorf("error during getting sdk catalog from the cache or the cloud datastore, err: %s", err.Error())
			return nil, err
		}
		defaultExamples, err := cp.db.GetDefaultExamples(ctx, sdks)
		for sdk, defaultExample := range defaultExamples {
			if err := cp.cache.SetDefaultPrecompiledObject(ctx, sdk, defaultExample); err != nil {
				logger.Errorf("error during setting a default example to the cache: %s", err.Error())
				return nil, err
			}
		}
		defaultExample, ok := defaultExamples[sdk]
		if !ok {
			return nil, fmt.Errorf("no default example found for this sdk: %s", sdk)
		}
		return defaultExample, nil
	}
	return defaultExample, nil
}
