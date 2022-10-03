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
	"time"

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
func (cp *CacheComponent) GetSdkCatalogFromCacheOrDatastore(ctx context.Context, cacheRequestTimeout time.Duration) ([]*entity.SDKEntity, error) {
	sdksCh := make(chan []*entity.SDKEntity, 1)
	errCh := make(chan error, 1)
	go func() {
		sdks, err := cp.cache.GetSdkCatalog(ctx)
		if err != nil {
			errCh <- err
		} else {
			sdksCh <- sdks
		}
	}()
	select {
	case sdks := <-sdksCh:
		return sdks, nil
	case e := <-errCh:
		logger.Errorf("error during getting the sdk catalog from the cache, err: %s", e.Error())
		return cp.getSdks(ctx)
	case <-time.After(cacheRequestTimeout):
		logger.Errorf("error during getting the sdk catalog from the cache: timeout")
		return cp.getSdks(ctx)
	}
}

func (cp *CacheComponent) getSdks(ctx context.Context) ([]*entity.SDKEntity, error) {
	sdks, err := cp.db.GetSDKs(ctx)
	if err != nil {
		logger.Errorf("error during getting the sdk catalog from the cloud datastore, err: %s", err.Error())
		return nil, err
	}
	if err = cp.cache.SetSdkCatalog(ctx, sdks); err != nil {
		logger.Errorf("error during setting the sdk catalog to the cache, err: %s", err.Error())
	}
	return sdks, nil
}

// GetCatalogFromCacheOrDatastore returns the example catalog from cache
// - If there is no catalog in the cache, gets the catalog from the Cloud Datastore and saves it to the cache
func (cp *CacheComponent) GetCatalogFromCacheOrDatastore(ctx context.Context, cacheRequestTimeout time.Duration) ([]*pb.Categories, error) {
	catCh := make(chan []*pb.Categories, 1)
	errCh := make(chan error, 1)
	go func() {
		catalog, err := cp.cache.GetCatalog(ctx)
		if err != nil {
			errCh <- err
		} else {
			catCh <- catalog
		}
	}()
	select {
	case cat := <-catCh:
		return cat, nil
	case e := <-errCh:
		logger.Errorf("error during getting the catalog from the cache, err: %s", e.Error())
		return cp.getCatalog(ctx, cacheRequestTimeout)
	case <-time.After(cacheRequestTimeout):
		logger.Errorf("error during getting the catalog from the cache: timeout")
		return cp.getCatalog(ctx, cacheRequestTimeout)
	}
}

func (cp *CacheComponent) getCatalog(ctx context.Context, cacheRequestTimeout time.Duration) ([]*pb.Categories, error) {
	sdkCatalog, err := cp.GetSdkCatalogFromCacheOrDatastore(ctx, cacheRequestTimeout)
	catalog, err := cp.db.GetCatalog(ctx, sdkCatalog)
	if err != nil {
		return nil, err
	}
	if len(catalog) == 0 {
		logger.Warn("example catalog is empty")
		return catalog, nil
	}
	if err = cp.cache.SetCatalog(ctx, catalog); err != nil {
		logger.Errorf("SetCatalog(): cache error: %s", err.Error())
	}
	return catalog, nil
}

// GetDefaultPrecompiledObjectFromCacheOrDatastore returns the default example from cache by sdk
// - If there is no a default example in the cache, gets the default example from the Cloud Datastore and saves it to the cache
func (cp *CacheComponent) GetDefaultPrecompiledObjectFromCacheOrDatastore(ctx context.Context, sdk pb.Sdk, cacheRequestTimeout time.Duration) (*pb.PrecompiledObject, error) {
	prObjCh := make(chan *pb.PrecompiledObject, 1)
	errCh := make(chan error, 1)
	go func() {
		defaultExample, err := cp.cache.GetDefaultPrecompiledObject(ctx, sdk)
		if err != nil {
			errCh <- err
		} else {
			prObjCh <- defaultExample
		}
	}()
	select {
	case prObj := <-prObjCh:
		return prObj, nil
	case e := <-errCh:
		logger.Errorf("error during getting the default precompiled object from the cache, err: %s", e.Error())
		return cp.getDefaultExample(ctx, sdk, cacheRequestTimeout)
	case <-time.After(cacheRequestTimeout):
		logger.Errorf("error during getting the default precompiled object from the cache: timeout")
		return cp.getDefaultExample(ctx, sdk, cacheRequestTimeout)
	}
}

func (cp *CacheComponent) getDefaultExample(ctx context.Context, sdk pb.Sdk, cacheRequestTimeout time.Duration) (*pb.PrecompiledObject, error) {
	sdks, err := cp.GetSdkCatalogFromCacheOrDatastore(ctx, cacheRequestTimeout)
	if err != nil {
		logger.Errorf("error during getting sdk catalog from the cache or the cloud datastore, err: %s", err.Error())
		return nil, err
	}
	defaultExamples, err := cp.db.GetDefaultExamples(ctx, sdks)
	for sdk, defaultExample := range defaultExamples {
		if err := cp.cache.SetDefaultPrecompiledObject(ctx, sdk, defaultExample); err != nil {
			logger.Errorf("error during setting a default example to the cache: %s", err.Error())
		}
	}
	defaultExample, ok := defaultExamples[sdk]
	if !ok {
		return nil, fmt.Errorf("no default example found for this sdk: %s", sdk)
	}
	return defaultExample, nil
}
