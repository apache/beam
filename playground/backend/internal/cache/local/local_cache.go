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

package local

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/cache"
	"beam.apache.org/playground/backend/internal/db/entity"
)

const (
	cleanupInterval = 5 * time.Second
)

type Cache struct {
	mu                        sync.RWMutex
	cleanupInterval           time.Duration
	items                     map[uuid.UUID]map[cache.SubKey]interface{}
	pipelinesExpiration       map[uuid.UUID]time.Time
	catalog                   []*pb.Categories
	sdkCatalog                []*entity.SDKEntity
	defaultPrecompiledObjects map[pb.Sdk]*pb.PrecompiledObject
}

func New(ctx context.Context) *Cache {
	items := make(map[uuid.UUID]map[cache.SubKey]interface{})
	pipelinesExpiration := make(map[uuid.UUID]time.Time)
	defaultPrecompiledObjects := make(map[pb.Sdk]*pb.PrecompiledObject)
	ls := &Cache{
		cleanupInterval:           cleanupInterval,
		items:                     items,
		pipelinesExpiration:       pipelinesExpiration,
		catalog:                   nil,
		defaultPrecompiledObjects: defaultPrecompiledObjects,
	}

	go ls.startGC(ctx)
	return ls

}

// GetValue returns value from cache. If not found or key is expired, GetValue returns an error.
func (lc *Cache) GetValue(_ context.Context, pipelineId uuid.UUID, subKey cache.SubKey) (interface{}, error) {
	lc.mu.RLock()
	value, found := lc.items[pipelineId][subKey]
	if !found {
		lc.mu.RUnlock()
		return nil, fmt.Errorf("value with pipelineId: %s and subKey: %s not found", pipelineId, subKey)
	}
	expTime, found := lc.pipelinesExpiration[pipelineId]
	lc.mu.RUnlock()

	if found && expTime.Before(time.Now()) {
		lc.mu.Lock()
		delete(lc.items[pipelineId], subKey)
		delete(lc.pipelinesExpiration, pipelineId)
		lc.mu.Unlock()
		return nil, fmt.Errorf("value with pipelineId: %s and subKey: %s is expired", pipelineId, subKey)
	}

	return value, nil
}

// SetValue puts element to cache.
// If a particular pipelineId does not contain in the cache, SetValue creates a new element for this pipelineId without expiration time.
// Use SetExpTime to set expiration time for cache elements.
// If data for a particular pipelineId is already contained in the cache, SetValue sets or updates the value for the specific subKey.
func (lc *Cache) SetValue(_ context.Context, pipelineId uuid.UUID, subKey cache.SubKey, value interface{}) error {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	_, ok := lc.items[pipelineId]
	if !ok {
		lc.items[pipelineId] = make(map[cache.SubKey]interface{})
	}

	switch subKey {
	case cache.RunOutputIndex, cache.LogsIndex:
		value = float64(value.(int))
	}

	lc.items[pipelineId][subKey] = value
	return nil
}

// SetExpTime sets expiration time to particular pipelineId in cache.
// If pipelineId doesn't present in the cache, SetExpTime returns an error.
func (lc *Cache) SetExpTime(_ context.Context, pipelineId uuid.UUID, expTime time.Duration) error {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if _, found := lc.items[pipelineId]; !found {
		return fmt.Errorf("%s pipeline id doesn't presented in cache", pipelineId.String())
	}
	lc.pipelinesExpiration[pipelineId] = time.Now().Add(expTime)
	return nil
}

func (lc *Cache) SetCatalog(_ context.Context, catalog []*pb.Categories) error {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.catalog = catalog
	return nil
}

func (lc *Cache) GetCatalog(_ context.Context) ([]*pb.Categories, error) {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	if lc.catalog == nil {
		return nil, fmt.Errorf("catalog is not found")
	}
	return lc.catalog, nil
}

func (lc *Cache) SetDefaultPrecompiledObject(_ context.Context, sdk pb.Sdk, precompiledObject *pb.PrecompiledObject) error {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.defaultPrecompiledObjects[sdk] = precompiledObject
	return nil
}

func (lc *Cache) GetDefaultPrecompiledObject(_ context.Context, sdk pb.Sdk) (*pb.PrecompiledObject, error) {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	defaultPrecompiledObject := lc.defaultPrecompiledObjects[sdk]
	if defaultPrecompiledObject == nil {
		return nil, fmt.Errorf("default precompiled obejct is not found for %s sdk", sdk.String())
	}
	return defaultPrecompiledObject, nil
}

func (lc *Cache) SetSdkCatalog(_ context.Context, sdks []*entity.SDKEntity) error {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.sdkCatalog = sdks
	return nil
}

func (lc *Cache) GetSdkCatalog(_ context.Context) ([]*entity.SDKEntity, error) {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	if lc.sdkCatalog == nil {
		return nil, fmt.Errorf("sdk catalog is not found")
	}
	return lc.sdkCatalog, nil
}

func (lc *Cache) startGC(ctx context.Context) {
	ticker := time.NewTicker(lc.cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
			if lc.items == nil {
				return
			}

			if pipelines := lc.expiredPipelines(); len(pipelines) != 0 {
				lc.clearItems(pipelines)
			}
		}
	}
}

func (lc *Cache) expiredPipelines() (pipelines []uuid.UUID) {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	for pipelineId, expTime := range lc.pipelinesExpiration {
		if expTime.Before(time.Now()) {
			pipelines = append(pipelines, pipelineId)
		}
	}
	return
}

func (lc *Cache) clearItems(pipelines []uuid.UUID) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	for _, pipeline := range pipelines {
		delete(lc.items, pipeline)
		delete(lc.pipelinesExpiration, pipeline)
	}
}
