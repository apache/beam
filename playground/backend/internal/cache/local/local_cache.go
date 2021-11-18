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
	"beam.apache.org/playground/backend/internal/cache"
	"context"
	"fmt"
	"github.com/google/uuid"
	"sync"
	"time"
)

const (
	cleanupInterval = 5 * time.Second
)

type Cache struct {
	sync.RWMutex
	cleanupInterval     time.Duration
	items               map[uuid.UUID]map[cache.SubKey]interface{}
	pipelinesExpiration map[uuid.UUID]time.Time
}

func New(ctx context.Context) *Cache {
	items := make(map[uuid.UUID]map[cache.SubKey]interface{})
	pipelinesExpiration := make(map[uuid.UUID]time.Time)
	ls := &Cache{
		cleanupInterval:     cleanupInterval,
		items:               items,
		pipelinesExpiration: pipelinesExpiration,
	}

	go ls.startGC(ctx)
	return ls

}

// GetValue returns value from cache. If not found or key is expired, GetValue returns an error.
func (lc *Cache) GetValue(ctx context.Context, pipelineId uuid.UUID, subKey cache.SubKey) (interface{}, error) {
	lc.RLock()
	value, found := lc.items[pipelineId][subKey]
	if !found {
		lc.RUnlock()
		return nil, fmt.Errorf("value with pipelineId: %s and subKey: %s not found", pipelineId, subKey)
	}
	expTime, found := lc.pipelinesExpiration[pipelineId]
	lc.RUnlock()

	if found && expTime.Before(time.Now()) {
		lc.Lock()
		delete(lc.items[pipelineId], subKey)
		delete(lc.pipelinesExpiration, pipelineId)
		lc.Unlock()
		return nil, fmt.Errorf("value with pipelineId: %s and subKey: %s is expired", pipelineId, subKey)
	}

	return value, nil
}

// SetValue puts element to cache.
// If a particular pipelineId does not contain in the cache, SetValue creates a new element for this pipelineId without expiration time.
// Use SetExpTime to set expiration time for cache elements.
// If data for a particular pipelineId is already contained in the cache, SetValue sets or updates the value for the specific subKey.
func (lc *Cache) SetValue(ctx context.Context, pipelineId uuid.UUID, subKey cache.SubKey, value interface{}) error {
	lc.Lock()
	defer lc.Unlock()

	_, ok := lc.items[pipelineId]
	if !ok {
		lc.items[pipelineId] = make(map[cache.SubKey]interface{})
	}
	lc.items[pipelineId][subKey] = value
	return nil
}

// SetExpTime sets expiration time to particular pipelineId in cache.
// If pipelineId doesn't present in the cache, SetExpTime returns an error.
func (lc *Cache) SetExpTime(ctx context.Context, pipelineId uuid.UUID, expTime time.Duration) error {
	lc.Lock()
	defer lc.Unlock()
	if _, found := lc.items[pipelineId]; !found {
		return fmt.Errorf("%s pipeline id doesn't presented in cache", pipelineId.String())
	}
	lc.pipelinesExpiration[pipelineId] = time.Now().Add(expTime)
	return nil
}

func (lc *Cache) startGC(ctx context.Context) {
	ticker := time.NewTicker(lc.cleanupInterval)
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
	lc.RLock()
	defer lc.RUnlock()
	for pipelineId, expTime := range lc.pipelinesExpiration {
		if expTime.Before(time.Now()) {
			pipelines = append(pipelines, pipelineId)
		}
	}
	return
}

func (lc *Cache) clearItems(pipelines []uuid.UUID) {
	lc.Lock()
	defer lc.Unlock()
	for _, pipeline := range pipelines {
		delete(lc.items, pipeline)
		delete(lc.pipelinesExpiration, pipeline)
	}
}
