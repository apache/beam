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

package cache

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"sync"
	"time"
)

const cleanupInterval = 5 * time.Second

type LocalCache struct {
	sync.RWMutex
	cleanupInterval     time.Duration
	items               map[uuid.UUID]map[SubKey]interface{}
	pipelinesExpiration map[uuid.UUID]time.Time
}

func newLocalCache(ctx context.Context) *LocalCache {
	items := make(map[uuid.UUID]map[SubKey]interface{})
	pipelinesExpiration := make(map[uuid.UUID]time.Time)
	ls := &LocalCache{
		cleanupInterval:     cleanupInterval,
		items:               items,
		pipelinesExpiration: pipelinesExpiration,
	}

	go ls.startGC()
	return ls

}

func (lc *LocalCache) GetValue(ctx context.Context, pipelineId uuid.UUID, subKey SubKey) (interface{}, error) {
	lc.RLock()
	value, found := lc.items[pipelineId][subKey]
	if !found {
		return nil, fmt.Errorf("value with pipelineId: %s and subKey: %s not found", pipelineId, subKey)
	}
	expTime := lc.pipelinesExpiration[pipelineId]
	lc.RUnlock()

	if expTime.Before(time.Now()) {
		lc.Lock()
		delete(lc.items[pipelineId], subKey)
		delete(lc.pipelinesExpiration, pipelineId)
		lc.Unlock()
		return nil, fmt.Errorf("value with pipelineId: %s and subKey: %s is expired", pipelineId, subKey)
	}

	return value, nil
}

func (lc *LocalCache) SetValue(ctx context.Context, pipelineId uuid.UUID, subKey SubKey, value interface{}) error {
	lc.Lock()
	defer lc.Unlock()

	_, ok := lc.items[pipelineId]
	if !ok {
		lc.items[pipelineId] = make(map[SubKey]interface{})
		lc.pipelinesExpiration[pipelineId] = time.Now().Add(time.Hour)
	}
	lc.items[pipelineId][subKey] = value
	return nil
}

func (lc *LocalCache) SetExpTime(ctx context.Context, pipelineId uuid.UUID, expTime time.Duration) error {
	lc.Lock()
	defer lc.Unlock()
	lc.pipelinesExpiration[pipelineId] = time.Now().Add(expTime)
	return nil
}

func (lc *LocalCache) startGC() {
	for {
		<-time.After(lc.cleanupInterval)

		if lc.items == nil {
			return
		}

		if pipelines := lc.expiredPipelines(); len(pipelines) != 0 {
			lc.clearItems(pipelines)
		}
	}
}

func (lc *LocalCache) expiredPipelines() (pipelines []uuid.UUID) {
	lc.RLock()
	defer lc.RUnlock()
	for pipelineId, expTime := range lc.pipelinesExpiration {
		if expTime.Before(time.Now()) {
			pipelines = append(pipelines, pipelineId)
		}
	}
	return
}

func (lc *LocalCache) clearItems(pipelines []uuid.UUID) {
	lc.Lock()
	defer lc.Unlock()
	for _, pipeline := range pipelines {
		delete(lc.items, pipeline)
		delete(lc.pipelinesExpiration, pipeline)
	}
}
