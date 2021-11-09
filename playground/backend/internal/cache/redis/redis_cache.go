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

package redis

import (
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/cache"
	"beam.apache.org/playground/backend/internal/logger"
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"time"
)

type Cache struct {
	*redis.Client
}

// New returns Redis implementation of Cache interface.
// In case of problem with connection to Redis returns error.
func New(ctx context.Context, addr string) (*Cache, error) {
	rc := Cache{redis.NewClient(&redis.Options{Addr: addr})}
	_, err := rc.Ping(ctx).Result()
	if err != nil {
		logger.Errorf("Redis Cache: connect to Redis: error during Ping operation, err: %s\n", err.Error())
		return nil, err
	}
	return &rc, nil
}

func (rc *Cache) GetValue(ctx context.Context, pipelineId uuid.UUID, subKey cache.SubKey) (interface{}, error) {
	marshSubKey, err := json.Marshal(subKey)
	if err != nil {
		logger.Errorf("Redis Cache: get value: error during marshal subKey: %s, err: %s\n", subKey, err.Error())
		return nil, err
	}
	value, err := rc.HGet(ctx, pipelineId.String(), string(marshSubKey)).Result()
	if err != nil {
		logger.Errorf("Redis Cache: get value: error during HGet operation for key: %s, subKey: %s, err: %s\n", pipelineId.String(), subKey, err.Error())
		return nil, err
	}

	return unmarshalBySubKey(subKey, value)
}

func (rc *Cache) SetValue(ctx context.Context, pipelineId uuid.UUID, subKey cache.SubKey, value interface{}) error {
	subKeyMarsh, err := json.Marshal(subKey)
	if err != nil {
		logger.Errorf("Redis Cache: set value: error during marshal subKey: %s, err: %s\n", subKey, err.Error())
		return err
	}
	valueMarsh, err := json.Marshal(value)
	if err != nil {
		logger.Errorf("Redis Cache: set value: error during marshal value: %s, err: %s\n", value, err.Error())
		return err
	}
	_, err = rc.HSet(ctx, pipelineId.String(), subKeyMarsh, valueMarsh).Result()
	if err != nil {
		logger.Errorf("Redis Cache: set value: error during HSet operation, err: %s\n", err.Error())
		return err
	}
	return nil
}

func (rc *Cache) SetExpTime(ctx context.Context, pipelineId uuid.UUID, expTime time.Duration) error {
	exists, err := rc.Exists(ctx, pipelineId.String()).Result()
	if err != nil {
		logger.Errorf("Redis Cache: set expiration time value: error during Exists operation for key: %s, err: %s\n", pipelineId, err.Error())
		return err
	}
	if exists == 0 {
		logger.Errorf("Redis Cache: set expiration time value: key doesn't exist, key: %s\n", pipelineId)
		return fmt.Errorf("key: %s doesn't exist", pipelineId)
	}

	_, err = rc.Expire(ctx, pipelineId.String(), expTime).Result()
	if err != nil {
		logger.Errorf("Redis Cache: set expiration time value: error during Expire operation for key: %s, err: %s\n", pipelineId, err.Error())
		return err
	}
	return nil
}

// unmarshalBySubKey unmarshal value by subKey
func unmarshalBySubKey(subKey cache.SubKey, value string) (result interface{}, err error) {
	switch subKey {
	case cache.Status:
		result = new(pb.Status)
		err = json.Unmarshal([]byte(value), &result)
	case cache.RunOutput, cache.RunError, cache.CompileOutput:
		result = ""
		err = json.Unmarshal([]byte(value), &result)
	}
	if err != nil {
		logger.Errorf("Redis Cache: get value: error during unmarshal value, err: %s\n", err.Error())
	}
	return
}
