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
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/cache"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/logger"
)

const (
	catalogExpire = 5 * time.Minute
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
	subKeyMarsh, err := json.Marshal(subKey)
	if err != nil {
		logger.Errorf("Redis Cache: get value: error during marshal subKey: %s, err: %s\n", subKey, err.Error())
		return nil, err
	}
	value, err := rc.HGet(ctx, pipelineId.String(), string(subKeyMarsh)).Result()
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

func (rc *Cache) SetCatalog(ctx context.Context, catalog []*pb.Categories) error {
	catalogMarsh, err := json.Marshal(catalog)
	if err != nil {
		logger.Errorf("Redis Cache: set catalog: error during marshal catalog: %s, err: %s\n", catalog, err.Error())
		return err
	}
	err = rc.Set(ctx, cache.ExamplesCatalog, catalogMarsh, catalogExpire).Err()
	if err != nil {
		logger.Errorf("Redis Cache: set catalog: error during Set operation, err: %s\n", err.Error())
		return err
	}
	return nil

}

func (rc *Cache) GetCatalog(ctx context.Context) ([]*pb.Categories, error) {
	value, err := rc.Get(ctx, cache.ExamplesCatalog).Result()
	if err != nil {
		logger.Warnf("Redis Cache: get catalog: error during Get operation for key: %s, err: %s\n", cache.ExamplesCatalog, err.Error())
		return nil, err
	}
	var result []*pb.Categories
	err = json.Unmarshal([]byte(value), &result)
	if err != nil {
		logger.Errorf("Redis Cache: get catalog: error during unmarshal catalog, err: %s\n", err.Error())
	}

	return result, nil
}

func (rc *Cache) SetDefaultPrecompiledObject(ctx context.Context, sdk pb.Sdk, precompiledObject *pb.PrecompiledObject) error {
	precompiledObjectMarsh, err := json.Marshal(precompiledObject)
	if err != nil {
		logger.Errorf("Redis Cache: set default precompiled object: error during marshal precompiled object: %s, err: %s\n", precompiledObject, err.Error())
		return err
	}
	sdkMarsh, err := json.Marshal(sdk)
	if err != nil {
		logger.Errorf("Redis Cache: set default precompiled object: error during marshal sdk: %s, err: %s\n", sdk, err.Error())
		return err
	}
	err = rc.HSet(ctx, cache.DefaultPrecompiledExamples, sdkMarsh, precompiledObjectMarsh).Err()
	if err != nil {
		logger.Errorf("Redis Cache: set default precompiled object: error during HGet operation, err: %s\n", err.Error())
		return err
	}
	return nil

}

func (rc *Cache) GetDefaultPrecompiledObject(ctx context.Context, sdk pb.Sdk) (*pb.PrecompiledObject, error) {
	sdkMarsh, err := json.Marshal(sdk)
	if err != nil {
		logger.Errorf("Redis Cache: get default precompiled object: error during marshal sdk: %s, err: %s\n", sdk, err.Error())
		return nil, err
	}

	value, err := rc.HGet(ctx, cache.DefaultPrecompiledExamples, string(sdkMarsh)).Result()
	if err != nil {
		logger.Errorf("Redis Cache: get default precompiled object: error during HGet operation for key: %s, subKey: %s, err: %s\n", cache.DefaultPrecompiledExamples, sdkMarsh, err.Error())
		return nil, err
	}

	result := new(pb.PrecompiledObject)
	err = json.Unmarshal([]byte(value), &result)
	if err != nil {
		logger.Errorf("Redis Cache: get default precompiled object: error during unmarshal value, err: %s\n", err.Error())
	}
	return result, nil
}

func (rc *Cache) SetSdkCatalog(ctx context.Context, sdks []*entity.SDKEntity) error {
	sdksMarsh, err := json.Marshal(sdks)
	if err != nil {
		logger.Errorf("Redis Cache: set sdk catalog: error during marshal sdk catalog: %s, err: %s\n", sdks, err.Error())
		return err
	}
	err = rc.Set(ctx, cache.SdksCatalog, sdksMarsh, 0).Err()
	if err != nil {
		logger.Errorf("Redis Cache: set sdk catalog: error during Set operation, err: %s\n", err.Error())
		return err
	}
	return nil
}

func (rc *Cache) GetSdkCatalog(ctx context.Context) ([]*entity.SDKEntity, error) {
	value, err := rc.Get(ctx, cache.SdksCatalog).Result()
	if err != nil {
		logger.Errorf("Redis Cache: get sdk catalog: error during Get operation for key: %s, err: %s\n", cache.SdksCatalog, err.Error())
		return nil, err
	}
	var result []*entity.SDKEntity
	err = json.Unmarshal([]byte(value), &result)
	if err != nil {
		logger.Errorf("Redis Cache: get sdk catalog: error during unmarshal sdk catalog, err: %s\n", err.Error())
	}

	return result, nil
}

// unmarshalBySubKey unmarshal value by subKey
func unmarshalBySubKey(subKey cache.SubKey, value string) (interface{}, error) {
	var result interface{}
	switch subKey {
	case cache.Status:
		result = new(pb.Status)
	case cache.RunOutput, cache.RunError, cache.ValidationOutput, cache.PreparationOutput, cache.CompileOutput, cache.Logs, cache.Graph:
		result = ""
	case cache.Canceled:
		result = false
	case cache.RunOutputIndex, cache.LogsIndex:
		result = 0
	}
	err := json.Unmarshal([]byte(value), &result)
	if err != nil {
		logger.Errorf("Redis Cache: get value: error during unmarshal value, err: %s\n", err.Error())
	}

	switch subKey {
	case cache.Status:
		result = *result.(*pb.Status)
	}

	return result, err
}
