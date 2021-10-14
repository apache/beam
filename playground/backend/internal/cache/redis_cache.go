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
	pb "beam.apache.org/playground/backend/internal/api"
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"google.golang.org/grpc/grpclog"
	"time"
)

const defaultExpirationTime = time.Minute * 15

type RedisCache struct {
	redisClient *redis.Client
}

// check connection to Redis
var ping = func(redisClient *redis.Client, ctx context.Context) (string, error) {
	return redisClient.Ping(ctx).Result()
}

// check if pipelineId exists as Redis key
var exists = func(redisClient *redis.Client, ctx context.Context, pipelineId uuid.UUID) (int64, error) {
	return redisClient.Exists(ctx, pipelineId.String()).Result()
}

// set expiration time for key
var expire = func(redisClient *redis.Client, ctx context.Context, key string, expTime time.Duration) (bool, error) {
	return redisClient.Expire(ctx, key, expTime).Result()
}

// put encoded value to {key}:{encoded subKey} structure
var hSet = func(redisClient *redis.Client, ctx context.Context, key string, subKeyMarsh, valueMarsh []byte) (int64, error) {
	return redisClient.HSet(ctx, key, subKeyMarsh, valueMarsh).Result()
}

// receive value from {key}:{subKey} structure
var hGet = func(redisClient *redis.Client, ctx context.Context, key, subKey string) (string, error) {
	return redisClient.HGet(ctx, key, subKey).Result()
}

// newRedisCache returns Redis implementation of Cache interface.
// In case of problem with connection to Redis returns error.
func newRedisCache(ctx context.Context, addr string) (*RedisCache, error) {
	rc := RedisCache{redisClient: redis.NewClient(&redis.Options{Addr: addr})}
	_, err := ping(rc.redisClient, ctx)
	if err != nil {
		grpclog.Errorf("Redis Cache: connect to Redis: error during Ping operation, err: %s", err.Error())
		return nil, err
	}
	return &rc, nil
}

func (rc *RedisCache) GetValue(ctx context.Context, pipelineId uuid.UUID, subKey SubKey) (interface{}, error) {
	marshSubKey, err := json.Marshal(subKey)
	if err != nil {
		grpclog.Errorf("Redis Cache: get value: error during marshal subKey: %s, err: %s", subKey, err.Error())
		return nil, err
	}
	value, err := hGet(rc.redisClient, ctx, pipelineId.String(), string(marshSubKey))
	if err != nil {
		grpclog.Errorf("Redis Cache: get value: error during HGet operation for key: %s, subKey: %s, err: %s", pipelineId.String(), subKey, err.Error())
		return nil, err
	}

	return unmarshalBySubKey(subKey, value)
}

func (rc *RedisCache) SetValue(ctx context.Context, pipelineId uuid.UUID, subKey SubKey, value interface{}) error {
	subKeyMarsh, err := json.Marshal(subKey)
	if err != nil {
		grpclog.Errorf("Redis Cache: set value: error during marshal subKey: %s, err: %s", subKey, err.Error())
		return err
	}
	valueMarsh, err := json.Marshal(value)
	if err != nil {
		grpclog.Errorf("Redis Cache: set value: error during marshal value: %s, err: %s", value, err.Error())
		return err
	}
	_, err = hSet(rc.redisClient, ctx, pipelineId.String(), subKeyMarsh, valueMarsh)
	if err != nil {
		grpclog.Errorf("Redis Cache: set value: error during HSet operation, err: %s", err.Error())
		return err
	}

	_, err = expire(rc.redisClient, ctx, pipelineId.String(), defaultExpirationTime)
	if err != nil {
		grpclog.Errorf("Redis Cache: set value: error during Expire operation, err: %s", err.Error())
		return err
	}
	return nil
}

func (rc *RedisCache) SetExpTime(ctx context.Context, pipelineId uuid.UUID, expTime time.Duration) error {
	exists, err := exists(rc.redisClient, ctx, pipelineId)
	if err != nil {
		grpclog.Errorf("Redis Cache: set expiration time value: error during Exists operation for key: %s, err: %s", pipelineId, err.Error())
		return err
	}
	if exists == 0 {
		grpclog.Errorf("Redis Cache: set expiration time value: key doesn't exist, key: %s", pipelineId)
		return fmt.Errorf("key: %s doesn't exist", pipelineId)
	}

	_, err = expire(rc.redisClient, ctx, pipelineId.String(), expTime)
	if err != nil {
		grpclog.Errorf("Redis Cache: set expiration time value: error during Expire operation for key: %s, err: %s", pipelineId, err.Error())
		return err
	}
	return nil
}

// unmarshalBySubKey unmarshal value by subKey
func unmarshalBySubKey(subKey SubKey, value string) (result interface{}, err error) {
	switch subKey {
	case SubKey_Status:
		result = new(pb.Status)
		err = json.Unmarshal([]byte(value), &result)
	case SubKey_RunOutput, SubKey_CompileOutput:
		result = ""
		err = json.Unmarshal([]byte(value), &result)
	}
	if err != nil {
		grpclog.Errorf("Redis Cache: get value: error during unmarshal value, err: %s", err.Error())
	}
	return
}
