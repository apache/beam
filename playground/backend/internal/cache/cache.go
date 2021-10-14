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
	"github.com/google/uuid"
	"os"
	"time"
)

type SubKey string

const (
	SubKey_Status        SubKey = "STATUS"
	SubKey_RunOutput     SubKey = "RUN_OUTPUT"
	SubKey_CompileOutput SubKey = "COMPILE_OUTPUT"
)

type Cache interface {
	// GetValue returns value from cache by pipelineId and subKey.
	GetValue(ctx context.Context, pipelineId uuid.UUID, subKey SubKey) (interface{}, error)

	// SetValue adds value to cache by pipelineId and subKey.
	SetValue(ctx context.Context, pipelineId uuid.UUID, subKey SubKey, value interface{}) error

	// SetExpTime adds expiration time of the pipeline to cache by pipelineId.
	SetExpTime(ctx context.Context, pipelineId uuid.UUID, expTime time.Duration) error
}

// NewCache returns new cache to save and read value
func NewCache(ctx context.Context, cacheType string) (Cache, error) {
	switch cacheType {
	case "remote":
		return newRedisCache(ctx, os.Getenv("remote_cache_address"))
	default:
		return newLocalCache(ctx), nil
	}
}
