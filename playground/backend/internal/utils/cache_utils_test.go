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

package utils

import (
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/cache"
	"beam.apache.org/playground/backend/internal/cache/local"
	"beam.apache.org/playground/backend/internal/cache/redis"
	"context"
	"github.com/go-redis/redismock/v8"
	"github.com/google/uuid"
	"testing"
)

func TestSetToCache(t *testing.T) {
	localCache := local.New(context.Background())
	key := uuid.New()
	subKey := cache.Status
	value := pb.Status_STATUS_FINISHED
	client, _ := redismock.NewClientMock()
	redisCache := &redis.Cache{Client: client}

	type args struct {
		ctx          context.Context
		cacheService cache.Cache
		key          uuid.UUID
		subKey       cache.SubKey
		value        interface{}
	}
	tests := []struct {
		name      string
		args      args
		checkFunc func() bool
		wantErr   bool
	}{
		{
			// Test case with calling SetToCache method with correct cacheService.
			// As a result, want to expected value from cache.
			name: "Set value without error",
			args: args{
				ctx:          context.Background(),
				cacheService: localCache,
				key:          key,
				subKey:       subKey,
				value:        value,
			},
			checkFunc: func() bool {
				getValue, err := localCache.GetValue(context.Background(), key, subKey)
				if err != nil {
					return false
				}
				if getValue != value {
					return false
				}
				return true
			},
			wantErr: false,
		},
		{
			name: "Error during HSet operation",
			args: args{
				ctx:          context.Background(),
				cacheService: redisCache,
				key:          key,
				subKey:       subKey,
				value:        value,
			},
			checkFunc: func() bool {
				return true
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := SetToCache(tt.args.cacheService, tt.args.key, tt.args.subKey, tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("SetToCache() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.checkFunc() {
				t.Error("SetToCache() doesn't set value to cache")
			}
		})
	}
}
