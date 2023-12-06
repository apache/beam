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
	"time"

	"github.com/redis/go-redis/v9"
)

// Validate interface implementations
var _ UInt64Setter = &RedisCache{}
var _ Decrementer = &RedisCache{}
var _ HealthChecker = &RedisCache{}

// RedisCache implements a Decrementer and a Refresher.
type RedisCache redis.Client

// Set implements Refresher's Set method using a redis cache where expiry of 0
// has no expiration. Returns any error from the redis client.
func (client *RedisCache) Set(ctx context.Context, key string, value uint64, expiry time.Duration) error {
	r := (*redis.Client)(client)
	return r.Set(ctx, key, value, expiry).Err()
}

// Decrement implements Decrementer's Decrement method using a redis cache.
// Returns an error when the key does not exist or from the redis client.
func (client *RedisCache) Decrement(ctx context.Context, key string) (int64, error) {
	r := (*redis.Client)(client)
	v, err := r.Exists(ctx, key).Result()
	if err != nil {
		return -1, err
	}
	if v == 0 {
		return -1, ErrNotExist
	}
	return r.Decr(ctx, key).Result()
}

// Alive implements HealthChecker's Alive checking the availability of a
// redis cache. Returns an error if no successful connection.
func (client *RedisCache) Alive(ctx context.Context) error {
	r := (*redis.Client)(client)
	return r.Ping(ctx).Err()
}
