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
	"time"

	"github.com/apache/beam/test-infra/pipelines/src/main/go/internal/logging"
	"github.com/redis/go-redis/v9"
)

const maxRetries = 3

var (
	logger = logging.New(
		context.Background(),
		"github.com/apache/beam/.test-infra/pipelines/src/main/go/internal/cache/redis",
		logging.LevelVariable)
)

// RedisCache implements a Decrementer, Publisher, Subscriber, and Refresher
// using redis as the underlying cache storage.
type RedisCache redis.Client

// Decrement a quota identified by quotaID.
func (q *RedisCache) Decrement(ctx context.Context, quotaID string) error {
	client := (*redis.Client)(q)
	txf := func(tx *redis.Tx) error {
		n, err := tx.Get(ctx, quotaID).Uint64()
		if err != nil && err != redis.Nil {
			return err
		}
		if n == 0 || err == redis.Nil {
			return fmt.Errorf("quota exhausted for quotaID: %s", quotaID)
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			return pipe.Decr(ctx, quotaID).Err()
		})

		return err
	}

	for i := 0; i < maxRetries; i++ {
		err := client.Watch(ctx, txf, quotaID)
		if err == nil {
			return nil
		}
		if err == redis.TxFailedErr {
			continue
		}
		return err
	}

	return fmt.Errorf("error: Decrement(%s) reached maximum number of retries: %v", quotaID, maxRetries)
}

// Refresh a cached quota with size identified by a quotaID every interval.
func (q *RedisCache) Refresh(ctx context.Context, quotaID string, size uint64, interval time.Duration) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := q.refresh(ctx, quotaID, size, interval); err != nil {
		return err
	}
	logger.Debug(ctx, "quota initialized",
		logging.String("quotaID", quotaID),
		logging.Uint64("size", size))

	tick := time.Tick(interval)
	for {
		select {
		case <-tick:
			if err := q.refresh(ctx, quotaID, size, interval); err != nil {
				return err
			}
			logger.Debug(ctx, "refreshed quota",
				logging.String("quotaID", quotaID),
				logging.Uint64("size", size))
		case <-ctx.Done():
			return nil
		}
	}
}

func (q *RedisCache) refresh(ctx context.Context, quotaID string, size uint64, interval time.Duration) error {
	client := (*redis.Client)(q)
	return client.SetEx(ctx, quotaID, size, interval).Err()
}

// Publish an Event of key.
func (q *RedisCache) Publish(ctx context.Context, key string, event Event) error {
	client := (*redis.Client)(q)
	return client.Publish(ctx, key, ([]byte)(event)).Err()
}

// Subscribe to events identified by keys.
func (q *RedisCache) Subscribe(ctx context.Context, events chan Event, keys ...string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	client := (*redis.Client)(q)
	sub := client.PSubscribe(ctx, keys...)
	channel := sub.Channel()
	logger.Debug(ctx, "opening channel", logging.Strings("keys", keys))
	for {
		select {
		case msg := <-channel:
			logger.Debug(ctx, "received channel message", logging.String("payload", msg.String()))
			events <- []byte(msg.Payload)
		case <-ctx.Done():
			return nil
		}
	}
}
