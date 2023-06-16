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

// Refresher executes a backend service whose single job is to refresh a quota.
// To execute, simply run as a normal go executable. The application relies on
// environment variables for ease and compatibility with conventional cloud
// deployments such as a Kubernetes deployment specification.
// The executable reports missing required environment variables and serves
// as its own self-documenting usage.
//
// See logging.LevelVariable for details on how to change the log level.
package main

import (
	"context"
	"strconv"
	"time"

	"github.com/apache/beam/test-infra/pipelines/src/main/go/internal/cache"
	"github.com/apache/beam/test-infra/pipelines/src/main/go/internal/environment"
	"github.com/apache/beam/test-infra/pipelines/src/main/go/internal/logging"
	"github.com/redis/go-redis/v9"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
)

var (
	required = []environment.Variable{
		cache.Host,
		cache.QuotaId,
		cache.QuotaSize,
		cache.QuotaRefreshInterval,
	}

	size     uint64
	interval time.Duration

	logger = logging.New(
		context.Background(),
		"github.com/apache/beam/.test-infra/pipelines/src/main/go/cmd/api_overuse_study/refresher",
		logging.LevelVariable)
	cacheRefresher cache.Refresher
	subscriber     cache.Subscriber

	env = logging.Any("env", environment.Map(required...))
)

func init() {
	ctx := context.Background()
	if err := vars(ctx); err != nil {
		logger.Fatal(ctx, err, env)
	}
}

func vars(ctx context.Context) error {
	var err error
	if err = environment.Missing(required...); err != nil {
		return err
	}
	logger.Debug(ctx, "environment", logging.Any("required", required), env)

	if size, err = strconv.ParseUint(cache.QuotaSize.Value(), 10, 64); err != nil {
		return err
	}

	if interval, err = time.ParseDuration(cache.QuotaRefreshInterval.Value()); err != nil {
		return err
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr: cache.Host.Value(),
	})

	rc := (*cache.RedisCache)(redisClient)
	cacheRefresher = rc
	subscriber = rc

	if err := redisClient.Ping(ctx).Err(); err != nil {
		return err
	}

	logger.Info(ctx, "pinged cache host ok",
		logging.String("host", cache.Host.Value()))

	return nil
}

func main() {
	ctx := context.Background()
	if err := run(); err != nil {
		logger.Fatal(ctx, err)
	}
}

func run() error {
	ctx, cancel := context.WithCancel(signals.SetupSignalHandler())
	defer cancel()
	errChan := make(chan error)
	evts := make(chan cache.Event)
	go func() {
		if err := subscriber.Subscribe(ctx, evts, cache.QuotaId.Value()); err != nil {
			errChan <- err
		}
	}()
	go func() {
		if err := cacheRefresher.Refresh(ctx, cache.QuotaId.Value(), size, interval); err != nil {
			errChan <- err
		}
	}()
	for {
		select {
		case evt := <-evts:
			logger.Info(ctx, "termination signal received",
				logging.String("event-payload", string(evt)),
				logging.Any("env", environment.Map(required...)))

			return nil
		case err := <-errChan:
			return err
		case <-ctx.Done():
			return nil
		}
	}
}
