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

// refresher is an executable that runs the cache.Refresher service.
package main

import (
	"context"
	"fmt"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/cache"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/common"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/environment"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/logging"
	"github.com/redis/go-redis/v9"
	"os"
	"os/signal"
	"time"
)

var (
	logger = logging.New("github.com/apache/beam/test-infra/mock-apis/src/main/go/cmd/refresher")

	loggerFields []logging.Field

	env = []environment.Variable{
		common.CacheHost,
		common.QuotaId,
		common.QuotaRefreshInterval,
		common.QuotaSize,
	}

	interval time.Duration
	size     uint64
	ref      *cache.Refresher
)

func init() {
	for _, v := range env {
		loggerFields = append(loggerFields, logging.Field{
			Key:   v.Key(),
			Value: v.Value(),
		})
	}
}

func main() {
	ctx := context.Background()
	if err := run(ctx); err != nil {
		err = fmt.Errorf("fatal: run() err = %w", err)
		logger.Fatal(ctx, err, loggerFields...)
	}
}

func run(ctx context.Context) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()
	if err := setup(ctx); err != nil {
		return err
	}

	errChan := make(chan error)
	go func() {
		if err := ref.Refresh(ctx, common.QuotaId.Value(), size, interval); err != nil {
			logger.Error(ctx, err, loggerFields...)
			errChan <- err
		}
	}()

	for {
		select {
		case err := <-errChan:
			return err
		case <-ctx.Done():
			return nil
		}
	}
}

func setup(ctx context.Context) error {
	var err error

	if err = environment.Missing(env...); err != nil {
		logger.Error(ctx, err, loggerFields...)
		return err
	}

	if size, err = common.QuotaSize.UInt64(); err != nil {
		logger.Error(ctx, err, loggerFields...)
		return err
	}

	if interval, err = common.QuotaRefreshInterval.Duration(); err != nil {
		logger.Error(ctx, err, loggerFields...)
		return err
	}

	r := redis.NewClient(&redis.Options{
		Addr: common.CacheHost.Value(),
	})
	rc := (*cache.RedisCache)(r)

	if ref, err = cache.NewRefresher(ctx, rc); err != nil {
		logger.Error(ctx, err, loggerFields...)
		return err
	}

	return nil
}
