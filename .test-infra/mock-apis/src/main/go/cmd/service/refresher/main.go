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
	"log/slog"
	"os"
	"os/signal"

	gcplogging "cloud.google.com/go/logging"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/cache"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/environment"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/logging"
	"github.com/redis/go-redis/v9"
)

var (
	env = []environment.Variable{
		environment.CacheHost,
		environment.QuotaId,
		environment.QuotaSize,
		environment.QuotaRefreshInterval,
	}
	logger   *slog.Logger
	logAttrs []slog.Attr
	opts     = &logging.Options{
		Name: "refresher",
	}
)

func init() {
	for _, v := range env {
		logAttrs = append(logAttrs, slog.Attr{
			Key:   v.Key(),
			Value: slog.StringValue(v.Value()),
		})
	}
}

func main() {
	ctx := context.Background()

	if !environment.ProjectId.Missing() {
		client, err := gcplogging.NewClient(ctx, environment.ProjectId.Value())
		if err != nil {
			slog.LogAttrs(ctx, slog.LevelError, err.Error(), logAttrs...)
			os.Exit(1)
		}

		opts.Client = client
	}

	logger = logging.New(opts)
	if err := run(ctx); err != nil {
		logger.LogAttrs(ctx, slog.LevelError, err.Error(), logAttrs...)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	if err := environment.Missing(env...); err != nil {
		return err
	}

	size, err := environment.QuotaSize.UInt64()
	if err != nil {
		return err
	}

	interval, err := environment.QuotaRefreshInterval.Duration()
	if err != nil {
		return err
	}

	r := redis.NewClient(&redis.Options{
		Addr: environment.CacheHost.Value(),
	})

	opts := &cache.Options{
		Logger: logger,
		Setter: (*cache.RedisCache)(r),
	}

	ref, err := cache.NewRefresher(ctx, opts)
	if err != nil {
		return err
	}

	errChan := make(chan error)
	go func() {
		if err := ref.Refresh(ctx, environment.QuotaId.Value(), size, interval); err != nil {
			errChan <- err
		}
	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		return nil
	}

}
