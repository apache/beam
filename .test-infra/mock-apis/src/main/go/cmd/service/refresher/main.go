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
