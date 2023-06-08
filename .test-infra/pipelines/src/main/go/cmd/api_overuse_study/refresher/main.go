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

	logger         = logging.MustLogger(context.Background(), "refresher")
	cacheRefresher cache.Refresher
	subscriber     cache.Subscriber
)

func init() {
	ctx := context.Background()
	if err := vars(ctx); err != nil {
		logger.Fatal(ctx, err.Error())
	}
}

func vars(ctx context.Context) error {
	var err error
	if err = environment.Missing(required...); err != nil {
		return err
	}
	logger.Debug(ctx, "environment", logging.Any("required", required))

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
		logger.Fatal(ctx, err.Error())
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
