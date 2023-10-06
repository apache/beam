package main

import (
	"context"
	"fmt"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/cache"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/common"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/environment"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/logging"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/service/echo"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"net"
	"os"
	"os/signal"
)

var (
	logger = logging.New("github.com/apache/beam/test-infra/mock-apis/src/main/go/cmd/echo")
	env    = []environment.Variable{
		common.CacheHost,
		common.Port,
	}
	loggingFields []logging.Field
	decrementer   cache.Decrementer
	address       string
)

func init() {
	ctx := context.Background()
	for _, v := range env {
		loggingFields = append(loggingFields, logging.Field{
			Key:   v.Key(),
			Value: v.Value(),
		})
	}
	if err := initE(ctx); err != nil {
		logger.Fatal(ctx, err, loggingFields...)
	}
}

func initE(ctx context.Context) error {
	if err := environment.Missing(env...); err != nil {
		return err
	}

	port, err := common.Port.Int()
	if err != nil {
		logger.Fatal(ctx, err, loggingFields...)
		return err
	}

	address = fmt.Sprintf(":%v", port)
	loggingFields = append(loggingFields, logging.Field{
		Key:   "address",
		Value: address,
	})

	r := redis.NewClient(&redis.Options{
		Addr: common.CacheHost.Value(),
	})
	rc := (*cache.RedisCache)(r)
	decrementer = rc

	return decrementer.Alive(ctx)
}

func main() {
	ctx := context.Background()
	if err := run(ctx); err != nil {
		logger.Error(ctx, err, loggingFields...)
	}
}

func run(ctx context.Context) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()
	logger.Info(ctx, "starting service")
	lis, err := net.Listen("tcp", address)
	if err != nil {
		logger.Error(ctx, err, loggingFields...)
		return err
	}

	s := grpc.NewServer()
	if err = echo.RegisterGrpc(s, decrementer); err != nil {
		logger.Error(ctx, err, loggingFields...)
		return err
	}

	defer s.GracefulStop()

	errChan := make(chan error)
	go func() {
		if err = s.Serve(lis); err != nil {
			logger.Error(ctx, err, loggingFields...)
			errChan <- err
		}
	}()

	for {
		select {
		case err = <-errChan:
			return err
		case <-ctx.Done():
			return nil
		}
	}
}
