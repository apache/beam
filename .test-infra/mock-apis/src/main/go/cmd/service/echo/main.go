package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"

	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/cache"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/common"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/environment"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/logging"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/service/echo"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	logger = logging.New("github.com/apache/beam/test-infra/mock-apis/src/main/go/cmd/echo")
	env    = []environment.Variable{
		common.CacheHost,
		common.GrpcPort,
		common.HttpPort,
	}
	loggingFields []logging.Field
	decrementer   cache.Decrementer
	grpcAddress   string

	httpAddress string
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

	grpcPort, err := common.GrpcPort.Int()
	if err != nil {
		logger.Fatal(ctx, err, loggingFields...)
		return err
	}
	grpcAddress = fmt.Sprintf(":%v", grpcPort)

	httpPort, err := common.HttpPort.Int()
	if err != nil {
		logger.Fatal(ctx, err, loggingFields...)
		return err
	}
	httpAddress = fmt.Sprintf(":%v", httpPort)

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
	logger.Info(ctx, "starting service", loggingFields...)
	s, handler, err := setup(ctx)
	if err != nil {
		return err
	}
	defer s.GracefulStop()

	lis, err := net.Listen("tcp", grpcAddress)
	if err != nil {
		logger.Error(ctx, err, loggingFields...)
		return err
	}

	errChan := make(chan error)
	go func() {
		if err := s.Serve(lis); err != nil {
			logger.Error(ctx, err, loggingFields...)
			errChan <- err
		}
	}()

	go func() {
		if err := http.ListenAndServe(httpAddress, handler); err != nil {
			errChan <- err
		}
	}()

	for {
		select {
		case err := <-errChan:
			return err
		case <-ctx.Done():
			logger.Info(ctx, "shutting down", loggingFields...)
			return nil
		}
	}
}

func setup(ctx context.Context) (*grpc.Server, http.Handler, error) {
	s := grpc.NewServer()

	if err := echo.RegisterGrpc(s, decrementer); err != nil {
		logger.Error(ctx, err, loggingFields...)
		return nil, nil, err
	}

	conn, err := grpc.DialContext(ctx, httpAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error(ctx, err, loggingFields...)
		return nil, nil, err
	}

	handler := echo.RegisterHttp(conn)

	return s, handler, nil
}
