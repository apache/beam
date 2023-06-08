package main

import (
	"context"
	"fmt"
	"net"

	"github.com/apache/beam/test-infra/pipelines/src/main/go/internal/cache"
	"github.com/apache/beam/test-infra/pipelines/src/main/go/internal/environment"
	"github.com/apache/beam/test-infra/pipelines/src/main/go/internal/k8s"
	"github.com/apache/beam/test-infra/pipelines/src/main/go/internal/logging"
	"github.com/apache/beam/test-infra/pipelines/src/main/go/internal/quota"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
)

var (
	port           environment.Variable = "PORT"
	refresherImage environment.Variable = "REFRESHER_IMAGE"
	namespace      environment.Variable = "NAMESPACE"

	spec = &quota.ServiceSpec{
		RefresherServiceSpec: &quota.RefresherServiceSpec{
			ContainerName: "refresher",
			Image:         refresherImage.Value(),
		},
	}

	logger = logging.MustLogger(context.Background(), "quota")

	required = []environment.Variable{
		port,
		cache.Host,
		namespace,
		refresherImage,
	}

	env     = environment.Map(required...)
	address = fmt.Sprintf(":%s", port.Value())
)

func init() {
	ctx := context.Background()
	if err := environment.Missing(required...); err != nil {
		logger.Fatal(ctx, err.Error(), logging.Any("env", env))
	}

	if err := vars(ctx); err != nil {
		logger.Fatal(ctx, err.Error(), logging.Any("env", env))
	}
}

func vars(ctx context.Context) error {
	redisClient := redis.NewClient(&redis.Options{
		Addr: cache.Host.Value(),
	})

	cacheClient := (*cache.RedisCache)(redisClient)

	spec.Cache = cacheClient
	spec.Publisher = cacheClient

	k8sClient, err := k8s.NewDefaultClient()
	if err != nil {
		return err
	}

	ns := k8sClient.Namespace(namespace.Value())
	spec.JobsClient = k8sClient.Jobs(ns)

	if err := redisClient.Ping(ctx).Err(); err != nil {
		return err
	}

	logger.Info(ctx, "pinged cache host ok", logging.String("host", cache.Host.Value()))

	return nil
}

func main() {
	ctx, cancel := context.WithCancel(signals.SetupSignalHandler())
	defer cancel()

	lis, err := net.Listen("tcp", address)
	if err != nil {
		logger.Error(ctx, err.Error(), logging.Any("env", env))
	}

	svc := grpc.NewServer()

	if err := quota.RegisterService(ctx, svc, spec); err != nil {
		logger.Error(ctx, err.Error(), logging.Any("env", env))
		return
	}

	go func() {
		if err := svc.Serve(lis); err != nil {
			logger.Error(ctx, err.Error(), logging.Any("env", env))
			return
		}
	}()

	logger.Info(ctx, "started quota service", logging.Any("env", env))

	for {
		select {
		case <-ctx.Done():
			svc.GracefulStop()
			return
		}
	}
}
