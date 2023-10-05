package test_framework

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/cache"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/common"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/environment"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/logging"
	echov1 "github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/proto/echo/v1"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/service/echo"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const (
	redisImage           = "redis:7.2"
	redisPort            = 6379
	QuotaSize     uint64 = 100
	QuotaInterval        = time.Second
)

var (
	QuotaId      = uuid.NewString()
	LoggerFields []logging.Field
	redisHost    string
	exposedPorts = []string{fmt.Sprintf("%v/tcp", redisPort)}

	container   testcontainers.Container
	Cache       *cache.RedisCache
	RedisClient *redis.Client

	EchoClient echov1.EchoServiceClient

	healthClient grpc_health_v1.HealthClient

	refresher *cache.Refresher

	timeoutChecks = 3 * time.Second

	lis net.Listener
	g   *grpc.Server

	logger       = logging.New("github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/test/test_framework")
	addressField logging.Field
)

func Run(ctx context.Context, m *testing.M) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := setup(ctx); err != nil {
		return err
	}
	errChan := make(chan error)
	done := make(chan int)
	go func() {
		done <- m.Run()
	}()
	for {
		select {
		case code := <-done:
			if err := teardown(ctx); err != nil {
				return err
			}
			os.Exit(code)
		case err := <-errChan:
			if e := teardown(ctx); e != nil {
				return errors.Join(err, e)
			}
			return err
		case <-ctx.Done():
			return nil
		}
	}
}

func Serve(ctx context.Context, errChan chan error, ready chan struct{}) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer g.GracefulStop()
	go func() {
		if err := refresher.Refresh(ctx, QuotaId, QuotaSize, QuotaInterval); err != nil {
			errChan <- fmt.Errorf("error refreshing cache: %w", err)
		}
	}()

	go func() {
		logger.Info(ctx, "starting services", addressField)
		if err := g.Serve(lis); err != nil {
			logger.Error(ctx, err, addressField)
			errChan <- err
		}
	}()

	go chain(ctx, ready, errChan, &healthChecker{
		timeout: timeoutChecks,
	}, &cacheChecker{
		timeout: timeoutChecks,
	})

	for {
		select {
		case <-ctx.Done():
			logger.Info(ctx, "shutting down services", addressField)
			return
		}
	}
}

func setup(ctx context.Context) error {
	if err := setupCache(ctx); err != nil {
		return err
	}

	if err := setupServiceAndClients(ctx); err != nil {
		return err
	}

	setupEnvironment()
	return nil
}

func setupCache(ctx context.Context) error {
	var err error
	req := testcontainers.ContainerRequest{
		Image:        redisImage,
		ExposedPorts: exposedPorts,
		WaitingFor:   wait.ForLog("Ready to accept connections"),
	}

	container, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return err
	}

	redisHost, err = container.Endpoint(ctx, "")
	if err != nil {
		return err
	}

	RedisClient = redis.NewClient(&redis.Options{
		Addr: redisHost,
	})

	Cache = (*cache.RedisCache)(RedisClient)

	refresher, err = cache.NewRefresher(ctx, Cache)
	if err != nil {
		return err
	}

	return nil
}

func setupServiceAndClients(ctx context.Context) error {
	var err error
	lis, err = net.Listen("tcp", ":")
	if err != nil {
		return err
	}

	opt := grpc.WithTransportCredentials(insecure.NewCredentials())
	echoConn, err := grpc.DialContext(ctx, lis.Addr().String(), opt)
	if err != nil {
		return err
	}
	EchoClient = echov1.NewEchoServiceClient(echoConn)

	healthConn, err := grpc.DialContext(ctx, lis.Addr().String(), opt)
	if err != nil {
		return err
	}
	healthClient = grpc_health_v1.NewHealthClient(healthConn)

	g = grpc.NewServer()
	if err = echo.RegisterGrpc(g, Cache); err != nil {
		return err
	}

	return echo.RegisterHttp(ctx, echoConn)
}

func setupEnvironment() {
	common.QuotaId.MustDefault(uuid.New().String())
	common.CacheHost.MustDefault(redisHost)
	common.QuotaSize.MustDefault(strconv.FormatUint(QuotaSize, 10))
	common.QuotaRefreshInterval.MustDefault(QuotaInterval.String())

	for _, v := range []environment.Variable{
		common.QuotaId,
		common.CacheHost,
		common.QuotaSize,
		common.QuotaRefreshInterval,
	} {
		LoggerFields = append(LoggerFields, logging.Field{
			Key:   v.Key(),
			Value: v.Value(),
		})
	}

	addressField = logging.Field{
		Key:   "addressField",
		Value: lis.Addr().String(),
	}
}

func teardown(ctx context.Context) error {
	return container.Terminate(ctx)
}

type healthChecker struct {
	timeout time.Duration
}

func (checker *healthChecker) Await(ctx context.Context, done chan struct{}, errChan chan error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	n := time.Millisecond * 3
	durationBetweenTries := checker.timeout / n
	tick := time.Tick(durationBetweenTries)
	watcher, err := healthClient.Watch(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		logger.Error(ctx, err, addressField)
		errChan <- err
	}
	for {
		select {
		case <-tick:
			n--
			if n <= 0 {
				return
			}
			_, err = watcher.Recv()
			if err == nil {
				done <- struct{}{}
				return
			}
			if n == 0 {
				logger.Error(ctx, err, addressField)
				errChan <- err
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

type cacheChecker struct {
	timeout time.Duration
}

func (checker *cacheChecker) Await(ctx context.Context, done chan struct{}, errChan chan error) {
	var err error
	var v int64
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	n := time.Millisecond * 3
	durationBetweenTries := checker.timeout / n
	tick := time.Tick(durationBetweenTries)
	for {
		select {
		case <-tick:
			n--
			if n <= 0 {
				errChan <- fmt.Errorf("failure checking cache with repeated attempts, err %w", err)
				return
			}
			v, err = RedisClient.Get(ctx, QuotaId).Int64()
			if err != nil || v <= 0 {
				continue
			}
			done <- struct{}{}
			return
		}
	}
}

type future interface {
	Await(ctx context.Context, done chan struct{}, errChan chan error)
}

func chain(ctx context.Context, done chan struct{}, errChan chan error, futures ...future) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	n := len(futures)
	if n == 0 {
		return
	}
	checks := make(chan struct{})
	errs := make(chan error)
	for _, f := range futures {
		go f.Await(ctx, checks, errs)
	}
	for {
		select {
		case err := <-errs:
			errChan <- err
			return
		case <-checks:
			n--
			if n <= 0 {
				done <- struct{}{}
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
