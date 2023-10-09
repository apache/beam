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

package echo

import (
	"context"
	"fmt"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/cache"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/logging"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/service/echo"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"net"
	"net/http"
	"reflect"
	"testing"
	"time"
)

var (
	// Cache related variables
	redisContainerReq = testcontainers.ContainerRequest{
		Image:        "redis:latest",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
	}
	container testcontainers.Container
	refresher *cache.Refresher
	quotaId          = uuid.NewString()
	size      uint64 = 10
	interval         = time.Second * 1
	r         *redis.Client
	rc        *cache.RedisCache

	// Endpoint related variables
	gLis net.Listener
	hLis net.Listener
	g    *grpc.Server
	h    http.Handler

	// Logger related variables
	t      = reflect.TypeOf((afterFunc)(nil))
	logger = logging.New(t.PkgPath())
	lf     []logging.Field
)

type afterFunc func()

func TestEcho(t *testing.T) {

}

func TestMain(m *testing.M) {
	ctx := context.Background()
	if err := run(ctx, time.Second*10, m); err != nil {
		panic(err)
	}

	logger.Info(ctx, "[run] terminating container", lf...)

	if err := container.Terminate(ctx); err != nil {
		panic(err)
	}
}

// TODO: clean up after https://github.com/apache/beam/issues/28859
func setupCache(ctx context.Context, ready afterFunc, errChan chan error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	context.AfterFunc(ctx, ready)
	containerReady := make(chan struct{})

	go func() {
		var err error
		container, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: redisContainerReq,
			Started:          true,
		})
		if err != nil {
			errChan <- err
		}
		containerReady <- struct{}{}
	}()

	for {
		select {
		case <-containerReady:
			endpoint, err := container.Endpoint(ctx, "")
			if err != nil {
				errChan <- err
				return
			}
			lf = append(lf, logging.Field{
				Key:   "cacheHost",
				Value: endpoint,
			})

			logger.Info(ctx, "[setupCache] instantiating cache.Refresher START", lf...)

			r = redis.NewClient(&redis.Options{
				Addr: endpoint,
			})
			rc = (*cache.RedisCache)(r)

			refresher, err = cache.NewRefresher(ctx, rc)
			if err != nil {
				errChan <- err
				return
			}

			logger.Info(ctx, "[setupCache] instantiating cache.Refresher OK", lf...)

			return

		case <-ctx.Done():
			if ctx.Err() != nil {
				errChan <- ctx.Err()
			}
		}
	}
}

// TODO: clean up after https://github.com/apache/beam/issues/28859
func run(ctx context.Context, timeout time.Duration, m *testing.M) error {
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	codeChan := make(chan int)
	errChan := make(chan error)
	setupReady := make(chan struct{})
	quotaReady := make(chan struct{})
	servicesReady := make(chan struct{})
	healthCheckReady := make(chan struct{})

	go setupCache(ctx, func() {
		logger.Info(ctx, "[run] setupCache done", lf...)
		defer refresher.Stop()
		setupReady <- struct{}{}
	}, errChan)

	for {
		select {
		case err := <-errChan:
			return err
		case <-healthCheckReady:
			logger.Info(ctx, "[run] health checks passed; running tests", lf...)
			go func() {
				codeChan <- m.Run()
			}()

		case <-servicesReady:
			logger.Info(ctx, "[run] services ready; performing health check", lf...)
			go func() {
				if err := checkServices(ctx, timeout, func() {
					healthCheckReady <- struct{}{}
				}); err != nil {
					errChan <- err
				}
			}()

		case <-quotaReady:
			logger.Info(ctx, "[run] quota ready", lf...)
			logger.Info(ctx, "[run] provisioning services START", lf...)
			go func() {
				var err error
				g = grpc.NewServer()
				defer g.GracefulStop()

				if gLis, err = net.Listen("tcp", ":"); err != nil {
					errChan <- err
				}

				if hLis, err = net.Listen("tcp", ":"); err != nil {
					errChan <- err
				}
				defer hLis.Close()

				if h, err = echo.Register(g, rc); err != nil {
					errChan <- err
				}
				lf = append(lf, logging.Field{
					Key:   "grpcEndpoint",
					Value: gLis.Addr().String(),
				}, logging.Field{
					Key:   "httpEndpoint",
					Value: hLis.Addr().String(),
				})
				logger.Info(ctx, "[run] provisioning services OK", lf...)
				servicesReady <- struct{}{}
			}()

		case <-setupReady:
			go func() {
				logger.Info(ctx, "[run] starting cache.Refresher", lf...)
				if err := refresher.Refresh(ctx, quotaId, size, interval); err != nil {
					errChan <- err
				}
			}()

			go func() {
				logger.Info(ctx, "[run] checking quota", lf...)
				if err := checkQuota(ctx, timeout, func() {
					quotaReady <- struct{}{}
				}); err != nil {
					errChan <- err
				}
			}()

		case <-codeChan:
			logger.Info(ctx, "[run] finished tests", lf...)
			return nil
		case <-ctx.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
	}
}

// TODO: clean up after https://github.com/apache/beam/issues/28859
func checkQuota(ctx context.Context, timeout time.Duration, ready afterFunc) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	context.AfterFunc(ctx, ready)

	v, err := r.Exists(ctx, quotaId).Result()
	logger.Debug(ctx, fmt.Sprintf("[quota check] quota: %v, err: %v", v, err), lf...)
	if err == nil && v > 0 {
		return nil
	}
	tick := time.Tick(time.Second)
	for {
		select {
		case <-tick:
			v, err := r.Exists(ctx, quotaId).Result()
			logger.Debug(ctx, fmt.Sprintf("[quota check] quota: %v, err: %v", v, err), lf...)
			if err == nil && v > 0 {
				return nil
			}

		case <-ctx.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
	}
}

// TODO: clean up after https://github.com/apache/beam/issues/28859
func checkServices(ctx context.Context, timeout time.Duration, ready afterFunc) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	context.AfterFunc(ctx, ready)

	tick := time.Tick(time.Second)
	for {
		select {
		case <-tick:
			v, err := r.Exists(ctx, quotaId).Result()
			if err == nil && v > 0 {
				return nil
			}

		case <-ctx.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
	}
}
