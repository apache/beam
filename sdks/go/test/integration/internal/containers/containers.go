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

// Package containers contains utilities for running test containers in integration tests.
package containers

import (
	"context"
	"testing"
	"time"

	retry "github.com/avast/retry-go/v4"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type ContainerOptionFn func(*testcontainers.ContainerRequest)

func WithEnv(env map[string]string) ContainerOptionFn {
	return func(option *testcontainers.ContainerRequest) {
		option.Env = env
	}
}

func WithHostname(hostname string) ContainerOptionFn {
	return func(option *testcontainers.ContainerRequest) {
		option.Hostname = hostname
	}
}

func WithPorts(ports []string) ContainerOptionFn {
	return func(option *testcontainers.ContainerRequest) {
		option.ExposedPorts = ports
	}
}

func WithWaitStrategy(waitStrategy wait.Strategy) ContainerOptionFn {
	return func(option *testcontainers.ContainerRequest) {
		option.WaitingFor = waitStrategy
	}
}

func NewContainer(
	ctx context.Context,
	t *testing.T,
	image string,
	maxRetries int,
	opts ...ContainerOptionFn,
) testcontainers.Container {
	t.Helper()

	request := testcontainers.ContainerRequest{Image: image}

	for _, opt := range opts {
		opt(&request)
	}

	genericRequest := testcontainers.GenericContainerRequest{
		ContainerRequest: request,
		Started:          true,
	}

	retryOpts := []retry.Option{
		retry.Attempts(uint(maxRetries)),
		retry.DelayType(func(n uint, err error, config *retry.Config) time.Duration {
			if n == 0 {
				return time.Second
			}
			return retry.BackOffDelay(n, err, config)
		}),
	}

	var container testcontainers.Container
	var err error
	err = retry.Do(func() error {
		container, err = testcontainers.GenericContainer(ctx, genericRequest)
		return err
	}, retryOpts...)
	if err != nil {
		t.Fatalf("failed to start container with %v retries: %v", maxRetries, err)
	}

	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Fatalf("error terminating container: %v", err)
		}
	})

	return container
}

func Port(
	ctx context.Context,
	t *testing.T,
	container testcontainers.Container,
	port nat.Port,
) string {
	t.Helper()

	mappedPort, err := container.MappedPort(ctx, port)
	if err != nil {
		t.Fatalf("error getting mapped port: %v", err)
	}

	return mappedPort.Port()
}
