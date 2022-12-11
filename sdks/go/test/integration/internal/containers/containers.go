package containers

import (
	"context"
	"testing"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
)

type ContainerOptionFn func(*testcontainers.ContainerRequest)

func WithPorts(ports []string) ContainerOptionFn {
	return func(option *testcontainers.ContainerRequest) {
		option.ExposedPorts = ports
	}
}

func NewContainer(
	ctx context.Context,
	t *testing.T,
	image string,
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

	container, err := testcontainers.GenericContainer(ctx, genericRequest)
	if err != nil {
		t.Fatalf("error creating container: %v", err)
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
