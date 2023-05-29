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

package spannerio

import (
	"context"
	"fmt"
	"google.golang.org/api/option/internaloption"
	"regexp"
	"testing"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	instancepb "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/beam/sdks/v2/go/test/integration/internal/containers"
)

const (
	spannerImage = "cloud-spanner-emulator/emulator:latest"
	maxRetries   = 5
)

var (
	spannerHost    = "localhost:9010"
	spannerPorts   = []string{"9010/tcp", "9020/tcp"}
	validDBPattern = regexp.MustCompile("^projects/(?P<project>[^/]+)/instances/(?P<instance>[^/]+)/databases/(?P<database>[^/]+)$")
)

func setUpTestContainer(ctx context.Context, t *testing.T) string {
	t.Helper()

	container := containers.NewContainer(
		ctx,
		t,
		spannerImage,
		maxRetries,
		containers.WithPorts(spannerPorts),
		containers.WithWaitStrategy(wait.ForLog("Cloud Spanner emulator running")),
	)

	mappedPort := containers.Port(ctx, t, container, nat.Port(spannerPorts[0]))

	hostIP, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("Unable to get spanner host: %v", err)
	}

	return fmt.Sprintf("%s:%s", hostIP, mappedPort)
}

func NewClient(ctx context.Context, t *testing.T, endpoint string, db string) *spanner.Client {
	t.Helper()

	opts := []option.ClientOption{
		option.WithEndpoint(endpoint),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
		internaloption.SkipDialSettingsValidation(),
	}

	client, err := spanner.NewClient(ctx, db, opts...)
	if err != nil {
		t.Fatalf("Unable to create spanner client: %v", err)
	}

	t.Cleanup(client.Close)

	return client
}

func NewAdminClient(ctx context.Context, t *testing.T, endpoint string) *database.DatabaseAdminClient {
	opts := []option.ClientOption{
		option.WithEndpoint(endpoint),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
		internaloption.SkipDialSettingsValidation(),
	}

	// Admin clients do not respect 'SPANNER_EMULATOR_HOST' currently.
	admin, err := database.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		t.Fatalf("Unable to create spanner admin client: %v", err)
	}

	t.Cleanup(func() {
		if err := admin.Close(); err != nil {
			t.Fatalf("Unable to close spanner admin client: %v", err)
		}
	})

	return admin
}

func NewInstanceAdminClient(ctx context.Context, t *testing.T, endpoint string) *instance.InstanceAdminClient {
	opts := []option.ClientOption{
		option.WithEndpoint(endpoint),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
		internaloption.SkipDialSettingsValidation(),
	}

	// Admin clients do not respect 'SPANNER_EMULATOR_HOST' currently.
	instanceAdmin, err := instance.NewInstanceAdminClient(ctx, opts...)
	if err != nil {
		t.Fatalf("Unable to create spanner instance admin client: %v", err)
	}

	t.Cleanup(func() {
		if err := instanceAdmin.Close(); err != nil {
			t.Fatalf("Unable to close spanner instance admin client: %v", err)
		}
	})

	return instanceAdmin
}

func CreateInstance(ctx context.Context, t *testing.T, instanceAdmin *instance.InstanceAdminClient, db string) {
	t.Helper()

	projectId, instanceId, _ := parseDatabaseName(t, db)

	op, err := instanceAdmin.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", projectId),
		InstanceId: instanceId,
		Instance: &instancepb.Instance{
			DisplayName: instanceId,
		},
	})

	if err != nil {
		t.Fatalf("Unable to create spanner instance create operation: %v", err)
	}

	if _, err := op.Wait(ctx); err != nil {
		t.Fatalf("Unable to create spanner instance: %v", err)
	}
}

func DeleteInstance(ctx context.Context, t *testing.T, instanceAdmin *instance.InstanceAdminClient, db string) {
	t.Helper()

	projectId, instanceId, _ := parseDatabaseName(t, db)

	err := instanceAdmin.DeleteInstance(ctx, &instancepb.DeleteInstanceRequest{Name: fmt.Sprintf("projects/%s/instances/%s", projectId, instanceId)})

	if err != nil {
		t.Fatalf("Unable to create spanner instance create operation: %v", err)
	}
}

func CreateDatabase(ctx context.Context, t *testing.T, adminClient *database.DatabaseAdminClient, db string) {
	t.Helper()

	projectId, instanceId, databaseId := parseDatabaseName(t, db)

	op, err := adminClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%v/instances/%s", projectId, instanceId),
		CreateStatement: "CREATE DATABASE `" + databaseId + "`",
	})

	if err != nil {
		t.Fatalf("Unable to create spanner database create operation: %v", err)
	}

	if _, err := op.Wait(ctx); err != nil {
		t.Fatalf("Unable to create spanner database: %v", err)
	}
}

func DropDatabase(ctx context.Context, t *testing.T, adminClient *database.DatabaseAdminClient, db string) {
	t.Helper()

	err := adminClient.DropDatabase(ctx, &adminpb.DropDatabaseRequest{Database: db})

	if err != nil {
		t.Fatalf("Unable to create spanner database create operation: %v", err)
	}
}

func CreateTable(ctx context.Context, t *testing.T, adminClient *database.DatabaseAdminClient, db string, ddls []string) {
	t.Helper()

	op, err := adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   db,
		Statements: ddls,
	})

	if err != nil {
		t.Fatalf("Unable to create spanner schema operation: %v", err)
	}

	if err := op.Wait(ctx); err != nil {
		t.Fatalf("Unable to create spanner schema: %v", err)
	}
}

func parseDatabaseName(t *testing.T, db string) (project, instance, database string) {
	matches := validDBPattern.FindStringSubmatch(db)
	if len(matches) == 0 {
		t.Fatalf("Failed to parse database name from %q according to pattern %q", db, validDBPattern.String())
	}
	return matches[1], matches[2], matches[3]
}
