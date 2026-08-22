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
	"bufio"
	"context"
	"fmt"
	"net"
	"os/exec"
	"regexp"
	"runtime"
	"syscall"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	instancepb "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const emulatorStartupTimeout = 30 * time.Second

var validDBPattern = regexp.MustCompile("^projects/(?P<project>[^/]+)/instances/(?P<instance>[^/]+)/databases/(?P<database>[^/]+)$")

// setUpSpannerEmulator starts a real Cloud Spanner emulator. This lets these
// tests run on CI runners that have no container runtime available: on
// Linux (the only platform our CI runs on) gcloud ships a native emulator
// binary via the "cloud-spanner-emulator" component
// (`gcloud components install cloud-spanner-emulator`), so we drive it
// through `gcloud emulators spanner start` there.
//
// gcloud does not ship that component for macOS at all - not even for its
// own Docker-backed --use-docker mode, which still refuses to run without
// the component being present. Since macOS is presumed to only ever be used
// for local development, never CI, we bypass gcloud entirely there and run
// the official emulator image directly via `docker run`.
func setUpSpannerEmulator(ctx context.Context, t *testing.T) string {
	t.Helper()

	if runtime.GOOS == "darwin" {
		return setUpSpannerEmulatorDocker(ctx, t)
	}

	if _, err := exec.LookPath("gcloud"); err != nil {
		t.Skip("gcloud CLI not found on PATH; skipping test that requires the Spanner emulator")
	}

	grpcPort := mustFreePort(t)
	restPort := mustFreePort(t)
	endpoint := fmt.Sprintf("127.0.0.1:%d", grpcPort)

	cmd := exec.Command("gcloud", "emulators", "spanner", "start",
		"--host-port="+endpoint,
		fmt.Sprintf("--rest-port=%d", restPort),
	)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("Unable to open spanner emulator output pipe: %v", err)
	}
	cmd.Stderr = cmd.Stdout

	if err := cmd.Start(); err != nil {
		t.Fatalf("Unable to start spanner emulator: %v", err)
	}

	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			t.Logf("[spanner emulator] %s", scanner.Text())
		}
	}()

	t.Cleanup(func() { stopEmulatorProcess(cmd) })

	waitForEmulator(ctx, t, endpoint)

	return endpoint
}

// spannerEmulatorImage is the official Cloud Spanner emulator image, used
// directly (bypassing gcloud) on platforms gcloud doesn't support.
const spannerEmulatorImage = "gcr.io/cloud-spanner-emulator/emulator"

// setUpSpannerEmulatorDocker runs the Spanner emulator as a detached Docker
// container, for platforms where gcloud can't run the emulator itself.
func setUpSpannerEmulatorDocker(ctx context.Context, t *testing.T) string {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("gcloud has no Spanner emulator support on this platform, and docker was not found on PATH; skipping test that requires the Spanner emulator")
	}

	grpcPort := mustFreePort(t)
	restPort := mustFreePort(t)
	endpoint := fmt.Sprintf("127.0.0.1:%d", grpcPort)

	containerName := fmt.Sprintf("spanner-emulator-test-%d", grpcPort)

	cmd := exec.Command("docker", "run", "-d", "--rm",
		"--name", containerName,
		"-p", fmt.Sprintf("127.0.0.1:%d:9010", grpcPort),
		"-p", fmt.Sprintf("127.0.0.1:%d:9020", restPort),
		spannerEmulatorImage,
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Unable to start spanner emulator container: %v\n%s", err, out)
	}

	t.Cleanup(func() {
		if out, err := exec.Command("docker", "stop", containerName).CombinedOutput(); err != nil {
			t.Logf("Unable to stop spanner emulator container %s: %v\n%s", containerName, err, out)
		}
	})

	waitForEmulator(ctx, t, endpoint)

	return endpoint
}

// mustFreePort asks the OS for an ephemeral port and immediately releases it,
// so the emulator process can bind to it. This has an inherent (small) race
// with any other process on the machine, which is an accepted trade-off for
// test infrastructure.
func mustFreePort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Unable to find a free port: %v", err)
	}
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port
}

func waitForEmulator(ctx context.Context, t *testing.T, endpoint string) {
	t.Helper()

	deadline := time.Now().Add(emulatorStartupTimeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", endpoint, time.Second)
		if err == nil {
			conn.Close()
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("Context cancelled while waiting for spanner emulator: %v", ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	t.Fatalf("Spanner emulator did not become ready at %s within %s", endpoint, emulatorStartupTimeout)
}

// stopEmulatorProcess terminates the emulator gracefully, falling back to a
// hard kill if it doesn't exit promptly.
func stopEmulatorProcess(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}

	_ = cmd.Process.Signal(syscall.SIGTERM)

	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
	}
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
