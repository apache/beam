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

package internal

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/jobservices"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/urns"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/worker"
	"golang.org/x/exp/slog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// TODO move environment handling to the worker package.

func runEnvironment(ctx context.Context, j *jobservices.Job, env string, wk *worker.W) error {
	logger := slog.With(slog.String("envID", wk.Env))
	// TODO fix broken abstraction.
	// We're starting a worker pool here, because that's the loopback environment.
	// It's sort of a mess, largely because of loopback, which has
	// a different flow from a provisioned docker container.
	e := j.Pipeline.GetComponents().GetEnvironments()[env]
	switch e.GetUrn() {
	case urns.EnvExternal:
		ep := &pipepb.ExternalPayload{}
		if err := (proto.UnmarshalOptions{}).Unmarshal(e.GetPayload(), ep); err != nil {
			logger.Error("unmarshing external environment payload", "error", err)
		}
		go func() {
			externalEnvironment(ctx, ep, wk)
			slog.Debug("environment stopped", slog.String("job", j.String()))
		}()
		return nil
	case urns.EnvDocker:
		dp := &pipepb.DockerPayload{}
		if err := (proto.UnmarshalOptions{}).Unmarshal(e.GetPayload(), dp); err != nil {
			logger.Error("unmarshing docker environment payload", "error", err)
		}
		return dockerEnvironment(ctx, logger, dp, wk, j.ArtifactEndpoint())
	default:
		return fmt.Errorf("environment %v with urn %v unimplemented", env, e.GetUrn())
	}
}

func externalEnvironment(ctx context.Context, ep *pipepb.ExternalPayload, wk *worker.W) {
	conn, err := grpc.Dial(ep.GetEndpoint().GetUrl(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(fmt.Sprintf("unable to dial sdk worker %v: %v", ep.GetEndpoint().GetUrl(), err))
	}
	defer conn.Close()
	pool := fnpb.NewBeamFnExternalWorkerPoolClient(conn)

	endpoint := &pipepb.ApiServiceDescriptor{
		Url: wk.Endpoint(),
	}
	pool.StartWorker(ctx, &fnpb.StartWorkerRequest{
		WorkerId:          wk.ID,
		ControlEndpoint:   endpoint,
		LoggingEndpoint:   endpoint,
		ArtifactEndpoint:  endpoint,
		ProvisionEndpoint: endpoint,
		Params:            ep.GetParams(),
	})
	// Job processing happens here, but orchestrated by other goroutines
	// This goroutine blocks until the context is cancelled, signalling
	// that the pool runner should stop the worker.
	<-ctx.Done()

	// Previous context cancelled so we need a new one
	// for this request.
	pool.StopWorker(context.Background(), &fnpb.StopWorkerRequest{
		WorkerId: wk.ID,
	})
}

func dockerEnvironment(ctx context.Context, logger *slog.Logger, dp *pipepb.DockerPayload, wk *worker.W, artifactEndpoint string) error {
	logger = logger.With("worker_id", wk.ID, "image", dp.GetContainerImage())
	// TODO: Ensure artifact server downloads to a consistent cache, and doesn't re-download artifacts.
	// Ensure Go worker rebuilds are consistent?
	// TODO: Fail sensibly when the image can't be downloaded or started without process crash.

	var credentialArgs []string
	// TODO better abstract cloud specific auths.
	const gcloudCredsEnv = "GOOGLE_APPLICATION_CREDENTIALS"
	gcloudCredsFile, ok := os.LookupEnv(gcloudCredsEnv)
	if ok {
		_, err := os.Stat(gcloudCredsFile)
		// File exists
		if err == nil {
			dockerGcloudCredsFile := "/docker_cred_file.json"
			credentialArgs = append(credentialArgs,
				"--mount",
				fmt.Sprintf("type=bind,source=%v,target=%v", gcloudCredsFile, dockerGcloudCredsFile),
				"--env",
				fmt.Sprintf("%v=%v", gcloudCredsEnv, dockerGcloudCredsFile),
			)
		}
	}

	logger.Info("attempting to pull docker image for environment")
	pullCmd := exec.CommandContext(ctx, "docker", "pull", dp.GetContainerImage())
	pullCmd.Start()
	pullCmd.Wait()

	runArgs := []string{"run", "-d", "--network=host"}
	runArgs = append(runArgs, credentialArgs...)
	runArgs = append(runArgs,
		dp.GetContainerImage(),
		fmt.Sprintf("--id=%v-%v", wk.JobKey, wk.Env),
		fmt.Sprintf("--control_endpoint=%v", wk.Endpoint()),
		fmt.Sprintf("--artifact_endpoint=%v", artifactEndpoint),
		fmt.Sprintf("--provision_endpoint=%v", wk.Endpoint()),
		fmt.Sprintf("--logging_endpoint=%v", wk.Endpoint()),
	)

	runCmd := exec.CommandContext(ctx, "docker", runArgs...)
	slog.Info("docker run command", "run_cmd", runCmd.String())
	var buf bytes.Buffer
	runCmd.Stdout = &buf
	if err := runCmd.Start(); err != nil {
		return fmt.Errorf("unable to start container image %v with docker for env %v", dp.GetContainerImage(), wk.Env)
	}
	if err := runCmd.Wait(); err != nil {
		return fmt.Errorf("docker run failed for image %v with docker for env %v", dp.GetContainerImage(), wk.Env)
	}

	containerID := strings.TrimSpace(buf.String())
	if containerID == "" {
		return fmt.Errorf("docker run failed for image %v with docker for env %v - no container ID", dp.GetContainerImage(), wk.Env)
	}
	logger.Info("docker container is started", "container_id", containerID)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		buf.Reset()
		inspectCmd := exec.CommandContext(ctx, "docker", "inspect", "-f", "{{.State.Status}}", containerID)
		inspectCmd.Stdout = &buf
		inspectCmd.Start()
		inspectCmd.Wait()

		status := strings.TrimSpace(buf.String())
		switch status {
		case "running":
			logger.Info("docker container is running", "container_id", containerID)
			return nil
		case "dead", "exited":
			logDumpCmd := exec.CommandContext(ctx, "docker", "container", "logs", containerID)
			buf.Reset()
			logDumpCmd.Stdout = &buf
			logDumpCmd.Stderr = &buf
			logDumpCmd.Start()
			logDumpCmd.Wait()
			logger.Error("SDK failed to start.", "final_status", status, "log", buf.String())
			return fmt.Errorf("docker run failed for image %v with docker for env %v - containerID %v - log:\n%v", dp.GetContainerImage(), wk.Env, containerID, buf.String())
		default:
			logger.Info("docker container status", "container_id", containerID, "status", status)
		}
	}
	return nil
}
