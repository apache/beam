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
	"io"
	"log/slog"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"time"

	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/jobservices"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/urns"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	dcli "github.com/moby/moby/client"
)

// TODO move environment handling to the worker package.

func runEnvironment(ctx context.Context, j *jobservices.Job, env string, wk *worker.W) error {
	logger := j.Logger.With(slog.String("envID", wk.Env))
	e := j.Pipeline.GetComponents().GetEnvironments()[env]

	if e.GetUrn() == urns.EnvAnyOf {
		// We've been given a choice!
		ap := &pipepb.AnyOfEnvironmentPayload{}
		if err := (proto.UnmarshalOptions{}).Unmarshal(e.GetPayload(), ap); err != nil {
			logger.Error("unmarshaling any environment payload", "error", err)
			return err
		}
		e = selectAnyOfEnv(ap)
		logger.Info("AnyEnv resolved", "selectedUrn", e.GetUrn(), "worker", wk.ID)
		// Process the environment as normal.
	}

	switch e.GetUrn() {
	case urns.EnvExternal:
		ep := &pipepb.ExternalPayload{}
		if err := (proto.UnmarshalOptions{}).Unmarshal(e.GetPayload(), ep); err != nil {
			logger.Error("unmarshaling external environment payload", "error", err)
			return err
		}
		go func() {
			externalEnvironment(ctx, ep, wk)
			logger.Debug("environment stopped", slog.String("job", j.String()))
		}()
		return nil
	case urns.EnvDocker:
		dp := &pipepb.DockerPayload{}
		if err := (proto.UnmarshalOptions{}).Unmarshal(e.GetPayload(), dp); err != nil {
			logger.Error("unmarshaling docker environment payload", "error", err)
			return err
		}
		return dockerEnvironment(ctx, logger, dp, wk, wk.ArtifactEndpoint)
	case urns.EnvProcess:
		pp := &pipepb.ProcessPayload{}
		if err := (proto.UnmarshalOptions{}).Unmarshal(e.GetPayload(), pp); err != nil {
			logger.Error("unmarshaling process environment payload", "error", err)
			return err
		}
		go func() {
			processEnvironment(ctx, logger, pp, wk)
			logger.Debug("environment stopped", slog.String("job", j.String()))
		}()
		return nil
	default:
		return fmt.Errorf("environment %v with urn %v unimplemented", env, e.GetUrn())
	}
}

func selectAnyOfEnv(ap *pipepb.AnyOfEnvironmentPayload) *pipepb.Environment {
	// Prefer external, then process, then docker, unknown environments are 0.
	ranks := map[string]int{
		urns.EnvDocker:   1,
		urns.EnvProcess:  5,
		urns.EnvExternal: 10,
	}

	envs := ap.GetEnvironments()

	slices.SortStableFunc(envs, func(a, b *pipepb.Environment) int {
		rankA := ranks[a.GetUrn()]
		rankB := ranks[b.GetUrn()]

		// Reverse the comparison so our favourite is at the front
		switch {
		case rankA > rankB:
			return -1 // Usually "greater than" would be 1
		case rankA < rankB:
			return 1
		}
		return 0
	})
	// Pick our favourite.
	return envs[0]
}

func externalEnvironment(ctx context.Context, ep *pipepb.ExternalPayload, wk *worker.W) {
	conn, err := grpc.Dial(ep.GetEndpoint().GetUrl(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(fmt.Sprintf("unable to dial sdk worker pool %v: %v", ep.GetEndpoint().GetUrl(), err))
	}
	defer conn.Close()
	pool := fnpb.NewBeamFnExternalWorkerPoolClient(conn)

	endpoint := &pipepb.ApiServiceDescriptor{
		Url: wk.Endpoint(),
	}

	// Use a background context for these workers to avoid pre-mature
	// cancelation issues when starting them.
	bgContext := context.Background()

	resp, err := pool.StartWorker(bgContext, &fnpb.StartWorkerRequest{
		WorkerId:          wk.ID,
		ControlEndpoint:   endpoint,
		LoggingEndpoint:   endpoint,
		ArtifactEndpoint:  endpoint,
		ProvisionEndpoint: endpoint,
		Params:            ep.GetParams(),
	})

	if str := resp.GetError(); err != nil || str != "" {
		panic(fmt.Sprintf("unable to start sdk worker %v error: %v, resp: %v", ep.GetEndpoint().GetUrl(), err, prototext.Format(resp)))
	}

	// Job processing happens here, but orchestrated by other goroutines
	// This goroutine blocks until the context is cancelled, signalling
	// that the pool runner should stop the worker.
	<-ctx.Done()

	// Previous context cancelled so we need a new one
	// for this request.
	_, err = pool.StopWorker(bgContext, &fnpb.StopWorkerRequest{
		WorkerId: wk.ID,
	})
	if err != nil {
		slog.Warn("StopWorker failed", "worker", wk, "error", err)
	}
	wk.Stop()
}

func dockerEnvironment(ctx context.Context, logger *slog.Logger, dp *pipepb.DockerPayload, wk *worker.W, artifactEndpoint string) error {
	logger = logger.With("worker_id", wk.ID, "image", dp.GetContainerImage())

	// TODO consider preserving client?
	cli, err := dcli.New(dcli.FromEnv)
	if err != nil {
		return fmt.Errorf("couldn't connect to docker:%w", err)
	}

	// TODO abstract mounting cloud specific auths better.
	const gcloudCredsEnv = "GOOGLE_APPLICATION_CREDENTIALS"
	gcloudCredsFile, ok := os.LookupEnv(gcloudCredsEnv)
	var mounts []mount.Mount
	var envs []string
	if ok {
		_, err := os.Stat(gcloudCredsFile)
		// File exists
		if err == nil {
			dockerGcloudCredsFile := "/docker_cred_file.json"
			mounts = append(mounts, mount.Mount{
				Type:   "bind",
				Source: gcloudCredsFile,
				Target: dockerGcloudCredsFile,
			})
			credEnv := fmt.Sprintf("%v=%v", gcloudCredsEnv, dockerGcloudCredsFile)
			envs = append(envs, credEnv)
		}
	} else {
		logger.Debug("local GCP credentials environment variable not found")
	}
	if _, err := cli.ImageInspect(ctx, dp.GetContainerImage()); err != nil {
		// We don't have a local image, so we should pull it.
		if rc, err := cli.ImagePull(ctx, dp.GetContainerImage(), dcli.ImagePullOptions{}); err == nil {
			// Copy the output, but discard it so we can wait until the image pull is finished.
			io.Copy(io.Discard, rc)
			rc.Close()
		} else {
			logger.Warn("unable to pull image and it's not local", "error", err)
		}
	}
	logger.Debug("creating container", "envs", envs, "mounts", mounts)

	cmd := []string{
		fmt.Sprintf("--id=%v", wk.ID),
		fmt.Sprintf("--control_endpoint=%v", wk.Endpoint()),
		fmt.Sprintf("--artifact_endpoint=%v", artifactEndpoint),
		fmt.Sprintf("--provision_endpoint=%v", wk.Endpoint()),
		fmt.Sprintf("--logging_endpoint=%v", wk.Endpoint()),
	}
	ccr, err := cli.ContainerCreate(ctx, dcli.ContainerCreateOptions{
		Config: &container.Config{
			Image: dp.GetContainerImage(),
			Cmd:   cmd,
			Env:   envs,
			Tty:   false,
		},
		HostConfig: &container.HostConfig{
			NetworkMode: "host",
			Mounts:      mounts,
			AutoRemove:  true,
		},
	})
	if err != nil {
		cli.Close()
		return fmt.Errorf("unable to create container image %v with docker for env %v, err: %w", dp.GetContainerImage(), wk.Env, err)
	}
	containerID := ccr.ID
	logger = logger.With("container", containerID)

	_, err = cli.ContainerStart(ctx, containerID, dcli.ContainerStartOptions{})
	if err != nil {
		cli.Close()
		return fmt.Errorf("unable to start container image %v with docker for env %v, err: %w", dp.GetContainerImage(), wk.Env, err)
	}

	logger.Debug("container started")
	logger.Debug("container start command", "cmd", cmd)

	// Start goroutine to wait on container state.
	go func() {
		defer cli.Close()
		defer wk.Stop()
		defer func() {
			logger.Debug("container stopped")
		}()

		bgctx := context.Background()

		// Wait for either context cancellation or container to stop
		type waitResult struct {
			err error
		}
		done := make(chan waitResult)
		go func() {
			result := cli.ContainerWait(bgctx, containerID, dcli.ContainerWaitOptions{
				Condition: container.WaitConditionNotRunning,
			})
			// Error is a channel in the new API
			err := <-result.Error
			done <- waitResult{err: err}
		}()

		select {
		case <-ctx.Done():
			rc, err := cli.ContainerLogs(bgctx, containerID, dcli.ContainerLogsOptions{Details: true, ShowStdout: true, ShowStderr: true})
			if err != nil {
				logger.Error("error fetching container logs error on context cancellation", "error", err)
			}
			if rc != nil {
				defer rc.Close()
				var buf bytes.Buffer
				stdcopy.StdCopy(&buf, &buf, rc)
				logger.Info("container being killed", slog.Any("cause", context.Cause(ctx)))
				msgs, err := strconv.Unquote(buf.String())
				if err != nil {
					msgs = buf.String()
				}
				logger.Debug("container log", "log", msgs)
			}
			// Can't use command context, since it's already canceled here.
			_, err = cli.ContainerKill(bgctx, containerID, dcli.ContainerKillOptions{})
			if err != nil {
				logger.Error("docker container kill error", "error", err)
			}
		case result := <-done:
			// Container stopped on its own
			if result.err != nil {
				logger.Error("docker container wait error", "error", result.err)
				// Fetch and log container output on error
				rc, err := cli.ContainerLogs(bgctx, containerID, dcli.ContainerLogsOptions{Details: true, ShowStdout: true, ShowStderr: true})
				if err != nil {
					logger.Error("failed to fetch container logs after wait error", "error", err)
				} else {
					defer rc.Close()
					var buf bytes.Buffer
					stdcopy.StdCopy(&buf, &buf, rc)
					logger.Error("container logs after wait error", "log", buf.String())
				}
			} else {
				logger.Info("container terminated on its own")
			}
		}
	}()

	return nil
}

func processEnvironment(ctx context.Context, logger *slog.Logger, pp *pipepb.ProcessPayload, wk *worker.W) {
	defer wk.Stop()

	cmd := exec.CommandContext(ctx, pp.GetCommand(), "--id='"+wk.ID+"'", "--provision_endpoint="+wk.Endpoint())
	logger.Debug("starting process", "cmd", cmd.String())

	cmd.WaitDelay = time.Millisecond * 100
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	cmd.Env = os.Environ()

	for k, v := range pp.GetEnv() {
		cmd.Env = append(cmd.Environ(), fmt.Sprintf("%v=%v", k, v))
	}
	if err := cmd.Start(); err != nil {
		logger.Error("process failed to start", "error", err)
		return
	}
	// Job processing happens here, but orchestrated by other goroutines
	// This call blocks until the context is cancelled, or the command exits.
	if err := cmd.Wait(); err != nil {
		logger.Error("process failed while running", "error", err)
	}
}
