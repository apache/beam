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

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/apache/beam/sdks/v2/go/container/tools"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/artifact"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"

	// Import gcs filesystem so that it can be used to upload heap dumps
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/gcs"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/diagnostics"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/execx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/grpcx"
)

var (
	// Contract: https://s.apache.org/beam-fn-api-container-contract.

	id                = flag.String("id", "", "Local identifier (required).")
	loggingEndpoint   = flag.String("logging_endpoint", "", "Local logging endpoint for FnHarness (required).")
	artifactEndpoint  = flag.String("artifact_endpoint", "", "Local artifact endpoint for FnHarness (required).")
	provisionEndpoint = flag.String("provision_endpoint", "", "Local provision endpoint for FnHarness (required).")
	controlEndpoint   = flag.String("control_endpoint", "", "Local control endpoint for FnHarness (required).")
	semiPersistDir    = flag.String("semi_persist_dir", "/tmp", "Local semi-persistent directory (optional).")
)

const (
	cloudProfilingJobName           = "CLOUD_PROF_JOB_NAME"
	cloudProfilingJobID             = "CLOUD_PROF_JOB_ID"
	enableGoogleCloudProfilerOption = "enable_google_cloud_profiler"
)

func configureGoogleCloudProfilerEnvVars(ctx context.Context, logger *tools.Logger, metadata map[string]string) error {
	if metadata == nil {
		return errors.New("enable_google_cloud_profiler is set to true, but no metadata is received from provision server, profiling will not be enabled")
	}
	jobName, nameExists := metadata["job_name"]
	if !nameExists {
		return errors.New("required job_name missing from metadata, profiling will not be enabled without it")
	}
	jobID, idExists := metadata["job_id"]
	if !idExists {
		return errors.New("required job_id missing from metadata, profiling will not be enabled without it")
	}
	os.Setenv(cloudProfilingJobName, jobName)
	os.Setenv(cloudProfilingJobID, jobID)
	logger.Printf(ctx, "Cloud Profiling Job Name: %v, Job IDL %v", jobName, jobID)
	return nil
}

func main() {
	flag.Parse()
	if *id == "" {
		log.Fatal("No id provided.")
	}
	if *provisionEndpoint == "" {
		log.Fatal("No provision endpoint provided.")
	}

	ctx := grpcx.WriteWorkerID(context.Background(), *id)

	info, err := tools.ProvisionInfo(ctx, *provisionEndpoint)
	if err != nil {
		log.Fatalf("Failed to obtain provisioning information: %v", err)
	}
	log.Printf("Provision info:\n%v", info)

	err = ensureEndpointsSet(info)
	if err != nil {
		log.Fatalf("Endpoint not set: %v", err)
	}
	logger := &tools.Logger{Endpoint: *loggingEndpoint}
	logger.Printf(ctx, "Initializing Go harness: %v", strings.Join(os.Args, " "))

	// (1) Obtain the pipeline options

	options, err := tools.ProtoToJSON(info.GetPipelineOptions())
	if err != nil {
		logger.Fatalf(ctx, "Failed to convert pipeline options: %v", err)
	}

	// (2) Retrieve the staged files.
	//
	// The Go SDK harness downloads the worker binary and invokes
	// it. The binary is required to be keyed as "worker", if there
	// are more than one artifact.

	dir := filepath.Join(*semiPersistDir, "staged")
	artifacts, err := artifact.Materialize(ctx, *artifactEndpoint, info.GetDependencies(), info.GetRetrievalToken(), dir)
	if err != nil {
		logger.Fatalf(ctx, "Failed to retrieve staged files: %v", err)
	}

	name, err := getGoWorkerArtifactName(ctx, logger, artifacts)
	if err != nil {
		logger.Fatalf(ctx, "Failed to get Go Worker Artifact Name: %v", err)
	}

	// (3) The persist dir may be on a noexec volume, so we must
	// copy the binary to a different location to execute.
	const prog = "/bin/worker"
	if err := copyExe(filepath.Join(dir, name), prog); err != nil {
		logger.Fatalf(ctx, "Failed to copy worker binary: %v", err)
	}

	args := []string{
		"--worker=true",
		"--id=" + *id,
		"--logging_endpoint=" + *loggingEndpoint,
		"--control_endpoint=" + *controlEndpoint,
		"--semi_persist_dir=" + *semiPersistDir,
	}
	if err := tools.MakePipelineOptionsFileAndEnvVar(options); err != nil {
		logger.Fatalf(ctx, "Failed to load pipeline options to worker: %v", err)
	}
	if info.GetStatusEndpoint() != nil {
		os.Setenv("STATUS_ENDPOINT", info.GetStatusEndpoint().GetUrl())
	}

	if len(info.GetRunnerCapabilities()) > 0 {
		os.Setenv("RUNNER_CAPABILITIES", strings.Join(info.GetRunnerCapabilities(), " "))
	}

	enableGoogleCloudProfiler := strings.Contains(options, enableGoogleCloudProfilerOption)
	if enableGoogleCloudProfiler {
		err := configureGoogleCloudProfilerEnvVars(ctx, logger, info.Metadata)
		if err != nil {
			logger.Printf(ctx, "could not configure Google Cloud Profiler variables, got %v", err)
		}
	}

	err = execx.Execute(prog, args...)

	if err != nil {
		var opt runtime.RawOptionsWrapper
		err := json.Unmarshal([]byte(options), &opt)
		if err == nil {
			if tempLocation, ok := opt.Options.Options["temp_location"]; ok {
				diagnostics.UploadHeapProfile(ctx, fmt.Sprintf("%v/heapProfiles/profile-%v-%d", strings.TrimSuffix(tempLocation, "/"), *id, time.Now().Unix()))
			}
		}
	}

	logger.Fatalf(ctx, "User program exited: %v", err)
}

func getGoWorkerArtifactName(ctx context.Context, logger *tools.Logger, artifacts []*pipepb.ArtifactInformation) (string, error) {
	const worker = "worker"
	name := worker

	switch len(artifacts) {
	case 0:
		return "", errors.New("no artifacts staged")
	case 1:
		name, _ = artifact.MustExtractFilePayload(artifacts[0])
		return name, nil
	default:
		for _, a := range artifacts {
			if a.GetRoleUrn() == artifact.URNGoWorkerBinaryRole {
				name, _ = artifact.MustExtractFilePayload(a)
				return name, nil
			}
		}
		// TODO(https://github.com/apache/beam/issues/21459): Remove legacy hack once aged out.
		for _, a := range artifacts {
			n, _ := artifact.MustExtractFilePayload(a)
			if n == worker {
				logger.Printf(ctx, "Go worker binary found with legacy name '%v'", worker)
				return n, nil
			}
		}
		return "", fmt.Errorf("no artifact named '%v' found", worker)
	}
}

func ensureEndpointsSet(info *fnpb.ProvisionInfo) error {
	// TODO(BEAM-8201): Simplify once flags are no longer used.
	if info.GetLoggingEndpoint().GetUrl() != "" {
		*loggingEndpoint = info.GetLoggingEndpoint().GetUrl()
	}
	if info.GetArtifactEndpoint().GetUrl() != "" {
		*artifactEndpoint = info.GetArtifactEndpoint().GetUrl()
	}
	if info.GetControlEndpoint().GetUrl() != "" {
		*controlEndpoint = info.GetControlEndpoint().GetUrl()
	}

	if *loggingEndpoint == "" {
		return errors.New("no logging endpoint provided")
	}
	if *artifactEndpoint == "" {
		return errors.New("no artifact endpoint provided")
	}
	if *controlEndpoint == "" {
		return errors.New("no control endpoint provided")
	}

	return nil
}

func copyExe(from, to string) error {
	src, err := os.Open(from)
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		return err
	}

	if _, err := io.Copy(dst, src); err != nil {
		return err
	}
	return dst.Close()
}
