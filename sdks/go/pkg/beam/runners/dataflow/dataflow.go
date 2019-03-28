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

// Package dataflow contains the Dataflow runner for submitting pipelines
// to Google Cloud Dataflow.
package dataflow

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"path"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/hooks"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	pb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/go/pkg/beam/options/jobopts"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/dataflow/dataflowlib"
	"github.com/apache/beam/sdks/go/pkg/beam/util/gcsx"
	"github.com/apache/beam/sdks/go/pkg/beam/x/hooks/perf"
	"github.com/golang/protobuf/proto"
)

// TODO(herohde) 5/16/2017: the Dataflow flags should match the other SDKs.

var (
	endpoint             = flag.String("dataflow_endpoint", "", "Dataflow endpoint (optional).")
	stagingLocation      = flag.String("staging_location", "", "GCS staging location (required).")
	image                = flag.String("worker_harness_container_image", "", "Worker harness container image (required).")
	labels               = flag.String("labels", "", "JSON-formatted map[string]string of job labels (optional).")
	numWorkers           = flag.Int64("num_workers", 0, "Number of workers (optional).")
	maxNumWorkers        = flag.Int64("max_num_workers", 0, "Maximum number of workers during scaling (optional).")
	autoscalingAlgorithm = flag.String("autoscaling_algorithm", "", "Autoscaling mode to use (optional).")
	zone                 = flag.String("zone", "", "GCP zone (optional)")
	region               = flag.String("region", "us-central1", "GCP Region (optional)")
	network              = flag.String("network", "", "GCP network (optional)")
	tempLocation         = flag.String("temp_location", "", "Temp location (optional)")
	machineType          = flag.String("worker_machine_type", "", "GCE machine type (optional)")
	minCPUPlatform       = flag.String("min_cpu_platform", "", "GCE minimum cpu platform (optional)")
	workerJar            = flag.String("dataflow_worker_jar", "", "Dataflow worker jar (optional)")

	dryRun         = flag.Bool("dry_run", false, "Dry run. Just print the job, but don't submit it.")
	teardownPolicy = flag.String("teardown_policy", "", "Job teardown policy (internal only).")

	// SDK options
	cpuProfiling     = flag.String("cpu_profiling", "", "Job records CPU profiles to this GCS location (optional)")
	sessionRecording = flag.String("session_recording", "", "Job records session transcripts")
)

func init() {
	// Note that we also _ import harness/init to setup the remote execution hook.
	beam.RegisterRunner("dataflow", Execute)

	perf.RegisterProfCaptureHook("gcs_profile_writer", gcsRecorderHook)
}

var unique int32

// Execute runs the given pipeline on Google Cloud Dataflow. It uses the
// default application credentials to submit the job.
func Execute(ctx context.Context, p *beam.Pipeline) error {
	// (1) Gather job options

	project := *gcpopts.Project
	if project == "" {
		return errors.New("no Google Cloud project specified. Use --project=<project>")
	}
	if *stagingLocation == "" {
		return errors.New("no GCS staging location specified. Use --staging_location=gs://<bucket>/<path>")
	}
	if *image == "" {
		*image = getContainerImage(ctx)
	}
	var jobLabels map[string]string
	if *labels != "" {
		if err := json.Unmarshal([]byte(*labels), &jobLabels); err != nil {
			return fmt.Errorf("error reading --label flag as JSON: %v", err)
		}
	}

	if *cpuProfiling != "" {
		perf.EnableProfCaptureHook("gcs_profile_writer", *cpuProfiling)
	}

	if *sessionRecording != "" {
		// TODO(wcn): BEAM-4017
		// It's a bit inconvenient for GCS because the whole object is written in
		// one pass, whereas the session logs are constantly appended. We wouldn't
		// want to hold all the logs in memory to flush at the end of the pipeline
		// as we'd blow out memory on the worker. The implementation of the
		// CaptureHook should create an internal buffer and write chunks out to GCS
		// once they get to an appropriate size (50M or so?)
	}
	if *autoscalingAlgorithm != "" {
		if *autoscalingAlgorithm != "NONE" && *autoscalingAlgorithm != "THROUGHPUT_BASED" {
			return errors.New("invalid autoscaling algorithm. Use --autoscaling_algorithm=(NONE|THROUGHPUT_BASED)")
		}
	}

	hooks.SerializeHooksToOptions()

	experiments := jobopts.GetExperiments()
	if *minCPUPlatform != "" {
		experiments = append(experiments, fmt.Sprintf("min_cpu_platform=%v", *minCPUPlatform))
	}

	opts := &dataflowlib.JobOptions{
		Name:           jobopts.GetJobName(),
		Experiments:    experiments,
		Options:        beam.PipelineOptions.Export(),
		Project:        project,
		Region:         *region,
		Zone:           *zone,
		Network:        *network,
		NumWorkers:     *numWorkers,
		MaxNumWorkers:  *maxNumWorkers,
		Algorithm:      *autoscalingAlgorithm,
		MachineType:    *machineType,
		Labels:         jobLabels,
		TempLocation:   *tempLocation,
		Worker:         *jobopts.WorkerBinary,
		WorkerJar:      *workerJar,
		TeardownPolicy: *teardownPolicy,
	}
	if opts.TempLocation == "" {
		opts.TempLocation = gcsx.Join(*stagingLocation, "tmp")
	}

	// (1) Build and submit

	edges, _, err := p.Build()
	if err != nil {
		return err
	}
	model, err := graphx.Marshal(edges, &graphx.Options{Environment: createEnvironment(ctx)})
	if err != nil {
		return fmt.Errorf("failed to generate model pipeline: %v", err)
	}

	// NOTE(herohde) 10/8/2018: the last segment of the names must be "worker" and "dataflow-worker.jar".
	id := fmt.Sprintf("go-%v-%v", atomic.AddInt32(&unique, 1), time.Now().UnixNano())

	modelURL := gcsx.Join(*stagingLocation, id, "model")
	workerURL := gcsx.Join(*stagingLocation, id, "worker")
	jarURL := gcsx.Join(*stagingLocation, id, "dataflow-worker.jar")

	if *dryRun {
		log.Info(ctx, "Dry-run: not submitting job!")

		log.Info(ctx, proto.MarshalTextString(model))
		job, err := dataflowlib.Translate(model, opts, workerURL, jarURL, modelURL)
		if err != nil {
			return err
		}
		dataflowlib.PrintJob(ctx, job)
		return nil
	}

	_, err = dataflowlib.Execute(ctx, model, opts, workerURL, jarURL, modelURL, *endpoint, false)
	return err
}
func gcsRecorderHook(opts []string) perf.CaptureHook {
	bucket, prefix, err := gcsx.ParseObject(opts[0])
	if err != nil {
		panic(fmt.Sprintf("Invalid hook configuration for gcsRecorderHook: %s", opts))
	}

	return func(ctx context.Context, spec string, r io.Reader) error {
		client, err := gcsx.NewClient(ctx, storage.ScopeReadWrite)
		if err != nil {
			return fmt.Errorf("couldn't establish GCS client: %v", err)
		}
		return gcsx.WriteObject(ctx, client, bucket, path.Join(prefix, spec), r)
	}
}

func getContainerImage(ctx context.Context) string {
	urn := jobopts.GetEnvironmentUrn(ctx)
	if urn == "" || urn == "beam:env:docker:v1" {
		return jobopts.GetEnvironmentConfig(ctx)
	}
	panic(fmt.Sprintf("Unsupported environment %v", urn))
}

func createEnvironment(ctx context.Context) pb.Environment {
	var environment pb.Environment
	switch urn := jobopts.GetEnvironmentUrn(ctx); urn {
	case "beam:env:process:v1":
		// TODO Support process based SDK Harness.
		panic(fmt.Sprintf("Unsupported environment %v", urn))
	case "beam:env:docker:v1":
		fallthrough
	default:
		config := *image
		payload := &pb.DockerPayload{ContainerImage: config}
		serializedPayload, err := proto.Marshal(payload)
		if err != nil {
			panic(fmt.Sprintf(
				"Failed to serialize Environment payload %v for config %v: %v", payload, config, err))
		}
		environment = pb.Environment{
			Url:     config,
			Urn:     urn,
			Payload: serializedPayload,
		}
	}
	return environment
}
