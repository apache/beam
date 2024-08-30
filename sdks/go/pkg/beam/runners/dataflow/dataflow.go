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
//
// This package infers Pipeline Options from flags automatically on job
// submission, for display in the Dataflow UI.
// Use the DontUseFlagAsPipelineOption function to prevent using a given
// flag as a PipelineOption.
package dataflow

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/pipelinex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/hooks"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/jobopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow/dataflowlib"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/gcsx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/hooks/perf"
)

// TODO(herohde) 5/16/2017: the Dataflow flags should match the other SDKs.

var (
	endpoint               = flag.String("dataflow_endpoint", "", "Dataflow endpoint (optional).")
	stagingLocation        = flag.String("staging_location", "", "GCS staging location (required).")
	workerHarnessImage     = flag.String("worker_harness_container_image", "", "Worker harness container image (optional). Deprecated in favor of the sdk_container_image flag.")
	image                  = flag.String("sdk_container_image", "", "Worker harness container image (optional).")
	labels                 = flag.String("labels", "", "JSON-formatted map[string]string of job labels (optional).")
	serviceAccountEmail    = flag.String("service_account_email", "", "Service account email (optional).")
	numWorkers             = flag.Int64("num_workers", 0, "Number of workers (optional).")
	workerHarnessThreads   = flag.Int64("number_of_worker_harness_threads", 0, "The number of threads per each worker harness process (optional).")
	maxNumWorkers          = flag.Int64("max_num_workers", 0, "Maximum number of workers during scaling (optional).")
	diskSizeGb             = flag.Int64("disk_size_gb", 0, "Size of root disk for VMs, in GB (optional).")
	diskType               = flag.String("disk_type", "", "Type of root disk for VMs (optional).")
	autoscalingAlgorithm   = flag.String("autoscaling_algorithm", "", "Autoscaling mode to use (optional).")
	zone                   = flag.String("zone", "", "GCP zone (optional)")
	kmsKey                 = flag.String("dataflow_kms_key", "", "The Cloud KMS key identifier used to encrypt data at rest (optional).")
	network                = flag.String("network", "", "GCP network (optional)")
	subnetwork             = flag.String("subnetwork", "", "GCP subnetwork (optional)")
	noUsePublicIPs         = flag.Bool("no_use_public_ips", false, "Workers must not use public IP addresses (optional)")
	usePublicIPs           = flag.Bool("use_public_ips", true, "Workers must use public IP addresses (optional)")
	tempLocation           = flag.String("temp_location", "", "Temp location (optional)")
	workerMachineType      = flag.String("worker_machine_type", "", "GCE machine type (optional)")
	machineType            = flag.String("machine_type", "", "alias of worker_machine_type (optional)")
	minCPUPlatform         = flag.String("min_cpu_platform", "", "GCE minimum cpu platform (optional)")
	workerRegion           = flag.String("worker_region", "", "Dataflow worker region (optional)")
	workerZone             = flag.String("worker_zone", "", "Dataflow worker zone (optional)")
	dataflowServiceOptions = flag.String("dataflow_service_options", "", "Comma separated list of additional job modes and configurations (optional)")
	flexRSGoal             = flag.String("flexrs_goal", "", "Which Flexible Resource Scheduling mode to run in (optional)")
	// TODO(https://github.com/apache/beam/issues/21604) Turn this on once TO_STRING is implemented
	// enableHotKeyLogging    = flag.Bool("enable_hot_key_logging", false, "Specifies that when a hot key is detected in the pipeline, the literal, human-readable key is printed in the user's Cloud Logging project (optional).")

	// Streaming update flags
	update           = flag.Bool("update", false, "Submit this job as an update to an existing Dataflow job (optional); the job name must match the existing job to update")
	transformMapping = flag.String("transform_name_mapping", "", "JSON-formatted mapping of old transform names to new transform names for pipeline updates (optional)")

	dryRun           = flag.Bool("dry_run", false, "Dry run. Just print the job, but don't submit it.")
	teardownPolicy   = flag.String("teardown_policy", "", "Job teardown policy (internal only).")
	templateLocation = flag.String("template_location", "", "GCS location to save the job graph. If set, the job is not submitted to Dataflow (optional.)")

	// SDK options
	cpuProfiling = flag.String("cpu_profiling", "", "Job records CPU profiles to this GCS location (optional)")
)

func init() {
	flag.BoolVar(jobopts.Async, "execute_async", false, "Asynchronous execution. Submit the job and return immediately. Alias of --async.")
}

// flagFilter filters flags that are already represented by the above flags
// or in the JobOpts to prevent them from appearing duplicated
// as PipelineOption display data.
//
// New flags that are already put into pipeline options
// should be added to this map.
// Don't filter temp_location since we need this included in PipelineOptions to correctly upload heap dumps.
var flagFilter = map[string]bool{
	"dataflow_endpoint":              true,
	"staging_location":               true,
	"worker_harness_container_image": true,
	"sdk_container_image":            true,
	"labels":                         true,
	"service_account_email":          true,
	"num_workers":                    true,
	"max_num_workers":                true,
	"disk_size_gb":                   true,
	"disk_type":                      true,
	"autoscaling_algorithm":          true,
	"zone":                           true,
	"network":                        true,
	"subnetwork":                     true,
	"no_use_public_ips":              true,
	"template_location":              true,
	"worker_machine_type":            true,
	"machine_type":                   true,
	"min_cpu_platform":               true,
	"dataflow_worker_jar":            true,
	"worker_region":                  true,
	"worker_zone":                    true,
	"teardown_policy":                true,
	"cpu_profiling":                  true,
	"session_recording":              true,
	"update":                         true,
	"transform_name_mapping":         true,

	// Job Options flags
	"endpoint":                 true,
	"job_name":                 true,
	"environment_type":         true,
	"environment_config":       true,
	"experiments":              true,
	"async":                    true,
	"retain_docker_containers": true,
	"parallelism":              true,

	// GCP opts
	"project": true,
	"region":  true,

	// Other common beam flags.
	"runner": true,

	// Don't filter these to note override.
	// "beam_strict": true,
	// "sdk_harness_container_image_override": true,
	// "worker_binary": true,
}

// DontUseFlagAsPipelineOption prevents a set flag from appearing
// as a PipelineOption in the Dataflow UI. Useful for sensitive,
// noisy, or irrelevant configuration.
func DontUseFlagAsPipelineOption(s string) {
	flagFilter[s] = true
}

func init() {
	// Note that we also _ import harness/init to setup the remote execution hook.
	beam.RegisterRunner("dataflow", Execute)
	beam.RegisterRunner("DataflowRunner", Execute)

	perf.RegisterProfCaptureHook("gcs_profile_writer", gcsRecorderHook)
}

var unique int32

// Helper function finding first non empty string. Used for handling alias options.
func firstNonEmpty(values ...*string) *string {
	for _, value := range values {
		if *value != "" {
			return value
		}
	}
	return values[0]
}

// Execute runs the given pipeline on Google Cloud Dataflow. It uses the
// default application credentials to submit the job.
func Execute(ctx context.Context, p *beam.Pipeline) (beam.PipelineResult, error) {
	if !beam.Initialized() {
		panic("Beam has not been initialized. Call beam.Init() before pipeline construction.")
	}

	edges, nodes, err := p.Build()
	if err != nil {
		return nil, err
	}
	streaming := !graph.Bounded(nodes)

	beam.PipelineOptions.LoadOptionsFromFlags(flagFilter)
	opts, err := getJobOptions(ctx, streaming)
	if err != nil {
		return nil, err
	}

	// (1) Build and submit
	// NOTE(herohde) 10/8/2018: the last segment of the names must be "worker".
	id := fmt.Sprintf("go-%v-%v", atomic.AddInt32(&unique, 1), time.Now().UnixNano())

	modelURL := gcsx.Join(*stagingLocation, id, "model")
	workerURL := gcsx.Join(*stagingLocation, id, "worker")
	xlangURL := gcsx.Join(*stagingLocation, id, "xlang")

	artifactURLs, err := dataflowlib.ResolveXLangArtifacts(ctx, edges, opts.Project, xlangURL)
	if err != nil {
		return nil, errors.WithContext(err, "resolving cross-language artifacts")
	}
	opts.ArtifactURLs = artifactURLs
	environment, err := graphx.CreateEnvironment(ctx, jobopts.GetEnvironmentUrn(ctx), getContainerImage)
	if err != nil {
		return nil, errors.WithContext(err, "creating environment for model pipeline")
	}
	model, err := graphx.Marshal(edges, &graphx.Options{
		Environment:           environment,
		PipelineResourceHints: jobopts.GetPipelineResourceHints(),
	})
	if err != nil {
		return nil, errors.WithContext(err, "generating model pipeline")
	}
	err = pipelinex.ApplySdkImageOverrides(model, jobopts.GetSdkImageOverrides())
	if err != nil {
		return nil, errors.WithContext(err, "applying container image overrides")
	}

	if *dryRun {
		log.Info(ctx, "Dry-run: not submitting job!")

		log.Info(ctx, model.String())
		job, err := dataflowlib.Translate(ctx, model, opts, workerURL, modelURL)
		if err != nil {
			return nil, err
		}
		dataflowlib.PrintJob(ctx, job)
		return nil, nil
	}

	return dataflowlib.Execute(ctx, model, opts, workerURL, modelURL, *endpoint, *jobopts.Async)
}

func isFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func getJobOptions(ctx context.Context, streaming bool) (*dataflowlib.JobOptions, error) {
	project := gcpopts.GetProjectFromFlagOrEnvironment(ctx)
	if project == "" {
		return nil, errors.New("no Google Cloud project specified. Use --project=<project>")
	}
	region := gcpopts.GetRegion(ctx)
	if region == "" {
		return nil, errors.New("no Google Cloud region specified. Use --region=<region>. See https://cloud.google.com/dataflow/docs/concepts/regional-endpoints")
	}
	if *stagingLocation == "" {
		return nil, errors.New("no GCS staging location specified. Use --staging_location=gs://<bucket>/<path>")
	}
	var jobLabels map[string]string
	if *labels != "" {
		if err := json.Unmarshal([]byte(*labels), &jobLabels); err != nil {
			return nil, errors.Wrapf(err, "error reading --label flag as JSON")
		}
	}

	if *cpuProfiling != "" {
		perf.EnableProfCaptureHook("gcs_profile_writer", *cpuProfiling)
	}

	if *autoscalingAlgorithm != "" {
		if *autoscalingAlgorithm != "NONE" && *autoscalingAlgorithm != "THROUGHPUT_BASED" {
			return nil, errors.New("invalid autoscaling algorithm. Use --autoscaling_algorithm=(NONE|THROUGHPUT_BASED)")
		}
	}

	if *flexRSGoal != "" {
		switch *flexRSGoal {
		case "FLEXRS_UNSPECIFIED", "FLEXRS_SPEED_OPTIMIZED", "FLEXRS_COST_OPTIMIZED":
			// valid values
		default:
			return nil, errors.Errorf("invalid flex resource scheduling goal. Got %q; Use --flexrs_goal=(FLEXRS_UNSPECIFIED|FLEXRS_SPEED_OPTIMIZED|FLEXRS_COST_OPTIMIZED)", *flexRSGoal)
		}
	}
	if !streaming && *transformMapping != "" {
		return nil, errors.New("provided transform_name_mapping for a batch pipeline, did you mean to construct a streaming pipeline?")
	}
	if !*update && *transformMapping != "" {
		return nil, errors.New("provided transform_name_mapping without setting the --update flag, so the pipeline would not be updated")
	}
	var updateTransformMapping map[string]string
	if *transformMapping != "" {
		if err := json.Unmarshal([]byte(*transformMapping), &updateTransformMapping); err != nil {
			return nil, errors.Wrapf(err, "error reading --transform_name_mapping flag as JSON")
		}
	}
	if *usePublicIPs == *noUsePublicIPs {
		useSet := isFlagPassed("use_public_ips")
		noUseSet := isFlagPassed("no_use_public_ips")
		// If use_public_ips was explicitly set but no_use_public_ips was not, use that value
		// We take the explicit value of no_use_public_ips if it was set but use_public_ips was not.
		if useSet && !noUseSet {
			*noUsePublicIPs = !*usePublicIPs
		} else if useSet && noUseSet {
			return nil, errors.New("exactly one of usePublicIPs and noUsePublicIPs must be true, please check that only one is true")
		}
	}

	hooks.SerializeHooksToOptions()

	experiments := jobopts.GetExperiments()
	// Ensure that we enable the same set of experiments across all SDKs
	// for runner v2.
	var fnApiSet, v2set, uwSet, portaSubmission, seSet, wsSet bool
	for _, e := range experiments {
		if strings.Contains(e, "beam_fn_api") {
			fnApiSet = true
		}
		if strings.Contains(e, "use_runner_v2") {
			v2set = true
		}
		if strings.Contains(e, "use_unified_worker") {
			uwSet = true
		}
		if strings.Contains(e, "use_portable_job_submission") {
			portaSubmission = true
		}
		if strings.Contains(e, "disable_runner_v2") || strings.Contains(e, "disable_runner_v2_until_2023") || strings.Contains(e, "disable_prime_runner_v2") {
			return nil, errors.New("detected one of the following experiments: disable_runner_v2 | disable_runner_v2_until_2023 | disable_prime_runner_v2. Disabling runner v2 is no longer supported as of Beam version 2.45.0+")
		}
	}
	// Enable default experiments.
	if !fnApiSet {
		experiments = append(experiments, "beam_fn_api")
	}
	if !v2set {
		experiments = append(experiments, "use_runner_v2")
	}
	if !uwSet {
		experiments = append(experiments, "use_unified_worker")
	}
	if !portaSubmission {
		experiments = append(experiments, "use_portable_job_submission")
	}

	// Ensure that streaming specific experiments are set for streaming pipelines
	// since runner v2 only supports using streaming engine.
	if streaming {
		if !seSet {
			experiments = append(experiments, "enable_streaming_engine")
		}
		if !wsSet {
			experiments = append(experiments, "enable_windmill_service")
		}
	}

	if *minCPUPlatform != "" {
		experiments = append(experiments, fmt.Sprintf("min_cpu_platform=%v", *minCPUPlatform))
	}

	var dfServiceOptions []string
	if *dataflowServiceOptions != "" {
		dfServiceOptions = strings.Split(*dataflowServiceOptions, ",")
	}

	beam.PipelineOptions.LoadOptionsFromFlags(flagFilter)
	opts := &dataflowlib.JobOptions{
		Name:                   jobopts.GetJobName(),
		Streaming:              streaming,
		Experiments:            experiments,
		DataflowServiceOptions: dfServiceOptions,
		Options:                beam.PipelineOptions.Export(),
		Project:                project,
		Region:                 region,
		Zone:                   *zone,
		KmsKey:                 *kmsKey,
		Network:                *network,
		Subnetwork:             *subnetwork,
		NoUsePublicIPs:         *noUsePublicIPs,
		NumWorkers:             *numWorkers,
		MaxNumWorkers:          *maxNumWorkers,
		WorkerHarnessThreads:   *workerHarnessThreads,
		DiskSizeGb:             *diskSizeGb,
		DiskType:               *diskType,
		Algorithm:              *autoscalingAlgorithm,
		FlexRSGoal:             *flexRSGoal,
		MachineType:            *firstNonEmpty(workerMachineType, machineType),
		Labels:                 jobLabels,
		ServiceAccountEmail:    *serviceAccountEmail,
		TempLocation:           *tempLocation,
		TemplateLocation:       *templateLocation,
		Worker:                 *jobopts.WorkerBinary,
		WorkerRegion:           *workerRegion,
		WorkerZone:             *workerZone,
		TeardownPolicy:         *teardownPolicy,
		ContainerImage:         getContainerImage(ctx),
		Update:                 *update,
		TransformNameMapping:   updateTransformMapping,
	}
	if opts.TempLocation == "" {
		opts.TempLocation = gcsx.Join(*stagingLocation, "tmp")
	}

	return opts, nil
}

func gcsRecorderHook(opts []string) perf.CaptureHook {
	bucket, prefix, err := gcsx.ParseObject(opts[0])
	if err != nil {
		panic(fmt.Sprintf("Invalid hook configuration for gcsRecorderHook: %s", opts))
	}

	return func(ctx context.Context, spec string, r io.Reader) error {
		client, err := gcsx.NewClient(ctx, storage.ScopeReadWrite)
		if err != nil {
			return errors.WithContext(err, "establishing GCS client")
		}
		return gcsx.WriteObject(ctx, client, bucket, path.Join(prefix, spec), r)
	}
}

func getContainerImage(ctx context.Context) string {
	urn := jobopts.GetEnvironmentUrn(ctx)
	if urn == "" || urn == "beam:env:docker:v1" {
		if *workerHarnessImage != "" {
			if *image != "" {
				panic("Both worker_harness_container_image and sdk_container_image cannot both be set. Prefer sdk_container_image, worker_harness_container_image is deprecated.")
			}
			return *workerHarnessImage
		}
		if *image != "" {
			return *image
		}
		envConfig := jobopts.GetEnvironmentConfig(ctx)
		if envConfig == core.DefaultDockerImage {
			// It's possible the user set the image exactly manually, but unlikely.
			// Prefer using the gcr.io image by default.
			// Note: This doesn't change the dev experience, which requires a user
			// to have a dev image.
			// However, RC versions should automatically be picked up, since
			// they are never tagged the RC number, just the main version.
			return "gcr.io/cloud-dataflow/v1beta3/beam_go_sdk:" + core.SdkVersion
		}
		return envConfig
	}
	panic(fmt.Sprintf("Unsupported environment %v", urn))
}
