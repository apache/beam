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

package dataflowlib

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"golang.org/x/oauth2/google"
	df "google.golang.org/api/dataflow/v1b3"
	"google.golang.org/protobuf/proto"

	// Importing to get the side effect of the remote execution hook. See init().
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/harness/init"
)

// JobOptions capture the various options for submitting jobs
// to Dataflow.
type JobOptions struct {
	// Name is the job name.
	Name string
	// Experiments are additional experiments.
	Experiments []string
	// DataflowServiceOptions are additional job modes and configurations for Dataflow
	DataflowServiceOptions []string
	// Pipeline options
	Options runtime.RawOptions

	Streaming           bool
	Project             string
	Region              string
	Zone                string
	KmsKey              string
	Network             string
	Subnetwork          string
	NoUsePublicIPs      bool
	NumWorkers          int64
	DiskSizeGb          int64
	DiskType            string
	MachineType         string
	Labels              map[string]string
	ServiceAccountEmail string
	WorkerRegion        string
	WorkerZone          string
	ContainerImage      string
	ArtifactURLs        []string // Additional packages for workers.
	FlexRSGoal          string
	EnableHotKeyLogging bool

	// Streaming update settings
	Update               bool
	TransformNameMapping map[string]string

	// Autoscaling settings
	Algorithm            string
	MaxNumWorkers        int64
	WorkerHarnessThreads int64

	TempLocation     string
	TemplateLocation string

	// Worker is the worker binary override.
	Worker string

	// -- Internal use only. Not supported in public Dataflow. --

	TeardownPolicy string
}

func containerImages(p *pipepb.Pipeline) ([]*df.SdkHarnessContainerImage, []string, error) {
	envs := p.GetComponents().GetEnvironments()
	ret := make([]*df.SdkHarnessContainerImage, 0, len(envs))
	display := make([]string, 0, len(envs))
	for id, env := range envs {
		var payload pipepb.DockerPayload
		if err := proto.Unmarshal(env.GetPayload(), &payload); err != nil {
			return nil, nil, fmt.Errorf("bad payload for env %v: %v", id, err)
		}
		singleCore := true
		for _, c := range env.GetCapabilities() {
			if c == graphx.URNMultiCore {
				singleCore = false
			}
		}
		ret = append(ret, &df.SdkHarnessContainerImage{
			ContainerImage:            payload.GetContainerImage(),
			UseSingleCorePerContainer: singleCore,
			Capabilities:              env.GetCapabilities(),
			EnvironmentId:             id,
		})
		display = append(display, payload.GetContainerImage())
	}
	return ret, display, nil
}

// Translate translates a pipeline to a Dataflow job.
func Translate(ctx context.Context, p *pipepb.Pipeline, opts *JobOptions, workerURL, modelURL string) (*df.Job, error) {
	// (1) Translate pipeline to v1b3 speak.

	jobType := "JOB_TYPE_BATCH"
	apiJobType := "FNAPI_BATCH"

	if opts.Streaming {
		jobType = "JOB_TYPE_STREAMING"
		apiJobType = "FNAPI_STREAMING"
	}

	dfImages, images, err := containerImages(p)
	if err != nil {
		return nil, err
	}

	packages := []*df.Package{{
		Name:     "worker",
		Location: workerURL,
	}}

	for _, url := range opts.ArtifactURLs {
		name := url[strings.LastIndexAny(url, "/")+1:]
		pkg := &df.Package{
			Name:     name,
			Location: url,
		}
		packages = append(packages, pkg)
	}

	ipConfiguration := "WORKER_IP_UNSPECIFIED"
	if opts.NoUsePublicIPs {
		ipConfiguration = "WORKER_IP_PRIVATE"
	}

	if err := validateWorkerSettings(ctx, opts); err != nil {
		return nil, err
	}

	job := &df.Job{
		ProjectId: opts.Project,
		Name:      opts.Name,
		Type:      jobType,
		Environment: &df.Environment{
			DebugOptions: &df.DebugOptions{
				EnableHotKeyLogging: opts.EnableHotKeyLogging,
			},
			FlexResourceSchedulingGoal: opts.FlexRSGoal,
			ServiceAccountEmail:        opts.ServiceAccountEmail,
			UserAgent: newMsg(userAgent{
				Name:    core.SdkName,
				Version: core.SdkVersion,
			}),
			Version: newMsg(version{
				JobType: apiJobType,
				Major:   "6",
			}),
			SdkPipelineOptions: newMsg(pipelineOptions{
				DisplayData: printOptions(opts, images),
				Options: dataflowOptions{
					PipelineURL:  modelURL,
					Region:       opts.Region,
					Experiments:  opts.Experiments,
					TempLocation: opts.TempLocation,
				},
				GoOptions: opts.Options,
			}),
			ServiceOptions:    opts.DataflowServiceOptions,
			ServiceKmsKeyName: opts.KmsKey,
			WorkerPools: []*df.WorkerPool{{
				AutoscalingSettings: &df.AutoscalingSettings{
					MaxNumWorkers: opts.MaxNumWorkers,
				},
				DiskSizeGb:                  opts.DiskSizeGb,
				DiskType:                    opts.DiskType,
				IpConfiguration:             ipConfiguration,
				Kind:                        "harness",
				Packages:                    packages,
				WorkerHarnessContainerImage: opts.ContainerImage,
				SdkHarnessContainerImages:   dfImages,
				NumWorkers:                  1,
				MachineType:                 opts.MachineType,
				Network:                     opts.Network,
				Subnetwork:                  opts.Subnetwork,
				Zone:                        opts.Zone,
			}},
			WorkerRegion:      opts.WorkerRegion,
			WorkerZone:        opts.WorkerZone,
			TempStoragePrefix: opts.TempLocation,
			Experiments:       opts.Experiments,
		},
		Labels:               opts.Labels,
		TransformNameMapping: opts.TransformNameMapping,
		Steps:                make([]*df.Step, 0),
	}

	workerPool := job.Environment.WorkerPools[0]

	if opts.NumWorkers > 0 {
		workerPool.NumWorkers = opts.NumWorkers
	}
	if opts.WorkerHarnessThreads > 0 {
		workerPool.NumThreadsPerWorker = opts.WorkerHarnessThreads
	}
	if opts.Algorithm != "" {
		workerPool.AutoscalingSettings.Algorithm = map[string]string{
			"NONE":             "AUTOSCALING_ALGORITHM_NONE",
			"THROUGHPUT_BASED": "AUTOSCALING_ALGORITHM_BASIC",
		}[opts.Algorithm]
	}
	if opts.TeardownPolicy != "" {
		workerPool.TeardownPolicy = opts.TeardownPolicy
	}

	return job, nil
}

// Submit submits a prepared job to Cloud Dataflow.
func Submit(ctx context.Context, client *df.Service, project, region string, job *df.Job, updateJob bool) (*df.Job, error) {
	if updateJob {
		runningJob, err := GetRunningJobByName(client, project, region, job.Name)
		if err != nil {
			return nil, err
		}
		job.ReplaceJobId = runningJob.Id
	}
	upd, err := client.Projects.Locations.Jobs.Create(project, region, job).Do()
	if err == nil {
		log.Infof(ctx, "Submitted job: %v", upd.Id)
	}
	return upd, err
}

// WaitForCompletion monitors the given job until completion. It logs any messages
// and state changes received.
func WaitForCompletion(ctx context.Context, client *df.Service, project, region, jobID string) error {
	for {
		j, err := client.Projects.Locations.Jobs.Get(project, region, jobID).Do()
		if err != nil {
			return errors.Wrap(err, "failed to get job")
		}

		terminal, msg, err := currentStateMessage(j.CurrentState, jobID)
		if err != nil {
			return err
		}
		log.Infof(ctx, msg)
		if terminal {
			return nil
		}

		time.Sleep(30 * time.Second)
	}
}

// currentStateMessage indicates if the state is terminal, and provides a message to log, or an error.
// Errors are always terminal.
func currentStateMessage(currentState, jobID string) (bool, string, error) {
	switch currentState {
	// Add all Terminal Success stats here.
	case "JOB_STATE_DONE", "JOB_STATE_CANCELLED", "JOB_STATE_DRAINED", "JOB_STATE_UPDATED":
		var state string
		switch currentState {
		case "JOB_STATE_DONE":
			state = "succeeded!"
		case "JOB_STATE_CANCELLED":
			state = "cancelled"
		case "JOB_STATE_DRAINED":
			state = "drained"
		case "JOB_STATE_UPDATED":
			state = "updated"
		}
		return true, fmt.Sprintf("Job %v %v", jobID, state), nil
	case "JOB_STATE_FAILED":
		return true, "", errors.Errorf("Job %s failed", jobID)
	case "JOB_STATE_RUNNING":
		return false, "Job still running ...", nil
	default:
		return false, fmt.Sprintf("Job state: %v ...", currentState), nil
	}
}

// NewClient creates a new dataflow client with default application credentials
// and CloudPlatformScope. The Dataflow endpoint is optionally overridden.
func NewClient(ctx context.Context, endpoint string) (*df.Service, error) {
	cl, err := google.DefaultClient(ctx, df.CloudPlatformScope)
	if err != nil {
		return nil, err
	}
	client, err := df.New(cl)
	if err != nil {
		return nil, err
	}
	if endpoint != "" {
		log.Infof(ctx, "Dataflow endpoint override: %s", endpoint)
		client.BasePath = endpoint
	}
	return client, nil
}

// GetRunningJobByName gets a Dataflow job running by its name and returns an
// error if none match.
func GetRunningJobByName(client *df.Service, project, region string, name string) (*df.Job, error) {
	jobsListCall := client.Projects.Locations.Jobs.List(project, region)
	jobsListCall.Filter("ACTIVE")
	jobsResponse, err := jobsListCall.Do()
	for len(jobsResponse.Jobs) > 0 {
		if err != nil {
			return nil, err
		}
		for _, job := range jobsResponse.Jobs {
			if job.Name == name {
				return job, nil
			}
		}

		jobsListCall.PageToken(jobsResponse.NextPageToken)
		jobsResponse, err = jobsListCall.Do()
	}
	return nil, errors.New(fmt.Sprintf("Unable to find running job with name %s", name))
}

// GetMetrics returns a collection of metrics describing the progress of a
// job by making a call to Cloud Monitoring service.
func GetMetrics(ctx context.Context, client *df.Service, project, region, jobID string) (*df.JobMetrics, error) {
	return client.Projects.Locations.Jobs.GetMetrics(project, region, jobID).Do()
}

// dataflowOptions provides Dataflow with non Go-specific pipeline options. These are the only
// pipeline options that are communicated to cross-language SDK harnesses, so any pipeline options
// needed for cross-language transforms in Dataflow must be declared here.
type dataflowOptions struct {
	Experiments  []string `json:"experiments,omitempty"`
	PipelineURL  string   `json:"pipelineUrl"`
	Region       string   `json:"region"`
	TempLocation string   `json:"tempLocation"`
}

func printOptions(opts *JobOptions, images []string) []*displayData {
	var ret []*displayData
	addIfNonEmpty := func(name string, value string) {
		if value != "" {
			ret = append(ret, newDisplayData(name, "", "options", value))
		}
	}

	addIfNonEmpty("name", opts.Name)
	addIfNonEmpty("experiments", strings.Join(opts.Experiments, ","))
	addIfNonEmpty("project", opts.Project)
	addIfNonEmpty("region", opts.Region)
	addIfNonEmpty("zone", opts.Zone)
	addIfNonEmpty("worker_region", opts.WorkerRegion)
	addIfNonEmpty("worker_zone", opts.WorkerZone)
	addIfNonEmpty("network", opts.Network)
	addIfNonEmpty("subnetwork", opts.Subnetwork)
	addIfNonEmpty("machine_type", opts.MachineType)
	addIfNonEmpty("container_images", strings.Join(images, ","))
	addIfNonEmpty("temp_location", opts.TempLocation)

	for k, v := range opts.Options.Options {
		ret = append(ret, newDisplayData(k, "", "go_options", v))
	}
	return ret
}

func validateWorkerSettings(ctx context.Context, opts *JobOptions) error {
	if opts.Zone != "" && opts.WorkerRegion != "" {
		return errors.New("cannot use option zone with workerRegion; prefer either workerZone or workerRegion")
	}
	if opts.Zone != "" && opts.WorkerZone != "" {
		return errors.New("cannot use option zone with workerZone; prefer workerZone")
	}
	if opts.WorkerZone != "" && opts.WorkerRegion != "" {
		return errors.New("workerRegion and workerZone options are mutually exclusive")
	}

	hasExperimentWorkerRegion := false
	for _, experiment := range opts.Experiments {
		if strings.HasPrefix(experiment, "worker_region") {
			hasExperimentWorkerRegion = true
			break
		}
	}

	if hasExperimentWorkerRegion && opts.WorkerRegion != "" {
		return errors.New("experiment worker_region and option workerRegion are mutually exclusive")
	}
	if hasExperimentWorkerRegion && opts.WorkerZone != "" {
		return errors.New("experiment worker_region and option workerZone are mutually exclusive")
	}
	if hasExperimentWorkerRegion && opts.Zone != "" {
		return errors.New("experiment worker_region and option Zone are mutually exclusive")
	}

	if opts.Zone != "" {
		log.Warn(ctx, "Option --zone is deprecated. Please use --workerZone instead.")
		opts.WorkerZone = opts.Zone
		opts.Zone = ""
	}

	numWorkers := opts.NumWorkers
	maxNumWorkers := opts.MaxNumWorkers
	if numWorkers < 0 {
		return fmt.Errorf("num_workers (%d) cannot be negative", numWorkers)
	}
	if maxNumWorkers < 0 {
		return fmt.Errorf("max_num_workers (%d) cannot be negative", maxNumWorkers)
	}
	if numWorkers > 0 && maxNumWorkers > 0 && numWorkers > maxNumWorkers {
		return fmt.Errorf("num_workers (%d) cannot exceed max_num_workers (%d)", numWorkers, maxNumWorkers)
	}
	return nil
}
