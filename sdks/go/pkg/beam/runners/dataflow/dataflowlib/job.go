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
	"strings"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	// Importing to get the side effect of the remote execution hook. See init().
	_ "github.com/apache/beam/sdks/go/pkg/beam/core/runtime/harness/init"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/pipelinex"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"golang.org/x/oauth2/google"
	df "google.golang.org/api/dataflow/v1b3"
)

// JobOptions capture the various options for submitting jobs
// to Dataflow.
type JobOptions struct {
	// Name is the job name.
	Name string
	// Experiments are additional experiments.
	Experiments []string
	// Pipeline options
	Options runtime.RawOptions

	Project             string
	Region              string
	Zone                string
	Network             string
	Subnetwork          string
	NoUsePublicIPs      bool
	NumWorkers          int64
	MachineType         string
	Labels              map[string]string
	ServiceAccountEmail string

	// Autoscaling settings
	Algorithm     string
	MaxNumWorkers int64

	TempLocation string

	// Worker is the worker binary override.
	Worker string
	// WorkerJar is a custom worker jar.
	WorkerJar string

	// -- Internal use only. Not supported in public Dataflow. --

	TeardownPolicy string
}

// Translate translates a pipeline to a Dataflow job.
func Translate(p *pipepb.Pipeline, opts *JobOptions, workerURL, jarURL, modelURL string) (*df.Job, error) {
	// (1) Translate pipeline to v1b3 speak.

	steps, err := translate(p)
	if err != nil {
		return nil, err
	}

	jobType := "JOB_TYPE_BATCH"
	apiJobType := "FNAPI_BATCH"

	streaming := !pipelinex.Bounded(p)
	if streaming {
		jobType = "JOB_TYPE_STREAMING"
		apiJobType = "FNAPI_STREAMING"
	}

	images := pipelinex.ContainerImages(p)
	if len(images) != 1 {
		return nil, errors.Errorf("Dataflow supports one container image only: %v", images)
	}

	packages := []*df.Package{{
		Name:     "worker",
		Location: workerURL,
	}}
	experiments := append(opts.Experiments, "beam_fn_api")

	if opts.WorkerJar != "" {
		jar := &df.Package{
			Name:     "dataflow-worker.jar",
			Location: jarURL,
		}
		packages = append(packages, jar)
		experiments = append(experiments, "use_staged_dataflow_worker_jar")
	}

	ipConfiguration := "WORKER_IP_UNSPECIFIED"
	if opts.NoUsePublicIPs {
		ipConfiguration = "WORKER_IP_PRIVATE"
	}

	job := &df.Job{
		ProjectId: opts.Project,
		Name:      opts.Name,
		Type:      jobType,
		Environment: &df.Environment{
			ServiceAccountEmail: opts.ServiceAccountEmail,
			UserAgent: newMsg(userAgent{
				Name:    "Apache Beam SDK for Go",
				Version: "0.5.0",
			}),
			Version: newMsg(version{
				JobType: apiJobType,
				Major:   "6",
			}),
			SdkPipelineOptions: newMsg(pipelineOptions{
				DisplayData: printOptions(opts, images),
				Options: dataflowOptions{
					PipelineURL: modelURL,
					Region:      opts.Region,
					Experiments: experiments,
				},
				GoOptions: opts.Options,
			}),
			WorkerPools: []*df.WorkerPool{{
				AutoscalingSettings: &df.AutoscalingSettings{
					MaxNumWorkers: opts.MaxNumWorkers,
				},
				IpConfiguration:             ipConfiguration,
				Kind:                        "harness",
				Packages:                    packages,
				WorkerHarnessContainerImage: images[0],
				NumWorkers:                  1,
				MachineType:                 opts.MachineType,
				Network:                     opts.Network,
				Subnetwork:                  opts.Subnetwork,
				Zone:                        opts.Zone,
			}},
			TempStoragePrefix: opts.TempLocation,
			Experiments:       experiments,
		},
		Labels: opts.Labels,
		Steps:  steps,
	}

	workerPool := job.Environment.WorkerPools[0]

	if opts.NumWorkers > 0 {
		workerPool.NumWorkers = opts.NumWorkers
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
	if streaming {
		// Add separate data disk for streaming jobs
		workerPool.DataDisks = []*df.Disk{{}}
	}

	return job, nil
}

// Submit submits a prepared job to Cloud Dataflow.
func Submit(ctx context.Context, client *df.Service, project, region string, job *df.Job) (*df.Job, error) {
	return client.Projects.Locations.Jobs.Create(project, region, job).Do()
}

// WaitForCompletion monitors the given job until completion. It logs any messages
// and state changes received.
func WaitForCompletion(ctx context.Context, client *df.Service, project, region, jobID string) error {
	for {
		j, err := client.Projects.Locations.Jobs.Get(project, region, jobID).Do()
		if err != nil {
			return errors.Wrap(err, "failed to get job")
		}

		switch j.CurrentState {
		case "JOB_STATE_DONE":
			log.Info(ctx, "Job succeeded!")
			return nil

		case "JOB_STATE_CANCELLED":
			log.Info(ctx, "Job cancelled")
			return nil

		case "JOB_STATE_FAILED":
			return errors.Errorf("job %s failed", jobID)

		case "JOB_STATE_RUNNING":
			log.Info(ctx, "Job still running ...")

		default:
			log.Infof(ctx, "Job state: %v ...", j.CurrentState)
		}

		time.Sleep(30 * time.Second)
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

type dataflowOptions struct {
	Experiments []string `json:"experiments,omitempty"`
	PipelineURL string   `json:"pipelineUrl"`
	Region      string   `json:"region"`
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
