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

package dataflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/user"
	"path"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
	// Importing to get the side effect of the remote execution hook. See init().
	_ "github.com/apache/beam/sdks/go/pkg/beam/core/runtime/harness/init"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/hooks"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/protox"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/go/pkg/beam/options/jobopts"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/universal/runnerlib"
	"github.com/apache/beam/sdks/go/pkg/beam/util/gcsx"
	"github.com/apache/beam/sdks/go/pkg/beam/x/hooks/perf"
	"github.com/golang/protobuf/proto"
	"golang.org/x/oauth2/google"
	df "google.golang.org/api/dataflow/v1b3"
	"google.golang.org/api/storage/v1"
)

// TODO(herohde) 5/16/2017: the Dataflow flags should match the other SDKs.

var (
	endpoint        = flag.String("dataflow_endpoint", "", "Dataflow endpoint (optional).")
	stagingLocation = flag.String("staging_location", "", "GCS staging location (required).")
	image           = flag.String("worker_harness_container_image", "", "Worker harness container image (required).")
	numWorkers      = flag.Int64("num_workers", 0, "Number of workers (optional).")
	zone            = flag.String("zone", "", "GCP zone (optional)")
	network         = flag.String("network", "", "GCP network (optional)")
	tempLocation    = flag.String("temp_location", "", "Temp location (optional)")
	machineType     = flag.String("worker_machine_type", "", "GCE machine type (optional)")

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

type dataflowOptions struct {
	PipelineURL string `json:"pipelineUrl"`
}

// Execute runs the given pipeline on Google Cloud Dataflow. It uses the
// default application credentials to submit the job.
func Execute(ctx context.Context, p *beam.Pipeline) error {
	project := *gcpopts.Project
	if project == "" {
		return errors.New("no Google Cloud project specified. Use --project=<project>")
	}
	if *stagingLocation == "" {
		return errors.New("no GCS staging location specified. Use --staging_location=gs://<bucket>/<path>")
	}
	if *image == "" {
		*image = jobopts.GetContainerImage(ctx)
	}
	jobName := jobopts.GetJobName()

	edges, _, err := p.Build()
	if err != nil {
		return err
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

	hooks.SerializeHooksToOptions()
	options := beam.PipelineOptions.Export()

	// (1) Upload Go binary and model to GCS.

	if *jobopts.WorkerBinary == "" {
		worker, err := runnerlib.BuildTempWorkerBinary(ctx)
		if err != nil {
			return err
		}
		defer os.Remove(worker)

		*jobopts.WorkerBinary = worker
	} else {
		log.Infof(ctx, "Using specified worker binary: '%v'", *jobopts.WorkerBinary)
	}

	binary, err := stageWorker(ctx, project, *stagingLocation, *jobopts.WorkerBinary)
	if err != nil {
		return err
	}

	model, err := graphx.Marshal(edges, &graphx.Options{ContainerImageURL: *image})
	if err != nil {
		return fmt.Errorf("failed to generate model pipeline: %v", err)
	}
	modelURL, err := stageModel(ctx, project, *stagingLocation, protox.MustEncode(model))
	if err != nil {
		return err
	}
	log.Info(ctx, proto.MarshalTextString(model))

	// (2) Translate pipeline to v1b3 speak.

	steps, err := translate(edges)
	if err != nil {
		return err
	}

	job := &df.Job{
		ProjectId: project,
		Name:      jobName,
		Type:      "JOB_TYPE_BATCH",
		Environment: &df.Environment{
			UserAgent: newMsg(userAgent{
				Name:    "Apache Beam SDK for Go",
				Version: "0.3.0",
			}),
			Version: newMsg(version{
				JobType: "FNAPI_BATCH",
				Major:   "6",
			}),
			SdkPipelineOptions: newMsg(pipelineOptions{
				DisplayData: findPipelineFlags(),
				Options: dataflowOptions{
					PipelineURL: modelURL,
				},
				GoOptions: options,
			}),
			WorkerPools: []*df.WorkerPool{{
				Kind: "harness",
				Packages: []*df.Package{{
					Location: binary,
					Name:     "worker",
				}},
				WorkerHarnessContainerImage: *image,
				NumWorkers:                  1,
				MachineType:                 *machineType,
				Network:                     *network,
				Zone:                        *zone,
			}},
			TempStoragePrefix: *stagingLocation + "/tmp",
			Experiments:       jobopts.GetExperiments(),
		},
		Steps: steps,
	}

	if *numWorkers > 0 {
		job.Environment.WorkerPools[0].NumWorkers = *numWorkers
	}
	if *teardownPolicy != "" {
		job.Environment.WorkerPools[0].TeardownPolicy = *teardownPolicy
	}
	if *tempLocation != "" {
		job.Environment.TempStoragePrefix = *tempLocation
	}
	printJob(ctx, job)

	if *dryRun {
		log.Info(ctx, "Dry-run: not submitting job!")
		return nil
	}

	// (4) Submit job.

	client, err := newClient(ctx, *endpoint)
	if err != nil {
		return err
	}
	upd, err := client.Projects.Jobs.Create(project, job).Do()
	if err != nil {
		return err
	}

	log.Infof(ctx, "Submitted job: %v", upd.Id)
	printJob(ctx, upd)
	if *endpoint == "" {
		log.Infof(ctx, "Console: https://console.cloud.google.com/dataflow/job/%v?project=%v", upd.Id, project)
	}
	log.Infof(ctx, "Logs: https://console.cloud.google.com/logs/viewer?project=%v&resource=dataflow_step%%2Fjob_id%%2F%v", project, upd.Id)

	if *jobopts.Async {
		return nil
	}

	time.Sleep(1 * time.Minute)
	for {
		j, err := client.Projects.Jobs.Get(project, upd.Id).Do()
		if err != nil {
			return fmt.Errorf("failed to get job: %v", err)
		}

		switch j.CurrentState {
		case "JOB_STATE_DONE":
			log.Info(ctx, "Job succeeded!")
			return nil

		case "JOB_STATE_FAILED":
			return fmt.Errorf("job %s failed", upd.Id)

		case "JOB_STATE_RUNNING":
			log.Info(ctx, "Job still running ...")

		default:
			log.Infof(ctx, "Job state: %v ...", j.CurrentState)
		}

		time.Sleep(30 * time.Second)
	}
}

// stageModel uploads the pipeline model to GCS as a unique object.
func stageModel(ctx context.Context, project, location string, model []byte) (string, error) {
	bucket, prefix, err := gcsx.ParseObject(location)
	if err != nil {
		return "", fmt.Errorf("invalid staging location %v: %v", location, err)
	}
	obj := path.Join(prefix, fmt.Sprintf("pipeline-%v", time.Now().UnixNano()))
	if *dryRun {
		full := fmt.Sprintf("gs://%v/%v", bucket, obj)
		log.Infof(ctx, "Dry-run: not uploading model %v", full)
		return full, nil
	}

	client, err := gcsx.NewClient(ctx, storage.DevstorageReadWriteScope)
	if err != nil {
		return "", err
	}
	return gcsx.Upload(client, project, bucket, obj, bytes.NewReader(model))
}

// stageWorker uploads the worker binary to GCS as a unique object.
func stageWorker(ctx context.Context, project, location, worker string) (string, error) {
	bucket, prefix, err := gcsx.ParseObject(location)
	if err != nil {
		return "", fmt.Errorf("invalid staging location %v: %v", location, err)
	}
	obj := path.Join(prefix, fmt.Sprintf("worker-%v", time.Now().UnixNano()))
	if *dryRun {
		full := fmt.Sprintf("gs://%v/%v", bucket, obj)
		log.Infof(ctx, "Dry-run: not uploading binary %v", full)
		return full, nil
	}

	client, err := gcsx.NewClient(ctx, storage.DevstorageReadWriteScope)
	if err != nil {
		return "", err
	}
	fd, err := os.Open(worker)
	if err != nil {
		return "", fmt.Errorf("failed to open worker binary %s: %v", worker, err)
	}
	defer fd.Close()
	defer os.Remove(worker)

	return gcsx.Upload(client, project, bucket, obj, fd)
}

func username() string {
	if u, err := user.Current(); err == nil {
		return u.Username
	}
	return "anon"
}

func findPipelineFlags() []*displayData {
	var ret []*displayData

	// TODO(herohde) 2/15/2017: decide if we want all set flags.
	flag.Visit(func(f *flag.Flag) {
		ret = append(ret, newDisplayData(f.Name, "", "flag", f.Value.(flag.Getter).Get()))
	})

	return ret
}

// newClient creates a new dataflow client with default application credentials
// and CloudPlatformScope. The Dataflow endpoint is optionally overridden.
func newClient(ctx context.Context, endpoint string) (*df.Service, error) {
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

func printJob(ctx context.Context, job *df.Job) {
	str, err := json.MarshalIndent(job, "", "  ")
	if err != nil {
		log.Infof(ctx, "Failed to print job %v: %v", job.Id, err)
	}
	log.Info(ctx, string(str))
}

func gcsRecorderHook(opts []string) perf.CaptureHook {
	bucket, prefix, err := gcsx.ParseObject(opts[0])
	if err != nil {
		panic(fmt.Sprintf("Invalid hook configuration for gcsRecorderHook: %s", opts))
	}

	return func(ctx context.Context, spec string, r io.Reader) error {
		client, err := gcsx.NewClient(ctx, storage.DevstorageReadWriteScope)
		if err != nil {
			return fmt.Errorf("couldn't establish GCS client: %v", err)
		}
		return gcsx.WriteObject(client, bucket, path.Join(prefix, spec), r)
	}
}
