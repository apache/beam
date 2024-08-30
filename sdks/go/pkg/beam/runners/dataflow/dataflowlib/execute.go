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

// Package dataflowlib translates a Beam pipeline model to the
// Dataflow API job model, for submission to Google Cloud Dataflow.
package dataflowlib

import (
	"context"
	"encoding/json"
	"os"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/protox"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/universal/runnerlib"
	df "google.golang.org/api/dataflow/v1b3"
	"google.golang.org/api/googleapi"
)

// Execute submits a pipeline as a Dataflow job.
func Execute(ctx context.Context, raw *pipepb.Pipeline, opts *JobOptions, workerURL, modelURL, endpoint string, async bool) (*dataflowPipelineResult, error) {
	// (1) Upload Go binary to GCS.
	presult := &dataflowPipelineResult{}

	bin := opts.Worker
	if bin == "" {
		if self, ok := runnerlib.IsWorkerCompatibleBinary(); ok {
			bin = self
			log.Infof(ctx, "Using running binary as worker binary: '%v'", bin)
		} else {
			// Cross-compile as last resort.

			var copts runnerlib.CompileOpts
			if strings.HasPrefix(opts.MachineType, "t2a") {
				copts.Arch = "arm64"
			}

			worker, err := runnerlib.BuildTempWorkerBinary(ctx, copts)
			if err != nil {
				return presult, err
			}
			defer os.Remove(worker)

			bin = worker
		}
	} else {
		log.Infof(ctx, "Using specified worker binary: '%v'", bin)
	}

	log.Infof(ctx, "Staging worker binary: %v", bin)
	hash, err := stageFile(ctx, opts.Project, workerURL, bin)
	if err != nil {
		return presult, err
	}
	log.Infof(ctx, "Staged worker binary: %v", workerURL)

	if err := graphx.UpdateDefaultEnvWorkerType(
		graphx.URNArtifactURLType,
		protox.MustEncode(&pipepb.ArtifactUrlPayload{
			Url:    workerURL,
			Sha256: hash,
		}), raw); err != nil {
		return presult, err
	}

	// (2) Upload model to GCS
	log.Info(ctx, raw.String())

	if err := StageModel(ctx, opts.Project, modelURL, protox.MustEncode(raw)); err != nil {
		return presult, err
	}
	log.Infof(ctx, "Staged model pipeline: %v", modelURL)

	// (3) Translate to v1b3 and submit

	job, err := Translate(ctx, raw, opts, workerURL, modelURL)
	if err != nil {
		return presult, err
	}
	PrintJob(ctx, job)

	if opts.TemplateLocation != "" {
		marshalled, err := job.MarshalJSON()
		if err != nil {
			return presult, err
		}
		if err := StageModel(ctx, opts.Project, opts.TemplateLocation, marshalled); err != nil {
			return presult, err
		}
		log.Infof(ctx, "Template staged to %v", opts.TemplateLocation)
		return nil, nil
	}

	client, err := NewClient(ctx, endpoint)
	if err != nil {
		return presult, err
	}
	upd, err := Submit(ctx, client, opts.Project, opts.Region, job, opts.Update)
	// When in async mode, if we get a 409 because we've already submitted an actively running job with the same name
	// just return the existing job as a convenience
	if gErr, ok := err.(*googleapi.Error); async && ok && gErr.Code == 409 {
		log.Info(ctx, "Unable to submit job because job with same name is already actively running. Querying Dataflow for existing job")
		upd, err = GetRunningJobByName(client, opts.Project, opts.Region, job.Name)
	}
	if err != nil {
		return presult, err
	}

	if endpoint == "" {
		log.Infof(ctx, "Console: https://console.cloud.google.com/dataflow/jobs/%v/%v?project=%v", opts.Region, upd.Id, opts.Project)
	}
	log.Infof(ctx, "Logs: https://console.cloud.google.com/logs/viewer?project=%v&resource=dataflow_step%%2Fjob_id%%2F%v", opts.Project, upd.Id)

	presult.jobID = upd.Id

	if async {
		return presult, nil
	}

	// (4) Wait for completion.
	err = WaitForCompletion(ctx, client, opts.Project, opts.Region, upd.Id)

	res, presultErr := newDataflowPipelineResult(ctx, client, raw, opts.Project, opts.Region, upd.Id)
	if presultErr != nil {
		if err != nil {
			return presult, errors.Wrap(err, presultErr.Error())
		}
		return presult, presultErr
	}
	return res, err
}

// PrintJob logs the Dataflow job.
func PrintJob(ctx context.Context, job *df.Job) {
	str, err := json.MarshalIndent(job, "", "  ")
	if err != nil {
		log.Infof(ctx, "Failed to print job %v: %v", job.Id, err)
	}
	log.Info(ctx, string(str))
}

type dataflowPipelineResult struct {
	jobID   string
	metrics *metrics.Results
}

func newDataflowPipelineResult(ctx context.Context, client *df.Service, p *pipepb.Pipeline, project, region, jobID string) (*dataflowPipelineResult, error) {
	res, err := GetMetrics(ctx, client, project, region, jobID)
	if err != nil {
		return &dataflowPipelineResult{jobID, nil}, errors.Wrap(err, "failed to get metrics")
	}
	return &dataflowPipelineResult{jobID, FromMetricUpdates(res.Metrics, p)}, nil
}

func (pr dataflowPipelineResult) Metrics() metrics.Results {
	return *pr.metrics
}

func (pr dataflowPipelineResult) JobID() string {
	return pr.jobID
}
