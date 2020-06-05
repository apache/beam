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

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/protox"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/universal/runnerlib"
	"github.com/golang/protobuf/proto"
	df "google.golang.org/api/dataflow/v1b3"
)

// Execute submits a pipeline as a Dataflow job.
func Execute(ctx context.Context, raw *pipepb.Pipeline, opts *JobOptions, workerURL, jarURL, modelURL, endpoint string, async bool) (string, error) {
	// (1) Upload Go binary to GCS.

	bin := opts.Worker
	if bin == "" {
		if self, ok := runnerlib.IsWorkerCompatibleBinary(); ok {
			bin = self
			log.Infof(ctx, "Using running binary as worker binary: '%v'", bin)
		} else {
			// Cross-compile as last resort.

			worker, err := runnerlib.BuildTempWorkerBinary(ctx)
			if err != nil {
				return "", err
			}
			defer os.Remove(worker)

			bin = worker
		}
	} else {
		log.Infof(ctx, "Using specified worker binary: '%v'", bin)
	}

	log.Infof(ctx, "Staging worker binary: %v", bin)

	if err := StageFile(ctx, opts.Project, workerURL, bin); err != nil {
		return "", err
	}
	log.Infof(ctx, "Staged worker binary: %v", workerURL)

	if opts.WorkerJar != "" {
		log.Infof(ctx, "Staging Dataflow worker jar: %v", opts.WorkerJar)

		if err := StageFile(ctx, opts.Project, jarURL, opts.WorkerJar); err != nil {
			return "", err
		}
		log.Infof(ctx, "Staged worker jar: %v", jarURL)
	}

	// (2) Fixup and upload model to GCS

	p, err := Fixup(raw)
	if err != nil {
		return "", err
	}
	log.Info(ctx, proto.MarshalTextString(p))

	if err := StageModel(ctx, opts.Project, modelURL, protox.MustEncode(p)); err != nil {
		return "", err
	}
	log.Infof(ctx, "Staged model pipeline: %v", modelURL)

	// (3) Translate to v1b3 and submit

	job, err := Translate(p, opts, workerURL, jarURL, modelURL)
	if err != nil {
		return "", err
	}
	PrintJob(ctx, job)

	client, err := NewClient(ctx, endpoint)
	if err != nil {
		return "", err
	}
	upd, err := Submit(ctx, client, opts.Project, opts.Region, job)
	if err != nil {
		return "", err
	}
	log.Infof(ctx, "Submitted job: %v", upd.Id)
	if endpoint == "" {
		log.Infof(ctx, "Console: https://console.cloud.google.com/dataflow/job/%v?project=%v", upd.Id, opts.Project)
	}
	log.Infof(ctx, "Logs: https://console.cloud.google.com/logs/viewer?project=%v&resource=dataflow_step%%2Fjob_id%%2F%v", opts.Project, upd.Id)

	if async {
		return upd.Id, nil
	}

	// (4) Wait for completion.

	return upd.Id, WaitForCompletion(ctx, client, opts.Project, opts.Region, upd.Id)
}

// PrintJob logs the Dataflow job.
func PrintJob(ctx context.Context, job *df.Job) {
	str, err := json.MarshalIndent(job, "", "  ")
	if err != nil {
		log.Infof(ctx, "Failed to print job %v: %v", job.Id, err)
	}
	log.Info(ctx, string(str))
}
