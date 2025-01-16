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

package runnerlib

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/beam/sdks/v2/go/container/tools"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/hooks"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
)

// JobOptions capture the various options for submitting jobs
// to universal runners.
type JobOptions struct {
	// Name is the job name.
	Name string
	// Experiments are additional experiments.
	Experiments []string

	// Worker is the worker binary override.
	Worker string

	// RetainDocker is an option to pass to the runner indicating the docker containers should be cached.
	RetainDocker bool

	// Indicates a limit on parallelism the runner should impose.
	Parallelism int

	// Loopback indicates this job is running in loopback mode and will reconnect to the local process.
	Loopback bool
}

// Prepare prepares a job to the given job service. It returns the preparation id
// artifact staging endpoint, and staging token if successful.
func Prepare(ctx context.Context, client jobpb.JobServiceClient, p *pipepb.Pipeline, opt *JobOptions) (id, endpoint, stagingToken string, err error) {
	hooks.SerializeHooksToOptions()
	beam.PipelineOptions.LoadOptionsFromFlags(nil)
	raw := runtime.RawOptionsWrapper{
		Options:      beam.PipelineOptions.Export(),
		AppName:      opt.Name,
		Experiments:  append(opt.Experiments, "beam_fn_api"),
		RetainDocker: opt.RetainDocker,
		Parallelism:  opt.Parallelism,
	}

	options, err := tools.OptionsToProto(raw)
	if err != nil {
		return "", "", "", errors.WithContext(err, "producing pipeline options")
	}
	req := &jobpb.PrepareJobRequest{
		Pipeline:        p,
		PipelineOptions: options,
		JobName:         opt.Name,
	}
	resp, err := client.Prepare(ctx, req)
	if err != nil {
		return "", "", "", errors.Wrap(err, "job failed to prepare")
	}
	return resp.GetPreparationId(), resp.GetArtifactStagingEndpoint().GetUrl(), resp.GetStagingSessionToken(), nil
}

// Submit submits a job to the given job service. It returns a jobID, if successful.
func Submit(ctx context.Context, client jobpb.JobServiceClient, id, token string) (string, error) {
	req := &jobpb.RunJobRequest{
		PreparationId:  id,
		RetrievalToken: token,
	}

	resp, err := client.Run(ctx, req)
	if err != nil {
		return "", errors.Wrap(err, "failed to submit job")
	}
	return resp.GetJobId(), nil
}

// WaitForCompletion monitors the given job until completion. It logs any messages
// and state changes received.
func WaitForCompletion(ctx context.Context, client jobpb.JobServiceClient, jobID string) error {
	stream, err := client.GetMessageStream(ctx, &jobpb.JobMessagesRequest{JobId: jobID})
	if err != nil {
		return errors.Wrap(err, "failed to get job stream")
	}

	mostRecentError := "<no error received>"
	var errReceived, jobFailed bool

	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				if jobFailed {
					// Connection finished, so time to exit, produce what we have.
					return errors.Errorf("job %v failed:\n%v", jobID, mostRecentError)
				}
				return nil
			}
			return err
		}

		switch {
		case msg.GetStateResponse() != nil:
			resp := msg.GetStateResponse()

			log.Infof(ctx, "Job[%v] state: %v", jobID, resp.GetState().String())

			switch resp.State {
			case jobpb.JobState_DONE, jobpb.JobState_CANCELLED:
				return nil
			case jobpb.JobState_FAILED:
				jobFailed = true
				if errReceived {
					return errors.Errorf("job %v failed:\n%v", jobID, mostRecentError)
				}
				// Otherwise we should wait for at least one error log from the runner.
			}

		case msg.GetMessageResponse() != nil:
			resp := msg.GetMessageResponse()

			text := fmt.Sprintf("%v (%v): %v", resp.GetTime(), resp.GetMessageId(), resp.GetMessageText())
			log.Output(ctx, messageSeverity(resp.GetImportance()), 1, text)

			if resp.GetImportance() >= jobpb.JobMessage_JOB_MESSAGE_ERROR {
				errReceived = true
				mostRecentError = resp.GetMessageText()

				if jobFailed {
					return errors.Errorf("job %v failed:\n%w", jobID, errors.New(mostRecentError))
				}
			}

		default:
			return errors.Errorf("unexpected job update: %v", msg.String())
		}
	}
}

func messageSeverity(importance jobpb.JobMessage_MessageImportance) log.Severity {
	switch importance {
	case jobpb.JobMessage_JOB_MESSAGE_ERROR:
		return log.SevError
	case jobpb.JobMessage_JOB_MESSAGE_WARNING:
		return log.SevWarn
	case jobpb.JobMessage_JOB_MESSAGE_BASIC:
		return log.SevInfo
	case jobpb.JobMessage_JOB_MESSAGE_DEBUG, jobpb.JobMessage_JOB_MESSAGE_DETAILED:
		return log.SevDebug
	default:
		return log.SevUnspecified
	}
}
