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

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	jobpb "github.com/apache/beam/sdks/go/pkg/beam/model/jobmanagement_v1"
	pb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/provision"
	"github.com/golang/protobuf/proto"
)

// JobOptions capture the various options for submitting jobs
// to universal runners.
type JobOptions struct {
	// Name is the job name.
	Name string
	// Experiments are additional experiments.
	Experiments []string

	// TODO(herohde) 3/17/2018: add further parametrization as needed

	// Worker is the worker binary override.
	Worker string

	// InternalJavaRunner is the class of the receiving Java runner. To be removed.
	InternalJavaRunner string
}

// Prepare prepares a job to the given job service. It returns the preparation id
// and artifact staging endpoint, if successful.
func Prepare(ctx context.Context, client jobpb.JobServiceClient, p *pb.Pipeline, opt *JobOptions) (id, endpoint string, err error) {
	raw := runtime.RawOptionsWrapper{
		Options:     beam.PipelineOptions.Export(),
		Runner:      opt.InternalJavaRunner,
		AppName:     opt.Name,
		Experiments: append(opt.Experiments, "beam_fn_api"),
	}

	options, err := provision.OptionsToProto(raw)
	if err != nil {
		return "", "", fmt.Errorf("failed to produce pipeline options: %v", err)
	}
	req := &jobpb.PrepareJobRequest{
		Pipeline:        p,
		PipelineOptions: options,
		JobName:         opt.Name,
	}
	resp, err := client.Prepare(ctx, req)
	if err != nil {
		return "", "", fmt.Errorf("failed to connect to job service: %v", err)
	}
	return resp.GetPreparationId(), resp.GetArtifactStagingEndpoint().GetUrl(), nil
}

// Submit submits a job to the given job service. It returns a jobID, if successful.
func Submit(ctx context.Context, client jobpb.JobServiceClient, id, token string) (string, error) {
	req := &jobpb.RunJobRequest{
		PreparationId: id,
		StagingToken:  token,
	}

	resp, err := client.Run(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to submit job: %v", err)
	}
	return resp.GetJobId(), nil
}

// WaitForCompletion monitors the given job until completion. It logs any messages
// and state changes received.
func WaitForCompletion(ctx context.Context, client jobpb.JobServiceClient, jobID string) error {
	stream, err := client.GetMessageStream(ctx, &jobpb.JobMessagesRequest{JobId: jobID})
	if err != nil {
		return fmt.Errorf("failed to get job stream: %v", err)
	}

	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		switch {
		case msg.GetStateResponse() != nil:
			resp := msg.GetStateResponse()

			log.Infof(ctx, "Job state: %v", resp.GetState().String())

			switch resp.State {
			case jobpb.JobState_DONE, jobpb.JobState_CANCELLED:
				return nil
			case jobpb.JobState_FAILED:
				return fmt.Errorf("job %v failed", jobID)
			}

		case msg.GetMessageResponse() != nil:
			resp := msg.GetMessageResponse()

			text := fmt.Sprintf("%v (%v): %v", resp.GetTime(), resp.GetMessageId(), resp.GetMessageText())
			log.Output(ctx, messageSeverity(resp.GetImportance()), 1, text)

		default:
			return fmt.Errorf("unexpected job update: %v", proto.MarshalTextString(msg))
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
