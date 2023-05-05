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

package jobservices

import (
	"context"
	"fmt"
	"sync/atomic"

	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"golang.org/x/exp/slog"
)

func (s *Server) nextId() string {
	v := atomic.AddUint32(&s.index, 1)
	return fmt.Sprintf("job-%03d", v)
}

func (s *Server) Prepare(ctx context.Context, req *jobpb.PrepareJobRequest) (*jobpb.PrepareJobResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Since jobs execute in the background, they should not be tied to a request's context.
	rootCtx, cancelFn := context.WithCancel(context.Background())
	job := &Job{
		key:      s.nextId(),
		Pipeline: req.GetPipeline(),
		jobName:  req.GetJobName(),
		options:  req.GetPipelineOptions(),

		msgChan:   make(chan string, 100),
		stateChan: make(chan jobpb.JobState_Enum, 1),
		RootCtx:   rootCtx,
		CancelFn:  cancelFn,
	}

	// Queue initial state of the job.
	job.state.Store(jobpb.JobState_STOPPED)
	job.stateChan <- job.state.Load().(jobpb.JobState_Enum)

	if err := isSupported(job.Pipeline.GetRequirements()); err != nil {
		slog.Error("unable to run job", err, slog.String("jobname", req.GetJobName()))
		return nil, err
	}
	s.jobs[job.key] = job
	return &jobpb.PrepareJobResponse{
		PreparationId:       job.key,
		StagingSessionToken: job.key,
		ArtifactStagingEndpoint: &pipepb.ApiServiceDescriptor{
			Url: s.Endpoint(),
		},
	}, nil
}

func (s *Server) Run(ctx context.Context, req *jobpb.RunJobRequest) (*jobpb.RunJobResponse, error) {
	s.mu.Lock()
	job := s.jobs[req.GetPreparationId()]
	s.mu.Unlock()

	// Bring up a background goroutine to allow the job to continue processing.
	go s.execute(job)

	return &jobpb.RunJobResponse{
		JobId: job.key,
	}, nil
}

// GetMessageStream subscribes to a stream of state changes and messages from the job
func (s *Server) GetMessageStream(req *jobpb.JobMessagesRequest, stream jobpb.JobService_GetMessageStreamServer) error {
	s.mu.Lock()
	job := s.jobs[req.GetJobId()]
	s.mu.Unlock()

	for {
		select {
		case msg := <-job.msgChan:
			stream.Send(&jobpb.JobMessagesResponse{
				Response: &jobpb.JobMessagesResponse_MessageResponse{
					MessageResponse: &jobpb.JobMessage{
						MessageText: msg,
						Importance:  jobpb.JobMessage_JOB_MESSAGE_BASIC,
					},
				},
			})

		case state, ok := <-job.stateChan:
			// TODO: Don't block job execution if WaitForCompletion isn't being run.
			// The state channel means the job may only execute if something is observing
			// the message stream, as the send on the state or message channel may block
			// once full.
			// Not a problem for tests or short lived batch, but would be hazardous for
			// asynchronous jobs.

			// Channel is closed, so the job must be done.
			if !ok {
				state = jobpb.JobState_DONE
			}
			job.state.Store(state)
			stream.Send(&jobpb.JobMessagesResponse{
				Response: &jobpb.JobMessagesResponse_StateResponse{
					StateResponse: &jobpb.JobStateEvent{
						State: state,
					},
				},
			})
			switch state {
			case jobpb.JobState_CANCELLED, jobpb.JobState_DONE, jobpb.JobState_DRAINED, jobpb.JobState_FAILED, jobpb.JobState_UPDATED:
				// Reached terminal state.
				return nil
			}
		}
	}

}

// GetJobMetrics Fetch metrics for a given job.
func (s *Server) GetJobMetrics(ctx context.Context, req *jobpb.GetJobMetricsRequest) (*jobpb.GetJobMetricsResponse, error) {
	j := s.getJob(req.GetJobId())
	if j == nil {
		return nil, fmt.Errorf("GetJobMetrics: unknown jobID: %v", req.GetJobId())
	}
	return &jobpb.GetJobMetricsResponse{
		Metrics: &jobpb.MetricResults{
			Attempted: j.metrics.Results(tentative),
			Committed: j.metrics.Results(committed),
		},
	}, nil
}
