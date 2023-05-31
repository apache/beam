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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/urns"
	"golang.org/x/exp/slog"
)

func (s *Server) nextId() string {
	v := atomic.AddUint32(&s.index, 1)
	return fmt.Sprintf("job-%03d", v)
}

type unimplementedError struct {
	feature string
	value   any
}

func (err unimplementedError) Error() string {
	return fmt.Sprintf("unsupported feature %q set with value %v", err.feature, err.value)
}

func (err unimplementedError) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("feature", err.feature),
		slog.Any("value", err.value))
}

// TODO migrate to errors.Join once Beam requires go1.20+
type joinError struct {
	errs []error
}

func (e *joinError) Error() string {
	var b []byte
	for i, err := range e.errs {
		if i > 0 {
			b = append(b, '\n')
		}
		b = append(b, err.Error()...)
	}
	return string(b)
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
		slog.Error("unable to run job", slog.String("error", err.Error()), slog.String("jobname", req.GetJobName()))
		return nil, err
	}
	var errs []error
	check := func(feature string, got, want any) {
		if got != want {
			err := unimplementedError{
				feature: feature,
				value:   got,
			}
			errs = append(errs, err)
		}
	}

	// Inspect Transforms for unsupported features.
	for _, t := range job.Pipeline.GetComponents().GetTransforms() {
		urn := t.GetSpec().GetUrn()
		switch urn {
		case urns.TransformImpulse,
			urns.TransformParDo,
			urns.TransformGBK,
			urns.TransformFlatten,
			urns.TransformCombinePerKey,
			urns.TransformAssignWindows:
		// Very few expected transforms types for submitted pipelines.
		// Most URNs are for the runner to communicate back to the SDK for execution.
		case "":
			// Composites can often have no spec
			if len(t.GetSubtransforms()) > 0 {
				continue
			}
			fallthrough
		default:
			check("PTransform.Spec.Urn", urn+" "+t.GetUniqueName(), "<doesn't exist>")
		}
	}

	// Inspect Windowing strategies for unsupported features.
	for _, ws := range job.Pipeline.GetComponents().GetWindowingStrategies() {
		check("WindowingStrategy.AllowedLateness", ws.GetAllowedLateness(), int64(0))
		check("WindowingStrategy.ClosingBehaviour", ws.GetClosingBehavior(), pipepb.ClosingBehavior_EMIT_IF_NONEMPTY)
		check("WindowingStrategy.AccumulationMode", ws.GetAccumulationMode(), pipepb.AccumulationMode_DISCARDING)
		if ws.GetWindowFn().GetUrn() != urns.WindowFnSession {
			check("WindowingStrategy.MergeStatus", ws.GetMergeStatus(), pipepb.MergeStatus_NON_MERGING)
		}
		check("WindowingStrategy.OnTimerBehavior", ws.GetOnTimeBehavior(), pipepb.OnTimeBehavior_FIRE_IF_NONEMPTY)
		check("WindowingStrategy.OutputTime", ws.GetOutputTime(), pipepb.OutputTime_END_OF_WINDOW)
		// Non nil triggers should fail.
		if ws.GetTrigger().GetDefault() == nil {
			check("WindowingStrategy.Trigger", ws.GetTrigger(), &pipepb.Trigger_Default{})
		}
	}
	if len(errs) > 0 {
		err := &joinError{errs: errs}
		slog.Error("unable to run job", slog.String("cause", "unimplemented features"), slog.String("jobname", req.GetJobName()), slog.String("errors", err.Error()))
		return nil, fmt.Errorf("found %v uses of features unimplemented in prism in job %v: %v", len(errs), req.GetJobName(), err)
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

// GetJobs returns the set of active jobs and associated metadata.
func (s *Server) GetJobs(context.Context, *jobpb.GetJobsRequest) (*jobpb.GetJobsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	resp := &jobpb.GetJobsResponse{}
	for key, job := range s.jobs {
		resp.JobInfo = append(resp.JobInfo, &jobpb.JobInfo{
			JobId:           key,
			JobName:         job.jobName,
			State:           job.state.Load().(jobpb.JobState_Enum),
			PipelineOptions: job.options,
		})
	}
	return resp, nil
}

// GetPipeline returns pipeline proto of the requested job id.
func (s *Server) GetPipeline(_ context.Context, req *jobpb.GetJobPipelineRequest) (*jobpb.GetJobPipelineResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[req.GetJobId()]
	if !ok {
		return nil, fmt.Errorf("job with id %v not found", req.GetJobId())
	}
	return &jobpb.GetJobPipelineResponse{
		Pipeline: j.Pipeline,
	}, nil
}
