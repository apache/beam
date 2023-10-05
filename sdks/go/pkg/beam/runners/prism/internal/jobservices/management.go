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
	"sync"
	"sync/atomic"

	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/urns"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slog"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	rootCtx, cancelFn := context.WithCancelCause(context.Background())
	job := &Job{
		key:        s.nextId(),
		Pipeline:   req.GetPipeline(),
		jobName:    req.GetJobName(),
		options:    req.GetPipelineOptions(),
		streamCond: sync.NewCond(&sync.Mutex{}),
		RootCtx:    rootCtx,
		CancelFn:   cancelFn,

		artifactEndpoint: s.Endpoint(),
	}

	// Queue initial state of the job.
	job.state.Store(jobpb.JobState_STOPPED)
	s.jobs[job.key] = job

	if err := isSupported(job.Pipeline.GetRequirements()); err != nil {
		job.Failed(err)
		slog.Error("unable to run job", slog.String("error", err.Error()), slog.String("jobname", req.GetJobName()))
		return nil, err
	}
	var errs []error
	check := func(feature string, got any, wants ...any) {
		for _, want := range wants {
			if got == want {
				return
			}
		}

		err := unimplementedError{
			feature: feature,
			value:   got,
		}
		errs = append(errs, err)
	}

	// Inspect Transforms for unsupported features.
	bypassedWindowingStrategies := map[string]bool{}
	ts := job.Pipeline.GetComponents().GetTransforms()
	for _, t := range ts {
		urn := t.GetSpec().GetUrn()
		switch urn {
		case urns.TransformImpulse,
			urns.TransformParDo,
			urns.TransformGBK,
			urns.TransformFlatten,
			urns.TransformCombinePerKey,
			urns.TransformCombineGlobally,      // Used by Java SDK
			urns.TransformCombineGroupedValues, // Used by Java SDK
			urns.TransformAssignWindows:
		// Very few expected transforms types for submitted pipelines.
		// Most URNs are for the runner to communicate back to the SDK for execution.
		case urns.TransformReshuffle:
			// Reshuffles use features we don't yet support, but we would like to
			// support them by making them the no-op they are, and be precise about
			// what we're ignoring.
			var cols []string
			for _, stID := range t.GetSubtransforms() {
				st := ts[stID]
				// Only check the outputs, since reshuffle re-instates any previous WindowingStrategy
				// so we still validate the strategy used by the input, avoiding skips.
				cols = append(cols, maps.Values(st.GetOutputs())...)
			}

			pcs := job.Pipeline.GetComponents().GetPcollections()
			for _, col := range cols {
				wsID := pcs[col].GetWindowingStrategyId()
				bypassedWindowingStrategies[wsID] = true
			}
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
	for wsID, ws := range job.Pipeline.GetComponents().GetWindowingStrategies() {
		check("WindowingStrategy.AllowedLateness", ws.GetAllowedLateness(), int64(0))
		check("WindowingStrategy.ClosingBehaviour", ws.GetClosingBehavior(), pipepb.ClosingBehavior_EMIT_IF_NONEMPTY)
		check("WindowingStrategy.AccumulationMode", ws.GetAccumulationMode(), pipepb.AccumulationMode_DISCARDING)
		if ws.GetWindowFn().GetUrn() != urns.WindowFnSession {
			check("WindowingStrategy.MergeStatus", ws.GetMergeStatus(), pipepb.MergeStatus_NON_MERGING)
		}
		if !bypassedWindowingStrategies[wsID] {
			check("WindowingStrategy.OnTimeBehavior", ws.GetOnTimeBehavior(), pipepb.OnTimeBehavior_FIRE_IF_NONEMPTY, pipepb.OnTimeBehavior_FIRE_ALWAYS)
			check("WindowingStrategy.OutputTime", ws.GetOutputTime(), pipepb.OutputTime_END_OF_WINDOW)
			// Non nil triggers should fail.
			if ws.GetTrigger().GetDefault() == nil {
				check("WindowingStrategy.Trigger", ws.GetTrigger(), &pipepb.Trigger_Default{})
			}
		}
	}
	if len(errs) > 0 {
		jErr := &joinError{errs: errs}
		slog.Error("unable to run job", slog.String("cause", "unimplemented features"), slog.String("jobname", req.GetJobName()), slog.String("errors", jErr.Error()))
		err := fmt.Errorf("found %v uses of features unimplemented in prism in job %v:\n%v", len(errs), req.GetJobName(), jErr)
		job.Failed(err)
		return nil, err
	}
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

// GetMessageStream subscribes to a stream of state changes and messages from the job. If throughput
// is high, this may cause losses of messages.
func (s *Server) GetMessageStream(req *jobpb.JobMessagesRequest, stream jobpb.JobService_GetMessageStreamServer) error {
	s.mu.Lock()
	job, ok := s.jobs[req.GetJobId()]
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("job with id %v not found", req.GetJobId())
	}

	job.streamCond.L.Lock()
	defer job.streamCond.L.Unlock()
	curMsg := job.minMsg
	curState := job.stateIdx

	state := job.state.Load().(jobpb.JobState_Enum)
	for {
		for (curMsg >= job.maxMsg || len(job.msgs) == 0) && curState > job.stateIdx {
			switch state {
			case jobpb.JobState_CANCELLED, jobpb.JobState_DONE, jobpb.JobState_DRAINED, jobpb.JobState_UPDATED:
				// Reached terminal state.
				return nil
			case jobpb.JobState_FAILED:
				// Ensure we send an error message with the cause of the job failure.
				stream.Send(&jobpb.JobMessagesResponse{
					Response: &jobpb.JobMessagesResponse_MessageResponse{
						MessageResponse: &jobpb.JobMessage{
							MessageText: job.failureErr.Error(),
							Importance:  jobpb.JobMessage_JOB_MESSAGE_ERROR,
						},
					},
				})
				return nil
			}
			job.streamCond.Wait()
			select { // Quit out if the external connection is done.
			case <-stream.Context().Done():
				return context.Cause(stream.Context())
			default:
			}
		}

		if curMsg < job.minMsg {
			// TODO report missed messages for this stream.
			curMsg = job.minMsg
		}
		for curMsg < job.maxMsg && len(job.msgs) > 0 {
			msg := job.msgs[curMsg-job.minMsg]
			stream.Send(&jobpb.JobMessagesResponse{
				Response: &jobpb.JobMessagesResponse_MessageResponse{
					MessageResponse: &jobpb.JobMessage{
						MessageText: msg,
						Importance:  jobpb.JobMessage_JOB_MESSAGE_BASIC,
					},
				},
			})
			curMsg++
		}
		if curState <= job.stateIdx {
			state = job.state.Load().(jobpb.JobState_Enum)
			curState = job.stateIdx + 1
			job.streamCond.L.Unlock()
			stream.Send(&jobpb.JobMessagesResponse{
				Response: &jobpb.JobMessagesResponse_StateResponse{
					StateResponse: &jobpb.JobStateEvent{
						State: state,
					},
				},
			})
			job.streamCond.L.Lock()
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

// GetState returns the current state of the job with the requested id.
func (s *Server) GetState(_ context.Context, req *jobpb.GetJobStateRequest) (*jobpb.JobStateEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[req.GetJobId()]
	if !ok {
		return nil, fmt.Errorf("job with id %v not found", req.GetJobId())
	}
	return &jobpb.JobStateEvent{
		State:     j.state.Load().(jobpb.JobState_Enum),
		Timestamp: timestamppb.New(j.stateTime),
	}, nil
}
