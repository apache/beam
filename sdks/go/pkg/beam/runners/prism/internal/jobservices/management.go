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
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/urns"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	// ErrCancel represents a pipeline cancellation by the user.
	ErrCancel = errors.New("pipeline canceled")
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

func getOnlyValue[K comparable, V any](in map[K]V) V {
	for _, v := range in {
		return v
	}
	panic("unreachable")
}

func (s *Server) Prepare(ctx context.Context, req *jobpb.PrepareJobRequest) (_ *jobpb.PrepareJobResponse, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Since jobs execute in the background, they should not be tied to a request's context.
	rootCtx, cancelFn := context.WithCancelCause(context.Background())
	// Wrap in a Once so it will only be invoked a single time for the job.
	terminalOnceWrap := sync.OnceFunc(s.jobTerminated)
	job := &Job{
		key:        s.nextId(),
		Pipeline:   req.GetPipeline(),
		jobName:    req.GetJobName(),
		options:    req.GetPipelineOptions(),
		streamCond: sync.NewCond(&sync.Mutex{}),
		RootCtx:    rootCtx,
		CancelFn: func(err error) {
			cancelFn(err)
			terminalOnceWrap()
		},
		Logger:           s.logger, // TODO substitute with a configured logger.
		artifactEndpoint: s.Endpoint(),
		mw:               s.mw,
	}
	// Stop the idle timer when a new job appears.
	if idleTimer := s.idleTimer.Load(); idleTimer != nil {
		idleTimer.Stop()
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
	ts := job.Pipeline.GetComponents().GetTransforms()
	var testStreamIds []string
	for tid, t := range ts {
		urn := t.GetSpec().GetUrn()
		switch urn {
		case urns.TransformImpulse,
			urns.TransformGBK,
			urns.TransformFlatten,
			urns.TransformCombinePerKey,
			urns.TransformCombineGlobally,      // Used by Java SDK
			urns.TransformCombineGroupedValues, // Used by Java SDK
			urns.TransformMerge,                // Used directly by Python SDK if "pre-optimized"
			urns.TransformPreCombine,           // Used directly by Python SDK if "pre-optimized"
			urns.TransformExtract,              // Used directly by Python SDK if "pre-optimized"
			urns.TransformAssignWindows:
		// Very few expected transforms types for submitted pipelines.
		// Most URNs are for the runner to communicate back to the SDK for execution.
		case urns.TransformReshuffle, urns.TransformRedistributeArbitrarily, urns.TransformRedistributeByKey:
			// Reshuffles and Redistributes are permitted and have special handling during optimization.

		case urns.TransformParDo:
			var pardo pipepb.ParDoPayload
			if err := proto.Unmarshal(t.GetSpec().GetPayload(), &pardo); err != nil {
				wrapped := fmt.Errorf("unable to unmarshal ParDoPayload for %v - %q: %w", tid, t.GetUniqueName(), err)
				job.Failed(wrapped)
				return nil, wrapped
			}

			isStateful := false

			// Validate all the state features
			for _, spec := range pardo.GetStateSpecs() {
				isStateful = true
				check("StateSpec.Protocol.Urn", spec.GetProtocol().GetUrn(),
					urns.UserStateBag, urns.UserStateMultiMap, urns.UserStateOrderedList)
			}
			// Validate all the timer features
			for _, spec := range pardo.GetTimerFamilySpecs() {
				isStateful = true
				check("TimerFamilySpecs.TimeDomain.Urn", spec.GetTimeDomain(), pipepb.TimeDomain_EVENT_TIME, pipepb.TimeDomain_PROCESSING_TIME)
			}

			// Check for a stateful SDF and direct user to https://github.com/apache/beam/issues/32139
			if pardo.GetRestrictionCoderId() != "" && isStateful {
				check("Splittable+Stateful DoFn", "See https://github.com/apache/beam/issues/32139 for information.")
			}

			// Validate whether the triggers on side inputs for are required for
			// expedient data processing..
			//
			// Currently triggered side inputs are not supported by prism, and will
			// not have early or late firings.
			//
			// This feature is required when the Side Input PCollection is unbounded
			// and is in the a Global Window. This can cause the pipeline to fully
			// stall while the input is being computed, and may never terminate.
			//
			// Other situations may not have desired results, but are valid behaviors
			// within the model.
			//
			// See https://github.com/apache/beam/issues/31438 for implementation tracking.
			for sideID := range pardo.GetSideInputs() {
				pcolID := t.GetInputs()[sideID]
				pcol := job.Pipeline.GetComponents().GetPcollections()[pcolID]
				wsID := pcol.GetWindowingStrategyId()
				ws := job.Pipeline.GetComponents().GetWindowingStrategies()[wsID]

				if pcol.GetIsBounded() == pipepb.IsBounded_BOUNDED ||
					ws.GetWindowFn().GetUrn() != urns.WindowFnGlobal {
					continue
				}

				// Within the Unbounded GlobalWindow space is a nich of expressed
				// user intent that they *do* want to wait for the end of the global
				// window for output. We should permit these pipelines, as there
				// is utility for this in testing situations anyway.
				switch trig := ws.GetTrigger().GetTrigger().(type) {
				case *pipepb.Trigger_Never_, *pipepb.Trigger_Default_:
					// Only one firing, at the end of the global window, and is
					// compatible with Prism's current execution.
					continue
				case *pipepb.Trigger_AfterEndOfWindow_:
					if early := trig.AfterEndOfWindow.GetEarlyFirings(); early == nil || early.GetNever() != nil {
						if ws.GetAllowedLateness() == 0 {
							// Late configuration doesn't matter, and there are no early firings.
							continue
						}
						if late := trig.AfterEndOfWindow.GetLateFirings(); late == nil || late.GetNever() != nil {
							// Lateness allowed, but but no firings anyway.
							continue
						}
					}
				}

				check("Unbounded GlobalWindow Triggered SideInput, are not currently supported by Prism. Sideinputs are only ready at end of window+allowed lateness. See https://github.com/apache/beam/issues/31438 for information.", prototext.Format(ws))
			}

		case urns.TransformTestStream:
			var testStream pipepb.TestStreamPayload
			if err := proto.Unmarshal(t.GetSpec().GetPayload(), &testStream); err != nil {
				wrapped := fmt.Errorf("unable to unmarshal TestStreamPayload for %v - %q: %w", tid, t.GetUniqueName(), err)
				job.Failed(wrapped)
				return nil, wrapped
			}

			t.EnvironmentId = "" // Unset the environment, to ensure it's handled prism side.
			testStreamIds = append(testStreamIds, tid)

		default:
			// Composites can often have some unknown urn, permit those.
			// Eg. The Python SDK has urns "beam:transform:generic_composite:v1", "beam:transform:pickled_python:v1",
			// as well as the deprecated "beam:transform:read:v1", but they are composites.
			// We don't do anything special with these high level composites, but
			// we may be dealing with their internal subgraph already, so we ignore this transform.
			if len(t.GetSubtransforms()) > 0 {
				continue
			}
			// This may be an "empty" composite without subtransforms or a payload.
			// These just do PCollection manipulation which is already represented in the Pipeline graph.
			// Simply ignore the composite at this stage, since the runner does nothing with them.
			if len(t.GetSpec().GetPayload()) == 0 {
				continue
			}
			// Another type of "empty" composite transforms without subtransforms but with
			// a non-empty payload and identical input/output pcollections
			if len(t.GetInputs()) == 1 && len(t.GetOutputs()) == 1 {
				inputID := getOnlyValue(t.GetInputs())
				outputID := getOnlyValue(t.GetOutputs())
				if inputID == outputID {
					slog.Warn("empty transform, with payload and identical input and output pcollection", "urn", urn, "name", t.GetUniqueName(), "pcoll", inputID)
					continue
				}
			}
			// Otherwise fail.
			slog.Warn("unknown transform, with payload", "urn", urn, "name", t.GetUniqueName(), "payload", t.GetSpec().GetPayload())
			check("PTransform.Spec.Urn", urn+" "+t.GetUniqueName(), "<doesn't exist>")
		}
	}
	// At most one test stream per pipeline.
	if len(testStreamIds) > 1 {
		check("Multiple TestStream Transforms in Pipeline", testStreamIds)
	}

	// Inspect Windowing strategies for unsupported features.
	for _, ws := range job.Pipeline.GetComponents().GetWindowingStrategies() {
		// Both Closing behaviors are identical without additional trigger firings.
		check("WindowingStrategy.ClosingBehaviour", ws.GetClosingBehavior(), pipepb.ClosingBehavior_EMIT_IF_NONEMPTY, pipepb.ClosingBehavior_EMIT_ALWAYS)
		check("WindowingStrategy.AccumulationMode", ws.GetAccumulationMode(), pipepb.AccumulationMode_DISCARDING, pipepb.AccumulationMode_ACCUMULATING)
		if ws.GetWindowFn().GetUrn() != urns.WindowFnSession {
			check("WindowingStrategy.MergeStatus", ws.GetMergeStatus(), pipepb.MergeStatus_NON_MERGING)
		} else if hasStatefulTriggers(ws.GetTrigger()) {
			// Technically for any merging windows, but per the above, we only support session windows presently.
			check("WindowingStrategy: Using stateful triggers with merging windows isn't currently supported in prism. See https://github.com/apache/beam/issues/31438 for information.", prototext.Format(ws))
		}
		check("WindowingStrategy.OnTimeBehavior", ws.GetOnTimeBehavior(), pipepb.OnTimeBehavior_FIRE_IF_NONEMPTY, pipepb.OnTimeBehavior_FIRE_ALWAYS)

		// Allow earliest and latest in pane to unblock running python tasks.
		// Tests actually using the set behavior will fail.
		check("WindowingStrategy.OutputTime", ws.GetOutputTime(), pipepb.OutputTime_END_OF_WINDOW,
			pipepb.OutputTime_EARLIEST_IN_PANE, pipepb.OutputTime_LATEST_IN_PANE)

		if hasUnsupportedTriggers(ws.GetTrigger()) {
			check("WindowingStrategy.Trigger", ws.GetTrigger().String())
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

func hasUnsupportedTriggers(tpb *pipepb.Trigger) bool {
	unsupported := false
	switch at := tpb.GetTrigger().(type) {
	case *pipepb.Trigger_AfterProcessingTime_, *pipepb.Trigger_AfterSynchronizedProcessingTime_:
		return true
	case *pipepb.Trigger_AfterAll_:
		for _, st := range at.AfterAll.GetSubtriggers() {
			unsupported = unsupported || hasUnsupportedTriggers(st)
		}
		return unsupported
	case *pipepb.Trigger_AfterAny_:
		for _, st := range at.AfterAny.GetSubtriggers() {
			unsupported = unsupported || hasUnsupportedTriggers(st)
		}
		return unsupported
	case *pipepb.Trigger_AfterEach_:
		for _, st := range at.AfterEach.GetSubtriggers() {
			unsupported = unsupported || hasUnsupportedTriggers(st)
		}
		return unsupported
	case *pipepb.Trigger_AfterEndOfWindow_:
		return hasUnsupportedTriggers(at.AfterEndOfWindow.GetEarlyFirings()) ||
			hasUnsupportedTriggers(at.AfterEndOfWindow.GetLateFirings())
	case *pipepb.Trigger_OrFinally_:
		return hasUnsupportedTriggers(at.OrFinally.GetMain()) ||
			hasUnsupportedTriggers(at.OrFinally.GetFinally())
	case *pipepb.Trigger_Repeat_:
		return hasUnsupportedTriggers(at.Repeat.GetSubtrigger())
	default:
		return false
	}
}

// hasStatefulTriggers checks if the triggers use state that might not function
// properly without proper merge handling.
func hasStatefulTriggers(tpb *pipepb.Trigger) bool {
	stateful := false
	switch at := tpb.GetTrigger().(type) {
	case *pipepb.Trigger_AfterProcessingTime_, *pipepb.Trigger_AfterSynchronizedProcessingTime_, *pipepb.Trigger_ElementCount_:
		// These triggers have state that needs care when used with merging windows.
		return true
	case *pipepb.Trigger_AfterAll_:
		for _, st := range at.AfterAll.GetSubtriggers() {
			stateful = stateful || hasStatefulTriggers(st)
		}
		return stateful
	case *pipepb.Trigger_AfterAny_:
		for _, st := range at.AfterAny.GetSubtriggers() {
			stateful = stateful || hasStatefulTriggers(st)
		}
		return stateful
	case *pipepb.Trigger_AfterEach_:
		for _, st := range at.AfterEach.GetSubtriggers() {
			stateful = stateful || hasStatefulTriggers(st)
		}
		return stateful
	case *pipepb.Trigger_AfterEndOfWindow_:
		return hasStatefulTriggers(at.AfterEndOfWindow.GetEarlyFirings()) ||
			hasStatefulTriggers(at.AfterEndOfWindow.GetLateFirings())
	case *pipepb.Trigger_OrFinally_:
		return hasStatefulTriggers(at.OrFinally.GetMain()) ||
			hasStatefulTriggers(at.OrFinally.GetFinally())
	case *pipepb.Trigger_Repeat_:
		return hasStatefulTriggers(at.Repeat.GetSubtrigger())
	default:
		return false
	}
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

// Cancel a Job requested by the CancelJobRequest for jobs not in an already terminal state.
// Otherwise, returns nil if Job does not exist or the Job's existing state as part of the CancelJobResponse.
func (s *Server) Cancel(_ context.Context, req *jobpb.CancelJobRequest) (*jobpb.CancelJobResponse, error) {
	s.mu.Lock()
	job, ok := s.jobs[req.GetJobId()]
	s.mu.Unlock()
	if !ok {
		return nil, nil
	}
	state := job.state.Load().(jobpb.JobState_Enum)
	switch state {
	case jobpb.JobState_CANCELLED, jobpb.JobState_DONE, jobpb.JobState_DRAINED, jobpb.JobState_UPDATED, jobpb.JobState_FAILED:
		// Already at terminal state.
		return &jobpb.CancelJobResponse{
			State: state,
		}, nil
	}
	job.SendMsg("canceling " + job.String())
	job.Canceling()
	job.CancelFn(ErrCancel)
	return &jobpb.CancelJobResponse{
		State: jobpb.JobState_CANCELLING,
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

// DescribePipelineOptions is a no-op since it's unclear how it is to function.
// Apparently only implemented in the Python SDK.
func (s *Server) DescribePipelineOptions(context.Context, *jobpb.DescribePipelineOptionsRequest) (*jobpb.DescribePipelineOptionsResponse, error) {
	return &jobpb.DescribePipelineOptionsResponse{
		Options: []*jobpb.PipelineOptionDescriptor{},
	}, nil
}

// GetStateStream returns the job state as it changes.
func (s *Server) GetStateStream(req *jobpb.GetJobStateRequest, stream jobpb.JobService_GetStateStreamServer) error {
	s.mu.Lock()
	job, ok := s.jobs[req.GetJobId()]
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("job with id %v not found", req.GetJobId())
	}

	job.streamCond.L.Lock()
	defer job.streamCond.L.Unlock()

	state := job.state.Load().(jobpb.JobState_Enum)
	for {
		job.streamCond.L.Unlock()
		stream.Send(&jobpb.JobStateEvent{
			State:     state,
			Timestamp: timestamppb.Now(),
		})
		job.streamCond.L.Lock()
		switch state {
		case jobpb.JobState_CANCELLED, jobpb.JobState_DONE, jobpb.JobState_DRAINED, jobpb.JobState_UPDATED, jobpb.JobState_FAILED:
			// Reached terminal state.
			return nil
		}
		newState := job.state.Load().(jobpb.JobState_Enum)
		for state == newState {
			select { // Quit out if the external connection is done.
			case <-stream.Context().Done():
				return context.Cause(stream.Context())
			default:
			}
			job.streamCond.Wait()
			newState = job.state.Load().(jobpb.JobState_Enum)
		}
		state = newState
	}
}
