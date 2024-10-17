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

// Package jobservices handles services necessary WRT handling jobs from
// SDKs. Nominally this is the entry point for most users, and a job's
// external interactions outside of pipeline execution.
//
// This includes handling receiving, staging, and provisioning artifacts,
// and orchestrating external workers, such as for loopback mode.
//
// Execution of jobs is abstracted away to an execute function specified
// at server construction time.
package jobservices

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/urns"
	"google.golang.org/protobuf/types/known/structpb"
)

var supportedRequirements = map[string]struct{}{
	urns.RequirementSplittableDoFn:     {},
	urns.RequirementStatefulProcessing: {},
	urns.RequirementBundleFinalization: {},
}

// TODO, move back to main package, and key off of executor handlers?
// Accept whole pipeline instead, and look at every PTransform too.
func isSupported(requirements []string) error {
	var unsupported []string
	for _, req := range requirements {
		if _, ok := supportedRequirements[req]; !ok {
			unsupported = append(unsupported, req)
		}
	}
	if len(unsupported) > 0 {
		sort.Strings(unsupported)
		return fmt.Errorf("prism runner doesn't support the following required features: %v", strings.Join(unsupported, ","))
	}
	return nil
}

// Job is an interface to the job services for executing pipelines.
// It allows the executor to communicate status, messages, and metrics
// back to callers of the Job Management API.
type Job struct {
	key     string
	jobName string

	artifactEndpoint string

	Pipeline *pipepb.Pipeline
	options  *structpb.Struct

	// Management side concerns.
	streamCond *sync.Cond
	// TODO, consider unifying messages and state to a single ordered buffer.
	minMsg, maxMsg int // logical indices into the message slice
	msgs           []string
	stateIdx       int
	state          atomic.Value // jobpb.JobState_Enum
	stateTime      time.Time
	failureErr     error

	// Context used to terminate this job.
	RootCtx  context.Context
	CancelFn context.CancelCauseFunc
	// Logger for this job.
	Logger *slog.Logger

	metrics metricsStore
}

func (j *Job) ArtifactEndpoint() string {
	return j.artifactEndpoint
}

func (j *Job) PipelineOptions() *structpb.Struct {
	return j.options
}

// ContributeTentativeMetrics returns the datachannel read index, and any unknown monitoring short ids.
func (j *Job) ContributeTentativeMetrics(payloads *fnpb.ProcessBundleProgressResponse) (map[string]int64, []string) {
	return j.metrics.ContributeTentativeMetrics(payloads)
}

// ContributeFinalMetrics returns any unknown monitoring short ids.
func (j *Job) ContributeFinalMetrics(payloads *fnpb.ProcessBundleResponse) []string {
	return j.metrics.ContributeFinalMetrics(payloads)
}

// AddMetricShortIDs populates metric short IDs with their metadata.
func (j *Job) AddMetricShortIDs(ids *fnpb.MonitoringInfosMetadataResponse) {
	j.metrics.AddShortIDs(ids)
}

func (j *Job) String() string {
	return fmt.Sprintf("%v[%v]", j.key, j.jobName)
}

func (j *Job) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("key", j.key),
		slog.String("name", j.jobName))
}

func (j *Job) JobKey() string {
	return j.key
}

func (j *Job) SendMsg(msg string) {
	j.streamCond.L.Lock()
	defer j.streamCond.L.Unlock()
	j.maxMsg++
	// Trim so we never have more than 120 messages, keeping the last 100 for sure
	// but amortize it so that messages are only trimmed every 20 messages beyond
	// that.
	// TODO, make this configurable
	const buffered, trigger = 100, 20
	if len(j.msgs) > buffered+trigger {
		copy(j.msgs[0:], j.msgs[trigger:])
		for k, n := len(j.msgs)-trigger, len(j.msgs); k < n; k++ {
			j.msgs[k] = ""
		}
		j.msgs = j.msgs[:len(j.msgs)-trigger]
		j.minMsg += trigger // increase the "min" message higher as a result.
	}
	j.msgs = append(j.msgs, msg)
	j.streamCond.Broadcast()
}

func (j *Job) sendState(state jobpb.JobState_Enum) {
	j.streamCond.L.Lock()
	defer j.streamCond.L.Unlock()
	old := j.state.Load()
	// Never overwrite a failed state with another one.
	if old != jobpb.JobState_FAILED {
		j.state.Store(state)
		j.stateTime = time.Now()
		j.stateIdx++
	}
	j.streamCond.Broadcast()
}

// Start indicates that the job is preparing to execute.
func (j *Job) Start() {
	j.sendState(jobpb.JobState_STARTING)
}

// Running indicates that the job is executing.
func (j *Job) Running() {
	j.sendState(jobpb.JobState_RUNNING)
}

// Done indicates that the job completed successfully.
func (j *Job) Done() {
	j.sendState(jobpb.JobState_DONE)
}

// Canceling indicates that the job is canceling.
func (j *Job) Canceling() {
	j.sendState(jobpb.JobState_CANCELLING)
}

// Canceled indicates that the job is canceled.
func (j *Job) Canceled() {
	j.sendState(jobpb.JobState_CANCELLED)
}

// Failed indicates that the job completed unsuccessfully.
func (j *Job) Failed(err error) {
	slog.Error("job failed", slog.Any("job", j), slog.Any("error", err))
	j.failureErr = err
	j.sendState(jobpb.JobState_FAILED)
	j.CancelFn(fmt.Errorf("jobFailed %v: %w", j, err))
}
