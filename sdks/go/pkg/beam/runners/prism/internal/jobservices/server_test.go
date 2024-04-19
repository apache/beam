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
	"sync"
	"testing"

	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/urns"
	"google.golang.org/protobuf/encoding/prototext"
)

// TestServer_Lifecycle validates that a server can start and stop.
func TestServer_Lifecycle(t *testing.T) {
	undertest := NewServer(0, func(j *Job) {
		t.Fatalf("unexpected call to execute: %v", j)
	})

	go undertest.Serve()

	undertest.Stop()
}

// Validates that a job can start and stop.
func TestServer_JobLifecycle(t *testing.T) {
	var called sync.WaitGroup
	called.Add(1)
	undertest := NewServer(0, func(j *Job) {
		called.Done()
	})
	ctx := context.Background()

	wantPipeline := &pipepb.Pipeline{
		Requirements: []string{urns.RequirementSplittableDoFn},
	}
	wantName := "testJob"

	resp, err := undertest.Prepare(ctx, &jobpb.PrepareJobRequest{
		Pipeline: wantPipeline,
		JobName:  wantName,
	})
	if err != nil {
		t.Fatalf("server.Prepare() = %v, want nil", err)
	}

	if got := resp.GetPreparationId(); got == "" {
		t.Fatalf("server.Prepare() = returned empty preparation ID, want non-empty: %v", prototext.Format(resp))
	}

	runResp, err := undertest.Run(ctx, &jobpb.RunJobRequest{
		PreparationId: resp.GetPreparationId(),
	})
	if err != nil {
		t.Fatalf("server.Run() = %v, want nil", err)
	}
	if got := runResp.GetJobId(); got == "" {
		t.Fatalf("server.Run() = returned empty preparation ID, want non-empty")
	}
	// If execute is never called, this doesn't unblock and timesout.
	called.Wait()
	t.Log("success!")
	// Nothing to cleanup because we didn't start the server.
}

// Validates that invoking Cancel cancels a running job.
func TestServer_RunThenCancel(t *testing.T) {
	var called sync.WaitGroup
	called.Add(1)
	undertest := NewServer(0, func(j *Job) {
		defer called.Done()
		j.state.Store(jobpb.JobState_RUNNING)
		if errors.Is(context.Cause(j.RootCtx), ErrCancel) {
			j.SendMsg("pipeline canceled " + j.String())
			j.Canceled()
			return
		}
	})
	ctx := context.Background()

	wantPipeline := &pipepb.Pipeline{
		Requirements: []string{urns.RequirementSplittableDoFn},
	}
	wantName := "testJob"

	resp, err := undertest.Prepare(ctx, &jobpb.PrepareJobRequest{
		Pipeline: wantPipeline,
		JobName:  wantName,
	})
	if err != nil {
		t.Fatalf("server.Prepare() = %v, want nil", err)
	}

	if got := resp.GetPreparationId(); got == "" {
		t.Fatalf("server.Prepare() = returned empty preparation ID, want non-empty: %v", prototext.Format(resp))
	}

	runResp, err := undertest.Run(ctx, &jobpb.RunJobRequest{
		PreparationId: resp.GetPreparationId(),
	})
	if err != nil {
		t.Fatalf("server.Run() = %v, want nil", err)
	}
	if got := runResp.GetJobId(); got == "" {
		t.Fatalf("server.Run() = returned empty preparation ID, want non-empty")
	}

	cancelResp, err := undertest.Cancel(ctx, &jobpb.CancelJobRequest{
		JobId: runResp.GetJobId(),
	})

	if err != nil {
		t.Fatalf("server.Canceling() = %v, want nil", err)
	}
	if cancelResp.State != jobpb.JobState_CANCELLING {
		t.Fatalf("server.Canceling() = %v, want %v", cancelResp.State, jobpb.JobState_CANCELLING)
	}

	called.Wait()

	stateResp, err := undertest.GetState(ctx, &jobpb.GetJobStateRequest{JobId: runResp.GetJobId()})
	if err != nil {
		t.Fatalf("server.GetState() = %v, want nil", err)
	}
	if stateResp.State != jobpb.JobState_CANCELLED {
		t.Fatalf("server.GetState() = %v, want %v", stateResp.State, jobpb.JobState_CANCELLED)
	}
}
