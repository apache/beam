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
	"io"
	"net"
	"sync"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/metricsx"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/urns"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestServer(t *testing.T) {
	wantName := "testJob"

	wantPipeline := &pipepb.Pipeline{
		Requirements: []string{urns.RequirementSplittableDoFn},
	}

	cmpOpts := []cmp.Option{protocmp.Transform(), cmpopts.EquateEmpty()}
	tests := []struct {
		name         string
		postRunState jobpb.JobState_Enum
		// noJobsCheck tests in the setting that the Job doesn't exist
		// postPrepCheck tests after Server Prepare invoked
		noJobsCheck, postPrepCheck func(context.Context, *testing.T, *Server)
		// postRunCheck tests after Server Run invoked
		postRunCheck func(context.Context, *testing.T, *Server, string)
	}{
		{
			name: "GetJobs",
			noJobsCheck: func(ctx context.Context, t *testing.T, undertest *Server) {

				resp, err := undertest.GetJobs(ctx, &jobpb.GetJobsRequest{})
				if err != nil {
					t.Fatalf("GetJobs() = %v, want nil", err)
				}
				if diff := cmp.Diff(&jobpb.GetJobsResponse{}, resp, cmpOpts...); diff != "" {
					t.Errorf("GetJobs() (-want, +got):\n%v", diff)
				}

			},
			postPrepCheck: func(ctx context.Context, t *testing.T, undertest *Server) {
				resp, err := undertest.GetJobs(ctx, &jobpb.GetJobsRequest{})
				if err != nil {
					t.Fatalf("GetJobs() = %v, want nil", err)
				}
				if diff := cmp.Diff(&jobpb.GetJobsResponse{
					JobInfo: []*jobpb.JobInfo{
						{
							JobId:   "job-001", // Expected initial JobID.
							JobName: wantName,
							State:   jobpb.JobState_STOPPED,
						},
					},
				}, resp, cmpOpts...); diff != "" {
					t.Errorf("GetJobs() (-want, +got):\n%v", diff)
				}
			},
			postRunCheck: func(ctx context.Context, t *testing.T, undertest *Server, jobID string) {
				resp, err := undertest.GetJobs(ctx, &jobpb.GetJobsRequest{})
				if err != nil {
					t.Fatalf("GetJobs() = %v, want nil", err)
				}
				if diff := cmp.Diff(&jobpb.GetJobsResponse{
					JobInfo: []*jobpb.JobInfo{
						{
							JobId:   jobID,
							JobName: wantName,
							State:   jobpb.JobState_DONE,
						},
					},
				}, resp, cmpOpts...); diff != "" {
					t.Errorf("GetJobs() (-want, +got):\n%v", diff)
				}
			},
		}, {
			name: "GetMetrics",
			noJobsCheck: func(ctx context.Context, t *testing.T, undertest *Server) {
				_, err := undertest.GetJobMetrics(ctx, &jobpb.GetJobMetricsRequest{JobId: "job-001"})
				if err == nil {
					t.Errorf("GetPipeline(\"job-001\") = %v, want not found error", err)
				}
			},
			postPrepCheck: func(ctx context.Context, t *testing.T, undertest *Server) {
				resp, err := undertest.GetJobMetrics(ctx, &jobpb.GetJobMetricsRequest{JobId: "job-001"})
				if err != nil {
					t.Errorf("GetPipeline(\"job-001\") = %v, want nil", err)
				}
				if diff := cmp.Diff(&jobpb.GetJobMetricsResponse{
					Metrics: &jobpb.MetricResults{},
				}, resp, cmpOpts...); diff != "" {
					t.Errorf("GetPipeline(\"job-001\") (-want, +got):\n%v", diff)
				}
			},
			postRunCheck: func(ctx context.Context, t *testing.T, undertest *Server, jobID string) {
				resp, err := undertest.GetJobMetrics(ctx, &jobpb.GetJobMetricsRequest{JobId: jobID})
				if err != nil {
					t.Errorf("GetPipeline(\"job-001\") = %v, want nil", err)
				}
				if diff := cmp.Diff(&jobpb.GetJobMetricsResponse{
					Metrics: &jobpb.MetricResults{
						Committed: []*pipepb.MonitoringInfo{
							{
								Urn:     metricsx.UrnToString(metricsx.UrnElementCount),
								Type:    metricsx.UrnToType(metricsx.UrnElementCount),
								Payload: []byte("\x01"),
								Labels: map[string]string{
									"PCOLLECTION": "id.out",
								},
							},
						},
					},
				}, resp, cmpOpts...); diff != "" {
					t.Errorf("GetPipeline(\"job-001\") (-want, +got):\n%v", diff)
				}
			},
		}, {
			name: "GetPipeline",
			noJobsCheck: func(ctx context.Context, t *testing.T, undertest *Server) {
				_, err := undertest.GetPipeline(ctx, &jobpb.GetJobPipelineRequest{JobId: "job-001"})
				if err == nil {
					t.Errorf("GetPipeline(\"job-001\") = %v, want not found error", err)
				}
			},
			postPrepCheck: func(ctx context.Context, t *testing.T, undertest *Server) {
				resp, err := undertest.GetPipeline(ctx, &jobpb.GetJobPipelineRequest{JobId: "job-001"})
				if err != nil {
					t.Errorf("GetPipeline(\"job-001\") = %v, want nil", err)
				}
				if diff := cmp.Diff(&jobpb.GetJobPipelineResponse{
					Pipeline: wantPipeline,
				}, resp, cmpOpts...); diff != "" {
					t.Errorf("GetPipeline(\"job-001\") (-want, +got):\n%v", diff)
				}
			},
			postRunCheck: func(ctx context.Context, t *testing.T, undertest *Server, jobID string) {
				resp, err := undertest.GetPipeline(ctx, &jobpb.GetJobPipelineRequest{JobId: jobID})
				if err != nil {
					t.Errorf("GetPipeline(\"job-001\") = %v, want nil", err)
				}
				if diff := cmp.Diff(&jobpb.GetJobPipelineResponse{
					Pipeline: wantPipeline,
				}, resp, cmpOpts...); diff != "" {
					t.Errorf("GetPipeline(\"job-001\") (-want, +got):\n%v", diff)
				}
			},
		},
		{
			name:         "Canceling",
			postRunState: jobpb.JobState_RUNNING,
			noJobsCheck: func(ctx context.Context, t *testing.T, undertest *Server) {
				id := "job-001"
				_, err := undertest.Cancel(ctx, &jobpb.CancelJobRequest{JobId: id})
				// Cancel currently returns nil, nil when Job not found
				if err != nil {
					t.Errorf("Cancel(%q) = %v, want not found error", id, err)
				}
			},
			postPrepCheck: func(ctx context.Context, t *testing.T, undertest *Server) {
				id := "job-001"
				resp, err := undertest.Cancel(ctx, &jobpb.CancelJobRequest{JobId: id})
				if err != nil {
					t.Errorf("Cancel(%q) = %v, want not found error", id, err)
				}
				if diff := cmp.Diff(&jobpb.CancelJobResponse{
					State: jobpb.JobState_CANCELLING,
				}, resp, cmpOpts...); diff != "" {
					t.Errorf("Cancel(%q) (-want, +got):\n%s", id, diff)
				}
			},
			postRunCheck: func(ctx context.Context, t *testing.T, undertest *Server, jobID string) {
				id := "job-001"
				resp, err := undertest.Cancel(ctx, &jobpb.CancelJobRequest{JobId: id})
				if err != nil {
					t.Errorf("Cancel(%q) = %v, want not found error", id, err)
				}
				if diff := cmp.Diff(&jobpb.CancelJobResponse{
					State: jobpb.JobState_CANCELLING,
				}, resp, cmpOpts...); diff != "" {
					t.Errorf("Cancel(%q) (-want, +got):\n%s", id, diff)
				}
			},
		},
	}
	for _, test := range tests {
		var called sync.WaitGroup
		called.Add(1)
		undertest := NewServer(0, func(j *Job) {
			countData, _ := metricsx.Int64Counter(1)
			sizeData, _ := metricsx.Int64Distribution(1, 0, 0, 0)

			shortIDCount := "elemCount"
			shortIDSize := "elemSize"
			j.AddMetricShortIDs(&fnpb.MonitoringInfosMetadataResponse{
				MonitoringInfo: map[string]*pipepb.MonitoringInfo{
					shortIDCount: {
						Urn:  metricsx.UrnToString(metricsx.UrnElementCount),
						Type: metricsx.UrnToType(metricsx.UrnElementCount),
						Labels: map[string]string{
							"PCOLLECTION": "id.out",
						},
					},
				},
			})
			j.ContributeFinalMetrics(&fnpb.ProcessBundleResponse{
				MonitoringData: map[string][]byte{
					shortIDCount: countData,
					shortIDSize:  sizeData,
				},
			})
			state := jobpb.JobState_DONE
			if test.postRunState != jobpb.JobState_UNSPECIFIED {
				state = test.postRunState
			}
			j.state.Store(state)
			called.Done()
		})

		ctx := context.Background()
		test.noJobsCheck(ctx, t, undertest)

		prepResp, err := undertest.Prepare(ctx, &jobpb.PrepareJobRequest{
			Pipeline: wantPipeline,
			JobName:  wantName,
		})
		if err != nil {
			t.Fatalf("Prepare(%v) = %v, want nil", wantName, err)
		}

		test.postPrepCheck(ctx, t, undertest)

		jrResp, err := undertest.Run(ctx, &jobpb.RunJobRequest{
			PreparationId: prepResp.GetPreparationId(),
		})
		if err != nil {
			t.Fatalf("Run(%v) = %v, want nil", wantName, err)
		}

		called.Wait()
		test.postRunCheck(ctx, t, undertest, jrResp.GetJobId())
	}
}

func TestGetMessageStream(t *testing.T) {
	wantName := "testJob"
	wantPipeline := &pipepb.Pipeline{
		Requirements: []string{urns.RequirementSplittableDoFn},
	}
	var called sync.WaitGroup
	called.Add(1)
	ctx, _, clientConn := serveTestServer(t, func(j *Job) {
		j.Start()
		j.SendMsg("job starting")
		j.Running()
		j.SendMsg("job running")
		j.SendMsg("job finished")
		j.Done()
		j.SendMsg("job done")
		called.Done()
	})
	jobCli := jobpb.NewJobServiceClient(clientConn)

	// PreJob submission
	msgStream, err := jobCli.GetMessageStream(ctx, &jobpb.JobMessagesRequest{
		JobId: "job-001",
	})
	if err != nil {
		t.Errorf("GetMessageStream: wanted successful connection, got %v", err)
	}
	_, err = msgStream.Recv()
	if err == nil {
		t.Error("wanted error on non-existent job, but didn't happen.")
	}

	prepResp, err := jobCli.Prepare(ctx, &jobpb.PrepareJobRequest{
		Pipeline: wantPipeline,
		JobName:  wantName,
	})
	if err != nil {
		t.Fatalf("Prepare(%v) = %v, want nil", wantName, err)
	}

	// Post Job submission
	msgStream, err = jobCli.GetMessageStream(ctx, &jobpb.JobMessagesRequest{
		JobId: "job-001",
	})
	if err != nil {
		t.Errorf("GetMessageStream: wanted successful connection, got %v", err)
	}
	stateResponse, err := msgStream.Recv()
	if err != nil {
		t.Errorf("GetMessageStream().Recv() = %v, want nil", err)
	}
	if got, want := stateResponse.GetStateResponse().GetState(), jobpb.JobState_STOPPED; got != want {
		t.Errorf("GetMessageStream().Recv() = %v, want %v", got, want)
	}

	_, err = jobCli.Run(ctx, &jobpb.RunJobRequest{
		PreparationId: prepResp.GetPreparationId(),
	})
	if err != nil {
		t.Fatalf("Run(%v) = %v, want nil", wantName, err)
	}

	called.Wait() // Wait for the job to terminate.

	receivedDone := false
	var msgCount int
	for {
		// Continue with the same message stream.
		resp, err := msgStream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break // successful message stream completion
			}
			t.Errorf("GetMessageStream().Recv() = %v, want nil", err)
		}
		switch {

		case resp.GetMessageResponse() != nil:
			msgCount++
		case resp.GetStateResponse() != nil:
			if resp.GetStateResponse().GetState() == jobpb.JobState_DONE {
				receivedDone = true
			}
		}
	}
	if got, want := msgCount, 4; got != want {
		t.Errorf("GetMessageStream() didn't correct number of messages, got %v, want %v", got, want)
	}
	if !receivedDone {
		t.Error("GetMessageStream() didn't return job done state")
	}
	msgStream.CloseSend()

	// Create a new message stream, we should still get a tail of messages (in this case, all of them)
	// And the final state.
	msgStream, err = jobCli.GetMessageStream(ctx, &jobpb.JobMessagesRequest{
		JobId: "job-001",
	})
	if err != nil {
		t.Errorf("GetMessageStream: wanted successful connection, got %v", err)
	}

	receivedDone = false
	msgCount = 0
	for {
		// Continue with the same message stream.
		resp, err := msgStream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break // successful message stream completion
			}
			t.Errorf("GetMessageStream().Recv() = %v, want nil", err)
		}
		switch {

		case resp.GetMessageResponse() != nil:
			msgCount++
		case resp.GetStateResponse() != nil:
			if resp.GetStateResponse().GetState() == jobpb.JobState_DONE {
				receivedDone = true
			}
		}
	}
	if got, want := msgCount, 4; got != want {
		t.Errorf("GetMessageStream() didn't correct number of messages, got %v, want %v", got, want)
	}
	if !receivedDone {
		t.Error("GetMessageStream() didn't return job done state")
	}
}

func TestGetMessageStream_BufferCycling(t *testing.T) {
	wantName := "testJob"
	wantPipeline := &pipepb.Pipeline{
		Requirements: []string{urns.RequirementSplittableDoFn},
	}
	var called sync.WaitGroup
	called.Add(1)
	ctx, _, clientConn := serveTestServer(t, func(j *Job) {
		j.Start()
		// Using an offset from the trigger amount to ensure expected
		// behavior (we can sometimes get more than the last 100 messages).
		for i := 0; i < 512; i++ {
			j.SendMsg(fmt.Sprintf("message number %v", i))
		}
		j.Done()
		called.Done()
	})
	jobCli := jobpb.NewJobServiceClient(clientConn)

	prepResp, err := jobCli.Prepare(ctx, &jobpb.PrepareJobRequest{
		Pipeline: wantPipeline,
		JobName:  wantName,
	})
	if err != nil {
		t.Fatalf("Prepare(%v) = %v, want nil", wantName, err)
	}
	_, err = jobCli.Run(ctx, &jobpb.RunJobRequest{
		PreparationId: prepResp.GetPreparationId(),
	})
	if err != nil {
		t.Fatalf("Run(%v) = %v, want nil", wantName, err)
	}

	called.Wait() // Wait for the job to terminate.

	// Create a new message stream, we should still get a tail of messages (in this case, all of them)
	// And the final state.
	msgStream, err := jobCli.GetMessageStream(ctx, &jobpb.JobMessagesRequest{
		JobId: "job-001",
	})
	if err != nil {
		t.Errorf("GetMessageStream: wanted successful connection, got %v", err)
	}

	receivedDone := false
	var msgCount int
	for {
		// Continue with the same message stream.
		resp, err := msgStream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break // successful message stream completion
			}
			t.Errorf("GetMessageStream().Recv() = %v, want nil", err)
		}
		switch {
		case resp.GetMessageResponse() != nil:
			msgCount++
		case resp.GetStateResponse() != nil:
			if resp.GetStateResponse().GetState() == jobpb.JobState_DONE {
				receivedDone = true
			}
		}
	}
	if got, want := msgCount, 112; got != want {
		t.Errorf("GetMessageStream() didn't correct number of messages, got %v, want %v", got, want)
	}
	if !receivedDone {
		t.Error("GetMessageStream() didn't return job done state")
	}

}

func serveTestServer(t *testing.T, execute func(j *Job)) (context.Context, *Server, *grpc.ClientConn) {
	t.Helper()
	ctx, cancelFn := context.WithCancel(context.Background())
	t.Cleanup(cancelFn)

	s := NewServer(0, execute)
	lis := bufconn.Listen(1024 * 64)
	s.lis = lis
	t.Cleanup(func() { s.Stop() })
	go s.Serve()

	clientConn, err := grpc.DialContext(ctx, "", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatal("couldn't create bufconn grpc connection:", err)
	}
	return ctx, s, clientConn
}
