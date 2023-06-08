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
	"sync"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/metricsx"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/urns"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestServer(t *testing.T) {
	wantName := "testJob"

	wantPipeline := &pipepb.Pipeline{
		Requirements: []string{urns.RequirementSplittableDoFn},
	}

	cmpOpts := []cmp.Option{protocmp.Transform(), cmpopts.EquateEmpty()}
	tests := []struct {
		name                       string
		noJobsCheck, postPrepCheck func(context.Context, *testing.T, *Server)
		postRunCheck               func(context.Context, *testing.T, *Server, string)
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
			j.state.Store(jobpb.JobState_DONE)
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

// TODO impelment message stream test, once message/State implementation is sync.Cond based.
