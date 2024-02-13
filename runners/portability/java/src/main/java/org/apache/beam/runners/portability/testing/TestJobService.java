/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.portability.testing;

import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent;
import org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.RunJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.RunJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc.JobServiceImplBase;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;

/**
 * A JobService for tests.
 *
 * <p>A {@link TestJobService} always returns a fixed staging endpoint, job preparation id, job id,
 * and job state. As soon as a job is run, it is put into the given job state.
 */
public class TestJobService extends JobServiceImplBase {

  private final ApiServiceDescriptor stagingEndpoint;
  private final String preparationId;
  private final String jobId;
  private final JobState.Enum jobState;
  private JobApi.MetricResults metrics;

  public TestJobService(
      ApiServiceDescriptor stagingEndpoint,
      String preparationId,
      String jobId,
      JobState.Enum jobState,
      JobApi.MetricResults metrics) {
    this.stagingEndpoint = stagingEndpoint;
    this.preparationId = preparationId;
    this.jobId = jobId;
    this.jobState = jobState;
    this.metrics = metrics;
  }

  @Override
  public void prepare(
      PrepareJobRequest request, StreamObserver<PrepareJobResponse> responseObserver) {
    responseObserver.onNext(
        PrepareJobResponse.newBuilder()
            .setPreparationId(preparationId)
            .setArtifactStagingEndpoint(stagingEndpoint)
            .setStagingSessionToken("TestStagingToken")
            .build());
    responseObserver.onCompleted();
  }

  @Override
  public void run(RunJobRequest request, StreamObserver<RunJobResponse> responseObserver) {
    responseObserver.onNext(RunJobResponse.newBuilder().setJobId(jobId).build());
    responseObserver.onCompleted();
  }

  @Override
  public void getState(GetJobStateRequest request, StreamObserver<JobStateEvent> responseObserver) {
    responseObserver.onNext(JobStateEvent.newBuilder().setState(jobState).build());
    responseObserver.onCompleted();
  }

  @Override
  public void getJobMetrics(
      JobApi.GetJobMetricsRequest request,
      StreamObserver<JobApi.GetJobMetricsResponse> responseObserver) {
    responseObserver.onNext(JobApi.GetJobMetricsResponse.newBuilder().setMetrics(metrics).build());
    responseObserver.onCompleted();
  }
}
