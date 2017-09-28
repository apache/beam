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

package org.apache.beam.runners.reference.job;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.beam.sdk.common.runner.v1.JobApi;
import org.apache.beam.sdk.common.runner.v1.JobApi.CancelJobRequest;
import org.apache.beam.sdk.common.runner.v1.JobApi.CancelJobResponse;
import org.apache.beam.sdk.common.runner.v1.JobApi.GetJobStateRequest;
import org.apache.beam.sdk.common.runner.v1.JobApi.GetJobStateResponse;
import org.apache.beam.sdk.common.runner.v1.JobApi.PrepareJobResponse;
import org.apache.beam.sdk.common.runner.v1.JobApi.RunJobRequest;
import org.apache.beam.sdk.common.runner.v1.JobServiceGrpc.JobServiceImplBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The ReferenceRunner uses the portability framework to execute a Pipeline on a single machine. */
public class ReferenceRunnerJobService extends JobServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(ReferenceRunnerJobService.class);

  public static ReferenceRunnerJobService create() {
    return new ReferenceRunnerJobService();
  }

  private ReferenceRunnerJobService() {}

  @Override
  public void prepare(
      JobApi.PrepareJobRequest request,
      StreamObserver<JobApi.PrepareJobResponse> responseObserver) {
    LOG.trace("{} {}", PrepareJobResponse.class.getSimpleName(), request);
    responseObserver.onError(Status.UNIMPLEMENTED.asException());
  }

  @Override
  public void run(
      JobApi.RunJobRequest request, StreamObserver<JobApi.RunJobResponse> responseObserver) {
    LOG.trace("{} {}", RunJobRequest.class.getSimpleName(), request);
    responseObserver.onError(Status.UNIMPLEMENTED.asException());
  }

  @Override
  public void getState(
      GetJobStateRequest request, StreamObserver<GetJobStateResponse> responseObserver) {
    LOG.trace("{} {}", GetJobStateRequest.class.getSimpleName(), request);
    responseObserver.onError(
        Status.NOT_FOUND
            .withDescription(String.format("Unknown Job ID %s", request.getJobId()))
            .asException());
  }

  @Override
  public void cancel(CancelJobRequest request, StreamObserver<CancelJobResponse> responseObserver) {
    LOG.trace("{} {}", CancelJobRequest.class.getSimpleName(), request);
    responseObserver.onError(
        Status.NOT_FOUND
            .withDescription(String.format("Unknown Job ID %s", request.getJobId()))
            .asException());
  }
}
