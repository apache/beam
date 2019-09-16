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
package org.apache.beam.runners.reference;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateResponse;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc.JobServiceBlockingStub;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.ByteString;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JobServicePipelineResult implements PipelineResult, AutoCloseable {

  private static final long POLL_INTERVAL_MS = 10 * 1000;

  private static final Logger LOG = LoggerFactory.getLogger(JobServicePipelineResult.class);

  private final ByteString jobId;
  private final CloseableResource<JobServiceBlockingStub> jobService;
  @Nullable private State terminationState;
  @Nullable private final Runnable cleanup;

  JobServicePipelineResult(
      ByteString jobId, CloseableResource<JobServiceBlockingStub> jobService, Runnable cleanup) {
    this.jobId = jobId;
    this.jobService = jobService;
    this.terminationState = null;
    this.cleanup = cleanup;
  }

  @Override
  public State getState() {
    if (terminationState != null) {
      return terminationState;
    }
    JobServiceBlockingStub stub = jobService.get();
    GetJobStateResponse response =
        stub.getState(GetJobStateRequest.newBuilder().setJobIdBytes(jobId).build());
    return getJavaState(response.getState());
  }

  @Override
  public State cancel() {
    JobServiceBlockingStub stub = jobService.get();
    CancelJobResponse response =
        stub.cancel(CancelJobRequest.newBuilder().setJobIdBytes(jobId).build());
    return getJavaState(response.getState());
  }

  @Nullable
  @Override
  public State waitUntilFinish(Duration duration) {
    if (duration.compareTo(Duration.millis(1)) < 1) {
      // Equivalent to infinite timeout.
      return waitUntilFinish();
    } else {
      CompletableFuture<State> result = CompletableFuture.supplyAsync(this::waitUntilFinish);
      try {
        return result.get(duration.getMillis(), TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        // Null result indicates a timeout.
        return null;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public State waitUntilFinish() {
    if (terminationState != null) {
      return terminationState;
    }
    JobServiceBlockingStub stub = jobService.get();
    GetJobStateRequest request = GetJobStateRequest.newBuilder().setJobIdBytes(jobId).build();
    GetJobStateResponse response = stub.getState(request);
    State lastState = getJavaState(response.getState());
    while (!lastState.isTerminal()) {
      try {
        Thread.sleep(POLL_INTERVAL_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      response = stub.getState(request);
      lastState = getJavaState(response.getState());
    }
    close();
    terminationState = lastState;
    return lastState;
  }

  @Override
  public MetricResults metrics() {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void close() {
    try (CloseableResource<JobServiceBlockingStub> jobService = this.jobService) {
      if (cleanup != null) {
        cleanup.run();
      }
    } catch (Exception e) {
      LOG.warn("Error cleaning up job service", e);
    }
  }

  private static State getJavaState(JobApi.JobState.Enum protoState) {
    switch (protoState) {
      case UNSPECIFIED:
        return State.UNKNOWN;
      case STOPPED:
        return State.STOPPED;
      case RUNNING:
        return State.RUNNING;
      case DONE:
        return State.DONE;
      case FAILED:
        return State.FAILED;
      case CANCELLED:
        return State.CANCELLED;
      case UPDATED:
        return State.UPDATED;
      case DRAINING:
        // TODO: Determine the correct mappings for the states below.
        return State.UNKNOWN;
      case DRAINED:
        return State.UNKNOWN;
      case STARTING:
        return State.RUNNING;
      case CANCELLING:
        return State.CANCELLED;
      default:
        LOG.warn("Unrecognized state from server: {}", protoState);
        return State.UNKNOWN;
    }
  }
}
