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
package org.apache.beam.runners.portability;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc.JobServiceBlockingStub;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ListenableFuture;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ListeningScheduledExecutorService;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.MoreExecutors;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JobServicePipelineResult implements PipelineResult, AutoCloseable {

  private static final long POLL_INTERVAL_MS = 3_000;

  private static final Logger LOG = LoggerFactory.getLogger(JobServicePipelineResult.class);
  private final ListeningScheduledExecutorService executorService =
      MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor());

  private final String jobId;
  private final JobServiceBlockingStub jobService;
  private final AtomicReference<State> latestState = new AtomicReference<>(State.UNKNOWN);
  private final Runnable cleanup;
  private final AtomicReference<PortableMetrics> jobMetrics =
      new AtomicReference<>(PortableMetrics.of(JobApi.MetricResults.getDefaultInstance()));
  private CompletableFuture<State> terminalStateFuture = new CompletableFuture<>();
  private CompletableFuture<MetricResults> metricResultsCompletableFuture =
      new CompletableFuture<>();

  JobServicePipelineResult(String jobId, JobServiceBlockingStub jobService, Runnable cleanup) {
    this.jobId = jobId;
    this.jobService = jobService;
    this.cleanup = cleanup;
  }

  @Override
  public State getState() {
    if (latestState.get().isTerminal()) {
      return latestState.get();
    }
    JobStateEvent response =
        jobService.getState(GetJobStateRequest.newBuilder().setJobId(jobId).build());
    State state = State.valueOf(response.getState().name());
    latestState.set(state);
    return state;
  }

  @Override
  public State cancel() {
    if (latestState.get().isTerminal()) {
      return latestState.get();
    }
    CancelJobResponse response =
        jobService.cancel(CancelJobRequest.newBuilder().setJobId(jobId).build());
    State state = State.valueOf(response.getState().name());
    latestState.set(state);
    return state;
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    if (latestState.get().isTerminal()) {
      return latestState.get();
    }
    try {
      return pollForTerminalState().get(duration.getMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public State waitUntilFinish() {
    if (latestState.get().isTerminal()) {
      return latestState.get();
    }
    try {
      return pollForTerminalState().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  void setTerminalStateFuture(CompletableFuture<State> terminalStateFuture) {
    this.terminalStateFuture = terminalStateFuture;
  }

  CompletableFuture<State> getTerminalStateFuture() {
    return this.terminalStateFuture;
  }

  void setMetricResultsCompletableFuture(
      CompletableFuture<MetricResults> metricResultsCompletableFuture) {
    this.metricResultsCompletableFuture = metricResultsCompletableFuture;
  }

  CompletableFuture<MetricResults> getMetricResultsCompletableFuture() {
    return this.metricResultsCompletableFuture;
  }

  CompletableFuture<State> pollForTerminalState() {
    CompletableFuture<State> completableFuture = new CompletableFuture<>();
    ListenableFuture<?> future =
        executorService.scheduleAtFixedRate(
            () -> {
              State state = getState();
              LOG.info("Job: {} latest state: {}", jobId, state);
              latestState.set(state);
              if (state.isTerminal()) {
                completableFuture.complete(state);
              }
            },
            0L,
            POLL_INTERVAL_MS,
            TimeUnit.MILLISECONDS);
    return completableFuture.whenComplete(
        (state, throwable) -> {
          checkState(
              state.isTerminal(),
              "future should have completed with a terminal state, got: %s",
              state);
          future.cancel(true);
          LOG.info("Job: {} reached terminal state: {}", jobId, state);
          if (throwable != null) {
            throw new RuntimeException(throwable);
          }
        });
  }

  CompletableFuture<MetricResults> pollForMetrics() {
    CompletableFuture<MetricResults> completableFuture = new CompletableFuture<>();
    ListenableFuture<?> future =
        executorService.scheduleAtFixedRate(
            () -> {
              if (latestState.get().isTerminal()) {
                completableFuture.complete(jobMetrics.get());
                return;
              }
              JobApi.GetJobMetricsRequest metricsRequest =
                  JobApi.GetJobMetricsRequest.newBuilder().setJobId(jobId).build();
              JobApi.MetricResults results = jobService.getJobMetrics(metricsRequest).getMetrics();
              jobMetrics.set(PortableMetrics.of(results));
            },
            0L,
            1L,
            TimeUnit.SECONDS);
    return completableFuture.whenComplete(
        ((metricResults, throwable) -> {
          checkState(
              latestState.get().isTerminal(),
              "future should have completed with a terminal state, got: %s",
              latestState.get());
          future.cancel(true);
          LOG.info("Job: {} latest metrics: {}", jobId, metricResults.toString());
        }));
  }

  @Override
  public MetricResults metrics() {
    return jobMetrics.get();
  }

  @Override
  public void close() {
    cleanup.run();
  }
}
