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

import static org.apache.beam.runners.core.metrics.Protos.fromProto;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.beam.model.jobmanagement.v1.JobApiMetrics;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc.JobServiceBlockingStub;
import org.apache.beam.model.pipeline.v1.PipelineMetrics;
import org.apache.beam.runners.core.construction.metrics.MetricFiltering;
import org.apache.beam.runners.core.construction.metrics.MetricKey;
import org.apache.beam.runners.core.metrics.Protos;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.ByteString;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JobServicePipelineResult implements PipelineResult, AutoCloseable {

  private static final long POLL_INTERVAL_MS = 10 * 1000;

  private static final Logger LOG = LoggerFactory.getLogger(JobServicePipelineResult.class);

  private final ByteString jobId;
  private final CloseableResource<JobServiceBlockingStub> jobService;
  @Nullable private State terminationState;
  @Nullable private Runnable cleanup;
  @Nullable private MetricResults metricResults;

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
    if (metricResults == null) {
      LOG.info("Fetching MetricResults");
      JobApi.GetJobMetricsResponse response =
          jobService
              .get()
              .getJobMetrics(JobApi.GetJobMetricsRequest.newBuilder().setJobIdBytes(jobId).build());
      List<JobApiMetrics.MetricStatus> statuses = response.getMetricStatusesList();
      List<MetricResult<Long>> counters = new ArrayList<>();
      List<MetricResult<DistributionResult>> distributions = new ArrayList<>();
      List<MetricResult<GaugeResult>> gauges =
          new ArrayList<>(); // TODO: plumb gauges through these metrics
      statuses.forEach(
          status -> {
            PipelineMetrics.MetricKey key = status.getMetricKey();
            JobApiMetrics.MetricResult metricResult = status.getMetricResult();
            if (metricResult.hasCounterResult()) {
              JobApiMetrics.CounterResult counterResult = metricResult.getCounterResult();
              counters.add(
                  MetricResult.create(
                      Protos.toProto(key.getMetricName()),
                      key.getStep(),
                      counterResult.getCommitted(),
                      counterResult.getAttempted()));
            } else {
              assert (metricResult.hasDistributionResult());
              JobApiMetrics.DistributionResult distributionResult =
                  metricResult.getDistributionResult();
              distributions.add(
                  MetricResult.create(
                      Protos.toProto(key.getMetricName()),
                      key.getStep(),
                      fromProto(distributionResult.getCommitted()),
                      fromProto(distributionResult.getAttempted())));
            }
          });
      metricResults =
          // TODO: factor this out to a named MetricResults subclass
          new MetricResults() {
            private <T> void printResults(
                List<MetricResult<T>> results, String name, PrintStream ps) {
              if (results.isEmpty()) {
                return;
              }
              ps.println(String.format("\t%d %s:", results.size(), name));
              for (MetricResult<T> result : results) {
                ps.println("\t\t" + result.toString());
              }
            }

            @Override
            public String toString() {
              final ByteArrayOutputStream baos = new ByteArrayOutputStream();
              try (PrintStream ps = new PrintStream(baos, true, "UTF-8")) {
                ps.println("MetricResults(");
                printResults(counters, "counters", ps);
                printResults(distributions, "distributions", ps);
                printResults(gauges, "gauges", ps);
                ps.println(")");
              } catch (UnsupportedEncodingException ignored) {
              }
              return new String(baos.toByteArray(), StandardCharsets.UTF_8);
            }

            @Override
            public MetricQueryResults queryMetrics(@Nullable MetricsFilter filter) {
              List<MetricResult<Long>> counterResults = new ArrayList<>();
              List<MetricResult<DistributionResult>> distributionResults = new ArrayList<>();
              List<MetricResult<GaugeResult>> gaugeResults = new ArrayList<>();
              counters.forEach(
                  counter -> {
                    if (MetricFiltering.matches(
                        filter, MetricKey.create(counter.getStep(), counter.getName()))) {
                      counterResults.add(counter);
                    }
                  });
              distributions.forEach(
                  distribution -> {
                    if (MetricFiltering.matches(
                        filter, MetricKey.create(distribution.getStep(), distribution.getName()))) {
                      distributionResults.add(distribution);
                    }
                  });
              gauges.forEach(
                  gauge -> {
                    if (MetricFiltering.matches(
                        filter, MetricKey.create(gauge.getStep(), gauge.getName()))) {
                      gaugeResults.add(gauge);
                    }
                  });
              return MetricQueryResults.create(counterResults, distributionResults, gaugeResults);
            }
          };
      LOG.info("Received MetricResults: {}", metricResults);
    }
    LOG.info("Returning MetricResults: {}", metricResults);
    return metricResults;
  }

  @Override
  public void close() {
    LOG.info("Cleaning up job service…");
    try (CloseableResource<JobServiceBlockingStub> jobService = this.jobService) {
      metrics();
      LOG.info("Got metrics during cleanup");
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
