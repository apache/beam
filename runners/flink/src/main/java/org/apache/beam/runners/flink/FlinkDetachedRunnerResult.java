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
package org.apache.beam.runners.flink;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.flink.metrics.FlinkMetricContainer;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Result of a detached execution of a {@link org.apache.beam.sdk.Pipeline} with Flink. In detached
 * execution, results and job execution are currently unavailable.
 */
public class FlinkDetachedRunnerResult implements PipelineResult {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkDetachedRunnerResult.class);

  private JobClient jobClient;
  private int jobCheckIntervalInSecs;

  FlinkDetachedRunnerResult(JobClient jobClient, int jobCheckIntervalInSecs) {
    this.jobClient = jobClient;
    this.jobCheckIntervalInSecs = jobCheckIntervalInSecs;
  }

  @Override
  public State getState() {
    try {
      return toBeamJobState(jobClient.getJobStatus().get());
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Fail to get flink job state", e);
    }
  }

  @Override
  public MetricResults metrics() {
    return MetricsContainerStepMap.asAttemptedOnlyMetricResults(getMetricsContainerStepMap());
  }

  private MetricsContainerStepMap getMetricsContainerStepMap() {
    try {
      return (MetricsContainerStepMap)
          jobClient
              .getAccumulators()
              .get()
              .getOrDefault(FlinkMetricContainer.ACCUMULATOR_NAME, new MetricsContainerStepMap());
    } catch (InterruptedException | ExecutionException e) {
      LOG.warn("Fail to get flink job accumulators", e);
      return new MetricsContainerStepMap();
    }
  }

  @Override
  public State cancel() throws IOException {
    try {
      this.jobClient.cancel().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Fail to cancel flink job", e);
    }
    return getState();
  }

  @Override
  public State waitUntilFinish() {
    return waitUntilFinish(Duration.millis(Long.MAX_VALUE));
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    long start = System.currentTimeMillis();
    long durationInMillis = duration.getMillis();
    State state = State.UNKNOWN;
    while (durationInMillis < 1 || (System.currentTimeMillis() - start) < durationInMillis) {
      state = getState();
      if (state.isTerminal()) {
        return state;
      }
      try {
        Thread.sleep(jobCheckIntervalInSecs * 1000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    if (state != null && !state.isTerminal()) {
      LOG.warn("Job is not finished in {} seconds", duration.getStandardSeconds());
    }
    return state;
  }

  private State toBeamJobState(JobStatus flinkJobStatus) {
    switch (flinkJobStatus) {
      case CANCELLING:
      case CREATED:
      case INITIALIZING:
      case FAILING:
      case RECONCILING:
      case RESTARTING:
      case RUNNING:
        return State.RUNNING;
      case FINISHED:
        return State.DONE;
      case CANCELED:
        return State.CANCELLED;
      case FAILED:
        return State.FAILED;
      case SUSPENDED:
        return State.STOPPED;
      default:
        return State.UNKNOWN;
    }
  }

  @Override
  public String toString() {
    return "FlinkDetachedRunnerResult{}";
  }
}
