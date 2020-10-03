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
package org.apache.beam.runners.jet;

import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.map.IMap;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;
import org.apache.beam.runners.core.metrics.MetricUpdates;
import org.apache.beam.runners.jet.metrics.JetMetricResults;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Jet specific implementation of {@link PipelineResult}. */
public class JetPipelineResult implements PipelineResult {

  private static final Logger LOG = LoggerFactory.getLogger(JetPipelineResult.class);

  private final Job job;
  private final JetMetricResults metricResults;
  private volatile State terminalState;

  private CompletableFuture<Void> completionFuture;

  JetPipelineResult(@Nonnull Job job, @Nonnull IMap<String, MetricUpdates> metricsAccumulator) {
    this.job = Objects.requireNonNull(job);
    // save the terminal state when the job completes because the `job` instance will become invalid
    // afterwards
    metricResults = new JetMetricResults(metricsAccumulator);
  }

  void setCompletionFuture(CompletableFuture<Void> completionFuture) {
    this.completionFuture = completionFuture;
  }

  void freeze(Throwable throwable) {
    metricResults.freeze();
    terminalState = throwable != null ? State.FAILED : State.DONE;
  }

  @Override
  public State getState() {
    if (terminalState != null) {
      return terminalState;
    }
    JobStatus status = job.getStatus();
    switch (status) {
      case COMPLETED:
        return State.DONE;
      case COMPLETING:
      case RUNNING:
      case STARTING:
        return State.RUNNING;
      case FAILED:
        return State.FAILED;
      case NOT_RUNNING:
      case SUSPENDED:
      case SUSPENDED_EXPORTING_SNAPSHOT:
        return State.STOPPED;
      default:
        LOG.warn("Unhandled " + JobStatus.class.getSimpleName() + ": " + status.name() + "!");
        return State.UNKNOWN;
    }
  }

  @Override
  public State cancel() throws IOException {
    if (terminalState != null) {
      throw new IllegalStateException("Job already completed");
    }
    try {
      job.cancel();
      job.join();
    } catch (CancellationException ignored) {
    } catch (Exception e) {
      throw new IOException("Failed to cancel the job: " + e, e);
    }
    return State.FAILED;
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    if (terminalState != null) {
      return terminalState;
    }

    try {
      completionFuture.get(duration.getMillis(), TimeUnit.MILLISECONDS);
      return State.DONE;
    } catch (InterruptedException | TimeoutException e) {
      return getState(); // job should be RUNNING or STOPPED
    } catch (ExecutionException e) {
      throw new CompletionException(e.getCause());
    }
  }

  @Override
  public State waitUntilFinish() {
    return waitUntilFinish(new Duration(Long.MAX_VALUE));
  }

  @Override
  public MetricResults metrics() {
    return metricResults;
  }
}
