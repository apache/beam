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
package org.apache.beam.runners.spark.structuredstreaming;

import static org.apache.beam.runners.core.metrics.MetricsContainerStepMap.asAttemptedOnlyMetricResults;
import static org.sparkproject.guava.base.Objects.firstNonNull;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import org.apache.beam.runners.spark.structuredstreaming.metrics.MetricsAccumulator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.spark.SparkException;
import org.joda.time.Duration;

public class SparkStructuredStreamingPipelineResult implements PipelineResult {

  private final Future<?> pipelineExecution;
  private final MetricsAccumulator metrics;
  private @Nullable final Runnable onTerminalState;
  private PipelineResult.State state;

  SparkStructuredStreamingPipelineResult(
      Future<?> pipelineExecution,
      MetricsAccumulator metrics,
      @Nullable final Runnable onTerminalState) {
    this.pipelineExecution = pipelineExecution;
    this.metrics = metrics;
    this.onTerminalState = onTerminalState;
    // pipelineExecution is expected to have started executing eagerly.
    this.state = State.RUNNING;
  }

  private static RuntimeException runtimeExceptionFrom(final Throwable e) {
    return (e instanceof RuntimeException) ? (RuntimeException) e : new RuntimeException(e);
  }

  /**
   * Unwrap cause of SparkException or UserCodeException as PipelineExecutionException. Otherwise,
   * return {@code exception} as RuntimeException.
   */
  private static RuntimeException unwrapCause(Throwable exception) {
    Throwable next = exception;
    while (next != null && (next instanceof SparkException || next instanceof UserCodeException)) {
      exception = next;
      next = next.getCause();
    }
    return exception == next
        ? runtimeExceptionFrom(exception)
        : new Pipeline.PipelineExecutionException(firstNonNull(next, exception));
  }

  private State awaitTermination(Duration duration)
      throws TimeoutException, ExecutionException, InterruptedException {
    pipelineExecution.get(duration.getMillis(), TimeUnit.MILLISECONDS);
    // Throws an exception if the job is not finished successfully in the given time.
    return PipelineResult.State.DONE;
  }

  @Override
  public PipelineResult.State getState() {
    return state;
  }

  @Override
  public PipelineResult.State waitUntilFinish() {
    return waitUntilFinish(Duration.millis(Long.MAX_VALUE));
  }

  @Override
  public State waitUntilFinish(final Duration duration) {
    try {
      State finishState = awaitTermination(duration);
      offerNewState(finishState);
    } catch (final TimeoutException e) {
      // ignore.
    } catch (final ExecutionException e) {
      offerNewState(PipelineResult.State.FAILED);
      throw unwrapCause(firstNonNull(e.getCause(), e));
    } catch (final Exception e) {
      offerNewState(PipelineResult.State.FAILED);
      throw unwrapCause(e);
    }

    return state;
  }

  @Override
  public MetricResults metrics() {
    return asAttemptedOnlyMetricResults(metrics.value());
  }

  @Override
  public PipelineResult.State cancel() throws IOException {
    offerNewState(PipelineResult.State.CANCELLED);
    return state;
  }

  private void offerNewState(State newState) {
    State oldState = this.state;
    this.state = newState;
    if (!oldState.isTerminal() && newState.isTerminal() && onTerminalState != null) {
      try {
        onTerminalState.run();
      } catch (Exception e) {
        throw unwrapCause(e);
      }
    }
  }
}
