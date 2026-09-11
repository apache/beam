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
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects.firstNonNull;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.apache.beam.runners.spark.structuredstreaming.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.structuredstreaming.translation.EvaluationContext;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.spark.SparkException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkStructuredStreamingPipelineResult implements PipelineResult {

  private static final Logger LOG =
      LoggerFactory.getLogger(SparkStructuredStreamingPipelineResult.class);

  /** Upper bound on how long {@link #cancel()} waits for the execution thread to end. */
  private static final long CANCEL_WAIT_SECONDS = 60;

  private final Future<?> pipelineExecution;
  // Supplies the context of the translated pipeline, null until translation has completed.
  private final Supplier<? extends @Nullable EvaluationContext> evaluationContext;
  private final MetricsAccumulator metrics;
  private final @Nullable Runnable onTerminalState;
  private final ExecutorService executor;
  private PipelineResult.State state;

  SparkStructuredStreamingPipelineResult(
      Future<?> pipelineExecution,
      Supplier<? extends @Nullable EvaluationContext> evaluationContext,
      MetricsAccumulator metrics,
      final @Nullable Runnable onTerminalState,
      ExecutorService executor) {
    this.pipelineExecution = pipelineExecution;
    this.evaluationContext = evaluationContext;
    this.metrics = metrics;
    this.onTerminalState = onTerminalState;
    this.executor = executor;
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

  /**
   * Cancels the execution and waits up to {@link #CANCEL_WAIT_SECONDS} for the execution thread to
   * end before the terminal state callback stops the session. An interrupted caller returns without
   * the callback, the state stays RUNNING.
   */
  @Override
  public PipelineResult.State cancel() throws IOException {
    EvaluationContext ctx = evaluationContext.get();
    if (ctx != null) {
      ctx.stop();
    }
    pipelineExecution.cancel(true);
    try {
      if (!executor.awaitTermination(CANCEL_WAIT_SECONDS, TimeUnit.SECONDS)) {
        LOG.warn(
            "Pipeline execution still running {} s after cancel, stopping the session anyway.",
            CANCEL_WAIT_SECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return state;
    }
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
