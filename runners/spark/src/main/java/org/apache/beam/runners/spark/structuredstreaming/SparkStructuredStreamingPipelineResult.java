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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
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

/**
 * Result of a pipeline submitted to the {@link SparkStructuredStreamingRunner}. The pipeline runs
 * asynchronously on a dedicated thread.
 */
public class SparkStructuredStreamingPipelineResult implements PipelineResult {

  private static final Logger LOG =
      LoggerFactory.getLogger(SparkStructuredStreamingPipelineResult.class);

  private final Future<?> pipelineExecution;
  // Supplies the context of the translated pipeline, null until translation has completed.
  private final Supplier<? extends @Nullable EvaluationContext> evaluationContext;
  private final MetricsAccumulator metrics;
  private final AtomicBoolean cancelRequested;
  private final Runnable cancelSparkJobs;
  private volatile PipelineResult.State state;

  SparkStructuredStreamingPipelineResult(
      Future<?> pipelineExecution,
      Supplier<? extends @Nullable EvaluationContext> evaluationContext,
      MetricsAccumulator metrics,
      AtomicBoolean cancelRequested,
      Runnable cancelSparkJobs) {
    this.pipelineExecution = pipelineExecution;
    this.evaluationContext = evaluationContext;
    this.metrics = metrics;
    this.cancelRequested = cancelRequested;
    this.cancelSparkJobs = cancelSparkJobs;
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

  @Override
  public PipelineResult.State getState() {
    return state;
  }

  @Override
  public PipelineResult.State waitUntilFinish() {
    return waitUntilFinish(Duration.millis(Long.MAX_VALUE));
  }

  /**
   * Waits up to {@code duration} for the execution thread. A pipeline that ends after {@link
   * #cancel()} is CANCELLED, any other failure is rethrown and the pipeline is FAILED.
   */
  @Override
  public State waitUntilFinish(final Duration duration) {
    try {
      pipelineExecution.get(duration.getMillis(), TimeUnit.MILLISECONDS);
      state = cancelRequested.get() ? State.CANCELLED : State.DONE;
    } catch (final TimeoutException e) {
      // ignore.
    } catch (final ExecutionException e) {
      if (cancelRequested.get()) {
        LOG.warn("Pipeline execution failed after cancel", e.getCause());
        state = State.CANCELLED;
        return state;
      }
      state = State.FAILED;
      throw unwrapCause(firstNonNull(e.getCause(), e));
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      state = State.FAILED;
      throw unwrapCause(e);
    } catch (final Exception e) {
      state = State.FAILED;
      throw unwrapCause(e);
    }

    return state;
  }

  @Override
  public MetricResults metrics() {
    return asAttemptedOnlyMetricResults(metrics.value());
  }

  /** Requests cancellation of the pipeline and returns immediately. */
  @Override
  public PipelineResult.State cancel() throws IOException {
    if (!state.isTerminal() && cancelRequested.compareAndSet(false, true)) {
      EvaluationContext ctx = evaluationContext.get();
      if (ctx != null) {
        ctx.stop();
      }
      cancelSparkJobs.run();
      state = State.CANCELLED;
    }
    return state;
  }
}
