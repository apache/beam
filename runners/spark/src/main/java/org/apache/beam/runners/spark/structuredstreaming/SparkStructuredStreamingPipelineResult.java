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

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.beam.runners.spark.structuredstreaming.metrics.MetricsAccumulator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.spark.SparkException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.joda.time.Duration;

/** Represents a Spark pipeline execution result. */
public class SparkStructuredStreamingPipelineResult implements PipelineResult {

  final Future pipelineExecution;
  final SparkSession sparkSession;
  PipelineResult.State state;

  boolean isStreaming;

  SparkStructuredStreamingPipelineResult(
      final Future<?> pipelineExecution, final SparkSession sparkSession) {
    this.pipelineExecution = pipelineExecution;
    this.sparkSession = sparkSession;
    // pipelineExecution is expected to have started executing eagerly.
    this.state = State.RUNNING;
    // TODO: Implement results on a streaming pipeline. Currently does not stream.
    this.isStreaming = false;
  }

  private static RuntimeException runtimeExceptionFrom(final Throwable e) {
    return (e instanceof RuntimeException) ? (RuntimeException) e : new RuntimeException(e);
  }

  private static RuntimeException beamExceptionFrom(final Throwable e) {
    // Scala doesn't declare checked exceptions in the bytecode, and the Java compiler
    // won't let you catch something that is not declared, so we can't catch
    // SparkException directly, instead we do an instanceof check.

    if (e instanceof SparkException) {
      if (e.getCause() != null && e.getCause() instanceof UserCodeException) {
        UserCodeException userException = (UserCodeException) e.getCause();
        return new Pipeline.PipelineExecutionException(userException.getCause());
      } else if (e.getCause() != null) {
        return new Pipeline.PipelineExecutionException(e.getCause());
      }
    }

    return runtimeExceptionFrom(e);
  }

  protected void stop() {
    try {
      // TODO: await any outstanding queries on the session if this is streaming.
      if (isStreaming) {
        for (StreamingQuery query : sparkSession.streams().active()) {
          query.stop();
        }
      }
    } catch (Exception e) {
      throw beamExceptionFrom(e);
    } finally {
      sparkSession.stop();
      if (Objects.equals(state, State.RUNNING)) {
        this.state = State.STOPPED;
      }
    }
  }

  private State awaitTermination(Duration duration)
      throws TimeoutException, ExecutionException, InterruptedException {
    pipelineExecution.get(duration.getMillis(), TimeUnit.MILLISECONDS);
    // Throws an exception if the job is not finished successfully in the given time.
    // TODO: all streaming functionality
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
      throw beamExceptionFrom(e.getCause());
    } catch (final Exception e) {
      offerNewState(PipelineResult.State.FAILED);
      throw beamExceptionFrom(e);
    }

    return state;
  }

  @Override
  public MetricResults metrics() {
    return asAttemptedOnlyMetricResults(MetricsAccumulator.getInstance().value());
  }

  @Override
  public PipelineResult.State cancel() throws IOException {
    offerNewState(PipelineResult.State.CANCELLED);
    return state;
  }

  private void offerNewState(State newState) {
    State oldState = this.state;
    this.state = newState;
    if (!oldState.isTerminal() && newState.isTerminal()) {
      stop();
    }
  }
}
