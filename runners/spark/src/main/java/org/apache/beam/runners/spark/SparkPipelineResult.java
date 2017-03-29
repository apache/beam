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

package org.apache.beam.runners.spark;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.beam.runners.spark.aggregators.SparkAggregators;
import org.apache.beam.runners.spark.metrics.SparkMetricResults;
import org.apache.beam.runners.spark.translation.SparkContextFactory;
import org.apache.beam.sdk.AggregatorValues;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.joda.time.Duration;

/** Represents a Spark pipeline execution result. */
public abstract class SparkPipelineResult implements PipelineResult {

  protected final Future pipelineExecution;
  protected JavaSparkContext javaSparkContext;
  protected PipelineResult.State state;
  private final SparkMetricResults metricResults = new SparkMetricResults();

  SparkPipelineResult(final Future<?> pipelineExecution, final JavaSparkContext javaSparkContext) {
    this.pipelineExecution = pipelineExecution;
    this.javaSparkContext = javaSparkContext;
    // pipelineExecution is expected to have started executing eagerly.
    this.state = State.RUNNING;
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

  protected abstract void stop();

  protected abstract State awaitTermination(Duration duration)
      throws TimeoutException, ExecutionException, InterruptedException;

  public <T> T getAggregatorValue(final String name, final Class<T> resultType) {
    return SparkAggregators.valueOf(name, resultType);
  }

  @Override
  public <T> AggregatorValues<T> getAggregatorValues(final Aggregator<?, T> aggregator) {
    return SparkAggregators.valueOf(aggregator);
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
    return metricResults;
  }

  @Override
  public PipelineResult.State cancel() throws IOException {
    offerNewState(PipelineResult.State.CANCELLED);
    return state;
  }

  /** Represents the result of running a batch pipeline. */
  static class BatchMode extends SparkPipelineResult {

    BatchMode(final Future<?> pipelineExecution, final JavaSparkContext javaSparkContext) {
      super(pipelineExecution, javaSparkContext);
    }

    @Override
    protected void stop() {
      SparkContextFactory.stopSparkContext(javaSparkContext);
      if (Objects.equals(state, State.RUNNING)) {
        this.state = State.STOPPED;
      }
    }

    @Override
    protected State awaitTermination(final Duration duration)
        throws TimeoutException, ExecutionException, InterruptedException {
      pipelineExecution.get(duration.getMillis(), TimeUnit.MILLISECONDS);
      return PipelineResult.State.DONE;
    }
  }

  /** Represents a streaming Spark pipeline result. */
  static class StreamingMode extends SparkPipelineResult {

    private final JavaStreamingContext javaStreamingContext;

    StreamingMode(
        final Future<?> pipelineExecution, final JavaStreamingContext javaStreamingContext) {
      super(pipelineExecution, javaStreamingContext.sparkContext());
      this.javaStreamingContext = javaStreamingContext;
    }

    @Override
    protected void stop() {
      javaStreamingContext.stop(false, true);
      // after calling stop, if exception occurs in "grace period" it won't propagate.
      // calling the StreamingContext's waiter with 0 msec will throw any error that might have
      // been thrown during the "grace period".
      try {
        javaStreamingContext.awaitTerminationOrTimeout(0);
      } catch (Exception e) {
        throw beamExceptionFrom(e);
      } finally {
        SparkContextFactory.stopSparkContext(javaSparkContext);
        if (Objects.equals(state, State.RUNNING)) {
          this.state = State.STOPPED;
        }
      }
    }

    @Override
    protected State awaitTermination(final Duration duration)
        throws ExecutionException, InterruptedException {
      pipelineExecution.get(); // execution is asynchronous anyway so no need to time-out.
      javaStreamingContext.awaitTerminationOrTimeout(duration.getMillis());

      State terminationState;
      switch (javaStreamingContext.getState()) {
        case ACTIVE:
          terminationState = State.RUNNING;
          break;
        case STOPPED:
          terminationState = State.DONE;
          break;
        default:
          terminationState = State.UNKNOWN;
          break;
      }
      return terminationState;
    }
  }

  private void offerNewState(State newState) {
    State oldState = this.state;
    this.state = newState;
    if (!oldState.isTerminal() && newState.isTerminal()) {
      stop();
    }
  }
}
