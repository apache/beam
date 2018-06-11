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
package org.apache.beam.sdk.extensions.euphoria.beam;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.util.Settings;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper around Beam's runner. Allows for {@link #executeSync(Flow) synchronous} and {@link
 * #submitAsync(Flow) asycnhronous} executions of {@link Flow}.
 */
public class BeamRunnerWrapper {

  private static final Logger LOG = LoggerFactory.getLogger(BeamRunnerWrapper.class);

  private final PipelineOptions options;
  private final Settings settings;
  private Duration allowedLateness = Duration.ZERO;

  private AccumulatorProvider.Factory accumulatorFactory = BeamAccumulatorProvider.getFactory();

  /**
   * Executor to submit flows, if closed all executions should be interrupted.
   */
  private final ExecutorService submitExecutor = Executors.newCachedThreadPool();

  private BeamRunnerWrapper(PipelineOptions options) {
    this(options, new Settings());
  }

  private BeamRunnerWrapper(PipelineOptions options, Settings settings) {
    this.options = options;
    this.settings = settings;
  }

  /**
   * @return wrapper around Beam's direct runner. It allows to run {@link Flow} locally.
   */
  public static BeamRunnerWrapper ofDirect() {
    final String[] args = {"--runner=DirectRunner"};
    final PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
    return new BeamRunnerWrapper(options).withAllowedLateness(java.time.Duration.ofHours(1));
  }

  /**
   * Blocks until a given {@link Flow} is executed.
   */
  public Result executeSync(Flow flow) {
    final Pipeline pipeline;
    if (flow instanceof BeamFlow && ((BeamFlow) flow).hasPipeline()) {
      pipeline = ((BeamFlow) flow).getPipeline();
    } else {
      pipeline =
          FlowTranslator.toPipeline(flow, accumulatorFactory, options, settings, allowedLateness);
    }
    final PipelineResult result = pipeline.run();
    // TODO handle result
    State state = result.waitUntilFinish();
    LOG.info("Pipeline result state: {}.", state);
    return new ExecutorResult(result);
  }

  static class ExecutorResult extends Result{

    private final PipelineResult result;

    public ExecutorResult(PipelineResult result) {
      this.result = result;
    }

    public PipelineResult getResult() {
      return result;
    }
  }

  public void setAccumulatorProvider(AccumulatorProvider.Factory accumulatorFactory) {
    this.accumulatorFactory = accumulatorFactory;
  }

  /**
   * Specify global allowed lateness for the executor.
   *
   * @param duration the allowed lateness for all windows
   * @return this
   */
  public BeamRunnerWrapper withAllowedLateness(java.time.Duration duration) {
    this.allowedLateness = Duration.millis(duration.toMillis());
    return this;
  }

  public CompletableFuture<Result> submitAsync(Flow flow) {
    return CompletableFuture.supplyAsync(() -> executeSync(flow), submitExecutor);
  }

  public void shutdown() {
    LOG.info("Shutting down executor.");
    submitExecutor.shutdownNow();
  }

  /**
   * Result of pipeline's run.
   */
  private static class Result {

  }
}
