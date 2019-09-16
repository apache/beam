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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.spark.aggregators.AggregatorsAccumulator;
import org.apache.beam.runners.spark.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.stateful.SparkTimerInternals;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.io.FileUtils;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The SparkRunner translate operations defined on a pipeline to a representation executable by
 * Spark, and then submitting the job to Spark to be executed. If we wanted to run a Beam pipeline
 * with the default options of a single threaded spark instance in local mode, we would do the
 * following:
 *
 * <p>{@code Pipeline p = [logic for pipeline creation] SparkPipelineResult result =
 * (SparkPipelineResult) p.run(); }
 *
 * <p>To create a pipeline runner to run against a different spark cluster, with a custom master url
 * we would do the following:
 *
 * <p>{@code Pipeline p = [logic for pipeline creation] SparkPipelineOptions options =
 * SparkPipelineOptionsFactory.create(); options.setSparkMaster("spark://host:port");
 * SparkPipelineResult result = (SparkPipelineResult) p.run(); }
 */
public final class TestSparkRunner extends PipelineRunner<SparkPipelineResult> {

  private static final Logger LOG = LoggerFactory.getLogger(TestSparkRunner.class);
  private final PipelineOptions options;
  private final SparkRunner delegate;

  private TestSparkRunner(PipelineOptions options) {
    this.delegate = SparkRunner.fromOptions(options);
    this.options = options;
  }

  public static TestSparkRunner fromOptions(PipelineOptions options) {
    return new TestSparkRunner(options);
  }

  @Override
  @SuppressWarnings("Finally")
  public SparkPipelineResult run(Pipeline pipeline) {
    // Default options suffice to set it up as a test runner
    TestSparkPipelineOptions testSparkOptions =
        PipelineOptionsValidator.validate(TestSparkPipelineOptions.class, options);

    boolean isForceStreaming = testSparkOptions.isForceStreaming();
    SparkPipelineResult result = null;

    // clear state of Aggregators, Metrics and Watermarks if exists.
    AggregatorsAccumulator.clear();
    MetricsAccumulator.clear();
    GlobalWatermarkHolder.clear();

    LOG.info("About to run test pipeline " + options.getJobName());

    // if the pipeline was executed in streaming mode, validate aggregators.
    if (isForceStreaming) {
      try {
        result = delegate.run(pipeline);
        awaitWatermarksOrTimeout(testSparkOptions, result);
        result.stop();
        PipelineResult.State finishState = result.getState();
        // assert finish state.
        assertThat(
            String.format("Finish state %s is not allowed.", finishState),
            finishState,
            isOneOf(PipelineResult.State.STOPPED, PipelineResult.State.DONE));
      } finally {
        try {
          // cleanup checkpoint dir.
          FileUtils.deleteDirectory(new File(testSparkOptions.getCheckpointDir()));
        } catch (IOException e) {
          throw new RuntimeException("Failed to clear checkpoint tmp dir.", e);
        }
      }
    } else {
      // for batch test pipelines, run and block until done.
      result = delegate.run(pipeline);
      result.waitUntilFinish();
      result.stop();
      PipelineResult.State finishState = result.getState();
      // assert finish state.
      assertThat(
          String.format("Finish state %s is not allowed.", finishState),
          finishState,
          is(PipelineResult.State.DONE));
      // assert via matchers.
      assertThat(result, testSparkOptions.getOnCreateMatcher());
      assertThat(result, testSparkOptions.getOnSuccessMatcher());
    }
    return result;
  }

  private static void awaitWatermarksOrTimeout(
      TestSparkPipelineOptions testSparkPipelineOptions, SparkPipelineResult result) {
    Long timeoutMillis =
        Duration.standardSeconds(checkNotNull(testSparkPipelineOptions.getTestTimeoutSeconds()))
            .getMillis();
    Long batchDurationMillis = testSparkPipelineOptions.getBatchIntervalMillis();
    Instant stopPipelineWatermark =
        new Instant(testSparkPipelineOptions.getStopPipelineWatermark());
    // we poll for pipeline status in batch-intervals. while this is not in-sync with Spark's
    // execution clock, this is good enough.
    // we break on timeout or end-of-time WM, which ever comes first.
    Instant globalWatermark;
    result.waitUntilFinish(Duration.millis(batchDurationMillis));
    do {
      SparkTimerInternals sparkTimerInternals =
          SparkTimerInternals.global(GlobalWatermarkHolder.get(batchDurationMillis));
      sparkTimerInternals.advanceWatermark();
      globalWatermark = sparkTimerInternals.currentInputWatermarkTime();
      // let another batch-interval period of execution, just to reason about WM propagation.
      Uninterruptibles.sleepUninterruptibly(batchDurationMillis, TimeUnit.MILLISECONDS);
    } while ((timeoutMillis -= batchDurationMillis) > 0
        && globalWatermark.isBefore(stopPipelineWatermark));
  }
}
