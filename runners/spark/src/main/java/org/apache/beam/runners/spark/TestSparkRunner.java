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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.core.construction.PTransformMatchers;
import org.apache.beam.runners.core.construction.ReplacementOutputs;
import org.apache.beam.runners.core.construction.UnboundedReadFromBoundedSource;
import org.apache.beam.runners.spark.aggregators.AggregatorsAccumulator;
import org.apache.beam.runners.spark.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.stateful.SparkTimerInternals;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.BoundedReadFromUnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.ValueWithRecordId;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.io.FileUtils;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The SparkRunner translate operations defined on a pipeline to a representation executable
 * by Spark, and then submitting the job to Spark to be executed. If we wanted to run a Beam
 * pipeline with the default options of a single threaded spark instance in local mode, we would do
 * the following:
 *
 * {@code
 * Pipeline p = [logic for pipeline creation]
 * SparkPipelineResult result = (SparkPipelineResult) p.run();
 * }
 *
 * <p>To create a pipeline runner to run against a different spark cluster, with a custom master url
 * we would do the following:
 *
 * {@code
 * Pipeline p = [logic for pipeline creation]
 * SparkPipelineOptions options = SparkPipelineOptionsFactory.create();
 * options.setSparkMaster("spark://host:port");
 * SparkPipelineResult result = (SparkPipelineResult) p.run();
 * }
 */
public final class TestSparkRunner extends PipelineRunner<SparkPipelineResult> {

  private static final Logger LOG = LoggerFactory.getLogger(TestSparkRunner.class);

  private SparkRunner delegate;
  private boolean isForceStreaming;

  private TestSparkRunner(TestSparkPipelineOptions options) {
    this.delegate = SparkRunner.fromOptions(options);
    this.isForceStreaming = options.isForceStreaming();
  }

  public static TestSparkRunner fromOptions(PipelineOptions options) {
    // Default options suffice to set it up as a test runner
    TestSparkPipelineOptions sparkOptions =
        PipelineOptionsValidator.validate(TestSparkPipelineOptions.class, options);
    return new TestSparkRunner(sparkOptions);
  }

  @Override
  public SparkPipelineResult run(Pipeline pipeline) {
    TestSparkPipelineOptions testSparkPipelineOptions =
        pipeline.getOptions().as(TestSparkPipelineOptions.class);
    //
    // if the pipeline forces execution as a streaming pipeline,
    // and the source is an adapted unbounded source (as bounded),
    // read it as unbounded source via UnboundedReadFromBoundedSource.
    if (isForceStreaming) {
      adaptBoundedReads(pipeline);
    }
    SparkPipelineResult result = null;

    int expectedNumberOfAssertions = PAssert.countAsserts(pipeline);

    // clear state of Aggregators, Metrics and Watermarks if exists.
    AggregatorsAccumulator.clear();
    MetricsAccumulator.clear();
    GlobalWatermarkHolder.clear();

    LOG.info("About to run test pipeline " + testSparkPipelineOptions.getJobName());

    // if the pipeline was executed in streaming mode, validate aggregators.
    if (isForceStreaming) {
      try {
        result = delegate.run(pipeline);
        awaitWatermarksOrTimeout(testSparkPipelineOptions, result);
        result.stop();
        PipelineResult.State finishState = result.getState();
        // assert finish state.
        assertThat(
            String.format("Finish state %s is not allowed.", finishState),
            finishState,
            isOneOf(PipelineResult.State.STOPPED, PipelineResult.State.DONE));

        // validate assertion succeeded (at least once).
        int successAssertions = 0;
        try {
          successAssertions = result.getAggregatorValue(PAssert.SUCCESS_COUNTER, Integer.class);
        } catch (NullPointerException e) {
          // No assertions registered will cause an NPE here.
        }
        Integer expectedAssertions = testSparkPipelineOptions.getExpectedAssertions() != null
            ? testSparkPipelineOptions.getExpectedAssertions() : expectedNumberOfAssertions;
        assertThat(
            String.format(
                "Expected %d successful assertions, but found %d.",
                expectedAssertions, successAssertions),
            successAssertions,
            is(expectedAssertions));
        // validate assertion didn't fail.
        int failedAssertions = 0;
        try {
          failedAssertions = result.getAggregatorValue(PAssert.FAILURE_COUNTER, Integer.class);
        } catch (NullPointerException e) {
          // No assertions registered will cause an NPE here.
        }
        assertThat(
            String.format("Found %d failed assertions.", failedAssertions),
            failedAssertions,
            is(0));

        LOG.info(
            String.format(
                "Successfully asserted pipeline %s with %d successful assertions.",
                testSparkPipelineOptions.getJobName(),
                successAssertions));
      } finally {
        try {
          // cleanup checkpoint dir.
          FileUtils.deleteDirectory(new File(testSparkPipelineOptions.getCheckpointDir()));
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
      assertThat(result, testSparkPipelineOptions.getOnCreateMatcher());
      assertThat(result, testSparkPipelineOptions.getOnSuccessMatcher());
    }
    return result;
  }

  private static void awaitWatermarksOrTimeout(
      TestSparkPipelineOptions testSparkPipelineOptions, SparkPipelineResult result) {
    Long timeoutMillis = Duration.standardSeconds(
        checkNotNull(testSparkPipelineOptions.getTestTimeoutSeconds())).getMillis();
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
          SparkTimerInternals.global(GlobalWatermarkHolder.get());
      sparkTimerInternals.advanceWatermark();
      globalWatermark = sparkTimerInternals.currentInputWatermarkTime();
      // let another batch-interval period of execution, just to reason about WM propagation.
      Uninterruptibles.sleepUninterruptibly(batchDurationMillis, TimeUnit.MILLISECONDS);
    } while ((timeoutMillis -= batchDurationMillis) > 0
          && globalWatermark.isBefore(stopPipelineWatermark));
  }

  @VisibleForTesting
  void adaptBoundedReads(Pipeline pipeline) {
    pipeline.replaceAll(
        Collections.singletonList(
            PTransformOverride.of(
                PTransformMatchers.classEqualTo(BoundedReadFromUnboundedSource.class),
                new AdaptedBoundedAsUnbounded.Factory())));
  }

  private static class AdaptedBoundedAsUnbounded<T> extends PTransform<PBegin, PCollection<T>> {
    private final BoundedReadFromUnboundedSource<T> source;

    AdaptedBoundedAsUnbounded(BoundedReadFromUnboundedSource<T> source) {
      this.source = source;
    }

    @SuppressWarnings("unchecked")
    @Override
    public PCollection<T> expand(PBegin input) {
      PTransform<PBegin, ? extends PCollection<ValueWithRecordId<T>>> replacingTransform =
          new UnboundedReadFromBoundedSource<>(source.getAdaptedSource());
      return (PCollection<T>) input.apply(replacingTransform)
          .apply("StripIds", ParDo.of(new ValueWithRecordId.StripIdsDoFn()));
    }

    static class Factory<T>
        implements PTransformOverrideFactory<
            PBegin, PCollection<T>, BoundedReadFromUnboundedSource<T>> {
      @Override
      public PTransformReplacement<PBegin, PCollection<T>> getReplacementTransform(
          AppliedPTransform<PBegin, PCollection<T>, BoundedReadFromUnboundedSource<T>> transform) {
        return PTransformReplacement.of(
            transform.getPipeline().begin(),
            new AdaptedBoundedAsUnbounded<T>(transform.getTransform()));
      }

      @Override
      public Map<PValue, ReplacementOutput> mapOutputs(
          Map<TupleTag<?>, PValue> outputs, PCollection<T> newOutput) {
        return ReplacementOutputs.singleton(outputs, newOutput);
      }
    }
  }
}
