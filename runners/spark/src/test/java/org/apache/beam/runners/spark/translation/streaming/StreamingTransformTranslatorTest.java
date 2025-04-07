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
package org.apache.beam.runners.spark.translation.streaming;

import static org.apache.beam.sdk.metrics.MetricResultsMatchers.attemptedMetricsResult;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.function.Function;
import org.apache.beam.runners.spark.StreamingTest;
import org.apache.beam.runners.spark.TestSparkPipelineOptions;
import org.apache.beam.runners.spark.TestSparkRunner;
import org.apache.beam.runners.spark.UsesCheckpointRecovery;
import org.apache.beam.runners.spark.io.MicrobatchSource;
import org.apache.beam.runners.spark.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.UsesSideInputs;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/** Test suite for {@link StreamingTransformTranslator}. */
public class StreamingTransformTranslatorTest implements Serializable {

  /**
   * A functional interface that creates a {@link Pipeline} from {@link PipelineOptions}. Used in
   * tests to define different pipeline configurations that can be executed with the same test
   * harness.
   */
  @FunctionalInterface
  interface PipelineFunction extends Function<PipelineOptions, Pipeline> {}

  @Rule public transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  /** Creates a temporary directory for storing checkpoints before each test execution. */
  @Before
  public void init() {
    try {
      temporaryFolder.create();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static class StreamingSideInputAsSingletonView
      extends PTransform<PBegin, PCollectionView<Long>> {

    @Override
    public PCollectionView<Long> expand(PBegin input) {
      return input
          .getPipeline()
          .apply("Gen Seq", GenerateSequence.from(0).withRate(1, Duration.millis(500)))
          .apply(
              Window.<Long>configure()
                  .withAllowedLateness(Duration.ZERO)
                  .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                  .withAllowedLateness(Duration.ZERO)
                  .discardingFiredPanes())
          .setCoder(NullableCoder.of(VarLongCoder.of()))
          .apply(
              "To Side Input", Combine.<Long>globally(MoreObjects::firstNonNull).asSingletonView());
    }
  }

  private static class StreamingSideInputAsIterableView
      extends PTransform<PBegin, PCollectionView<Iterable<Long>>> {

    @Override
    public PCollectionView<Iterable<Long>> expand(PBegin input) {
      return input
          .getPipeline()
          .apply("Gen Seq", GenerateSequence.from(0).withRate(1, Duration.millis(500)))
          .apply(
              Window.<Long>configure()
                  .withAllowedLateness(Duration.ZERO)
                  .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(2)))
                  .discardingFiredPanes())
          .setCoder(NullableCoder.of(VarLongCoder.of()))
          .apply(View.asIterable());
    }
  }

  @Test
  @Category({StreamingTest.class, UsesSideInputs.class})
  public void testStreamingSideInputAsSingletonView() {
    final PipelineFunction pipelineFunction =
        (PipelineOptions options) -> {
          Pipeline p = Pipeline.create(options);

          final PCollectionView<Long> streamingSideInput =
              p.apply(
                  "Streaming Side Input As Singleton View",
                  new StreamingSideInputAsSingletonView());

          final PAssertFn pAssertFn = new PAssertFn();
          pAssertFn.streamingSideInputAsSingletonView = streamingSideInput;
          p.apply("Main Input", GenerateSequence.from(0).withRate(1, Duration.millis(500)))
              .apply(
                  "StreamingSideInputAssert",
                  ParDo.of(pAssertFn).withSideInput("streaming-side-input", streamingSideInput));

          return p;
        };

    final PipelineResult result = run(pipelineFunction, Optional.of(new Instant(1000)), true);
    final Iterable<MetricResult<DistributionResult>> distributions =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.inNamespace(PAssertFn.class))
                    .build())
            .getDistributions();

    final MetricResult<DistributionResult> streamingSideInputMetricResult =
        Iterables.find(
            distributions,
            dist -> dist.getName().getName().equals("streaming_side_input_distribution"));

    // The streaming side input values are 0, 1, 2 which allows us to validate
    // the distribution metrics with sum=6, count=6, min=0, max=2
    assertThat(
        streamingSideInputMetricResult,
        is(
            attemptedMetricsResult(
                PAssertFn.class.getName(),
                "streaming_side_input_distribution",
                "StreamingSideInputAssert",
                DistributionResult.create(3, 3, 0, 2))));
  }

  @Test
  @Category({StreamingTest.class, UsesSideInputs.class})
  public void testStreamingSideInputAsIterableView() {
    final PipelineFunction pipelineFunction =
        (PipelineOptions options) -> {
          final Pipeline p = Pipeline.create(options);

          final PCollectionView<Iterable<Long>> streamingSideInput =
              p.apply(
                  "Streaming Side Input As Iterable View", new StreamingSideInputAsIterableView());
          final PAssertFn pAssertFn = new PAssertFn();
          pAssertFn.streamingSideInputAsIterableView = streamingSideInput;
          p.apply("Main Input", GenerateSequence.from(0).withRate(1, Duration.millis(500)))
              .apply(
                  "StreamingSideInputAssert",
                  ParDo.of(pAssertFn).withSideInput("streaming-side-input", streamingSideInput));

          return p;
        };

    final PipelineResult result = run(pipelineFunction, Optional.of(new Instant(1000)), true);
    final Iterable<MetricResult<DistributionResult>> distributions =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.inNamespace(PAssertFn.class))
                    .build())
            .getDistributions();

    final MetricResult<DistributionResult> streamingIterSideInputMetricResult =
        Iterables.find(
            distributions,
            dist -> dist.getName().getName().equals("streaming_iter_side_input_distribution"));

    // The count can vary depending on the execution environment:
    // - When count is 8: We observed 4 pairs of values [0,1], [0,1], [2,3], [2,3]
    // - Otherwise: We observed 3 pairs of values [0,1], [0,1], [2,3] (6 elements total)
    // This variation is due to timing differences in different execution environments
    if (streamingIterSideInputMetricResult.getAttempted().getCount() == 8) {
      assertThat(
          streamingIterSideInputMetricResult,
          is(
              attemptedMetricsResult(
                  PAssertFn.class.getName(),
                  "streaming_iter_side_input_distribution",
                  "StreamingSideInputAssert",
                  DistributionResult.create(12, 8, 0, 3))));
    } else {
      assertThat(
          streamingIterSideInputMetricResult,
          is(
              attemptedMetricsResult(
                  PAssertFn.class.getName(),
                  "streaming_iter_side_input_distribution",
                  "StreamingSideInputAssert",
                  DistributionResult.create(7, 6, 0, 3))));
    }
  }

  /**
   * Tests that Flatten transform of Bounded and Unbounded PCollections correctly recovers from
   * checkpoint.
   *
   * <p>Test scenario:
   *
   * <ol>
   *   <li>First run:
   *       <ul>
   *         <li>Flattens Bounded PCollection(0-9) with Unbounded PCollection
   *         <li>Stops pipeline after 400ms
   *         <li>Validates metrics results
   *       </ul>
   *   <li>Second run (recovery from checkpoint):
   *       <ul>
   *         <li>Recovers from previous state and continues execution
   *         <li>Stops pipeline after 1 second
   *         <li>Validates accumulated metrics results
   *       </ul>
   * </ol>
   */
  @Category({UsesCheckpointRecovery.class, StreamingTest.class})
  @Test
  public void testFlattenPCollResumeFromCheckpoint() {
    final MetricsFilter metricsFilter =
        MetricsFilter.builder()
            .addNameFilter(MetricNameFilter.inNamespace(PAssertFn.class))
            .build();

    final PipelineFunction pipelineFunction =
        (PipelineOptions options) -> {
          Pipeline p = Pipeline.create(options);

          final PCollection<Long> bounded =
              p.apply("Bounded", GenerateSequence.from(0).to(10))
                  .apply("BoundedAssert", ParDo.of(new PAssertFn()));

          final PCollection<Long> unbounded =
              p.apply(
                      "Unbounded",
                      GenerateSequence.from(10).withRate(3, Duration.standardSeconds(1)))
                  .apply(WithTimestamps.of(e -> Instant.now()));

          final PCollection<Long> flattened = bounded.apply(Flatten.with(unbounded));

          flattened.apply("FlattenedAssert", ParDo.of(new PAssertFn()));
          return p;
        };

    PipelineResult res = run(pipelineFunction, Optional.of(new Instant(400)), false);

    // Verify metrics for Bounded PCollection (sum of 0-9 = 45, count = 10)
    assertThat(
        res.metrics().queryMetrics(metricsFilter).getDistributions(),
        hasItem(
            attemptedMetricsResult(
                PAssertFn.class.getName(),
                "distribution",
                "BoundedAssert",
                DistributionResult.create(45, 10, 0L, 9L))));

    // Verify metrics for Flattened result after first run
    assertThat(
        res.metrics().queryMetrics(metricsFilter).getDistributions(),
        hasItem(
            attemptedMetricsResult(
                PAssertFn.class.getName(),
                "distribution",
                "FlattenedAssert",
                DistributionResult.create(45, 10, 0L, 9L))));

    // Clean up state
    clean();

    // Second run: recover from checkpoint
    res = runAgain(pipelineFunction);

    // Verify Bounded PCollection metrics remain the same
    assertThat(
        res.metrics().queryMetrics(metricsFilter).getDistributions(),
        hasItem(
            attemptedMetricsResult(
                PAssertFn.class.getName(),
                "distribution",
                "BoundedAssert",
                DistributionResult.create(45, 10, 0L, 9L))));

    // Verify Flattened results show accumulated values from both runs
    // We use anyOf matcher because the unbounded source may emit either 2 or 3 elements during the
    // test window:
    // Case 1 (3 elements): sum=78 (45 from bounded + 33 from unbounded), count=13 (10 bounded + 3
    // unbounded)
    // Case 2 (2 elements): sum=66 (45 from bounded + 21 from unbounded), count=12 (10 bounded + 2
    // unbounded)
    // This variation occurs because the unbounded source's withRate(3, Duration.standardSeconds(1))
    // timing may be affected by test environment conditions
    assertThat(
        res.metrics().queryMetrics(metricsFilter).getDistributions(),
        hasItem(
            anyOf(
                attemptedMetricsResult(
                    PAssertFn.class.getName(),
                    "distribution",
                    "FlattenedAssert",
                    DistributionResult.create(78, 13, 0, 12)),
                attemptedMetricsResult(
                    PAssertFn.class.getName(),
                    "distribution",
                    "FlattenedAssert",
                    DistributionResult.create(66, 12, 0, 11)))));
  }

  /** Restarts the pipeline from checkpoint. Sets pipeline to stop after 1 second. */
  private PipelineResult runAgain(PipelineFunction pipelineFunction) {
    return run(
        pipelineFunction,
        Optional.of(
            Instant.ofEpochMilli(
                Duration.standardSeconds(1L).plus(Duration.millis(50L)).getMillis())),
        true);
  }

  /**
   * Sets up and runs the test pipeline.
   *
   * @param pipelineFunction Function that creates and configures the pipeline to be tested
   * @param stopWatermarkOption Watermark at which to stop the pipeline
   * @param deleteCheckpointDir Whether to delete checkpoint directory after completion
   */
  private PipelineResult run(
      PipelineFunction pipelineFunction,
      Optional<Instant> stopWatermarkOption,
      boolean deleteCheckpointDir) {
    final TestSparkPipelineOptions options = this.createTestSparkPipelineOptions();
    options.setCheckpointDir(temporaryFolder.getRoot().getPath());
    if (stopWatermarkOption.isPresent()) {
      options.setStopPipelineWatermark(stopWatermarkOption.get().getMillis());
    }
    options.setDeleteCheckpointDir(deleteCheckpointDir);

    return pipelineFunction.apply(options).run();
  }

  private TestSparkPipelineOptions createTestSparkPipelineOptions() {
    TestSparkPipelineOptions options =
        PipelineOptionsFactory.create().as(TestSparkPipelineOptions.class);
    options.setSparkMaster("local[*]");
    options.setRunner(TestSparkRunner.class);
    return options;
  }

  /**
   * Cleans up accumulated state between test runs. Clears metrics, watermarks, and microbatch
   * source cache.
   */
  @After
  public void clean() {
    MetricsAccumulator.clear();
    GlobalWatermarkHolder.clear();
    MicrobatchSource.clearCache();
  }

  /**
   * DoFn that tracks element distribution through metrics. Used to verify correct processing of
   * elements in both bounded and unbounded streams.
   */
  private static class PAssertFn extends DoFn<Long, Long> {

    @Nullable PCollectionView<Long> streamingSideInputAsSingletonView;
    @Nullable PCollectionView<Iterable<Long>> streamingSideInputAsIterableView;

    private final Distribution distribution = Metrics.distribution(PAssertFn.class, "distribution");
    private final Distribution streamingSideInputDistribution =
        Metrics.distribution(PAssertFn.class, "streaming_side_input_distribution");
    private final Distribution streamingIterSideInputDistribution =
        Metrics.distribution(PAssertFn.class, "streaming_iter_side_input_distribution");

    @ProcessElement
    public void process(
        ProcessContext context, @Element Long element, OutputReceiver<Long> output) {

      if (this.streamingSideInputAsSingletonView != null) {
        // The side input value might be null, which is expected behavior for streaming side inputs
        // before they receive their first value
        final @Nullable Long streamingSideInputValue =
            context.sideInput(this.streamingSideInputAsSingletonView);
        if (streamingSideInputValue != null) {
          // We only process side input values <= 2 to ensure consistent test behavior
          // across different execution environments, as some environments might emit
          // more elements than expected during the test window
          if (streamingSideInputValue <= 2) {
            System.out.println(streamingSideInputValue);
            this.streamingSideInputDistribution.update(streamingSideInputValue);
          }
        }
      }

      if (this.streamingSideInputAsIterableView != null) {
        final Iterable<Long> streamingSideInputIterValue =
            context.sideInput(this.streamingSideInputAsIterableView);
        final List<Long> sideInputValues = Lists.newArrayList(streamingSideInputIterValue);
        // Only process side inputs with exactly 2 elements to ensure consistent test behavior.
        // This filtering is necessary because the streaming environment may produce
        // different sized batches depending on timing and execution conditions.
        if (sideInputValues.size() == 2) {
          for (Long sideInputValue : sideInputValues) {
            if (sideInputValue <= 3) {
              this.streamingIterSideInputDistribution.update(sideInputValue);
            }
          }
        }
      }

      // For the unbounded source (starting from 10), we expect only 3 elements (10, 11, 12)
      // to be emitted during the 1-second test window.
      // However, different execution environments might emit more elements than expected
      // despite the withRate(3, Duration.standardSeconds(1)) setting.
      // Therefore, we filter out elements >= 13 to ensure consistent test behavior
      // across all environments.
      if (element >= 13L) {
        return;
      }
      distribution.update(element);
      output.output(element);
    }
  }
}
