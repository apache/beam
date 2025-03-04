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

import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.runners.spark.StreamingTest;
import org.apache.beam.runners.spark.TestSparkPipelineOptions;
import org.apache.beam.runners.spark.TestSparkRunner;
import org.apache.beam.runners.spark.UsesCheckpointRecovery;
import org.apache.beam.runners.spark.io.MicrobatchSource;
import org.apache.beam.runners.spark.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Optional;
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

  @Rule public transient TemporaryFolder temporaryFolder = new TemporaryFolder();
  public transient Pipeline p;

  /** Creates a temporary directory for storing checkpoints before each test execution. */
  @Before
  public void init() {
    try {
      temporaryFolder.create();
    } catch (IOException e) {
      throw new RuntimeException(e);
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

    PipelineResult res = run(Optional.of(new Instant(400)), false);

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
    res = runAgain();

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
  private PipelineResult runAgain() {
    return run(
        Optional.of(
            Instant.ofEpochMilli(
                Duration.standardSeconds(1L).plus(Duration.millis(50L)).getMillis())),
        true);
  }

  /**
   * Sets up and runs the test pipeline.
   *
   * @param stopWatermarkOption Watermark at which to stop the pipeline
   * @param deleteCheckpointDir Whether to delete checkpoint directory after completion
   */
  private PipelineResult run(Optional<Instant> stopWatermarkOption, boolean deleteCheckpointDir) {
    TestSparkPipelineOptions options =
        PipelineOptionsFactory.create().as(TestSparkPipelineOptions.class);
    options.setSparkMaster("local[*]");
    options.setRunner(TestSparkRunner.class);
    options.setCheckpointDir(temporaryFolder.getRoot().getPath());
    if (stopWatermarkOption.isPresent()) {
      options.setStopPipelineWatermark(stopWatermarkOption.get().getMillis());
    }
    options.setDeleteCheckpointDir(deleteCheckpointDir);

    p = Pipeline.create(options);

    final PCollection<Long> bounded =
        p.apply("Bounded", GenerateSequence.from(0).to(10))
            .apply("BoundedAssert", ParDo.of(new PAssertFn()));

    final PCollection<Long> unbounded =
        p.apply("Unbounded", GenerateSequence.from(10).withRate(3, Duration.standardSeconds(1)))
            .apply(WithTimestamps.of(e -> Instant.now()));

    final PCollection<Long> flattened = bounded.apply(Flatten.with(unbounded));

    flattened.apply("FlattenedAssert", ParDo.of(new PAssertFn()));
    return p.run();
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
    private final Distribution distribution = Metrics.distribution(PAssertFn.class, "distribution");

    @ProcessElement
    public void process(@Element Long element, OutputReceiver<Long> output) {
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
