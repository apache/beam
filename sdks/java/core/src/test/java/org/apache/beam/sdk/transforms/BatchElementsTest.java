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
package org.apache.beam.sdk.transforms;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BatchElementsTest implements Serializable {

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Rule public transient Timeout globalTimeout = Timeout.seconds(120);

  // Helpers

  private static BatchElements.BatchConfig constantConfig(int size) {
    return BatchElements.BatchConfig.builder()
        .withMinBatchSize(size)
        .withMaxBatchSize(size)
        .withTargetBatchDurationSecs(10.0)
        .withTargetBatchOverhead(0.05)
        .withVariance(0.0)
        .build();
  }

  // BatchConfig validation

  @Test
  public void testBatchConfigDefaults() {
    BatchElements.BatchConfig config = BatchElements.BatchConfig.defaults();
    assertEquals(1, config.minBatchSize);
    assertEquals(10_000, config.maxBatchSize);
    assertEquals(0.05, config.targetBatchOverhead, 1e-9);
    assertEquals(10.0, config.targetBatchDurationSecs, 1e-9);
    assertEquals(0.25, config.variance, 1e-9);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBatchConfigMinGreaterThanMaxThrows() {
    BatchElements.BatchConfig.builder().withMinBatchSize(100).withMaxBatchSize(10).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBatchConfigZeroTargetDurationThrows() {
    BatchElements.BatchConfig.builder().withTargetBatchDurationSecs(0.0).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBatchConfigNegativeTargetDurationThrows() {
    BatchElements.BatchConfig.builder().withTargetBatchDurationSecs(-5.0).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBatchConfigZeroOverheadThrows() {
    BatchElements.BatchConfig.builder().withTargetBatchOverhead(0.0).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBatchConfigOverheadAboveOneThrows() {
    BatchElements.BatchConfig.builder().withTargetBatchOverhead(1.5).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBatchConfigNegativeFixedCostDurationThrows() {
    BatchElements.BatchConfig.builder().withTargetBatchDurationSecsWithFixedCost(-5.0).build();
  }

  //  BatchSizeEstimator unit tests

  @Test
  public void testEstimatorConstantBatchSize() {
    BatchElements.BatchConfig config = constantConfig(42);
    BatchElements.BatchSizeEstimator estimator = new BatchElements.BatchSizeEstimator(config);
    // When min == max, always return that size
    for (int i = 0; i < 10; i++) {
      assertEquals(42, estimator.nextBatchSize());
    }
  }

  @Test
  public void testEstimatorColdStartReturnsMinBatchSize() {
    BatchElements.BatchConfig config =
        BatchElements.BatchConfig.builder()
            .withMinBatchSize(5)
            .withMaxBatchSize(500)
            .withTargetBatchDurationSecs(10.0)
            .withTargetBatchOverhead(0.05)
            .build();
    BatchElements.BatchSizeEstimator estimator = new BatchElements.BatchSizeEstimator(config);
    // No timing data yet — should return minBatchSize
    assertEquals(5, estimator.nextBatchSize());
  }

  @Test
  public void testEstimatorGrowsAfterTimingData() {
    BatchElements.BatchConfig config =
        BatchElements.BatchConfig.builder()
            .withMinBatchSize(1)
            .withMaxBatchSize(500)
            .withTargetBatchDurationSecs(10.0)
            .withTargetBatchOverhead(0.05)
            .withVariance(0.0)
            .build();
    BatchElements.BatchSizeEstimator estimator = new BatchElements.BatchSizeEstimator(config);

    // Warm up with some fake recordings
    int size = estimator.nextBatchSize();
    try (BatchElements.BatchSizeEstimator.Stopwatch sw = estimator.recordTime(size)) {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    size = estimator.nextBatchSize();
    try (BatchElements.BatchSizeEstimator.Stopwatch sw = estimator.recordTime(size)) {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    int grown = estimator.nextBatchSize();
    assertTrue("Estimator should grow beyond minBatchSize after data, got: " + grown, grown > 1);
  }

  @Test
  public void testEstimatorNeverExceedsMaxBatchSize() {
    BatchElements.BatchConfig config =
        BatchElements.BatchConfig.builder()
            .withMinBatchSize(1)
            .withMaxBatchSize(10)
            .withTargetBatchDurationSecs(10.0)
            .withTargetBatchOverhead(0.05)
            .withVariance(0.0)
            .build();
    BatchElements.BatchSizeEstimator estimator = new BatchElements.BatchSizeEstimator(config);

    for (int i = 0; i < 50; i++) {
      int next = estimator.nextBatchSize();
      assertTrue("Batch size " + next + " exceeds max of 10", next <= 10);
      try (BatchElements.BatchSizeEstimator.Stopwatch sw = estimator.recordTime(next)) {
        Thread.sleep(10 + i);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testEstimatorNeverGoesBelowMinBatchSize() {
    BatchElements.BatchConfig config =
        BatchElements.BatchConfig.builder()
            .withMinBatchSize(7)
            .withMaxBatchSize(500)
            .withTargetBatchDurationSecs(10.0)
            .withTargetBatchOverhead(0.05)
            .build();
    BatchElements.BatchSizeEstimator estimator = new BatchElements.BatchSizeEstimator(config);

    for (int i = 0; i < 20; i++) {
      int next = estimator.nextBatchSize();
      assertTrue("Batch size " + next + " is below min of 7", next >= 7);
      try (BatchElements.BatchSizeEstimator.Stopwatch sw = estimator.recordTime(next)) {
        Thread.sleep(10 + i);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testIgnoreNextTimingReplaysBatchSize() {
    BatchElements.BatchConfig config =
        BatchElements.BatchConfig.builder()
            .withMinBatchSize(1)
            .withMaxBatchSize(500)
            .withTargetBatchDurationSecs(10.0)
            .withTargetBatchOverhead(0.05)
            .withVariance(0.0)
            .build();
    BatchElements.BatchSizeEstimator estimator = new BatchElements.BatchSizeEstimator(config);

    estimator.ignoreNextTiming();
    int first = estimator.nextBatchSize();

    // After ignoreNextTiming, the stopwatch will set replayLastBatchSize
    try (BatchElements.BatchSizeEstimator.Stopwatch sw = estimator.recordTime(first)) {}

    // Next call should replay the same size
    int replayed = estimator.nextBatchSize();
    assertEquals(
        "Expected replay of batch size " + first + " but got " + replayed, first, replayed);
  }

  // GlobalWindows pipeline tests
  @Test
  @Category(NeedsRunner.class)
  public void testConstantBatchInGlobalWindow() {
    // Mirrors Python: test_constant_batch
    // Runner bundle boundaries are not fixed, so partial batches may be emitted at bundle end.
    PCollection<Integer> output =
        pipeline
            .apply("Create", Create.of(range(35)))
            .apply("Batch", BatchElements.withConfig(constantConfig(10)))
            .apply(
                "Sizes",
                MapElements.via(
                    new SimpleFunction<List<Integer>, Integer>() {
                      @Override
                      public Integer apply(List<Integer> batch) {
                        return batch.size();
                      }
                    }));

    PAssert.that(output).satisfies(sizes -> assertBatchSizesWithinLimitAndTotal(sizes, 10, 35));
    pipeline.run().waitUntilFinish();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testPipelineRespectsMaxBatchSizeAndPreservesElements() {
    BatchElements.BatchConfig config =
        BatchElements.BatchConfig.builder()
            .withMinBatchSize(1)
            .withMaxBatchSize(15)
            .withTargetBatchDurationSecs(10.0)
            .withTargetBatchOverhead(0.05)
            .withVariance(0.0)
            .build();

    PCollection<Integer> sizes =
        pipeline
            .apply("Create", Create.of(range(200)))
            .apply("Batch", BatchElements.withConfig(config))
            .apply(
                "Sizes",
                MapElements.via(
                    new SimpleFunction<List<Integer>, Integer>() {
                      @Override
                      public Integer apply(List<Integer> input) {
                        return input.size();
                      }
                    }));

    PAssert.that(sizes)
        .satisfies(
            s -> {
              int total = 0;
              for (int size : s) {
                assertTrue("Batch size must be > 0", size > 0);
                assertTrue("Batch size " + size + " exceeded maxBatchSize 15", size <= 15);
                total += size;
              }
              assertEquals("All 200 elements must be present", 200, total);
              return null;
            });

    pipeline.run().waitUntilFinish();
  }

  // WindowAware pipeline tests

  @Test
  @Category(NeedsRunner.class)
  public void testWindowedBatches() {
    // 47 elements across FixedWindows(30s); runner bundle boundaries may split batches.
    List<TimestampedValue<Integer>> timestamped = new ArrayList<>();
    for (int i = 0; i < 47; i++) {
      timestamped.add(TimestampedValue.of(i, new Instant((long) i * 1000)));
    }

    PCollection<Integer> sizes =
        pipeline
            .apply("Create", Create.timestamped(timestamped))
            .apply(
                "Window",
                Window.<Integer>into(FixedWindows.of(Duration.standardSeconds(30)))
                    .withAllowedLateness(Duration.ZERO)
                    .discardingFiredPanes())
            .apply("Batch", BatchElements.withConfig(constantConfig(10)))
            .apply(
                "Sizes",
                MapElements.via(
                    new SimpleFunction<List<Integer>, Integer>() {
                      @Override
                      public Integer apply(List<Integer> input) {
                        return input.size();
                      }
                    }));

    PAssert.that(sizes).satisfies(s -> assertBatchSizesWithinLimitAndTotal(s, 10, 47));

    pipeline.run().waitUntilFinish();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testCrossWindowIsolation() {
    // Elements from different windows must NEVER appear in the same batch
    List<TimestampedValue<String>> elements = new ArrayList<>();
    for (int i = 0; i < 2000; i++) {
      // 4 windows of 2s each, 500 elements per window
      long ts = (long) (i / 500) * 2000L + (i % 500);
      elements.add(TimestampedValue.of("w" + (i / 500) + "-e" + i, new Instant(ts)));
    }

    PCollection<Boolean> isolationChecks =
        pipeline
            .apply("Create", Create.timestamped(elements))
            .apply(
                "Window",
                Window.<String>into(FixedWindows.of(Duration.standardSeconds(2)))
                    .withAllowedLateness(Duration.ZERO)
                    .discardingFiredPanes())
            .apply("Batch", BatchElements.withConfig(constantConfig(50)))
            .apply(
                "CheckIsolation",
                MapElements.via(
                    new SimpleFunction<List<String>, Boolean>() {
                      @Override
                      public Boolean apply(List<String> batch) {
                        // All elements in a batch must share the same window prefix
                        String firstWindow = batch.get(0).substring(0, 2);
                        for (String el : batch) {
                          if (!el.startsWith(firstWindow)) {
                            throw new AssertionError(
                                "Cross-window contamination: "
                                    + el
                                    + " in batch starting with "
                                    + firstWindow);
                          }
                        }
                        return true;
                      }
                    }));

    PAssert.that(isolationChecks)
        .satisfies(
            checks -> {
              int count = 0;
              for (boolean ok : checks) {
                assertTrue(ok);
                count++;
              }
              assertTrue("Expected at least one batch", count > 0);
              return null;
            });

    pipeline.run().waitUntilFinish();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWindowedBatchesPreserveAllElements() {
    // Total elements across all windows must equal input count
    int numElements = 500;
    List<TimestampedValue<Integer>> elements = new ArrayList<>();
    for (int i = 0; i < numElements; i++) {
      elements.add(TimestampedValue.of(i, new Instant((long) (i / 100) * 5000L + i)));
    }

    PCollection<Integer> sizes =
        pipeline
            .apply("Create", Create.timestamped(elements))
            .apply(
                "Window",
                Window.<Integer>into(FixedWindows.of(Duration.standardSeconds(5)))
                    .withAllowedLateness(Duration.ZERO)
                    .discardingFiredPanes())
            .apply("Batch", BatchElements.withConfig(constantConfig(30)))
            .apply(
                "Sizes",
                MapElements.via(
                    new SimpleFunction<List<Integer>, Integer>() {
                      @Override
                      public Integer apply(List<Integer> input) {
                        return input.size();
                      }
                    }));

    PAssert.that(sizes).satisfies(s -> assertBatchSizesWithinLimitAndTotal(s, 30, numElements));

    pipeline.run().waitUntilFinish();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWindowedBatchMaxSizeRespected() {
    int maxBatch = 20;
    List<TimestampedValue<Integer>> elements = new ArrayList<>();
    for (int i = 0; i < 300; i++) {
      elements.add(TimestampedValue.of(i, new Instant((long) (i / 150) * 3000L + i)));
    }

    BatchElements.BatchConfig config =
        BatchElements.BatchConfig.builder()
            .withMinBatchSize(1)
            .withMaxBatchSize(maxBatch)
            .withTargetBatchDurationSecs(10.0)
            .withTargetBatchOverhead(0.05)
            .withVariance(0.0)
            .build();

    PCollection<Integer> sizes =
        pipeline
            .apply("Create", Create.timestamped(elements))
            .apply(
                "Window",
                Window.<Integer>into(FixedWindows.of(Duration.standardSeconds(3)))
                    .withAllowedLateness(Duration.ZERO)
                    .discardingFiredPanes())
            .apply("Batch", BatchElements.withConfig(config))
            .apply(
                "Sizes",
                MapElements.via(
                    new SimpleFunction<List<Integer>, Integer>() {
                      @Override
                      public Integer apply(List<Integer> input) {
                        return input.size();
                      }
                    }));

    PAssert.that(sizes)
        .satisfies(
            s -> {
              int total = 0;
              for (int size : s) {
                assertTrue("Batch size must be > 0", size > 0);
                assertTrue("Batch size " + size + " exceeded max " + maxBatch, size <= maxBatch);
                total += size;
              }
              assertEquals("All 300 elements must be present", 300, total);
              return null;
            });

    pipeline.run().waitUntilFinish();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWindowRoutingGlobalWindowUsesGlobalDoFn() {
    // A plain Create (no windowing) should route through GlobalWindowsBatchingDoFn.
    // We validate this indirectly: output is correct and no windowing errors occur.
    PCollection<Integer> sizes =
        pipeline
            .apply("Create", Create.of(range(25)))
            .apply("Batch", BatchElements.withConfig(constantConfig(10)))
            .apply(
                "Sizes",
                MapElements.via(
                    new SimpleFunction<List<Integer>, Integer>() {
                      @Override
                      public Integer apply(List<Integer> input) {
                        return input.size();
                      }
                    }));

    PAssert.that(sizes).satisfies(s -> assertBatchSizesWithinLimitAndTotal(s, 10, 25));

    pipeline.run().waitUntilFinish();
  }

  // LinearRegression unit tests

  @Test
  public void testLinearRegressionPerfectFit() throws Exception {
    double[] ab = linearRegression(new double[] {1, 2, 3, 4, 5}, new double[] {3, 5, 7, 9, 11});

    assertEquals(1.0, ab[0], 1e-9);
    assertEquals(2.0, ab[1], 1e-9);
  }

  @Test
  public void testLinearRegressionRepeatedXsUsesMeanTimePerElement() throws Exception {
    double[] ab = linearRegression(new double[] {5, 5, 5, 5}, new double[] {10, 15, 20, 25});

    assertEquals(0.0, ab[0], 1e-9);
    assertEquals(3.5, ab[1], 1e-9);
  }

  // Utility

  private static Void assertBatchSizesWithinLimitAndTotal(
      Iterable<Integer> sizes, int maxBatchSize, int expectedTotal) {
    int total = 0;
    for (int size : sizes) {
      assertTrue("Batch size must be > 0", size > 0);
      assertTrue("Batch size " + size + " exceeded max " + maxBatchSize, size <= maxBatchSize);
      total += size;
    }
    assertEquals("All elements must be present", expectedTotal, total);
    return null;
  }

  private static double[] linearRegression(double[] xs, double[] ys) throws Exception {
    BatchElements.BatchSizeEstimator estimator =
        new BatchElements.BatchSizeEstimator(BatchElements.BatchConfig.defaults());
    Method method =
        BatchElements.BatchSizeEstimator.class.getDeclaredMethod(
            "linearRegression", double[].class, double[].class);
    method.setAccessible(true);
    return (double[]) method.invoke(estimator, xs, ys);
  }

  private static List<Integer> range(int n) {
    List<Integer> list = new ArrayList<>(n);
    for (int i = 0; i < n; i++) list.add(i);
    return list;
  }
}
