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

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.TestUtils;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests for the ApproximateUnique aggregator transform.
 */
@RunWith(JUnit4.class)
public class ApproximateUniqueTest implements Serializable {
  // implements Serializable just to make it easy to use anonymous inner OldDoFn subclasses

  @Test
  public void testEstimationErrorToSampleSize() {
    assertEquals(40000, ApproximateUnique.sampleSizeFromEstimationError(0.01));
    assertEquals(10000, ApproximateUnique.sampleSizeFromEstimationError(0.02));
    assertEquals(2500, ApproximateUnique.sampleSizeFromEstimationError(0.04));
    assertEquals(1600, ApproximateUnique.sampleSizeFromEstimationError(0.05));
    assertEquals(400, ApproximateUnique.sampleSizeFromEstimationError(0.1));
    assertEquals(100, ApproximateUnique.sampleSizeFromEstimationError(0.2));
    assertEquals(25, ApproximateUnique.sampleSizeFromEstimationError(0.4));
    assertEquals(16, ApproximateUnique.sampleSizeFromEstimationError(0.5));
  }

  @Test
  @Category(RunnableOnService.class)
  public void testApproximateUniqueWithSmallInput() {
    Pipeline p = TestPipeline.create();

    PCollection<Integer> input = p.apply(
        Create.of(Arrays.asList(1, 2, 3, 3)));

    PCollection<Long> estimate = input
        .apply(ApproximateUnique.<Integer>globally(1000));

    PAssert.thatSingleton(estimate).isEqualTo(3L);

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testApproximateUniqueWithDuplicates() {
    runApproximateUniqueWithDuplicates(100, 100, 100);
    runApproximateUniqueWithDuplicates(1000, 1000, 100);
    runApproximateUniqueWithDuplicates(1500, 1000, 100);
    runApproximateUniqueWithDuplicates(10000, 1000, 100);
  }

  private void runApproximateUniqueWithDuplicates(int elementCount,
      int uniqueCount, int sampleSize) {

    assert elementCount >= uniqueCount;
    List<Double> elements = Lists.newArrayList();
    for (int i = 0; i < elementCount; i++) {
      elements.add(1.0 / (i % uniqueCount + 1));
    }
    Collections.shuffle(elements);

    Pipeline p = TestPipeline.create();
    PCollection<Double> input = p.apply(Create.of(elements));
    PCollection<Long> estimate =
        input.apply(ApproximateUnique.<Double>globally(sampleSize));

    PAssert.thatSingleton(estimate).satisfies(new VerifyEstimateFn(uniqueCount, sampleSize));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testApproximateUniqueWithSkewedDistributions() {
    runApproximateUniqueWithSkewedDistributions(100, 100, 100);
    runApproximateUniqueWithSkewedDistributions(10000, 10000, 100);
    runApproximateUniqueWithSkewedDistributions(10000, 1000, 100);
    runApproximateUniqueWithSkewedDistributions(10000, 200, 100);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testApproximateUniqueWithSkewedDistributionsAndLargeSampleSize() {
    runApproximateUniqueWithSkewedDistributions(10000, 2000, 1000);
  }

  private void runApproximateUniqueWithSkewedDistributions(int elementCount,
      final int uniqueCount, final int sampleSize) {
    List<Integer> elements = Lists.newArrayList();
    // Zipf distribution with approximately elementCount items.
    double s = 1 - 1.0 * uniqueCount / elementCount;
    double maxCount = Math.pow(uniqueCount, s);
    for (int k = 0; k < uniqueCount; k++) {
      int count = Math.max(1, (int) Math.round(maxCount * Math.pow(k, -s)));
      // Element k occurs count times.
      for (int c = 0; c < count; c++) {
        elements.add(k);
      }
    }

    Pipeline p = TestPipeline.create();
    PCollection<Integer> input = p.apply(Create.of(elements));
    PCollection<Long> estimate =
        input.apply(ApproximateUnique.<Integer>globally(sampleSize));

    PAssert.thatSingleton(estimate).satisfies(new VerifyEstimateFn(uniqueCount, sampleSize));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testApproximateUniquePerKey() {
    List<KV<Long, Long>> elements = Lists.newArrayList();
    List<Long> keys = ImmutableList.of(20L, 50L, 100L);
    int elementCount = 1000;
    int sampleSize = 100;
    // Use the key as the number of unique values.
    for (long uniqueCount : keys) {
      for (long value = 0; value < elementCount; value++) {
        elements.add(KV.of(uniqueCount, value % uniqueCount));
      }
    }

    Pipeline p = TestPipeline.create();
    PCollection<KV<Long, Long>> input = p.apply(Create.of(elements));
    PCollection<KV<Long, Long>> counts =
        input.apply(ApproximateUnique.<Long, Long>perKey(sampleSize));

    PAssert.that(counts).satisfies(new VerifyEstimatePerKeyFn(sampleSize));

    p.run();

  }

  /**
   * Applies {@link ApproximateUnique} for different sample sizes and verifies
   * that the estimation error falls within the maximum allowed error of
   * {@code 2 / sqrt(sampleSize)}.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testApproximateUniqueWithDifferentSampleSizes() {
    runApproximateUniquePipeline(16);
    runApproximateUniquePipeline(64);
    runApproximateUniquePipeline(128);
    runApproximateUniquePipeline(256);
    runApproximateUniquePipeline(512);
    runApproximateUniquePipeline(1000);
    runApproximateUniquePipeline(1024);
    try {
      runApproximateUniquePipeline(15);
      fail("Accepted sampleSize < 16");
    } catch (IllegalArgumentException e) {
      assertTrue("Expected an exception due to sampleSize < 16", e.getMessage()
          .startsWith("ApproximateUnique needs a sampleSize >= 16"));
    }
  }

  @Test
  public void testApproximateUniqueGetName() {
    assertEquals("ApproximateUnique.PerKey", ApproximateUnique.<Long, Long>perKey(16).getName());
    assertEquals("ApproximateUnique.Globally", ApproximateUnique.<Integer>globally(16).getName());
  }

  /**
   * Applies {@code ApproximateUnique(sampleSize)} verifying that the estimation
   * error falls within the maximum allowed error of {@code 2/sqrt(sampleSize)}.
   */
  private static void runApproximateUniquePipeline(int sampleSize) {
    Pipeline p = TestPipeline.create();

    PCollection<String> input = p.apply(Create.of(TEST_LINES));
    PCollection<Long> approximate = input.apply(ApproximateUnique.<String>globally(sampleSize));
    final PCollectionView<Long> exact =
        input
            .apply(RemoveDuplicates.<String>create())
            .apply(Count.<String>globally())
            .apply(View.<Long>asSingleton());

    PCollection<KV<Long, Long>> approximateAndExact = approximate
        .apply(ParDo.of(new OldDoFn<Long, KV<Long, Long>>() {
              @Override
              public void processElement(ProcessContext c) {
                c.output(KV.of(c.element(), c.sideInput(exact)));
              }
            })
            .withSideInputs(exact));

    PAssert.that(approximateAndExact).satisfies(new VerifyEstimatePerKeyFn(sampleSize));

    p.run();
  }

  private static final int TEST_PAGES = 100;
  private static final List<String> TEST_LINES =
      new ArrayList<>(TEST_PAGES * TestUtils.LINES.size());

  static {
    for (int i = 0; i < TEST_PAGES; i++) {
      TEST_LINES.addAll(TestUtils.LINES);
    }
  }

  /**
   * Checks that the estimation error, i.e., the difference between
   * {@code uniqueCount} and {@code estimate} is less than
   * {@code 2 / sqrt(sampleSize}).
   */
  private static void verifyEstimate(long uniqueCount, int sampleSize, long estimate) {
    if (uniqueCount < sampleSize) {
      assertEquals("Number of hashes is less than the sample size. "
          + "Estimate should be exact", uniqueCount, estimate);
    }

    double error = 100.0 * Math.abs(estimate - uniqueCount) / uniqueCount;
    double maxError = 100.0 * 2 / Math.sqrt(sampleSize);

    assertTrue("Estimate= " + estimate + " Actual=" + uniqueCount + " Error="
        + error + "%, MaxError=" + maxError + "%.", error < maxError);

    assertTrue("Estimate= " + estimate + " Actual=" + uniqueCount + " Error="
        + error + "%, MaxError=" + maxError + "%.", error < maxError);
  }

  private static class VerifyEstimateFn implements SerializableFunction<Long, Void> {
    private long uniqueCount;
    private int sampleSize;

    public VerifyEstimateFn(long uniqueCount, int sampleSize) {
      this.uniqueCount = uniqueCount;
      this.sampleSize = sampleSize;
    }

    @Override
    public Void apply(Long estimate) {
      verifyEstimate(uniqueCount, sampleSize, estimate);
      return null;
    }
  }

  private static class VerifyEstimatePerKeyFn
      implements SerializableFunction<Iterable<KV<Long, Long>>, Void> {

    private int sampleSize;

    public VerifyEstimatePerKeyFn(int sampleSize) {
      this.sampleSize = sampleSize;
    }

    @Override
    public Void apply(Iterable<KV<Long, Long>> estimatePerKey) {
      for (KV<Long, Long> result : estimatePerKey) {
        verifyEstimate(result.getKey(), sampleSize, result.getValue());
      }
      return null;
    }
  }

  @Test
  public void testDisplayData() {
    ApproximateUnique.Globally<Integer> specifiedSampleSize = ApproximateUnique.globally(1234);
    ApproximateUnique.PerKey<String, Integer> specifiedMaxError = ApproximateUnique.perKey(0.1234);

    assertThat(DisplayData.from(specifiedSampleSize), hasDisplayItem("sampleSize", 1234));

    DisplayData maxErrorDisplayData = DisplayData.from(specifiedMaxError);
    assertThat(maxErrorDisplayData, hasDisplayItem("maximumEstimationError", 0.1234));
    assertThat("calculated sampleSize should be included", maxErrorDisplayData,
        hasDisplayItem("sampleSize"));
  }
}
