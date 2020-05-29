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
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.beam.sdk.TestUtils;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.testing.CombineFnTester;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ApproximateUnique.ApproximateUniqueCombineFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.hamcrest.Matcher;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;

/** Tests for the ApproximateUnique transform. */
public class ApproximateUniqueTest implements Serializable {
  // implements Serializable just to make it easy to use anonymous inner DoFn subclasses

  @Rule public final transient TestPipeline p = TestPipeline.create();
  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  private static class VerifyEstimateFn implements SerializableFunction<Long, Void> {
    private final long uniqueCount;
    private final int sampleSize;

    private VerifyEstimateFn(final long uniqueCount, final int sampleSize) {
      this.uniqueCount = uniqueCount;
      this.sampleSize = sampleSize;
    }

    @Override
    public Void apply(final Long estimate) {
      verifyEstimate(uniqueCount, sampleSize, estimate);
      return null;
    }
  }

  /**
   * Checks that the estimation error, i.e., the difference between {@code uniqueCount} and {@code
   * estimate} is less than {@code 2 / sqrt(sampleSize}).
   */
  private static void verifyEstimate(
      final long uniqueCount, final int sampleSize, final long estimate) {
    if (uniqueCount < sampleSize) {
      assertEquals(
          "Number of hashes is less than the sample size. " + "Estimate should be exact",
          uniqueCount,
          estimate);
    }

    final double error = 100.0 * Math.abs(estimate - uniqueCount) / uniqueCount;
    final double maxError = 100.0 * 2 / Math.sqrt(sampleSize);

    assertTrue(
        "Estimate="
            + estimate
            + " Actual="
            + uniqueCount
            + " Error="
            + error
            + "%, MaxError="
            + maxError
            + "%.",
        error < maxError);

    assertTrue(
        "Estimate="
            + estimate
            + " Actual="
            + uniqueCount
            + " Error="
            + error
            + "%, MaxError="
            + maxError
            + "%.",
        error < maxError);
  }

  private static Matcher<Long> estimateIsWithinRangeFor(
      final long uniqueCount, final int sampleSize) {
    if (uniqueCount <= sampleSize) {
      return is(uniqueCount);
    } else {
      long maxError = (long) Math.ceil(2.0 * uniqueCount / Math.sqrt(sampleSize));
      return both(lessThan(uniqueCount + maxError)).and(greaterThan(uniqueCount - maxError));
    }
  }

  private static class VerifyEstimatePerKeyFn
      implements SerializableFunction<Iterable<KV<Long, Long>>, Void> {

    private final int sampleSize;

    private VerifyEstimatePerKeyFn(final int sampleSize) {
      this.sampleSize = sampleSize;
    }

    @Override
    public Void apply(final Iterable<KV<Long, Long>> estimatePerKey) {
      for (final KV<Long, Long> result : estimatePerKey) {
        verifyEstimate(result.getKey(), sampleSize, result.getValue());
      }
      return null;
    }
  }

  /** Tests for ApproximateUnique with duplicates. */
  @RunWith(Parameterized.class)
  public static class ApproximateUniqueWithDuplicatesTest extends ApproximateUniqueTest {

    @Parameterized.Parameter public int elementCount;

    @Parameterized.Parameter(1)
    public int uniqueCount;

    @Parameterized.Parameter(2)
    public int sampleSize;

    @Parameterized.Parameters(name = "total_{0}_unique_{1}_sample_{2}")
    public static Iterable<Object[]> data() throws IOException {
      return ImmutableList.<Object[]>builder()
          .add(
              new Object[] {100, 100, 100},
              new Object[] {1000, 1000, 100},
              new Object[] {1500, 1000, 100},
              new Object[] {10000, 1000, 100})
          .build();
    }

    private void runApproximateUniqueWithDuplicates(
        final int elementCount, final int uniqueCount, final int sampleSize) {

      assert elementCount >= uniqueCount;
      final List<Double> elements = Lists.newArrayList();
      for (int i = 0; i < elementCount; i++) {
        elements.add(1.0 / (i % uniqueCount + 1));
      }
      Collections.shuffle(elements);

      final PCollection<Double> input = p.apply(Create.of(elements));
      final PCollection<Long> estimate = input.apply(ApproximateUnique.globally(sampleSize));

      PAssert.thatSingleton(estimate).satisfies(new VerifyEstimateFn(uniqueCount, sampleSize));

      p.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testApproximateUniqueWithDuplicates() {
      runApproximateUniqueWithDuplicates(elementCount, uniqueCount, sampleSize);
    }
  }

  /** Tests for ApproximateUnique with different sample sizes. */
  @RunWith(Parameterized.class)
  public static class ApproximateUniqueVariationsTest extends ApproximateUniqueTest {

    private static final int TEST_PAGES = 100;
    private static final List<String> TEST_LINES =
        new ArrayList<>(TEST_PAGES * TestUtils.LINES.size());

    static {
      for (int i = 0; i < TEST_PAGES; i++) {
        TEST_LINES.addAll(TestUtils.LINES);
      }
    }

    @Parameterized.Parameter public int sampleSize;

    @Parameterized.Parameters(name = "sampleSize_{0}")
    public static Iterable<Object[]> data() throws IOException {
      return ImmutableList.<Object[]>builder()
          .add(
              new Object[] {16},
              new Object[] {64},
              new Object[] {128},
              new Object[] {256},
              new Object[] {512},
              new Object[] {1000},
              new Object[] {2014},
              new Object[] {15})
          .build();
    }

    /**
     * Applies {@code ApproximateUnique(sampleSize)} verifying that the estimation error falls
     * within the maximum allowed error of {@code 2/sqrt(sampleSize)}.
     */
    private void runApproximateUniquePipeline(final int sampleSize) {
      final PCollection<String> input = p.apply(Create.of(TEST_LINES));
      final PCollection<Long> approximate = input.apply(ApproximateUnique.globally(sampleSize));
      final PCollectionView<Long> exact =
          input.apply(Distinct.create()).apply(Count.globally()).apply(View.asSingleton());

      final PCollection<KV<Long, Long>> approximateAndExact =
          approximate.apply(
              ParDo.of(
                      new DoFn<Long, KV<Long, Long>>() {

                        @ProcessElement
                        public void processElement(final ProcessContext c) {
                          c.output(KV.of(c.element(), c.sideInput(exact)));
                        }
                      })
                  .withSideInputs(exact));

      PAssert.that(approximateAndExact).satisfies(new VerifyEstimatePerKeyFn(sampleSize));

      p.run();
    }

    /**
     * Applies {@link ApproximateUnique} for different sample sizes and verifies that the estimation
     * error falls within the maximum allowed error of {@code 2 / sqrt(sampleSize)}.
     */
    @Test
    @Category(NeedsRunner.class)
    public void testApproximateUniqueWithDifferentSampleSizes() {
      if (sampleSize > 16) {
        runApproximateUniquePipeline(sampleSize);
      } else {
        try {
          p.enableAbandonedNodeEnforcement(false);
          runApproximateUniquePipeline(15);
          fail("Accepted sampleSize < 16");
        } catch (final IllegalArgumentException e) {
          assertTrue(
              "Expected an exception due to sampleSize < 16",
              e.getMessage().startsWith("ApproximateUnique needs a sampleSize >= 16"));
        }
      }
    }
  }

  /** Test ApproximateUniqueCombineFn. TestPipeline does seem to test merging partial results. */
  @RunWith(JUnit4.class)
  public static class ApproximateUniqueCombineFnTest {

    private void runCombineFnTest(long elementCount, long uniqueCount, int sampleSize) {
      List<Double> input =
          LongStream.range(0, elementCount)
              .mapToObj(i -> 1.0 / (i % uniqueCount + 1))
              .collect(Collectors.toList());

      CombineFnTester.testCombineFn(
          new ApproximateUniqueCombineFn<>(sampleSize, DoubleCoder.of()),
          input,
          estimateIsWithinRangeFor(uniqueCount, sampleSize));
    }

    @Test
    public void testFnWithSmallerFractionOfUniques() {
      runCombineFnTest(1000, 100, 16);
    }

    @Test
    public void testWithLargerFractionOfUniques() {
      runCombineFnTest(1000, 800, 100);
    }

    @Test
    public void testWithLargeSampleSize() {
      runCombineFnTest(200, 100, 150);
    }
  }

  /** Further tests for ApproximateUnique. */
  @RunWith(JUnit4.class)
  public static class ApproximateUniqueMiscTest extends ApproximateUniqueTest {

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
    @Category(NeedsRunner.class)
    public void testApproximateUniqueWithSmallInput() {
      final PCollection<Integer> input = p.apply(Create.of(Arrays.asList(1, 2, 3, 3)));

      final PCollection<Long> estimate = input.apply(ApproximateUnique.globally(1000));

      PAssert.thatSingleton(estimate).isEqualTo(3L);

      p.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testApproximateUniqueWithSkewedDistributionsAndLargeSampleSize() {
      runApproximateUniqueWithSkewedDistributions(10000, 2000, 1000);
    }

    private void runApproximateUniqueWithSkewedDistributions(
        final int elementCount, final int uniqueCount, final int sampleSize) {
      final List<Integer> elements = Lists.newArrayList();
      // Zipf distribution with approximately elementCount items.
      final double s = 1 - 1.0 * uniqueCount / elementCount;
      final double maxCount = Math.pow(uniqueCount, s);
      for (int k = 0; k < uniqueCount; k++) {
        final int count = Math.max(1, (int) Math.round(maxCount * Math.pow(k, -s)));
        // Element k occurs count times.
        for (int c = 0; c < count; c++) {
          elements.add(k);
        }
      }

      final PCollection<Integer> input = p.apply(Create.of(elements));
      final PCollection<Long> estimate = input.apply(ApproximateUnique.globally(sampleSize));

      PAssert.thatSingleton(estimate).satisfies(new VerifyEstimateFn(uniqueCount, sampleSize));

      p.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testApproximateUniquePerKey() {
      final List<KV<Long, Long>> elements = Lists.newArrayList();
      final List<Long> keys = ImmutableList.of(20L, 50L, 100L);
      final int elementCount = 1000;
      final int sampleSize = 100;
      // Use the key as the number of unique values.
      for (final long uniqueCount : keys) {
        for (long value = 0; value < elementCount; value++) {
          elements.add(KV.of(uniqueCount, value % uniqueCount));
        }
      }

      final PCollection<KV<Long, Long>> input = p.apply(Create.of(elements));
      final PCollection<KV<Long, Long>> counts = input.apply(ApproximateUnique.perKey(sampleSize));

      PAssert.that(counts).satisfies(new VerifyEstimatePerKeyFn(sampleSize));

      p.run();
    }

    @Test
    public void testApproximateUniqueGetName() {
      assertEquals("ApproximateUnique.PerKey", ApproximateUnique.<Long, Long>perKey(16).getName());
      assertEquals("ApproximateUnique.Globally", ApproximateUnique.<Integer>globally(16).getName());
    }

    @Test
    public void testDisplayData() {
      final ApproximateUnique.Globally<Integer> specifiedSampleSize =
          ApproximateUnique.globally(1234);
      final ApproximateUnique.PerKey<String, Integer> specifiedMaxError =
          ApproximateUnique.perKey(0.1234);

      assertThat(DisplayData.from(specifiedSampleSize), hasDisplayItem("sampleSize", 1234));

      final DisplayData maxErrorDisplayData = DisplayData.from(specifiedMaxError);
      assertThat(maxErrorDisplayData, hasDisplayItem("maximumEstimationError", 0.1234));
      assertThat(
          "calculated sampleSize should be included",
          maxErrorDisplayData,
          hasDisplayItem("sampleSize"));
    }

    @Test
    public void testGlobalWindowErrorMessageShows() {

      PCollection<Integer> input = p.apply(Create.of(1, 2, 3, 4));
      PCollection<Integer> windowed =
          input.apply(Window.into(FixedWindows.of(Duration.standardDays(1))));

      String expectedMsg =
          ApproximateUnique.combineFn(16, input.getCoder())
              .getFn()
              .getIncompatibleGlobalWindowErrorMessage();
      exceptionRule.expect(IllegalStateException.class);
      exceptionRule.expectMessage(expectedMsg);
      windowed.apply(ApproximateUnique.globally(16));
    }
  }
}
