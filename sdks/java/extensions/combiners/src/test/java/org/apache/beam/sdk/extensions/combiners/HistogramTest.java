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
package org.apache.beam.sdk.extensions.combiners;

import static org.apache.beam.sdk.testing.CombineFnTester.testCombineFn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.combiners.Histogram.BoundsInclusivity;
import org.apache.beam.sdk.extensions.combiners.Histogram.BucketBounds;
import org.apache.beam.sdk.extensions.combiners.Histogram.HistogramAccumulator;
import org.apache.beam.sdk.extensions.combiners.Histogram.HistogramAccumulatorCoder;
import org.apache.beam.sdk.extensions.combiners.Histogram.HistogramCombineFn;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Doubles;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Longs;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/** Tests for {@link Histogram}. */
public class HistogramTest {

  @RunWith(JUnit4.class)
  @SuppressWarnings("unchecked")
  public static class HistogramTransformTest {

    private static final BucketBounds BUCKET_BOUNDS = BucketBounds.exponential(1.0, 2.0, 4);
    private static final int INPUT_SIZE = 20;

    static final List<KV<String, Double>> TABLE =
        Arrays.asList(
            KV.of("a", 1.0),
            KV.of("a", 2.0),
            KV.of("a", 3.0),
            KV.of("a", 5.0),
            KV.of("a", 4.5),
            KV.of("a", 10.0),
            KV.of("b", 1.0),
            KV.of("b", 5.0),
            KV.of("b", 5.0),
            KV.of("b", 20.0));

    @Rule public TestPipeline testPipeline = TestPipeline.create();

    @Test
    @Category(NeedsRunner.class)
    public void testHistogramGlobally() {
      PCollection<Number> input = generateInputPCollection(testPipeline, INPUT_SIZE);
      PCollection<List<Long>> counts = input.apply(Histogram.globally(BUCKET_BOUNDS));

      PAssert.that(counts).containsInAnyOrder(Longs.asList(1, 1, 2, 4, 8, 4));

      testPipeline.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testHistogramPerKey() {
      PCollection<KV<String, Double>> input =
          testPipeline.apply(
              Create.of(TABLE).withCoder(KvCoder.of(StringUtf8Coder.of(), DoubleCoder.of())));
      PCollection<KV<String, List<Long>>> counts = input.apply(Histogram.perKey(BUCKET_BOUNDS));

      PAssert.that(counts)
          .containsInAnyOrder(
              KV.of("a", Longs.asList(0, 1, 2, 2, 1, 0)),
              KV.of("b", Longs.asList(0, 1, 0, 2, 0, 1)));

      testPipeline.run();
    }

    private PCollection<Number> generateInputPCollection(Pipeline p, int size) {
      return p.apply(
          "CreateInputValuesUpTo(" + size + ")",
          Create.of(IntStream.range(0, size).boxed().collect(Collectors.toList())));
    }
  }

  /** Tests for {@link BucketBounds}. */
  @RunWith(JUnit4.class)
  public static class BucketBoundsTest {

    @Test
    public void testExponentialBucketBoundsWithoutSpecifiedInclusivity() {
      BucketBounds bucketBounds = BucketBounds.exponential(1.0, 2.0, 4);

      assertEquals(Arrays.asList(1.0, 2.0, 4.0, 8.0, 16.0), bucketBounds.getBounds());
      assertEquals(
          BoundsInclusivity.LOWER_BOUND_INCLUSIVE_UPPER_BOUND_EXCLUSIVE,
          bucketBounds.getBoundsInclusivity());
    }

    @Test
    public void testExponentialBucketBoundsThrowsIllegalArgumentExceptionForScale() {
      assertThrows(IllegalArgumentException.class, () -> BucketBounds.exponential(-0.4, 2.0, 4));
    }

    @Test
    public void testExponentialBucketBoundsThrowsIllegalArgumentExceptionForGrowthFactor() {
      assertThrows(IllegalArgumentException.class, () -> BucketBounds.exponential(1.0, 0.5, 4));
    }

    @Test
    public void
        testExponentialBucketBoundsThrowsIllegalArgumentExceptionForZeroNumberBoundedOfBuckets() {
      assertThrows(IllegalArgumentException.class, () -> BucketBounds.exponential(1.0, 2.0, 0));
    }

    @Test
    public void
        testExponentialBucketBoundsThrowsIllegalArgumentExceptionForMaxIntNumberBoundedOfBuckets() {
      assertThrows(
          IllegalArgumentException.class,
          () -> BucketBounds.exponential(1.0, 2.0, Integer.MAX_VALUE));
    }

    @Test
    public void testExponentialBucketBoundsThrowsIllegalArgumentExceptionForOverflownDoubleBound() {
      // As Double.MAX_VALUE is 1.797 * 10^308, we used 10^310 to overflow the max double type.
      assertThrows(IllegalArgumentException.class, () -> BucketBounds.exponential(1.0, 10.0, 310));
    }

    @Test
    public void testLinearBucketBoundsWithoutSpecifiedInclusivity() {
      BucketBounds bucketBounds = BucketBounds.linear(1.0, 2.0, 4);

      assertEquals(Arrays.asList(1.0, 3.0, 5.0, 7.0, 9.0), bucketBounds.getBounds());
      assertEquals(
          BoundsInclusivity.LOWER_BOUND_INCLUSIVE_UPPER_BOUND_EXCLUSIVE,
          bucketBounds.getBoundsInclusivity());
    }

    @Test
    public void testExplicitBucketBoundsWithoutSpecifiedInclusivity() {
      BucketBounds bucketBounds = BucketBounds.explicit(Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0));

      assertEquals(Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0), bucketBounds.getBounds());
      assertEquals(
          BoundsInclusivity.LOWER_BOUND_INCLUSIVE_UPPER_BOUND_EXCLUSIVE,
          bucketBounds.getBoundsInclusivity());
    }
  }

  /** Tests for {@link HistogramAccumulator}. */
  @RunWith(JUnit4.class)
  public static class HistogramAccumulatorTest {

    private static final int NUM_BUCKETS = 2;

    @Test
    public void testNumberOfBucketsThrowsException() {
      assertThrows(IllegalArgumentException.class, () -> new HistogramAccumulator(NUM_BUCKETS));
    }
  }

  /** Tests for {@link HistogramAccumulatorCoder}. */
  @RunWith(JUnit4.class)
  public static class HistogramAccumulatorCoderTest {

    private static final HistogramAccumulatorCoder CODER = new HistogramAccumulatorCoder();

    @Test
    public void testEncode_NullAccumulator_throwsNullPointerException() {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

      assertThrows(NullPointerException.class, () -> CODER.encode(null, outputStream));
    }
  }

  @RunWith(Parameterized.class)
  public static class HistogramAccumulatorCoderParameterizedTest {

    private static final int NUM_BOUNDED_BUCKETS = 160;
    private static final HistogramAccumulatorCoder CODER = new HistogramAccumulatorCoder();
    private static final HistogramCombineFn<Double> COMBINE_FN =
        HistogramCombineFn.create(BucketBounds.linear(1.0, 1.0, NUM_BOUNDED_BUCKETS));

    private final List<Double> inputList;

    public HistogramAccumulatorCoderParameterizedTest(List<Double> inputList) {
      this.inputList = inputList;
    }

    @Parameters
    public static Collection<Object[]> data() {
      List<Double> emptyInput = Doubles.asList();
      List<Double> oneInput = Doubles.asList(2.0);
      List<Double> theFirst10Elements = Doubles.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
      List<Double> theMiddle10Elements = Doubles.asList(51, 52, 53, 54, 55, 56, 57, 58, 59, 60);
      List<Double> theLast10Elements =
          Doubles.asList(151, 152, 153, 154, 155, 156, 157, 158, 159, 160);

      List<Double> theMiddle80Elements = new ArrayList<>(80);
      for (int i = 40; i < 120; i++) {
        theMiddle80Elements.add(i + 0.5);
      }

      List<Double> fullInput = new ArrayList<>(NUM_BOUNDED_BUCKETS + 2);
      for (int i = 0; i < NUM_BOUNDED_BUCKETS + 2; i++) {
        fullInput.add(i + 0.5);
      }

      return Arrays.asList(
          new Object[][] {
            {emptyInput},
            {oneInput},
            {theFirst10Elements},
            {theMiddle10Elements},
            {theLast10Elements},
            {theMiddle80Elements},
            {fullInput},
          });
    }

    @Test
    public void testAccumulatorCorrectlyEncodedAndDecoded() throws IOException {
      HistogramAccumulator initialAccumulator = COMBINE_FN.createAccumulator();
      for (Double input : inputList) {
        COMBINE_FN.addInput(initialAccumulator, input);
      }

      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      CODER.encode(initialAccumulator, outputStream);

      ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
      HistogramAccumulator decodedAccumulator = CODER.decode(inputStream);

      assertNotNull(decodedAccumulator);
      assertEquals(initialAccumulator, decodedAccumulator);
    }
  }

  /** Tests for {@link HistogramCombineFn}. */
  @RunWith(JUnit4.class)
  public static class HistogramCombineFnTest {

    private static final HistogramCombineFn<Double> histogramCombineFn =
        HistogramCombineFn.create(BucketBounds.linear(1.0, 2.0, 4));
    private static final HistogramAccumulator histogramAccumulator =
        histogramCombineFn.createAccumulator();

    @Test
    public void testAddInput_null_throwsNullPointerException() {
      assertThrows(
          NullPointerException.class,
          () -> histogramCombineFn.addInput(histogramAccumulator, null));
    }

    @Test
    public void testAddInput_nan_throwsIllegalArgumentException() {
      assertThrows(
          IllegalArgumentException.class,
          () -> histogramCombineFn.addInput(histogramAccumulator, Double.NaN));
    }

    @Test
    public void testAddInput_positiveInfinity_throwsIllegalArgumentException() {
      assertThrows(
          IllegalArgumentException.class,
          () -> histogramCombineFn.addInput(histogramAccumulator, Double.POSITIVE_INFINITY));
    }

    @Test
    public void testAddInput_negativeInfinity_throwsIllegalArgumentException() {
      assertThrows(
          IllegalArgumentException.class,
          () -> histogramCombineFn.addInput(histogramAccumulator, Double.NEGATIVE_INFINITY));
    }
  }

  @RunWith(Parameterized.class)
  public static class HistogramCombineFnParameterizedTest {

    private static final List<Double> BOUNDS = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0);

    private final List<Double> inputList;
    private final BoundsInclusivity boundsInclusivity;
    private final List<Long> expectedOutput;

    public HistogramCombineFnParameterizedTest(
        List<Double> inputList, BoundsInclusivity boundsInclusivity, List<Long> expectedOutput) {
      this.inputList = inputList;
      this.boundsInclusivity = boundsInclusivity;
      this.expectedOutput = expectedOutput;
    }

    @Parameters
    public static Collection<Object[]> data() {
      return Arrays.asList(
          new Object[][] {
            // Empty input - 0 for all bucket counts.
            {
              Doubles.asList(),
              BoundsInclusivity.LOWER_BOUND_INCLUSIVE_UPPER_BOUND_EXCLUSIVE,
              Longs.asList(0L, 0L, 0L, 0L, 0L, 0L)
            },
            // One input into the first bounded bucket - 1 in the second index only.
            {
              Doubles.asList(1.5),
              BoundsInclusivity.LOWER_BOUND_INCLUSIVE_UPPER_BOUND_EXCLUSIVE,
              Longs.asList(0L, 1L, 0L, 0L, 0L, 0L)
            },
            {
              Doubles.asList(1.5),
              BoundsInclusivity.LOWER_BOUND_EXCLUSIVE_UPPER_BOUND_INCLUSIVE,
              Longs.asList(0L, 1L, 0L, 0L, 0L, 0L)
            },
            // One input into the second bounded bucket - 1 in the third index only.
            {
              Doubles.asList(2.5),
              BoundsInclusivity.LOWER_BOUND_INCLUSIVE_UPPER_BOUND_EXCLUSIVE,
              Longs.asList(0L, 0L, 1L, 0L, 0L, 0L)
            },
            {
              Doubles.asList(2.5),
              BoundsInclusivity.LOWER_BOUND_EXCLUSIVE_UPPER_BOUND_INCLUSIVE,
              Longs.asList(0L, 0L, 1L, 0L, 0L, 0L)
            },
            // Two inputs into the first bounded bucket - 2 in the second index only.
            {
              Doubles.asList(1.5, 1.5),
              BoundsInclusivity.LOWER_BOUND_INCLUSIVE_UPPER_BOUND_EXCLUSIVE,
              Longs.asList(0L, 2L, 0L, 0L, 0L, 0L)
            },
            {
              Doubles.asList(1.5, 1.5),
              BoundsInclusivity.LOWER_BOUND_EXCLUSIVE_UPPER_BOUND_INCLUSIVE,
              Longs.asList(0L, 2L, 0L, 0L, 0L, 0L)
            },
            // Middle values of the bounds one for each bucket - 1s in all buckets.
            {
              Doubles.asList(0.5, 1.5, 2.5, 3.5, 4.5, 5.5),
              BoundsInclusivity.LOWER_BOUND_INCLUSIVE_UPPER_BOUND_EXCLUSIVE,
              Longs.asList(1L, 1L, 1L, 1L, 1L, 1L)
            },
            {
              Doubles.asList(0.5, 1.5, 2.5, 3.5, 4.5, 5.5),
              BoundsInclusivity.LOWER_BOUND_EXCLUSIVE_UPPER_BOUND_INCLUSIVE,
              Longs.asList(1L, 1L, 1L, 1L, 1L, 1L)
            },
            // Value on the bounds of buckets - all 1s except for first
            // and last (based on the bound inclusivity).
            {
              Doubles.asList(0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0),
              BoundsInclusivity.LOWER_BOUND_INCLUSIVE_UPPER_BOUND_EXCLUSIVE,
              Longs.asList(1L, 1L, 1L, 1L, 1L, 2L)
            },
            {
              Doubles.asList(0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0),
              BoundsInclusivity.LOWER_BOUND_EXCLUSIVE_UPPER_BOUND_INCLUSIVE,
              Longs.asList(2L, 1L, 1L, 1L, 1L, 1L)
            }
          });
    }

    @Test
    public void testBoundsInclusivity() {
      testCombineFn(
          HistogramCombineFn.create(BucketBounds.explicit(BOUNDS, boundsInclusivity)),
          inputList,
          expectedOutput);
    }
  }
}
