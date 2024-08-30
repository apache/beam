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
package org.apache.beam.sdk.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.math.IntMath;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link HistogramData}. */
@RunWith(JUnit4.class)
public class HistogramDataTest {

  @Test
  public void testOutOfRangeWarning() {
    HistogramData histogramData = HistogramData.linear(0, 20, 5);
    histogramData.record(100);
    assertThat(histogramData.getTotalCount(), equalTo(1L));
  }

  @Test
  public void testCheckBoundaryBuckets() {
    HistogramData histogramData = HistogramData.linear(0, 20, 5);
    histogramData.record(0);
    histogramData.record(99.9);
    assertThat(histogramData.getCount(0), equalTo(1L));
    assertThat(histogramData.getCount(4), equalTo(1L));
  }

  @Test
  public void testFractionalBuckets() {
    HistogramData histogramData1 = HistogramData.linear(0, 10.0 / 3, 3);
    histogramData1.record(3.33);
    histogramData1.record(6.66);
    assertThat(histogramData1.getCount(0), equalTo(1L));
    assertThat(histogramData1.getCount(1), equalTo(1L));

    HistogramData histogramData2 = HistogramData.linear(0, 10.0 / 3, 3);
    histogramData2.record(3.34);
    histogramData2.record(6.67);
    assertThat(histogramData2.getCount(1), equalTo(1L));
    assertThat(histogramData2.getCount(2), equalTo(1L));
  }

  @Test
  public void testP50() {
    HistogramData histogramData1 = HistogramData.linear(0, 0.2, 50);
    histogramData1.record(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    assertThat(String.format("%.3f", histogramData1.p50()), equalTo("4.200"));

    HistogramData histogramData2 = HistogramData.linear(0, 0.02, 50);
    histogramData2.record(0, 0, 0);
    assertThat(String.format("%.3f", histogramData2.p50()), equalTo("0.010"));
  }

  @Test
  public void testP90() {
    HistogramData histogramData1 = HistogramData.linear(0, 0.2, 50);
    histogramData1.record(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    assertThat(String.format("%.3f", histogramData1.p90()), equalTo("8.200"));

    HistogramData histogramData2 = HistogramData.linear(0, 0.02, 50);
    histogramData2.record(0, 0, 0);
    assertThat(String.format("%.3f", histogramData2.p90()), equalTo("0.018"));
  }

  @Test
  public void testP99() {
    HistogramData histogramData1 = HistogramData.linear(0, 0.2, 50);
    histogramData1.record(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    assertThat(String.format("%.3f", histogramData1.p99()), equalTo("9.180"));

    HistogramData histogramData2 = HistogramData.linear(0, 0.02, 50);
    histogramData2.record(0, 0, 0);
    assertThat(String.format("%.3f", histogramData2.p99()), equalTo("0.020"));
  }

  @Test
  public void testP90Negative() {
    HistogramData histogramData1 = HistogramData.linear(-10, 0.2, 50);
    histogramData1.record(-1, -2, -3, -4, -5, -6, -7, -8, -9, -10);
    assertThat(String.format("%.3f", histogramData1.p90()), equalTo("-1.800"));

    HistogramData histogramData2 = HistogramData.linear(-1, 0.02, 50);
    histogramData2.record(-1, -1, -1);
    assertThat(String.format("%.3f", histogramData2.p90()), equalTo("-0.982"));
  }

  @Test
  public void testP90NegativeToPositive() {
    HistogramData histogramData1 = HistogramData.linear(-5, 0.2, 50);
    histogramData1.record(-1, -2, -3, -4, -5, 0, 1, 2, 3, 4);
    assertThat(String.format("%.3f", histogramData1.p90()), equalTo("3.200"));

    HistogramData histogramData2 = HistogramData.linear(-0.5, 0.02, 50);
    histogramData2.record(-0.5, -0.5, -0.5);
    assertThat(String.format("%.3f", histogramData2.p90()), equalTo("-0.482"));
  }

  @Test
  public void testP50NegativeInfinity() {
    HistogramData histogramData = HistogramData.linear(0, 0.2, 50);
    histogramData.record(-1, -2, -3, -4, -5, 0, 1, 2, 3, 4);
    assertThat(histogramData.p50(), equalTo(Double.NEGATIVE_INFINITY));
    assertThat(
        histogramData.getPercentileString("meows", "cats"),
        equalTo("Total number of meows: 10, P99: 4 cats, P90: 3 cats, P50: -Infinity cats"));
  }

  @Test
  public void testP50PositiveInfinity() {
    HistogramData histogramData = HistogramData.linear(0, 0.2, 50);
    histogramData.record(6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
    assertThat(histogramData.p50(), equalTo(Double.POSITIVE_INFINITY));
    assertThat(
        histogramData.getPercentileString("meows", "cats"),
        equalTo(
            "Total number of meows: 10, P99: Infinity cats, P90: Infinity cats, P50: Infinity cats"));
  }

  @Test
  public void testEmptyP99() {
    HistogramData histogramData = HistogramData.linear(0, 0.2, 50);
    assertThat(histogramData.p99(), equalTo(Double.NaN));
  }

  @Test
  public void testClear() {
    HistogramData histogramData = HistogramData.linear(0, 0.2, 50);
    histogramData.record(-1, 1, 2, 3);
    assertThat(histogramData.getTotalCount(), equalTo(4L));
    assertThat(histogramData.getCount(5), equalTo(1L));
    histogramData.clear();
    assertThat(histogramData.getTotalCount(), equalTo(0L));
    assertThat(histogramData.getCount(5), equalTo(0L));
  }

  @Test
  public void testUpdateUsingDoublesAndCumulative() {
    HistogramData data = HistogramData.linear(0, 2, 2);
    data.record(-1); // to -Inf bucket
    data.record(0); // bucket 0
    data.record(1);
    data.record(3); // bucket 1
    data.record(4); // to Inf bucket
    assertThat(data.getCount(0), equalTo(2L));
    assertThat(data.getCount(1), equalTo(1L));
    assertThat(data.getTotalCount(), equalTo(5L));

    // Now try updating it with another HistogramData
    HistogramData data2 = HistogramData.linear(0, 2, 2);
    data2.record(-1); // to -Inf bucket
    data2.record(-1); // to -Inf bucket
    data2.record(0); // bucket 0
    data2.record(0);
    data2.record(1);
    data2.record(1);
    data2.record(3); // bucket 1
    data2.record(3);
    data2.record(4); // to Inf bucket
    data2.record(4);
    assertThat(data2.getCount(0), equalTo(4L));
    assertThat(data2.getCount(1), equalTo(2L));
    assertThat(data2.getTotalCount(), equalTo(10L));

    data.update(data2);
    assertThat(data.getCount(0), equalTo(6L));
    assertThat(data.getCount(1), equalTo(3L));
    assertThat(data.getTotalCount(), equalTo(15L));
  }

  @Test
  public void testIncrementBucketCountByIndex() {
    HistogramData data = HistogramData.linear(0, 2, 2);
    data.incBottomBucketCount(1);
    data.incBucketCount(0, 2);
    data.incBucketCount(1, 3);
    data.incTopBucketCount(4);

    assertThat(data.getBottomBucketCount(), equalTo(1L));
    assertThat(data.getCount(0), equalTo(2L));
    assertThat(data.getCount(1), equalTo(3L));
    assertThat(data.getTopBucketCount(), equalTo(4L));
    assertThat(data.getTotalCount(), equalTo(10L));
  }

  // The following tests cover exponential buckets.
  @Test
  public void testExponentialBuckets_PositiveScaleRecord() {
    // Buckets will be:
    // Index        Range
    // Underflow    (-inf, 0)
    // 0            [0, sqrt(2))
    // 1            [sqrt(2), 2)
    // i            [2^(i/2), 2^((i+1)/2))
    HistogramData data = HistogramData.exponential(1, 40);

    data.record(-1);
    assertThat(data.getBottomBucketCount(), equalTo(1L));

    data.record(0, 1);
    assertThat(data.getCount(0), equalTo(2L));

    data.record(2);
    assertThat(data.getTotalCount(), equalTo(4L));
    assertThat(data.getCount(2), equalTo(1L));

    // 10th bucket contains range [2^5, 2^5.5) ~= [32, 45.25)
    for (int i = 32; i <= 45; i++) {
      data.record(i);
    }
    assertThat(data.getCount(10), equalTo(14L));

    // 30th bucket contains range [2^15, 2^15.5) ~= [32768, 46340.9)
    for (int i = 32768; i < 32768 + 100; i++) {
      data.record(i);
    }
    assertThat(data.getCount(30), equalTo(100L));
    for (int i = 46340; i > 46340 - 100; i--) {
      data.record(i);
    }
    assertThat(data.getCount(30), equalTo(200L));
  }

  @Test
  public void testExponentialBuckets_ZeroScaleRecord() {
    // Buckets will be:
    // Index        Range
    // Underflow    (-inf, 0)
    // 0            [0, 2)
    // 1            [2, 2^2]
    // i            [2^i, 2^(i+1))
    HistogramData data = HistogramData.exponential(0, 20);

    data.record(-1);
    assertThat(data.getBottomBucketCount(), equalTo(1L));

    data.record(0, 1);
    assertThat(data.getCount(0), equalTo(2L));

    data.record(4, 5, 6, 7);
    assertThat(data.getCount(2), equalTo(4L));

    for (int i = 32; i < 64; i++) {
      data.record(i);
    }
    assertThat(data.getCount(5), equalTo(32L));

    for (int i = IntMath.pow(2, 16); i < IntMath.pow(2, 16) + 100; i++) {
      data.record(i);
    }
    assertThat(data.getCount(16), equalTo(100L));

    Long expectedTotalCount = Long.valueOf(100 + 32 + 4 + 2 + 1);
    assertThat(data.getTotalCount(), equalTo(expectedTotalCount));
  }

  @Test
  public void testExponentialBuckets_NegativeScalesRecord() {
    // Buckets will be:
    // Index        Range
    // Underflow    (-inf, 0)
    // 0            [0, 4)
    // 1            [4, 4^2]
    // i            [4^i, 4^(i+1))
    HistogramData data = HistogramData.exponential(-1, 20);

    data.record(-1);
    assertThat(data.getBottomBucketCount(), equalTo(1L));

    data.record(0, 1, 2);
    assertThat(data.getCount(0), equalTo(3L));

    data.record(16, 17, 32, 33, 62, 63);
    assertThat(data.getCount(2), equalTo(6L));

    for (int i = IntMath.pow(4, 5); i < IntMath.pow(4, 5) + 20; i++) {
      data.record(i);
    }
    assertThat(data.getCount(5), equalTo(20L));

    Long expectedTotalCount = Long.valueOf(20 + 6 + 3 + 1);
    assertThat(data.getTotalCount(), equalTo(expectedTotalCount));
  }

  @Test
  public void testExponentialBuckets_BucketSize() {
    HistogramData zeroScaleBucket = HistogramData.exponential(0, 20);
    assertThat(zeroScaleBucket.getBucketType().getBucketSize(0), equalTo(2.0));
    // 10th bucket contains [2^10, 2^11).
    assertThat(zeroScaleBucket.getBucketType().getBucketSize(10), equalTo(1024.0));

    HistogramData positiveScaleBucket = HistogramData.exponential(1, 20);
    assertThat(positiveScaleBucket.getBucketType().getBucketSize(0), equalTo(Math.sqrt(2)));
    // 10th bucket contains [2^5, 2^5.5).
    assertThat(positiveScaleBucket.getBucketType().getBucketSize(10), closeTo(13.2, .1));

    HistogramData negativeScaleBucket = HistogramData.exponential(-1, 20);
    assertThat(negativeScaleBucket.getBucketType().getBucketSize(0), equalTo(4.0));
    // 10th bucket contains [2^20, 2^22).
    assertThat(negativeScaleBucket.getBucketType().getBucketSize(10), equalTo(3145728.0));
  }

  @Test
  public void testExponentialBuckets_NumBuckets() {
    // Validate that numBuckets clipping WAI.
    HistogramData zeroScaleBucket = HistogramData.exponential(0, 200);
    assertThat(zeroScaleBucket.getBucketType().getNumBuckets(), equalTo(32));

    HistogramData positiveScaleBucket = HistogramData.exponential(3, 500);
    assertThat(positiveScaleBucket.getBucketType().getNumBuckets(), equalTo(32 * 8));

    HistogramData negativeScaleBucket = HistogramData.exponential(-3, 500);
    assertThat(negativeScaleBucket.getBucketType().getNumBuckets(), equalTo(4));
  }

  @Test
  public void testStatistics_mean() {
    HistogramData histogram = HistogramData.linear(0, 10, 10);

    for (int i = 0; i < 10; i++) {
      histogram.record(i * 10.0);
    }

    assertThat(histogram.getMean(), equalTo(45.0));
  }

  @Test
  public void testStatistics_sumOfSquaredDeviations() {
    HistogramData histogram = HistogramData.linear(0, 10, 10);

    for (int i = 0; i < 10; i++) {
      histogram.record(i * 10.0);
    }

    assertThat(histogram.getSumOfSquaredDeviations(), equalTo(8250.0));
  }

  @Test
  public void testGetAndReset_resetSucceeds() {
    // Records values from [20, 50) in three buckets that have width 10.
    HistogramData originalHistogram = HistogramData.linear(20, 10, 3);
    originalHistogram.record(15.0, 25.0, 35.0, 45.0, 55.0);
    originalHistogram.getAndReset();

    HistogramData emptyHistogramData = HistogramData.linear(20, 10, 3);
    assertThat(originalHistogram, equalTo(emptyHistogramData));
    assertThat(originalHistogram.getMean(), equalTo(0.0));
    assertThat(originalHistogram.getSumOfSquaredDeviations(), equalTo(0.0));
    assertThat(originalHistogram.getTopBucketMean(), equalTo(0.0));
    assertThat(originalHistogram.getBottomBucketMean(), equalTo(0.0));
  }

  @Test
  public void testGetAndReset_getSucceeds() {
    // Records values from [20, 50) in three buckets that have width 10.
    HistogramData originalHistogram = HistogramData.linear(20, 10, 3);
    originalHistogram.record(15.0, 25.0, 35.0, 45.0, 55.0);
    HistogramData copyHistogram = originalHistogram.getAndReset();

    HistogramData duplicateHistogram = HistogramData.linear(20, 10, 3);
    duplicateHistogram.record(15.0, 25.0, 35.0, 45.0, 55.0);
    assertThat(copyHistogram, equalTo(duplicateHistogram));
    assertThat(copyHistogram.getBucketType(), equalTo(originalHistogram.getBucketType()));
    assertThat(copyHistogram.getMean(), equalTo(35.0));
    assertThat(copyHistogram.getSumOfSquaredDeviations(), equalTo(1000.0));
    assertThat(copyHistogram.getTopBucketMean(), equalTo(55.0));
    assertThat(copyHistogram.getBottomBucketMean(), equalTo(15.0));
  }

  @Test
  public void recordUnderflowValue() {
    // 'histogram' to record values from [0, 32). Values outside this range are
    // recorded in the overflow/underflow bin.
    HistogramData histogram = HistogramData.exponential(0, 5);

    assertThat(histogram.getTopBucketCount(), equalTo(0L));
    assertThat(histogram.getTopBucketMean(), equalTo(0.0));

    histogram.record(32);
    assertThat(histogram.getTopBucketCount(), equalTo(1L));
    assertThat(histogram.getTopBucketMean(), equalTo(32.0));

    histogram.record(40.0, 48.0, 56.0);
    assertThat(histogram.getTopBucketCount(), equalTo(4L));
    assertThat(histogram.getTopBucketMean(), equalTo(44.0));
  }

  @Test
  public void recordOverflowValue() {
    // 'histogram' to record values from [50, 150). Values outside this range are
    // recorded in the overflow/underflow bin.
    HistogramData histogram = HistogramData.linear(50, 10, 10);

    assertThat(histogram.getBottomBucketCount(), equalTo(0L));
    assertThat(histogram.getBottomBucketMean(), equalTo(0.0));

    histogram.record(-20);
    assertThat(histogram.getBottomBucketCount(), equalTo(1L));
    assertThat(histogram.getBottomBucketMean(), equalTo(-20.0));

    histogram.record(-30.0, -40.0);
    assertThat(histogram.getBottomBucketCount(), equalTo(3L));
    assertThat(histogram.getBottomBucketMean(), equalTo(-30.0));

    histogram.clear();
    histogram.record(25.0, 40.0);
    assertThat(histogram.getBottomBucketCount(), equalTo(2L));
    assertThat(histogram.getBottomBucketMean(), equalTo(32.5));
  }
}
