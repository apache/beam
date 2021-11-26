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
import static org.hamcrest.Matchers.equalTo;

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
}
