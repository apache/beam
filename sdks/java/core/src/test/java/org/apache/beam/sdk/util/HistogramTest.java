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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Histogram}. */
@RunWith(JUnit4.class)
public class HistogramTest {

  @Test
  public void testLargerFrom() {
    assertThrows(RuntimeException.class, () -> Histogram.of(2.0, 1.0));
  }

  @Test
  public void testNegativeRange() {
    assertThrows(RuntimeException.class, () -> Histogram.of(-1.0, 1.0));
  }

  @Test
  public void testRecordOutOfRange() {
    Histogram histogram = Histogram.of(0, 100);
    assertThrows(RuntimeException.class, () -> histogram.record(100));
  }

  @Test
  public void testIgnoreOutOfRange() {
    Histogram histogram = Histogram.of(0, 100, 5, true);
    histogram.record(100);
  }

  @Test
  public void testCheckBoundaryBuckets() {
    Histogram histogram = Histogram.of(0, 100, 5, false);
    histogram.record(0);
    histogram.record(99.9);
    assertThat(histogram.getCount(0), equalTo(1L));
    assertThat(histogram.getCount(4), equalTo(1L));
  }

  @Test
  public void testFractionalBuckets() {
    Histogram histogram1 = Histogram.of(0, 10, 3, false);
    histogram1.record(3.33);
    histogram1.record(6.66);
    assertThat(histogram1.getCount(0), equalTo(1L));
    assertThat(histogram1.getCount(1), equalTo(1L));

    Histogram histogram2 = Histogram.of(0, 10, 3, false);
    histogram2.record(3.34);
    histogram2.record(6.67);
    assertThat(histogram2.getCount(1), equalTo(1L));
    assertThat(histogram2.getCount(2), equalTo(1L));
  }

  @Test
  public void testP50() {
    Histogram histogram1 = Histogram.of(0, 10);
    histogram1.record(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    assertThat(String.format("%.3f", histogram1.p50()), equalTo("4.200"));

    Histogram histogram2 = Histogram.of(0, 1);
    histogram2.record(0, 0, 0);
    assertThat(String.format("%.3f", histogram2.p50()), equalTo("0.010"));
  }

  @Test
  public void testP90() {
    Histogram histogram1 = Histogram.of(0, 10);
    histogram1.record(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    assertThat(String.format("%.3f", histogram1.p90()), equalTo("8.200"));

    Histogram histogram2 = Histogram.of(0, 1);
    histogram2.record(0, 0, 0);
    assertThat(String.format("%.3f", histogram2.p90()), equalTo("0.018"));
  }

  @Test
  public void testP99() {
    Histogram histogram1 = Histogram.of(0, 10);
    histogram1.record(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    assertThat(String.format("%.3f", histogram1.p99()), equalTo("9.180"));

    Histogram histogram2 = Histogram.of(0, 1);
    histogram2.record(0, 0, 0);
    assertThat(String.format("%.3f", histogram2.p99()), equalTo("0.020"));
  }

  @Test
  public void testEmptyP99() {
    Histogram histogram = Histogram.of(0, 10);
    assertThrows(RuntimeException.class, () -> histogram.p99());
  }

  @Test
  public void testClear() {
    Histogram histogram = Histogram.of(0, 10);
    histogram.record(1, 2, 3);
    assertThat(histogram.getTotalCount(), equalTo(3L));
    assertThat(histogram.getCount(5), equalTo(1L));
    histogram.clear();
    assertThat(histogram.getTotalCount(), equalTo(0L));
    assertThat(histogram.getCount(5), equalTo(0L));
  }
}
