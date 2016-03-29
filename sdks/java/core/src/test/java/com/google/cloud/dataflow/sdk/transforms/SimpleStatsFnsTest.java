/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.transforms;

import static org.junit.Assert.assertEquals;

import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests of Min, Max, Mean, and Sum.
 */
@RunWith(JUnit4.class)
public class SimpleStatsFnsTest {
  static final double DOUBLE_COMPARISON_ACCURACY = 1e-7;

  private static class TestCase<NumT extends Number & Comparable<NumT>> {
    final List<NumT> data;
    final NumT min;
    final NumT max;
    final NumT sum;
    final Double mean;

    @SafeVarargs
    @SuppressWarnings("all")
    public TestCase(NumT min, NumT max, NumT sum, NumT... values) {
      this.data = Arrays.asList(values);
      this.min = min;
      this.max = max;
      this.sum = sum;
      this.mean =
          values.length == 0 ? Double.NaN : sum.doubleValue() / values.length;
    }
  }

  static final List<TestCase<Double>> DOUBLE_CASES = Arrays.asList(
      new TestCase<>(-312.31, 6312.31, 11629.13,
          -312.31, 29.13, 112.158, 6312.31, -312.158, -312.158, 112.158,
          -312.31, 6312.31, 0.0),
      new TestCase<>(3.14, 3.14, 3.14, 3.14),
      new TestCase<>(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0.0));

  static final List<TestCase<Long>> LONG_CASES = Arrays.asList(
      new TestCase<>(-50000000000000000L,
          70000000000000000L,
          60000033123213121L,
          0L, 1L, 10000000000000000L, -50000000000000000L,
          70000000000000000L, 0L, 10000000000000000L, -1L,
          -50000000000000000L, 70000000000000000L, 33123213121L),
      new TestCase<>(3L, 3L, 3L, 3L),
      new TestCase<>(Long.MAX_VALUE, Long.MIN_VALUE, 0L));

  static final List<TestCase<Integer>> INTEGER_CASES = Arrays.asList(
      new TestCase<>(-3, 6, 22,
          1, -3, 2, 6, 3, 4, -3, 5, 6, 1),
      new TestCase<>(3, 3, 3, 3),
      new TestCase<>(Integer.MAX_VALUE, Integer.MIN_VALUE, 0));

  @Test
  public void testInstantStats() {
    assertEquals(new Instant(1000), Min.MinFn.<Instant>naturalOrder().apply(
        Arrays.asList(new Instant(1000), new Instant(2000))));
    assertEquals(null, Min.MinFn.<Instant>naturalOrder().apply(
        Collections.<Instant>emptyList()));
    assertEquals(new Instant(5000), Min.MinFn.<Instant>naturalOrder(new Instant(5000)).apply(
        Collections.<Instant>emptyList()));

    assertEquals(new Instant(2000), Max.MaxFn.<Instant>naturalOrder().apply(
        Arrays.asList(new Instant(1000), new Instant(2000))));
    assertEquals(null, Max.MaxFn.<Instant>naturalOrder().apply(
        Collections.<Instant>emptyList()));
    assertEquals(new Instant(5000), Max.MaxFn.<Instant>naturalOrder(new Instant(5000)).apply(
        Collections.<Instant>emptyList()));
  }

  @Test
  public void testDoubleStats() {
    for (TestCase<Double> t : DOUBLE_CASES) {
      assertEquals(t.sum, new Sum.SumDoubleFn().apply(t.data),
          DOUBLE_COMPARISON_ACCURACY);
      assertEquals(t.min, new Min.MinDoubleFn().apply(t.data),
          DOUBLE_COMPARISON_ACCURACY);
      assertEquals(t.max, new Max.MaxDoubleFn().apply(t.data),
          DOUBLE_COMPARISON_ACCURACY);
      assertEquals(t.mean, new Mean.MeanFn<Double>().apply(t.data),
          DOUBLE_COMPARISON_ACCURACY);
    }
  }

  @Test
  public void testIntegerStats() {
    for (TestCase<Integer> t : INTEGER_CASES) {
      assertEquals(t.sum, new Sum.SumIntegerFn().apply(t.data));
      assertEquals(t.min, new Min.MinIntegerFn().apply(t.data));
      assertEquals(t.max, new Max.MaxIntegerFn().apply(t.data));
      assertEquals(t.mean, new Mean.MeanFn<Integer>().apply(t.data));
    }
  }

  @Test
  public void testLongStats() {
    for (TestCase<Long> t : LONG_CASES) {
      assertEquals(t.sum, new Sum.SumLongFn().apply(t.data));
      assertEquals(t.min, new Min.MinLongFn().apply(t.data));
      assertEquals(t.max, new Max.MaxLongFn().apply(t.data));
      assertEquals(t.mean, new Mean.MeanFn<Long>().apply(t.data));
    }
  }
}
