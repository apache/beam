/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.util.common;

import static com.google.cloud.dataflow.sdk.util.Values.asBoolean;
import static com.google.cloud.dataflow.sdk.util.Values.asDouble;
import static com.google.cloud.dataflow.sdk.util.Values.asLong;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.AND;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.MAX;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.MEAN;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.MIN;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.OR;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.SUM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.cloud.dataflow.sdk.util.CloudCounterUtils;
import com.google.cloud.dataflow.sdk.util.common.Counter.CounterMean;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Unit tests for the {@link Counter} API.
 */
@RunWith(JUnit4.class)
public class CounterTest {

  private static MetricUpdate flush(Counter<?> c) {
    // TODO: Move this out into a separate Counter test.
    return CounterTestUtils.extractCounterUpdate(c, true);
  }

  private static final double EPSILON = 0.00000000001;

  @Test
  public void testNameKindAndCloudCounterRepresentation() {
    Counter<Long> c1 = Counter.longs("c1", SUM);
    Counter<Double> c2 = Counter.doubles("c2", MAX);
    Counter<Double> c3 = Counter.doubles("c3", MIN);
    Counter<Double> c4 = Counter.doubles("c4", MEAN);
    Counter<Integer> c5 = Counter.ints("c5", MIN);
    Counter<Boolean> c6 = Counter.booleans("c6", AND);
    Counter<Boolean> c7 = Counter.booleans("c7", OR);

    assertEquals("c1", c1.getName());
    assertEquals(SUM, c1.getKind());
    MetricUpdate cc = flush(c1);
    assertEquals("c1", cc.getName().getName());
    assertEquals("SUM", cc.getKind());
    assertEquals(0L, asLong(cc.getScalar()).longValue());
    c1.addValue(123L).addValue(-13L);
    cc = flush(c1);
    assertEquals(110L, asLong(cc.getScalar()).longValue());

    assertEquals("c2", c2.getName());
    assertEquals(MAX, c2.getKind());
    cc = flush(c2);
    assertEquals("c2", cc.getName().getName());
    assertEquals("MAX", cc.getKind());
    assertEquals(Double.NEGATIVE_INFINITY, asDouble(cc.getScalar()), EPSILON);
    c2.resetToValue(0.0).addValue(Math.PI).addValue(Math.E);
    cc = flush(c2);
    assertEquals(Math.PI, asDouble(cc.getScalar()), EPSILON);

    assertEquals("c3", c3.getName());
    assertEquals(MIN, c3.getKind());
    c3.addValue(Math.PI).addValue(-Math.PI).addValue(-Math.sqrt(2));
    cc = flush(c3);
    assertEquals("c3", cc.getName().getName());
    assertEquals("MIN", cc.getKind());
    assertEquals(-Math.PI, asDouble(cc.getScalar()), EPSILON);

    assertEquals("c4", c4.getName());
    assertEquals(MEAN, c4.getKind());
    cc = flush(c4); // zero-count means are not sent to the service
    assertEquals(null, cc);
    c4.addValue(Math.PI).addValue(Math.E).addValue(Math.sqrt(2));
    cc = flush(c4);
    assertEquals("c4", cc.getName().getName());
    assertEquals("MEAN", cc.getKind());
    Object ms = cc.getMeanSum();
    Object mc = cc.getMeanCount();
    assertEquals(Math.PI + Math.E + Math.sqrt(2), asDouble(ms), EPSILON);
    assertEquals(3, asLong(mc).longValue());
    c4.addValue(2.0).addValue(5.0);
    cc = flush(c4);
    ms = cc.getMeanSum();
    mc = cc.getMeanCount();
    assertEquals(7.0, asDouble(ms), EPSILON);
    assertEquals(2L, asLong(mc).longValue());

    assertEquals("c5", c5.getName());
    assertEquals(MIN, c5.getKind());
    cc = flush(c5);
    assertEquals("c5", cc.getName().getName());
    assertEquals("MIN", cc.getKind());
    assertEquals(Integer.MAX_VALUE, asLong(cc.getScalar()).longValue());
    c5.addValue(123).addValue(-13);
    cc = flush(c5);
    assertEquals(-13, asLong(cc.getScalar()).longValue());

    assertEquals("c6", c6.getName());
    assertEquals(AND, c6.getKind());
    cc = flush(c6);
    assertEquals("c6", cc.getName().getName());
    assertEquals("AND", cc.getKind());
    assertEquals(true, asBoolean(cc.getScalar()));
    c6.addValue(false);
    cc = flush(c6);
    assertEquals(false, asBoolean(cc.getScalar()));

    assertEquals("c7", c7.getName());
    assertEquals(OR, c7.getKind());
    cc = flush(c7);
    assertEquals("c7", cc.getName().getName());
    assertEquals("OR", cc.getKind());
    assertEquals(false, asBoolean(cc.getScalar()));
    c7.addValue(true);
    cc = flush(c7);
    assertEquals(true, asBoolean(cc.getScalar()));
  }

  @Test
  public void testCompatibility() {
    // Equal counters are compatible, of all kinds.
    assertTrue(
        Counter.longs("c", SUM).isCompatibleWith(Counter.longs("c", SUM)));
    assertTrue(
        Counter.ints("c", SUM).isCompatibleWith(Counter.ints("c", SUM)));
    assertTrue(
        Counter.doubles("c", SUM).isCompatibleWith(Counter.doubles("c", SUM)));
    assertTrue(
        Counter.booleans("c", OR).isCompatibleWith(
            Counter.booleans("c", OR)));

    // The name, kind, and type of the counter must match.
    assertFalse(
        Counter.longs("c", SUM).isCompatibleWith(Counter.longs("c2", SUM)));
    assertFalse(
        Counter.longs("c", SUM).isCompatibleWith(Counter.longs("c", MAX)));
    assertFalse(
        Counter.longs("c", SUM).isCompatibleWith(Counter.ints("c", SUM)));

    // The value of the counters are ignored.
    assertTrue(
        Counter.longs("c", SUM).resetToValue(666L).isCompatibleWith(
            Counter.longs("c", SUM).resetToValue(42L)));
  }


  private void assertOK(long total, long delta, Counter<Long> c) {
    assertEquals(total, c.getAggregate().longValue());
    assertEquals(delta, c.getAndResetDelta().longValue());
  }

  private void assertOK(double total, double delta, Counter<Double> c) {
    assertEquals(total, asDouble(c.getAggregate()), EPSILON);
    assertEquals(delta, asDouble(c.getAndResetDelta()), EPSILON);
  }


  // Tests for SUM.

  @Test
  public void testSumLong() {
    Counter<Long> c = Counter.longs("sum-long", SUM);
    long expectedTotal = 0;
    long expectedDelta = 0;
    assertOK(expectedTotal, expectedDelta, c);

    c.addValue(13L).addValue(42L).addValue(0L);
    expectedTotal += 55;
    expectedDelta += 55;
    assertOK(expectedTotal, expectedDelta, c);

    c.resetToValue(120L).addValue(17L).addValue(37L);
    expectedTotal = expectedDelta = 174;
    assertOK(expectedTotal, expectedDelta, c);

    flush(c);
    expectedDelta = 0;
    assertOK(expectedTotal, expectedDelta, c);

    c.addValue(15L).addValue(42L);
    expectedTotal += 57;
    expectedDelta += 57;
    assertOK(expectedTotal, expectedDelta, c);

    c.resetToValue(100L).addValue(17L).addValue(49L);
    expectedTotal = expectedDelta = 166;
    assertOK(expectedTotal, expectedDelta, c);
  }

  @Test
  public void testSumDouble() {
    Counter<Double> c = Counter.doubles("sum-double", SUM);
    double expectedTotal = 0.0;
    double expectedDelta = 0.0;
    assertOK(expectedTotal, expectedDelta, c);

    c.addValue(Math.E).addValue(Math.PI).addValue(0.0);
    expectedTotal += Math.E + Math.PI;
    expectedDelta += Math.E + Math.PI;
    assertOK(expectedTotal, expectedDelta, c);

    c.resetToValue(Math.sqrt(2)).addValue(2 * Math.PI).addValue(3 * Math.E);
    expectedTotal = expectedDelta = Math.sqrt(2) + 2 * Math.PI + 3 * Math.E;
    assertOK(expectedTotal, expectedDelta, c);

    flush(c);
    expectedDelta = 0.0;
    assertOK(expectedTotal, expectedDelta, c);

    c.addValue(7 * Math.PI).addValue(5 * Math.E);
    expectedTotal += 7 * Math.PI + 5 * Math.E;
    expectedDelta += 7 * Math.PI + 5 * Math.E;
    assertOK(expectedTotal, expectedDelta, c);

    c.resetToValue(Math.sqrt(17)).addValue(17.0).addValue(49.0);
    expectedTotal = expectedDelta = Math.sqrt(17.0) + 17.0 + 49.0;
    assertOK(expectedTotal, expectedDelta, c);
  }


  // Tests for MAX.

  @Test
  public void testMaxLong() {
    Counter<Long> c = Counter.longs("max-long", MAX);
    long expectedTotal = Long.MIN_VALUE;
    long expectedDelta = Long.MIN_VALUE;
    assertOK(expectedTotal, expectedDelta, c);

    c.addValue(13L).addValue(42L).addValue(Long.MIN_VALUE);
    expectedTotal = expectedDelta = 42;
    assertOK(expectedTotal, expectedDelta, c);

    c.resetToValue(120L).addValue(17L).addValue(37L);
    expectedTotal = expectedDelta = 120;
    assertOK(expectedTotal, expectedDelta, c);

    flush(c);
    expectedDelta = Long.MIN_VALUE;
    assertOK(expectedTotal, expectedDelta, c);

    c.addValue(42L).addValue(15L);
    expectedDelta = 42;
    assertOK(expectedTotal, expectedDelta, c);

    c.resetToValue(100L).addValue(171L).addValue(49L);
    expectedTotal = expectedDelta = 171;
    assertOK(expectedTotal, expectedDelta, c);
  }

  @Test
  public void testMaxDouble() {
    Counter<Double> c = Counter.doubles("max-double", MAX);
    double expectedTotal = Double.NEGATIVE_INFINITY;
    double expectedDelta = Double.NEGATIVE_INFINITY;
    assertOK(expectedTotal, expectedDelta, c);

    c.addValue(Math.E).addValue(Math.PI).addValue(Double.NEGATIVE_INFINITY);
    expectedTotal = expectedDelta = Math.PI;
    assertOK(expectedTotal, expectedDelta, c);

    c.resetToValue(Math.sqrt(12345)).addValue(2 * Math.PI).addValue(3 * Math.E);
    expectedTotal = expectedDelta = Math.sqrt(12345);
    assertOK(expectedTotal, expectedDelta, c);

    flush(c);
    expectedDelta = Double.NEGATIVE_INFINITY;
    assertOK(expectedTotal, expectedDelta, c);

    c.addValue(7 * Math.PI).addValue(5 * Math.E);
    expectedDelta = 7 * Math.PI;
    assertOK(expectedTotal, expectedDelta, c);

    c.resetToValue(Math.sqrt(17)).addValue(171.0).addValue(49.0);
    expectedTotal = expectedDelta = 171.0;
    assertOK(expectedTotal, expectedDelta, c);
  }


  // Tests for MIN.

  @Test
  public void testMinLong() {
    Counter<Long> c = Counter.longs("min-long", MIN);
    long expectedTotal = Long.MAX_VALUE;
    long expectedDelta = Long.MAX_VALUE;
    assertOK(expectedTotal, expectedDelta, c);

    c.addValue(13L).addValue(42L).addValue(Long.MAX_VALUE);
    expectedTotal = expectedDelta = 13;
    assertOK(expectedTotal, expectedDelta, c);

    c.resetToValue(120L).addValue(17L).addValue(37L);
    expectedTotal = expectedDelta = 17;
    assertOK(expectedTotal, expectedDelta, c);

    flush(c);
    expectedDelta = Long.MAX_VALUE;
    assertOK(expectedTotal, expectedDelta, c);

    c.addValue(42L).addValue(18L);
    expectedDelta = 18;
    assertOK(expectedTotal, expectedDelta, c);

    c.resetToValue(100L).addValue(171L).addValue(49L);
    expectedTotal = expectedDelta = 49;
    assertOK(expectedTotal, expectedDelta, c);
  }

  @Test
  public void testMinDouble() {
    Counter<Double> c = Counter.doubles("min-double", MIN);
    double expectedTotal = Double.POSITIVE_INFINITY;
    double expectedDelta = Double.POSITIVE_INFINITY;
    assertOK(expectedTotal, expectedDelta, c);

    c.addValue(Math.E).addValue(Math.PI).addValue(Double.POSITIVE_INFINITY);
    expectedTotal = expectedDelta = Math.E;
    assertOK(expectedTotal, expectedDelta, c);

    c.resetToValue(Math.sqrt(12345)).addValue(2 * Math.PI).addValue(3 * Math.E);
    expectedTotal = expectedDelta = 2 * Math.PI;
    assertOK(expectedTotal, expectedDelta, c);

    flush(c);
    expectedDelta = Double.POSITIVE_INFINITY;
    assertOK(expectedTotal, expectedDelta, c);

    c.addValue(7 * Math.PI).addValue(5 * Math.E);
    expectedDelta = 5 * Math.E;
    assertOK(expectedTotal, expectedDelta, c);

    c.resetToValue(Math.sqrt(17)).addValue(171.0).addValue(0.0);
    expectedTotal = expectedDelta = 0.0;
    assertOK(expectedTotal, expectedDelta, c);
  }


  // Tests for MEAN.

  private void assertMean(long s, long sd, long c, long cd, Counter<Long> cn) {
    CounterMean<Long> mean = cn.getMean();
    CounterMean<Long> deltaMean = cn.getAndResetMeanDelta();
    assertEquals(s, mean.getAggregate().longValue());
    assertEquals(sd, deltaMean.getAggregate().longValue());
    assertEquals(c, mean.getCount());
    assertEquals(cd, deltaMean.getCount());
  }

  private void assertMean(double s, double sd, long c, long cd,
      Counter<Double> cn) {
    CounterMean<Double> mean = cn.getMean();
    CounterMean<Double> deltaMean = cn.getAndResetMeanDelta();
    assertEquals(s, mean.getAggregate().doubleValue(), EPSILON);
    assertEquals(sd, deltaMean.getAggregate().doubleValue(), EPSILON);
    assertEquals(c, mean.getCount());
    assertEquals(cd, deltaMean.getCount());
  }

  @Test
  public void testMeanLong() {
    Counter<Long> c = Counter.longs("mean-long", MEAN);
    long expTotal = 0;
    long expDelta = 0;
    long expCountTotal = 0;
    long expCountDelta = 0;
    assertMean(expTotal, expDelta, expCountTotal, expCountDelta, c);

    c.addValue(13L).addValue(42L).addValue(0L);
    expTotal += 55;
    expDelta += 55;
    expCountTotal += 3;
    expCountDelta += 3;
    assertMean(expTotal, expDelta, expCountTotal, expCountDelta, c);

    c.resetMeanToValue(1L, 120L).addValue(17L).addValue(37L);
    expTotal = expDelta = 174;
    assertMean(expTotal, expDelta, expCountTotal, expCountDelta, c);

    flush(c);
    expDelta = 0;
    expCountDelta = 0;
    assertMean(expTotal, expDelta, expCountTotal, expCountDelta, c);

    c.addValue(15L).addValue(42L);
    expTotal += 57;
    expDelta += 57;
    expCountTotal += 2;
    expCountDelta += 2;
    assertMean(expTotal, expDelta, expCountTotal, expCountDelta, c);

    c.resetMeanToValue(3L, 100L).addValue(17L).addValue(49L);
    expTotal = expDelta = 166;
    expCountTotal = expCountDelta = 5;
    assertMean(expTotal, expDelta, expCountTotal, expCountDelta, c);
  }

  @Test
  public void testMeanDouble() {
    Counter<Double> c = Counter.doubles("mean-double", MEAN);
    double expTotal = 0.0;
    double expDelta = 0.0;
    long expCountTotal = 0;
    long expCountDelta = 0;
    assertMean(expTotal, expDelta, expCountTotal, expCountDelta, c);

    c.addValue(Math.E).addValue(Math.PI).addValue(0.0);
    expTotal += Math.E + Math.PI;
    expDelta += Math.E + Math.PI;
    expCountTotal += 3;
    expCountDelta += 3;
    assertMean(expTotal, expDelta, expCountTotal, expCountDelta, c);

    c.resetMeanToValue(1L, Math.sqrt(2)).addValue(2 * Math.PI)
        .addValue(3 * Math.E);
    expTotal = expDelta = Math.sqrt(2) + 2 * Math.PI + 3 * Math.E;
    assertMean(expTotal, expDelta, expCountTotal, expCountDelta, c);

    flush(c);
    expDelta = 0.0;
    expCountDelta = 0;
    assertMean(expTotal, expDelta, expCountTotal, expCountDelta, c);

    c.addValue(7 * Math.PI).addValue(5 * Math.E);
    expTotal += 7 * Math.PI + 5 * Math.E;
    expDelta += 7 * Math.PI + 5 * Math.E;
    expCountTotal += 2;
    expCountDelta += 2;
    assertMean(expTotal, expDelta, expCountTotal, expCountDelta, c);

    c.resetMeanToValue(3L, Math.sqrt(17)).addValue(17.0).addValue(49.0);
    expTotal = expDelta = Math.sqrt(17.0) + 17.0 + 49.0;
    expCountTotal = expCountDelta = 5;
    assertMean(expTotal, expDelta, expCountTotal, expCountDelta, c);
  }


  // Test for AND and OR.
  private void assertBool(boolean total, boolean delta, Counter<Boolean> c) {
    assertEquals(total, c.getAggregate().booleanValue());
    assertEquals(delta, c.getAndResetDelta().booleanValue());
  }

  @Test
  public void testBoolAnd() {
    Counter<Boolean> c = Counter.booleans("bool-and", AND);
    boolean expectedTotal = true;
    boolean expectedDelta = true;
    assertBool(expectedTotal, expectedDelta, c);

    c.addValue(true);
    assertBool(expectedTotal, expectedDelta, c);

    c.addValue(false);
    expectedTotal = expectedDelta = false;
    assertBool(expectedTotal, expectedDelta, c);

    c.resetToValue(true).addValue(true);
    expectedTotal = expectedDelta = true;
    assertBool(expectedTotal, expectedDelta, c);

    c.addValue(false);
    expectedTotal = expectedDelta = false;
    assertBool(expectedTotal, expectedDelta, c);

    flush(c);
    expectedDelta = true;
    assertBool(expectedTotal, expectedDelta, c);

    c.addValue(false);
    expectedDelta = false;
    assertBool(expectedTotal, expectedDelta, c);
  }

  @Test
  public void testBoolOr() {
    Counter<Boolean> c = Counter.booleans("bool-or", OR);
    boolean expectedTotal = false;
    boolean expectedDelta = false;
    assertBool(expectedTotal, expectedDelta, c);

    c.addValue(false);
    assertBool(expectedTotal, expectedDelta, c);

    c.addValue(true);
    expectedTotal = expectedDelta = true;
    assertBool(expectedTotal, expectedDelta, c);

    c.resetToValue(false).addValue(false);
    expectedTotal = expectedDelta = false;
    assertBool(expectedTotal, expectedDelta, c);

    c.addValue(true);
    expectedTotal = expectedDelta = true;
    assertBool(expectedTotal, expectedDelta, c);

    flush(c);
    expectedDelta = false;
    assertBool(expectedTotal, expectedDelta, c);

    c.addValue(true);
    expectedDelta = true;
    assertBool(expectedTotal, expectedDelta, c);
  }


  // Incompatibility tests.

  @Test(expected = IllegalArgumentException.class)
  public void testSumBool() {
    Counter.booleans("counter", SUM);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMinBool() {
    Counter.booleans("counter", MIN);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMaxBool() {
    Counter.booleans("counter", MAX);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMeanBool() {
    Counter.booleans("counter", MEAN);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAndLong() {
    Counter.longs("counter", AND);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAndDouble() {
    Counter.doubles("counter", AND);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testOrLong() {
    Counter.longs("counter", OR);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testOrDouble() {
    Counter.doubles("counter", OR);
  }

  @Test
  public void testExtraction() {
    Counter<?>[] counters = {Counter.longs("c1", SUM),
                             Counter.doubles("c2", MAX)};
    CounterSet set = new CounterSet();
    for (Counter<?> c : counters) {
      set.addCounter(c);
    }

    Set<MetricUpdate> cloudCountersFromSet = new HashSet<>(
        CloudCounterUtils.extractCounters(set, true));

    Set<MetricUpdate> cloudCountersFromArray =
        new HashSet<>(CounterTestUtils.extractCounterUpdates(Arrays.asList(counters), true));

    assertEquals(cloudCountersFromSet, cloudCountersFromArray);
    assertEquals(2, cloudCountersFromSet.size());
  }
}
