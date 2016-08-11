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
package org.apache.beam.sdk.util.common;

import static org.apache.beam.sdk.util.Values.asDouble;
import static org.apache.beam.sdk.util.common.Counter.AggregationKind.AND;
import static org.apache.beam.sdk.util.common.Counter.AggregationKind.MAX;
import static org.apache.beam.sdk.util.common.Counter.AggregationKind.MEAN;
import static org.apache.beam.sdk.util.common.Counter.AggregationKind.MIN;
import static org.apache.beam.sdk.util.common.Counter.AggregationKind.OR;
import static org.apache.beam.sdk.util.common.Counter.AggregationKind.SUM;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.util.common.Counter.CommitState;
import org.apache.beam.sdk.util.common.Counter.CounterMean;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for the {@link Counter} API.
 */
@RunWith(JUnit4.class)
public class CounterTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static void flush(Counter<?> c) {
    switch (c.getKind()) {
      case SUM:
      case MAX:
      case MIN:
      case AND:
      case OR:
        c.getAndResetDelta();
        break;
      case MEAN:
        c.getAndResetMeanDelta();
        break;
      default:
        throw new IllegalArgumentException("Unknown counter kind " + c.getKind());
    }
  }

  private static final double EPSILON = 0.00000000001;

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

    Counter<Long> other = Counter.longs("sum-long", SUM);
    other.addValue(12L);
    expectedDelta = 12L;
    expectedTotal += 12L;
    c.merge(other);
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

    Counter<Double> other = Counter.doubles("sum-double", SUM);
    other.addValue(12 * Math.PI);
    expectedDelta = 12 * Math.PI;
    expectedTotal += 12 * Math.PI;
    c.merge(other);
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

    Counter<Long> other = Counter.longs("max-long", MAX);
    other.addValue(12L);
    expectedDelta = 12L;
    c.merge(other);
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

    Counter<Double> other = Counter.doubles("max-double", MAX);
    other.addValue(12 * Math.PI);
    expectedDelta = 12 * Math.PI;
    c.merge(other);
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

    Counter<Long> other = Counter.longs("min-long", MIN);
    other.addValue(42L);
    expectedTotal = expectedDelta = 42L;
    c.merge(other);
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

    Counter<Double> other = Counter.doubles("min-double", MIN);
    other.addValue(42 * Math.E);
    expectedDelta = 42 * Math.E;
    c.merge(other);
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

    Counter<Long> other = Counter.longs("mean-long", MEAN);
    other.addValue(12L).addValue(44L).addValue(-5L);
    expTotal += 12L + 44L - 5L;
    expDelta += 12L + 44L - 5L;
    expCountTotal += 3;
    expCountDelta += 3;
    c.merge(other);
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

    Counter<Double> other = Counter.doubles("mean-double", MEAN);
    other.addValue(3 * Math.PI).addValue(12 * Math.E);
    expTotal += 3 * Math.PI + 12 * Math.E;
    expDelta += 3 * Math.PI + 12 * Math.E;
    expCountTotal += 2;
    expCountDelta += 2;
    c.merge(other);
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
  public void testMergeIncompatibleCounters() {
    Counter<Long> longSums = Counter.longs("longsums", SUM);
    Counter<Long> longMean = Counter.longs("longmean", MEAN);
    Counter<Long> longMin = Counter.longs("longmin", MIN);

    Counter<Long> otherLongSums = Counter.longs("othersums", SUM);
    Counter<Long> otherLongMean = Counter.longs("otherlongmean", MEAN);

    Counter<Double> doubleSums = Counter.doubles("doublesums", SUM);
    Counter<Double> doubleMean = Counter.doubles("doublemean", MEAN);

    Counter<Boolean> boolAnd = Counter.booleans("and", AND);
    Counter<Boolean> boolOr = Counter.booleans("or", OR);

    List<Counter<Long>> longCounters =
        Arrays.asList(longSums, longMean, longMin, otherLongSums, otherLongMean);
    for (Counter<Long> left : longCounters) {
      for (Counter<Long> right : longCounters) {
        if (left != right) {
          assertIncompatibleMerge(left, right);
        }
      }
    }

    assertIncompatibleMerge(doubleSums, doubleMean);
    assertIncompatibleMerge(boolAnd, boolOr);
  }

  private <T> void assertIncompatibleMerge(Counter<T> left, Counter<T> right) {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Counters");
    thrown.expectMessage("are incompatible");
    left.merge(right);
  }

  @Test
  public void testDirtyBit() {
    Counter<Long> longSum = Counter.longs("long-sum", SUM);
    Counter<Long> longMean = Counter.longs("long-mean", MEAN);
    Counter<Double> doubleSum = Counter.doubles("double-sum", SUM);
    Counter<Double> doubleMean = Counter.doubles("double-sum", MEAN);
    Counter<Integer> intSum = Counter.ints("int-sum", SUM);
    Counter<Integer> intMean = Counter.ints("int-sum", MEAN);
    Counter<Boolean> boolAnd = Counter.booleans("and", AND);

    // Test counters are not dirty and are COMMITTED initially.
    assertFalse(longSum.isDirty());
    assertFalse(longMean.isDirty());
    assertFalse(doubleSum.isDirty());
    assertFalse(doubleMean.isDirty());
    assertFalse(intSum.isDirty());
    assertFalse(intMean.isDirty());
    assertFalse(boolAnd.isDirty());

    assertEquals(CommitState.COMMITTED, longSum.commitState.get());
    assertEquals(CommitState.COMMITTED, longMean.commitState.get());
    assertEquals(CommitState.COMMITTED, doubleSum.commitState.get());
    assertEquals(CommitState.COMMITTED, doubleMean.commitState.get());
    assertEquals(CommitState.COMMITTED, intSum.commitState.get());
    assertEquals(CommitState.COMMITTED, intMean.commitState.get());
    assertEquals(CommitState.COMMITTED, boolAnd.commitState.get());

    // Test counters are dirty after mutating.
    longSum.addValue(1L);
    longMean.resetMeanToValue(1L, 1L);
    doubleSum.addValue(1.0);
    doubleMean.resetMeanToValue(1L, 1.0);
    intSum.addValue(1);
    intMean.resetMeanToValue(1, 1);
    boolAnd.addValue(true);

    assertTrue(longSum.isDirty());
    assertTrue(longMean.isDirty());
    assertTrue(doubleSum.isDirty());
    assertTrue(doubleMean.isDirty());
    assertTrue(intSum.isDirty());
    assertTrue(intMean.isDirty());
    assertTrue(boolAnd.isDirty());

    assertEquals(CommitState.DIRTY, longSum.commitState.get());
    assertEquals(CommitState.DIRTY, longMean.commitState.get());
    assertEquals(CommitState.DIRTY, doubleSum.commitState.get());
    assertEquals(CommitState.DIRTY, doubleMean.commitState.get());
    assertEquals(CommitState.DIRTY, intSum.commitState.get());
    assertEquals(CommitState.DIRTY, intMean.commitState.get());
    assertEquals(CommitState.DIRTY, boolAnd.commitState.get());

    // Test counters are dirty and are COMMITTING.
    assertTrue(longSum.committing());
    assertTrue(longMean.committing());
    assertTrue(doubleSum.committing());
    assertTrue(doubleMean.committing());
    assertTrue(intSum.committing());
    assertTrue(intMean.committing());
    assertTrue(boolAnd.committing());

    assertTrue(longSum.isDirty());
    assertTrue(longMean.isDirty());
    assertTrue(doubleSum.isDirty());
    assertTrue(doubleMean.isDirty());
    assertTrue(intSum.isDirty());
    assertTrue(intMean.isDirty());
    assertTrue(boolAnd.isDirty());

    assertEquals(CommitState.COMMITTING, longSum.commitState.get());
    assertEquals(CommitState.COMMITTING, longMean.commitState.get());
    assertEquals(CommitState.COMMITTING, doubleSum.commitState.get());
    assertEquals(CommitState.COMMITTING, doubleMean.commitState.get());
    assertEquals(CommitState.COMMITTING, intSum.commitState.get());
    assertEquals(CommitState.COMMITTING, intMean.commitState.get());
    assertEquals(CommitState.COMMITTING, boolAnd.commitState.get());

    // Test counters are dirty again after mutating.
    longSum.addValue(1L);
    longMean.resetMeanToValue(1L, 1L);
    doubleSum.addValue(1.0);
    doubleMean.resetMeanToValue(1L, 1.0);
    intSum.addValue(1);
    intMean.resetMeanToValue(1, 1);
    boolAnd.addValue(true);

    assertFalse(longSum.committed());
    assertFalse(longMean.committed());
    assertFalse(doubleSum.committed());
    assertFalse(doubleMean.committed());
    assertFalse(intSum.committed());
    assertFalse(intMean.committed());
    assertFalse(boolAnd.committed());

    assertTrue(longSum.isDirty());
    assertTrue(longMean.isDirty());
    assertTrue(doubleSum.isDirty());
    assertTrue(doubleMean.isDirty());
    assertTrue(intSum.isDirty());
    assertTrue(intMean.isDirty());
    assertTrue(boolAnd.isDirty());

    assertEquals(CommitState.DIRTY, longSum.commitState.get());
    assertEquals(CommitState.DIRTY, longMean.commitState.get());
    assertEquals(CommitState.DIRTY, doubleSum.commitState.get());
    assertEquals(CommitState.DIRTY, doubleMean.commitState.get());
    assertEquals(CommitState.DIRTY, intSum.commitState.get());
    assertEquals(CommitState.DIRTY, intMean.commitState.get());
    assertEquals(CommitState.DIRTY, boolAnd.commitState.get());

    // Test counters are not dirty and are COMMITTED.
    assertTrue(longSum.committing());
    assertTrue(longMean.committing());
    assertTrue(doubleSum.committing());
    assertTrue(doubleMean.committing());
    assertTrue(intSum.committing());
    assertTrue(intMean.committing());
    assertTrue(boolAnd.committing());

    assertTrue(longSum.committed());
    assertTrue(longMean.committed());
    assertTrue(doubleSum.committed());
    assertTrue(doubleMean.committed());
    assertTrue(intSum.committed());
    assertTrue(intMean.committed());
    assertTrue(boolAnd.committed());

    assertFalse(longSum.isDirty());
    assertFalse(longMean.isDirty());
    assertFalse(doubleSum.isDirty());
    assertFalse(doubleMean.isDirty());
    assertFalse(intSum.isDirty());
    assertFalse(intMean.isDirty());
    assertFalse(boolAnd.isDirty());

    assertEquals(CommitState.COMMITTED, longSum.commitState.get());
    assertEquals(CommitState.COMMITTED, longMean.commitState.get());
    assertEquals(CommitState.COMMITTED, doubleSum.commitState.get());
    assertEquals(CommitState.COMMITTED, doubleMean.commitState.get());
    assertEquals(CommitState.COMMITTED, intSum.commitState.get());
    assertEquals(CommitState.COMMITTED, intMean.commitState.get());
    assertEquals(CommitState.COMMITTED, boolAnd.commitState.get());
  }
}
