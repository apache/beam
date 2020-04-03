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
package org.apache.beam.runners.dataflow.worker.counters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import org.apache.beam.runners.dataflow.worker.NameContextsForTests;
import org.apache.beam.runners.dataflow.worker.counters.Counter.CommitState;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory.CounterDistribution;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory.CounterMean;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for the {@link Counter} API. */
@RunWith(JUnit4.class)
public class CounterTest {

  private final CounterName name = CounterName.named("undertest");
  private final CounterName name2 = CounterName.named("othername");

  private final CounterFactory counters = new CounterFactory();

  @Rule public ExpectedException thrown = ExpectedException.none();

  private static final double EPSILON = 0.00000000001;

  @Test
  public void testCompatibility() {
    // Equal counters are compatible, of all kinds.
    assertEquals(counters.longSum(name), counters.longSum(name));
    assertEquals(counters.intSum(name), counters.intSum(name));
    assertEquals(counters.doubleSum(name), counters.doubleSum(name));
    assertEquals(counters.booleanOr(name), counters.booleanOr(name));

    // The name, kind, and type of the counter must match.
    assertFalse(counters.longSum(name).equals(counters.longSum(name2)));
    assertFalse(counters.longSum(name).equals(counters.longMax(name)));
    assertFalse(counters.longSum(name).equals(counters.intSum(name)));

    // The value of the counters are ignored.
    assertEquals(counters.longSum(name).addValue(666L), counters.longSum(name).addValue(42L));
  }

  // Tests for SUM.

  @Test
  public void testSumLong() {
    Counter<Long, Long> c = counters.longSum(name);
    assertEquals(0L, (long) c.getAggregate());

    c.addValue(13L).addValue(42L).addValue(0L);
    assertEquals(13L + 42L, (long) c.getAggregate());

    c.getAndReset();
    c.addValue(120L).addValue(17L).addValue(37L);
    assertEquals(120L + 17L + 37L, (long) c.getAggregate());

    c.addValue(15L).addValue(42L);
    assertEquals(120L + 17L + 37L + 15L + 42L, (long) c.getAggregate());

    c.getAndReset();
    c.addValue(100L).addValue(17L).addValue(49L);
    assertEquals(100L + 17L + 49L, (long) c.getAggregate());

    assertEquals(
        "getAndReset should return previous value", 100L + 17L + 49L, (long) c.getAndReset());
    assertEquals("getAndReset should have reset value", 0, (long) c.getAggregate());
  }

  @Test
  public void testSumDouble() {
    Counter<Double, Double> c = counters.doubleSum(name);

    c.addValue(Math.E).addValue(Math.PI).addValue(0.0);
    assertEquals(Math.E + Math.PI, c.getAggregate(), EPSILON);

    c.getAndReset();
    c.addValue(Math.sqrt(2)).addValue(2 * Math.PI).addValue(3 * Math.E);
    assertEquals(Math.sqrt(2) + 2 * Math.PI + 3 * Math.E, c.getAggregate(), EPSILON);

    assertEquals(
        "getAndReset should return previous value",
        Math.sqrt(2) + 2 * Math.PI + 3 * Math.E,
        c.getAndReset(),
        EPSILON);
    assertEquals("getAndReset should have reset value", 0.0, c.getAggregate(), EPSILON);
  }

  // Tests for MAX.

  @Test
  public void testMaxLong() {
    Counter<Long, Long> c = counters.longMax(name);
    assertEquals(Long.MIN_VALUE, (long) c.getAggregate());

    c.addValue(13L).addValue(42L).addValue(0L);
    assertEquals(42L, (long) c.getAggregate());

    c.getAndReset();
    c.addValue(120L).addValue(17L).addValue(37L);
    assertEquals(120L, (long) c.getAggregate());

    c.addValue(15L).addValue(42L);
    assertEquals(120L, (long) c.getAggregate());

    c.addValue(137L);
    assertEquals(137L, (long) c.getAggregate());

    c.getAndReset();
    c.addValue(100L).addValue(17L).addValue(49L);
    assertEquals(100L, (long) c.getAggregate());

    assertEquals("getAndReset should return previous value", 100L, (long) c.getAndReset());
    assertEquals("getAndReset should have reset value", Long.MIN_VALUE, (long) c.getAggregate());
  }

  @Test
  public void testMaxDouble() {
    Counter<Double, Double> c = counters.doubleMax(name);
    assertEquals(Double.NEGATIVE_INFINITY, c.getAggregate(), EPSILON);

    c.addValue(Math.E).addValue(Math.PI).addValue(Double.NEGATIVE_INFINITY);
    assertEquals(Math.PI, c.getAggregate(), EPSILON);

    c.getAndReset();
    c.addValue(Math.sqrt(12345)).addValue(2 * Math.PI).addValue(3 * Math.E);
    assertEquals(Math.sqrt(12345), c.getAggregate(), EPSILON);

    assertEquals(
        "getAndReset should return previous value", Math.sqrt(12345), c.getAndReset(), EPSILON);
    assertEquals(Double.NEGATIVE_INFINITY, c.getAggregate(), EPSILON);

    c.addValue(7 * Math.PI).addValue(5 * Math.E);
    assertEquals(7 * Math.PI, c.getAggregate(), EPSILON);

    c.getAndReset();
    c.addValue(Math.sqrt(17)).addValue(171.0).addValue(49.0);
    assertEquals(171.0, c.getAggregate(), EPSILON);
  }

  // Tests for MIN.

  @Test
  public void testMinLong() {
    Counter<Long, Long> c = counters.longMin(name);
    assertEquals(Long.MAX_VALUE, (long) c.getAggregate());

    c.addValue(13L).addValue(42L).addValue(Long.MAX_VALUE);
    assertEquals(13L, (long) c.getAggregate());

    c.getAndReset();
    c.addValue(120L).addValue(17L).addValue(37L);
    assertEquals(17L, (long) c.getAggregate());

    assertEquals("getAndReset should return previous value", 17L, (long) c.getAndReset());
    assertEquals(
        "getAndReset should have reset the value", Long.MAX_VALUE, (long) c.getAggregate());

    c.addValue(42L).addValue(18L);
    assertEquals(18L, (long) c.getAggregate());
  }

  @Test
  public void testMinDouble() {
    Counter<Double, Double> c = counters.doubleMin(name);
    assertEquals(Double.POSITIVE_INFINITY, c.getAggregate(), EPSILON);

    c.addValue(Math.E).addValue(Math.PI).addValue(Double.POSITIVE_INFINITY);
    assertEquals(Math.E, c.getAggregate(), EPSILON);

    c.getAndReset();
    c.addValue(Math.sqrt(12345)).addValue(2 * Math.PI).addValue(3 * Math.E);
    assertEquals(2 * Math.PI, c.getAggregate(), EPSILON);

    assertEquals("getAndReset should return previous value", 2 * Math.PI, c.getAndReset(), EPSILON);
    assertEquals(
        "getAndReset should have reset the value",
        Double.POSITIVE_INFINITY,
        c.getAggregate(),
        EPSILON);

    c.getAndReset();
    c.addValue(Math.sqrt(17)).addValue(171.0).addValue(0.0);
    assertEquals(0.0, c.getAggregate(), EPSILON);
  }

  // Tests for MEAN.

  private void assertMean(long s, long c, Counter<Long, CounterMean<Long>> cn) {
    CounterMean<Long> mean = cn.getAggregate();
    assertEquals(s, mean.getAggregate().longValue());
    assertEquals(c, mean.getCount());
  }

  private void assertMean(double s, long c, Counter<Double, CounterMean<Double>> cn) {
    CounterMean<Double> mean = cn.getAggregate();
    assertEquals(s, mean.getAggregate().doubleValue(), EPSILON);
    assertEquals(c, mean.getCount());
  }

  @Test
  public void testMeanLong() {
    Counter<Long, CounterMean<Long>> c = counters.longMean(name);
    assertMean(0, 0, c);

    c.addValue(13L).addValue(42L).addValue(0L);
    assertMean(13 + 42 + 0, 3, c);

    c.getAndReset();
    c.addValue(120L).addValue(17L).addValue(37L);
    assertMean(120 + 17 + 37, 3, c);

    CounterMean<Long> mean = c.getAndReset();
    assertEquals(
        "getAndReset should return previous value", 120 + 17 + 37, (long) mean.getAggregate());
    assertEquals("getAndReset should return previous count", 3, mean.getCount());

    // getAndReset should reset the value
    assertMean(0, 0, c);

    c.getAndReset();
    c.addValue(33L).addValue(33L).addValue(34L).addValue(17L).addValue(49L);
    assertMean(166, 5, c);
  }

  @Test
  public void testMeanDouble() {
    Counter<Double, CounterMean<Double>> c = counters.doubleMean(name);
    double expTotal = 0.0;
    long expCountTotal = 0;
    assertMean(expTotal, expCountTotal, c);

    c.addValue(Math.E).addValue(Math.PI).addValue(0.0);
    expTotal += Math.E + Math.PI;
    expCountTotal += 3;
    assertMean(expTotal, expCountTotal, c);

    c.getAndReset();
    c.addValue(Math.sqrt(2)).addValue(2 * Math.PI).addValue(3 * Math.E);
    expTotal = Math.sqrt(2) + 2 * Math.PI + 3 * Math.E;
    assertMean(expTotal, expCountTotal, c);

    CounterMean<Double> mean = c.getAndReset();
    assertEquals(
        "getAndReset should return previous value",
        expTotal,
        (double) mean.getAggregate(),
        EPSILON);
    assertEquals("getAndReset should return previous count", expCountTotal, mean.getCount());

    assertMean(0, 0, c);

    c.getAndReset();
    c.addValue(Math.sqrt(17)).addValue(0.0).addValue(0.0).addValue(17.0).addValue(49.0);
    expTotal = Math.sqrt(17.0) + 17.0 + 49.0;
    expCountTotal = 5;
    assertMean(expTotal, expCountTotal, c);
  }

  @Test
  public void testDistribution() {
    Counter<Long, CounterDistribution> c = counters.distribution(name);

    CounterDistribution expected =
        CounterDistribution.builder()
            .minMax(Long.MAX_VALUE, 0L)
            .count(0L)
            .sum(0L)
            .sumOfSquares(0f)
            .buckets(0, new ArrayList<>())
            .build();
    assertEquals(expected, c.getAggregate());

    c.addValue(2L).addValue(10L).addValue(4L);

    expected =
        CounterDistribution.builder()
            .minMax(2L, 10L)
            .count(3)
            .sum(2L + 10L + 4L)
            .sumOfSquares(4L + 100L + 16L)
            .buckets(2, Lists.newArrayList(2L, 0L, 1L))
            .build();
    assertEquals(expected, c.getAggregate());

    c.getAndReset();
    c.addValue(0L).addValue(0L);

    expected =
        CounterDistribution.builder()
            .minMax(0L, 0L)
            .count(2L)
            .sum(0L)
            .sumOfSquares(0f)
            .buckets(0, Lists.newArrayList(2L))
            .build();
    assertEquals(expected, c.getAggregate());

    CounterDistribution distribution = c.getAndReset();
    assertEquals("getAndReset should return previous value", expected, distribution);

    expected =
        CounterDistribution.builder()
            .minMax(Long.MAX_VALUE, 0L)
            .count(0L)
            .sum(0L)
            .sumOfSquares(0f)
            .buckets(0, new ArrayList<>())
            .build();
    assertEquals(expected, c.getAggregate());
  }

  @Test
  public void testBoolAnd() {
    Counter<Boolean, Boolean> c = counters.booleanAnd(name);
    assertTrue(c.getAggregate());

    c.addValue(true);
    assertTrue(c.getAggregate());

    c.addValue(false);
    assertFalse(c.getAggregate());

    c.getAndReset();
    c.addValue(true).addValue(true);
    assertTrue(c.getAggregate());

    c.addValue(false);
    assertFalse(c.getAggregate());

    assertFalse(c.getAndReset());
    assertTrue(c.getAggregate());

    c.addValue(false);
    assertFalse(c.getAggregate());
  }

  @Test
  public void testBoolOr() {
    Counter<Boolean, Boolean> c = counters.booleanOr(name);
    assertFalse(c.getAggregate());

    c.addValue(false);
    assertFalse(c.getAggregate());

    c.addValue(true);
    assertTrue(c.getAggregate());

    c.getAndReset();
    c.addValue(false).addValue(false);
    assertFalse(c.getAggregate());

    c.addValue(true);
    assertTrue(c.getAggregate());

    assertTrue(c.getAndReset());
    assertFalse(c.getAggregate());

    c.addValue(true);
    assertTrue(c.getAggregate());
  }

  @Test
  public void testDirtyBit() {
    verifyDirtyBit(counters.longSum(CounterName.named("long-sum")), 1L);
    verifyDirtyBit(counters.longMean(CounterName.named("long-mean")), 1L);
    verifyDirtyBit(counters.doubleSum(CounterName.named("double-sum")), 1.0);
    verifyDirtyBit(counters.doubleMean(CounterName.named("double-mean")), 1.0);
    verifyDirtyBit(counters.intSum(CounterName.named("int-sum")), 1);
    verifyDirtyBit(counters.intMean(CounterName.named("int-mean")), 1);
    verifyDirtyBit(counters.booleanAnd(CounterName.named("and")), true);
  }

  /** Verify dirty bit is set correctly through various Counter state transitions */
  private <InputT> void verifyDirtyBit(Counter<InputT, ?> counter, InputT sampleValue) {
    String name = String.format("counter '%s'", counter.getName().name());

    // Test counters are not dirty and are COMMITTED initially.
    assertFalse(
        String.format("%s should not be dirty on initialization.", name), counter.isDirty());
    assertEquals(
        String.format("%s should not be COMMITTED on initialization.", name),
        CommitState.COMMITTED,
        counter.commitState.get());

    // Test counters are dirty after mutating.
    counter.addValue(sampleValue);
    assertTrue(String.format("%s should be dirty after mutating.", name), counter.isDirty());
    assertEquals(
        String.format("%s should have DIRTY state after mutating.", name),
        CommitState.DIRTY,
        counter.commitState.get());

    // Test counters are dirty and are COMMITTING.
    assertTrue(
        String.format("Committing %s should succeed when in DIRTY state.", name),
        counter.committing());
    assertTrue(String.format("%s should be dirty after committing.", name), counter.isDirty());
    assertEquals(
        String.format("%s should have COMMITTING state after mutating.", name),
        CommitState.COMMITTING,
        counter.commitState.get());

    // Test counters are dirty again after mutating.
    counter.addValue(sampleValue);
    assertFalse(
        String.format("Marking %s committed should succeed after mutating.", name),
        counter.committed());
    assertTrue(
        String.format("%s should be dirty after marking committed.", name), counter.isDirty());
    assertEquals(
        String.format("%s state should be DIRTY after marking committed.", name),
        CommitState.DIRTY,
        counter.commitState.get());

    // Test counters are not dirty and are COMMITTED.
    assertTrue(
        String.format("Committing %s should succeed when in DIRTY state.", name),
        counter.committing());
    assertTrue(
        String.format("Marking %s committed should succeed after committing.", name),
        counter.committed());
    assertFalse(
        String.format("%s should be dirty after being marked committed.", name), counter.isDirty());
    assertEquals(
        String.format("%s should have COMMITTED state after marking committed.", name),
        CommitState.COMMITTED,
        counter.commitState.get());
  }

  @Test
  public void testStructuredNames() {
    Counter<?, ?> unstructured = counters.intSum(name);
    Counter<?, ?> structuredOriginal =
        counters.intSum(name.withOriginalName(NameContextsForTests.nameContextForTest()));
    Counter<?, ?> structuredSystem =
        counters.intSum(name.withSystemName(NameContextsForTests.nameContextForTest()));
    Counter<?, ?> structuredCompatible =
        counters.intSum(name.withOriginalName(NameContextsForTests.nameContextForTest()));

    // unstructured is equal to nothing
    assertFalse(unstructured.equals(structuredOriginal));
    assertFalse(unstructured.equals(structuredSystem));
    assertFalse(unstructured.equals(structuredCompatible));

    // structuredOriginal is only equal to structuredCompatible
    assertEquals(structuredOriginal, structuredCompatible);
    assertFalse(structuredOriginal.equals(structuredSystem));

    // structuredSystem is equal to nothing
    assertFalse(structuredSystem.equals(structuredCompatible));
  }
}
