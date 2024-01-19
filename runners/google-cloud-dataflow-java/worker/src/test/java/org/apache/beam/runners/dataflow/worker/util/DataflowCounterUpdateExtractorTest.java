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
package org.apache.beam.runners.dataflow.worker.util;

import static org.apache.beam.runners.dataflow.worker.NameContextsForTests.nameContextForTest;
import static org.apache.beam.runners.dataflow.worker.util.CounterHamcrestMatchers.CounterKindMatcher.hasKind;
import static org.apache.beam.runners.dataflow.worker.util.CounterHamcrestMatchers.CounterNameMatcher.hasName;
import static org.apache.beam.runners.dataflow.worker.util.CounterHamcrestMatchers.CounterStructuredNameMatcher.hasStructuredName;
import static org.apache.beam.runners.dataflow.worker.util.CounterHamcrestMatchers.CounterUpdateBooleanValueMatcher.hasBooleanValue;
import static org.apache.beam.runners.dataflow.worker.util.CounterHamcrestMatchers.CounterUpdateDistributionMatcher.hasDistribution;
import static org.apache.beam.runners.dataflow.worker.util.CounterHamcrestMatchers.CounterUpdateDoubleCountMatcher.hasDoubleCount;
import static org.apache.beam.runners.dataflow.worker.util.CounterHamcrestMatchers.CounterUpdateDoubleSumMatcher.hasDoubleSum;
import static org.apache.beam.runners.dataflow.worker.util.CounterHamcrestMatchers.CounterUpdateDoubleValueMatcher.hasDoubleValue;
import static org.apache.beam.runners.dataflow.worker.util.CounterHamcrestMatchers.CounterUpdateIntegerCountMatcher.hasIntegerCount;
import static org.apache.beam.runners.dataflow.worker.util.CounterHamcrestMatchers.CounterUpdateIntegerSumMatcher.hasIntegerSum;
import static org.apache.beam.runners.dataflow.worker.util.CounterHamcrestMatchers.CounterUpdateIntegerValueMatcher.hasIntegerValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;

import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.SplitInt64;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleReadCounter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DataflowCounterUpdateExtractor}. */
@RunWith(JUnit4.class)
public class DataflowCounterUpdateExtractorTest {

  private static final String COUNTER_NAME = "CounterName";
  private final CounterFactory counterFactory = new CounterFactory();

  @Test
  public void testNameKindAndCloudCounterRepresentation() {
    Counter<Long, ?> c1 = counterFactory.longSum(CounterName.named("c1"));
    Counter<Double, ?> c2 = counterFactory.doubleMax(CounterName.named("c2"));
    Counter<Double, ?> c3 = counterFactory.doubleMin(CounterName.named("c3"));
    Counter<Double, ?> c4 = counterFactory.doubleMean(CounterName.named("c4"));
    Counter<Integer, ?> c5 = counterFactory.intMin(CounterName.named("c5"));
    Counter<Boolean, ?> c6 = counterFactory.booleanAnd(CounterName.named("c6"));
    Counter<Boolean, ?> c7 = counterFactory.booleanOr(CounterName.named("c7"));
    Counter<Integer, ?> c8 = counterFactory.intMean(CounterName.named("c8"));
    Counter<Long, ?> c9 = counterFactory.distribution(CounterName.named("c9"));

    assertThat(
        c1.extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE),
        allOf(hasName("c1"), hasKind("SUM"), hasIntegerValue(0)));
    c1.addValue(123L).addValue(-13L);
    assertThat(
        c1.extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE), hasIntegerValue(110));

    assertThat(
        c2.extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE),
        allOf(hasName("c2"), hasKind("MAX"), hasDoubleValue(Double.NEGATIVE_INFINITY)));
    c2.getAndReset();
    c2.addValue(Math.PI).addValue(Math.E);
    assertThat(
        c2.extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE), hasDoubleValue(Math.PI));

    c3.addValue(Math.PI).addValue(-Math.PI).addValue(-Math.sqrt(2));
    assertThat(
        c3.extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE),
        allOf(hasName("c3"), hasKind("MIN"), hasDoubleValue(-Math.PI)));

    // zero-count means are not sent to the service
    assertThat(c4.extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE), is(nullValue()));
    c4.addValue(Math.PI).addValue(Math.E).addValue(Math.sqrt(2));
    assertThat(
        c4.extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE),
        allOf(
            hasName("c4"), hasKind("MEAN"),
            hasDoubleSum(Math.PI + Math.E + Math.sqrt(2)), hasDoubleCount(3)));
    c4.addValue(2.0).addValue(5.0);
    assertThat(
        c4.extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE),
        allOf(hasDoubleSum(7.0), hasDoubleCount(2)));

    assertThat(
        c5.extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE),
        allOf(hasName("c5"), hasKind("MIN"), hasIntegerValue(Integer.MAX_VALUE)));
    c5.addValue(123).addValue(-13);
    assertThat(
        c5.extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE), hasIntegerValue(-13));

    assertThat(
        c6.extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE),
        allOf(hasName("c6"), hasKind("AND"), hasBooleanValue(true)));
    c6.addValue(false);
    assertThat(
        c6.extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE), hasBooleanValue(false));

    assertThat(
        c7.extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE),
        allOf(hasName("c7"), hasKind("OR"), hasBooleanValue(false)));
    c7.addValue(true);
    assertThat(
        c7.extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE), hasBooleanValue(true));

    c8.addValue(1).addValue(2).addValue(3).addValue(4).addValue(5);
    assertThat(
        c8.extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE),
        allOf(hasName("c8"), hasKind("MEAN"), hasIntegerSum(15), hasIntegerCount(5)));

    c9.addValue(1L).addValue(0L).addValue(1L).addValue(9L).addValue(19L);
    assertThat(
        c9.extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE),
        allOf(
            hasName("c9"),
            hasKind("DISTRIBUTION"),
            hasDistribution(
                CounterFactory.CounterDistribution.builder()
                    .minMax(0L, 19L)
                    .count(5L)
                    .sum(30L)
                    .sumOfSquares(444f)
                    .firstBucketOffset(0)
                    .buckets(Lists.newArrayList(1L, 2L, 0L, 1L, 1L))
                    .build())));
  }

  private SplitInt64 createSplitInt(int highBits, long lowBits) {
    SplitInt64 splitInt = new SplitInt64();
    // low bits 0 high bits negative
    splitInt.setHighBits(highBits);
    splitInt.setLowBits(lowBits);
    return splitInt;
  }

  @Test
  public void testSplitIntToLong() {
    // high bits negative max, low bits 0
    assertEquals(
        0x80000000_00000000L,
        DataflowCounterUpdateExtractor.splitIntToLong(createSplitInt(Integer.MIN_VALUE, 0L)));
    // high bits 0, low bits 0
    assertEquals(
        0x00000000_00000000L, DataflowCounterUpdateExtractor.splitIntToLong(createSplitInt(0, 0L)));
    // high bits positive max, low bits 0
    assertEquals(
        0x7fffffff_00000000L,
        DataflowCounterUpdateExtractor.splitIntToLong(createSplitInt(Integer.MAX_VALUE, 0L)));

    // high bits negative, low bits max
    assertEquals(
        0x80000000_ffffffffL,
        DataflowCounterUpdateExtractor.splitIntToLong(
            createSplitInt(Integer.MIN_VALUE, 0xffffffffL)));
    // high bits 0, low bits max
    assertEquals(
        0x00000000_ffffffffL,
        DataflowCounterUpdateExtractor.splitIntToLong(createSplitInt(0, 0xffffffffL)));
    // high bits positive, low bits max
    assertEquals(
        0x7fffffff_ffffffffL,
        DataflowCounterUpdateExtractor.splitIntToLong(
            createSplitInt(Integer.MAX_VALUE, 0xffffffffL)));
  }

  @Test
  public void testCloudCounterRepresentationCaseNegative() {
    Counter<Double, ?> c1 = counterFactory.doubleSum(CounterName.named("c1"));
    Counter<Integer, ?> c2 = counterFactory.intSum(CounterName.named("c2"));
    Counter<Double, ?> c3 = counterFactory.doubleMean(CounterName.named("c3"));
    Counter<Integer, ?> c4 = counterFactory.intMean(CounterName.named("c4"));
    Counter<Double, ?> c5 = counterFactory.doubleMin(CounterName.named("c5"));
    Counter<Integer, ?> c6 = counterFactory.intMin(CounterName.named("c6"));
    Counter<Double, ?> c7 = counterFactory.doubleMax(CounterName.named("c7"));
    Counter<Integer, ?> c8 = counterFactory.intMax(CounterName.named("c8"));

    c1.addValue(-1D).addValue(-1D);
    assertThat(
        ((Counter<?, ?>) c1).extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE),
        hasDoubleValue(-2D));

    c2.addValue(-1).addValue(-1);
    assertThat(
        ((Counter<?, ?>) c2).extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE),
        hasIntegerValue(-2));

    c3.addValue(-1.5D).addValue(-2.5D).addValue(-3D);
    assertThat(
        ((Counter<?, ?>) c3).extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE),
        allOf(hasDoubleSum(-7D), hasDoubleCount(3)));

    c4.addValue(-1).addValue(-2).addValue(-3);
    assertThat(
        ((Counter<?, ?>) c4).extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE),
        allOf(hasIntegerSum(-6), hasIntegerCount(3)));

    c5.addValue(-1D).addValue(-2D);
    assertThat(
        ((Counter<?, ?>) c5).extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE),
        hasDoubleValue(-2D));

    c6.addValue(-1).addValue(-2);
    assertThat(
        ((Counter<?, ?>) c6).extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE),
        hasIntegerValue(-2));

    c7.addValue(-1D).addValue(-2D);
    assertThat(
        ((Counter<?, ?>) c7).extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE),
        hasDoubleValue(-1D));

    c8.addValue(-1).addValue(-2);
    assertThat(
        ((Counter<?, ?>) c8).extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE),
        hasIntegerValue(-1));
  }

  @Test
  public void testCloudCounterRepresentationNoDelta() {
    Counter<Integer, ?> c1 = counterFactory.intSum(CounterName.named("c1"));

    c1.addValue(1);
    assertThat(
        ((Counter<?, ?>) c1).extractUpdate(false, DataflowCounterUpdateExtractor.INSTANCE),
        hasIntegerValue(1));
    assertThat(
        ((Counter<?, ?>) c1).extractUpdate(false, DataflowCounterUpdateExtractor.INSTANCE),
        hasIntegerValue(1));

    c1.addValue(2);
    assertThat(
        ((Counter<?, ?>) c1).extractUpdate(false, DataflowCounterUpdateExtractor.INSTANCE),
        hasIntegerValue(3));
    assertThat(
        ((Counter<?, ?>) c1).extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE),
        hasIntegerValue(3));
    assertThat(
        ((Counter<?, ?>) c1).extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE),
        hasIntegerValue(0));
  }

  @Test
  public void testExtractUnstructuredNameCorrectly() {
    CounterName unstructuredName = CounterName.named(COUNTER_NAME);
    Counter<?, ?> unstructured = counterFactory.intSum(unstructuredName);

    // unstructured counter should not have a structured name
    CounterUpdate counterUpdate =
        unstructured.extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE);
    assertThat(counterUpdate, not(hasStructuredName()));
    assertThat(counterUpdate, hasName(COUNTER_NAME));
  }

  @Test
  public void testExtractStructuredNameCorrectly() {
    CounterName unstructuredName = CounterName.named(COUNTER_NAME);
    CounterName structuredOriginalName = unstructuredName.withOriginalName(nameContextForTest());
    CounterName structuredSystemName = unstructuredName.withSystemName(nameContextForTest());

    Counter<?, ?> structuredOriginal = counterFactory.intSum(structuredOriginalName);
    Counter<?, ?> structuredSystem = counterFactory.intSum(structuredSystemName);

    // Other two counters should be structured and should not conflict
    assertThat(
        structuredOriginal.extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE),
        hasStructuredName(structuredOriginalName, "SUM"));
    assertThat(
        structuredSystem.extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE),
        hasStructuredName(structuredSystemName, "SUM"));
  }

  @Test
  public void testExtractStructuredNameWithIoInfo() {
    CounterName counterName =
        CounterName.named(COUNTER_NAME).withOriginalRequestingStepName("stepReq").withInputIndex(1);
    Counter<?, ?> structuredOriginal = counterFactory.intSum(counterName);
    assertThat(
        structuredOriginal.extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE),
        hasStructuredName(counterName, "SUM"));
  }

  @Test
  public void testExtractShuffleReadCounter() {
    CounterName counterName =
        ShuffleReadCounter.generateCounterName(
            "originalShuffleStepName", "originalExecutingStepName");
    Counter<?, ?> counter = counterFactory.longSum(counterName);
    counter.extractUpdate(true, DataflowCounterUpdateExtractor.INSTANCE);
    hasStructuredName(counterName, "SUM");
  }
}
