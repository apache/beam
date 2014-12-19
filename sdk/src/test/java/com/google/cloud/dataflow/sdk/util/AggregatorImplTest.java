/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
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

package com.google.cloud.dataflow.sdk.util;

import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.MAX;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.MIN;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.SUM;

import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Max;
import com.google.cloud.dataflow.sdk.transforms.Min;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.CounterTestUtils;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for the {@link Aggregator} API.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class AggregatorImplTest {
  @Rule
  public final ExpectedException expectedEx = ExpectedException.none();

  private static final String AGGREGATOR_NAME = "aggregator_name";

  @SuppressWarnings("rawtypes")
  private <V> void testAggregator(List<V> items,
                                  SerializableFunction<Iterable<V>, V> combiner,
                                  Counter expectedCounter) {
    CounterSet counters = new CounterSet();
    Aggregator<V> aggregator = new AggregatorImpl<V, Iterable<V>, V>(
        AGGREGATOR_NAME, combiner, counters.getAddCounterMutator());
    for (V item : items) {
      aggregator.addValue(item);
    }

    List<MetricUpdate> cloudCounterSet = CounterTestUtils.extractCounterUpdates(counters, false);
    Assert.assertEquals(cloudCounterSet.size(), 1);
    Assert.assertEquals(cloudCounterSet.get(0),
                        CounterTestUtils.extractCounterUpdate(expectedCounter, false));
  }

  @Test
  public void testSumInteger() throws Exception {
    testAggregator(Arrays.asList(2, 4, 1, 3), new Sum.SumIntegerFn(),
                   Counter.ints(AGGREGATOR_NAME, SUM).resetToValue(10));
  }

  @Test
  public void testSumLong() throws Exception {
    testAggregator(Arrays.asList(2L, 4L, 1L, 3L), new Sum.SumLongFn(),
                   Counter.longs(AGGREGATOR_NAME, SUM).resetToValue(10L));
  }

  @Test
  public void testSumDouble() throws Exception {
    testAggregator(Arrays.asList(2.0, 4.1, 1.0, 3.1), new Sum.SumDoubleFn(),
                   Counter.doubles(AGGREGATOR_NAME, SUM).resetToValue(10.2));
  }

  @Test
  public void testMinInteger() throws Exception {
    testAggregator(Arrays.asList(2, 4, 1, 3), new Min.MinIntegerFn(),
                   Counter.ints(AGGREGATOR_NAME, MIN).resetToValue(1));
  }

  @Test
  public void testMinLong() throws Exception {
    testAggregator(Arrays.asList(2L, 4L, 1L, 3L), new Min.MinLongFn(),
                   Counter.longs(AGGREGATOR_NAME, MIN).resetToValue(1L));
  }

  @Test
  public void testMinDouble() throws Exception {
    testAggregator(Arrays.asList(2.0, 4.1, 1.0, 3.1), new Min.MinDoubleFn(),
                   Counter.doubles(AGGREGATOR_NAME, MIN).resetToValue(1.0));
  }

  @Test
  public void testMaxInteger() throws Exception {
    testAggregator(Arrays.asList(2, 4, 1, 3), new Max.MaxIntegerFn(),
                   Counter.ints(AGGREGATOR_NAME, MAX).resetToValue(4));
  }

  @Test
  public void testMaxLong() throws Exception {
    testAggregator(Arrays.asList(2L, 4L, 1L, 3L), new Max.MaxLongFn(),
                   Counter.longs(AGGREGATOR_NAME, MAX).resetToValue(4L));
  }

  @Test
  public void testMaxDouble() throws Exception {
    testAggregator(Arrays.asList(2.0, 4.1, 1.0, 3.1), new Max.MaxDoubleFn(),
                   Counter.doubles(AGGREGATOR_NAME, MAX).resetToValue(4.1));
  }

  @Test
  public void testCompatibleDuplicateNames() throws Exception {
    CounterSet counters = new CounterSet();
    Aggregator<Integer> aggregator1 =
        new AggregatorImpl<Integer, Iterable<Integer>, Integer>(
            AGGREGATOR_NAME, new Sum.SumIntegerFn(),
            counters.getAddCounterMutator());

    Aggregator<Integer> aggregator2 =
        new AggregatorImpl<Integer, Iterable<Integer>, Integer>(
            AGGREGATOR_NAME, new Sum.SumIntegerFn(),
            counters.getAddCounterMutator());

    // The duplicate aggregators should update the same counter.
    aggregator1.addValue(3);
    aggregator2.addValue(4);
    Assert.assertEquals(
        new CounterSet(Counter.ints(AGGREGATOR_NAME, SUM).resetToValue(7)),
        counters);
  }

  @Test
  public void testIncompatibleDuplicateNames() throws Exception {
    CounterSet counters = new CounterSet();
    new AggregatorImpl<Integer, Iterable<Integer>, Integer>(
        AGGREGATOR_NAME, new Sum.SumIntegerFn(),
        counters.getAddCounterMutator());

    expectedEx.expect(IllegalArgumentException.class);
    expectedEx.expectMessage(Matchers.containsString(
        "aggregator's name collides with an existing aggregator or "
        + "system-provided counter of an incompatible type"));
    new AggregatorImpl<Long, Iterable<Long>, Long>(
        AGGREGATOR_NAME, new Sum.SumLongFn(),
        counters.getAddCounterMutator());
    }

  @Test
  public void testUnsupportedCombineFn() throws Exception {
    expectedEx.expect(IllegalArgumentException.class);
    expectedEx.expectMessage(Matchers.containsString("unsupported combiner"));
    new AggregatorImpl<>(
        AGGREGATOR_NAME,
        new Combine.CombineFn<Integer, List<Integer>, Integer>() {
          @Override
          public List<Integer> createAccumulator() { return null; }
          @Override
          public void addInput(List<Integer> accumulator, Integer input) { }
          @Override
          public List<Integer> mergeAccumulators(Iterable<List<Integer>> accumulators) {
            return null; }
          @Override
          public Integer extractOutput(List<Integer> accumulator) { return null; }
        },
        (new CounterSet()).getAddCounterMutator());
  }

  @Test
  public void testUnsupportedSerializableFunction() throws Exception {
    expectedEx.expect(IllegalArgumentException.class);
    expectedEx.expectMessage(Matchers.containsString("unsupported combiner"));
    new AggregatorImpl<Integer, Iterable<Integer>, Integer>(
        AGGREGATOR_NAME,
        new SerializableFunction<Iterable<Integer>, Integer>() {
          @Override
          public Integer apply(Iterable<Integer> input) { return null; }
        },
        (new CounterSet()).getAddCounterMutator());
  }
}
