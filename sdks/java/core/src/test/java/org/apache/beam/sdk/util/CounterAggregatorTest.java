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

import static org.apache.beam.sdk.util.common.Counter.AggregationKind.MAX;
import static org.apache.beam.sdk.util.common.Counter.AggregationKind.MIN;
import static org.apache.beam.sdk.util.common.Counter.AggregationKind.SUM;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Combine.IterableCombineFn;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.util.common.Counter;
import org.apache.beam.sdk.util.common.CounterProvider;
import org.apache.beam.sdk.util.common.CounterSet;
import org.apache.beam.sdk.util.common.CounterSet.AddCounterMutator;

import com.google.common.collect.Iterables;

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
public class CounterAggregatorTest {
  @Rule
  public final ExpectedException expectedEx = ExpectedException.none();

  private static final String AGGREGATOR_NAME = "aggregator_name";

  @SuppressWarnings("rawtypes")
  private <V, AccumT> void testAggregator(List<V> items,
                                      Combine.CombineFn<V, AccumT, V> combiner,
                                      Counter expectedCounter) {
    CounterSet counters = new CounterSet();
    Aggregator<V, V> aggregator = new CounterAggregator<>(
        AGGREGATOR_NAME, combiner, counters.getAddCounterMutator());
    for (V item : items) {
      aggregator.addValue(item);
    }

    assertEquals(Iterables.getOnlyElement(counters), expectedCounter);
  }

  @Test
  public void testGetName() {
    String name = "testAgg";
    CounterAggregator<Long, long[], Long> aggregator = new CounterAggregator<>(
        name, new Sum.SumLongFn(),
        new CounterSet().getAddCounterMutator());

    assertEquals(name, aggregator.getName());
  }

  @Test
  public void testGetCombineFn() {
    CombineFn<Long, ?, Long> combineFn = new Min.MinLongFn();

    CounterAggregator<Long, ?, Long> aggregator = new CounterAggregator<>("foo",
        combineFn, new CounterSet().getAddCounterMutator());

    assertEquals(combineFn, aggregator.getCombineFn());
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
  public void testCounterProviderCallsProvidedCounterAddValue() {
    @SuppressWarnings("unchecked")
    CombineFn<String, ?, String> combiner = mock(CombineFn.class,
        withSettings().extraInterfaces(CounterProvider.class));
    @SuppressWarnings("unchecked")
    CounterProvider<String> provider = (CounterProvider<String>) combiner;

    @SuppressWarnings("unchecked")
    Counter<String> mockCounter = mock(Counter.class);
    String name = "foo";
    when(provider.getCounter(name)).thenReturn(mockCounter);

    AddCounterMutator addCounterMutator = mock(AddCounterMutator.class);
    when(addCounterMutator.addCounter(mockCounter)).thenReturn(mockCounter);

    Aggregator<String, String> aggregator =
        new CounterAggregator<>(name, combiner, addCounterMutator);

    aggregator.addValue("bar_baz");

    verify(mockCounter).addValue("bar_baz");
    verify(addCounterMutator).addCounter(mockCounter);
  }


  @Test
  public void testCompatibleDuplicateNames() throws Exception {
    CounterSet counters = new CounterSet();
    Aggregator<Integer, Integer> aggregator1 = new CounterAggregator<>(
        AGGREGATOR_NAME, new Sum.SumIntegerFn(),
        counters.getAddCounterMutator());

    Aggregator<Integer, Integer> aggregator2 = new CounterAggregator<>(
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
    new CounterAggregator<>(
        AGGREGATOR_NAME, new Sum.SumIntegerFn(),
        counters.getAddCounterMutator());

    expectedEx.expect(IllegalArgumentException.class);
    expectedEx.expectMessage(Matchers.containsString(
        "aggregator's name collides with an existing aggregator or "
        + "system-provided counter of an incompatible type"));
    new CounterAggregator<>(
        AGGREGATOR_NAME, new Sum.SumLongFn(),
        counters.getAddCounterMutator());
    }

  @Test
  public void testUnsupportedCombineFn() throws Exception {
    expectedEx.expect(IllegalArgumentException.class);
    expectedEx.expectMessage(Matchers.containsString("unsupported combiner"));
    new CounterAggregator<>(
        AGGREGATOR_NAME,
        new Combine.CombineFn<Integer, List<Integer>, Integer>() {
          @Override
          public List<Integer> createAccumulator() {
            return null;
          }
          @Override
          public List<Integer> addInput(List<Integer> accumulator, Integer input) {
            return null;
          }
          @Override
          public List<Integer> mergeAccumulators(Iterable<List<Integer>> accumulators) {
            return null;
          }
          @Override
          public Integer extractOutput(List<Integer> accumulator) {
            return null;
          }
        }, (new CounterSet()).getAddCounterMutator());
  }

  @Test
  public void testUnsupportedSerializableFunction() throws Exception {
    expectedEx.expect(IllegalArgumentException.class);
    expectedEx.expectMessage(Matchers.containsString("unsupported combiner"));
    CombineFn<Integer, List<Integer>, Integer> combiner = IterableCombineFn
        .<Integer>of(new SerializableFunction<Iterable<Integer>, Integer>() {
          @Override
          public Integer apply(Iterable<Integer> input) {
            return null;
          }
        });
    new CounterAggregator<>(AGGREGATOR_NAME, combiner,
        (new CounterSet()).getAddCounterMutator());
  }
}
