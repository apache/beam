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
package org.apache.beam.runners.spark.translation;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.CombineFnUtil;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;

/** * Test suite for {@link SparkCombineFn}. */
public class SparkCombineFnTest {

  final SerializablePipelineOptions opts =
      new SerializablePipelineOptions(PipelineOptionsFactory.create());
  CombineWithContext.CombineFnWithContext<Integer, Long, Long> combineFn;

  @Before
  public void setUp() {
    combineFn =
        (CombineWithContext.CombineFnWithContext<Integer, Long, Long>)
            CombineFnUtil.toFnWithContext(getSumFn());
  }

  @Test
  public void testGlobalWindowCombineFn() throws Exception {
    SparkCombineFn<KV<String, Integer>, Integer, Long, Long> sparkCombineFn =
        SparkCombineFn.keyed(
            combineFn, opts, Collections.emptyMap(), WindowingStrategy.globalDefault());

    WindowedValue<KV<String, Integer>> first = input("key", 1, Instant.now());
    WindowedValue<KV<String, Integer>> second = input("key", 2, Instant.now());
    WindowedValue<KV<String, Integer>> third = input("key", 3, Instant.now());
    SparkCombineFn.WindowedAccumulator<KV<String, Integer>, Integer, Long, ?> c1 =
        sparkCombineFn.createCombiner(first);
    SparkCombineFn.WindowedAccumulator<KV<String, Integer>, Integer, Long, ?> c2 =
        sparkCombineFn.createCombiner(third);
    sparkCombineFn.mergeValue(c1, second);
    SparkCombineFn.WindowedAccumulator<KV<String, Integer>, Integer, Long, ?> c3 =
        sparkCombineFn.mergeCombiners(c1, c2);
    assertEquals(6, (long) Iterables.getOnlyElement(sparkCombineFn.extractOutput(c3)).getValue());
  }

  @Test
  public void testGlobalCombineFn() throws Exception {
    SparkCombineFn<Integer, Integer, Long, Long> sparkCombineFn =
        SparkCombineFn.globally(
            combineFn, opts, Collections.emptyMap(), WindowingStrategy.globalDefault());

    WindowedValue<Integer> first = inputValue(1, Instant.now());
    WindowedValue<Integer> second = inputValue(2, Instant.now());
    WindowedValue<Integer> third = inputValue(3, Instant.now());
    SparkCombineFn.WindowedAccumulator<Integer, Integer, Long, ?> c1 =
        sparkCombineFn.createCombiner(first);
    SparkCombineFn.WindowedAccumulator<Integer, Integer, Long, ?> c2 =
        sparkCombineFn.createCombiner(third);
    sparkCombineFn.mergeValue(c1, second);
    SparkCombineFn.WindowedAccumulator<Integer, Integer, Long, ?> c3 =
        sparkCombineFn.mergeCombiners(c1, c2);
    assertEquals(6, (long) Iterables.getOnlyElement(sparkCombineFn.extractOutput(c3)).getValue());
  }

  @Test
  public void testSessionCombineFn() throws Exception {
    WindowingStrategy<Object, IntervalWindow> strategy =
        WindowingStrategy.of(Sessions.withGapDuration(Duration.millis(1000)));

    SparkCombineFn<KV<String, Integer>, Integer, Long, Long> sparkCombineFn =
        SparkCombineFn.keyed(combineFn, opts, Collections.emptyMap(), strategy);

    Instant now = Instant.ofEpochMilli(0);
    WindowedValue<KV<String, Integer>> first =
        input("key", 1, now.plus(Duration.millis(5000)), strategy.getWindowFn());
    WindowedValue<KV<String, Integer>> second =
        input("key", 2, now.plus(Duration.millis(1000)), strategy.getWindowFn());
    WindowedValue<KV<String, Integer>> third =
        input("key", 3, now.plus(Duration.millis(500)), strategy.getWindowFn());
    SparkCombineFn.WindowedAccumulator<KV<String, Integer>, Integer, Long, ?> c1 =
        sparkCombineFn.createCombiner(first);
    SparkCombineFn.WindowedAccumulator<KV<String, Integer>, Integer, Long, ?> c2 =
        sparkCombineFn.createCombiner(third);
    sparkCombineFn.mergeValue(c1, second);
    SparkCombineFn.WindowedAccumulator<KV<String, Integer>, Integer, Long, ?> c3 =
        sparkCombineFn.mergeCombiners(c1, c2);
    Iterable<WindowedValue<Long>> output = sparkCombineFn.extractOutput(c3);
    assertEquals(2, Iterables.size(output));
    List<String> format =
        StreamSupport.stream(output.spliterator(), false)
            .map(val -> val.getValue() + ":" + val.getTimestamp().getMillis())
            .collect(Collectors.toList());
    assertEquals(Lists.newArrayList("5:1999", "1:5999"), format);
  }

  @Test
  public void testSlidingCombineFnNonMerging() throws Exception {
    WindowingStrategy<Object, IntervalWindow> strategy =
        WindowingStrategy.of(SlidingWindows.of(Duration.millis(3000)).every(Duration.millis(1000)));

    SparkCombineFn<KV<String, Integer>, Integer, Long, Long> sparkCombineFn =
        SparkCombineFn.keyed(
            combineFn,
            opts,
            Collections.emptyMap(),
            strategy,
            SparkCombineFn.WindowedAccumulator.Type.NON_MERGING);

    Instant now = Instant.ofEpochMilli(0);
    WindowedValue<KV<String, Integer>> first =
        input("key", 1, now.plus(Duration.millis(5000)), strategy.getWindowFn());
    WindowedValue<KV<String, Integer>> second =
        input("key", 2, now.plus(Duration.millis(1500)), strategy.getWindowFn());
    WindowedValue<KV<String, Integer>> third =
        input("key", 3, now.plus(Duration.millis(500)), strategy.getWindowFn());
    SparkCombineFn.WindowedAccumulator<KV<String, Integer>, Integer, Long, ?> c1 =
        sparkCombineFn.createCombiner(first);
    SparkCombineFn.WindowedAccumulator<KV<String, Integer>, Integer, Long, ?> c2 =
        sparkCombineFn.createCombiner(third);
    sparkCombineFn.mergeValue(c1, second);
    SparkCombineFn.WindowedAccumulator<KV<String, Integer>, Integer, Long, ?> c3 =
        sparkCombineFn.mergeCombiners(c1, c2);
    Iterable<WindowedValue<Long>> output = sparkCombineFn.extractOutput(c3);
    assertEquals(7, Iterables.size(output));
    List<String> format =
        StreamSupport.stream(output.spliterator(), false)
            .map(val -> val.getValue() + ":" + val.getTimestamp().getMillis())
            .collect(Collectors.toList());
    assertUnorderedEquals(
        Lists.newArrayList("3:999", "5:1999", "5:2999", "2:3999", "1:5999", "1:6999", "1:7999"),
        format);
  }

  @Test
  public void testSlidingCombineFnExplode() throws Exception {
    WindowingStrategy<Object, IntervalWindow> strategy =
        WindowingStrategy.of(SlidingWindows.of(Duration.millis(3000)).every(Duration.millis(1000)));

    SparkCombineFn<KV<String, Integer>, Integer, Long, Long> sparkCombineFn =
        SparkCombineFn.keyed(
            combineFn,
            opts,
            Collections.emptyMap(),
            strategy,
            SparkCombineFn.WindowedAccumulator.Type.EXPLODE_WINDOWS);

    Instant now = Instant.ofEpochMilli(0);
    WindowedValue<KV<String, Integer>> first =
        input("key", 1, now.plus(Duration.millis(5000)), strategy.getWindowFn());
    WindowedValue<KV<String, Integer>> second =
        input("key", 2, now.plus(Duration.millis(1500)), strategy.getWindowFn());
    WindowedValue<KV<String, Integer>> third =
        input("key", 3, now.plus(Duration.millis(500)), strategy.getWindowFn());

    Map<KV<String, BoundedWindow>, List<WindowedValue<KV<String, Integer>>>> groupByKeyAndWindow;
    groupByKeyAndWindow =
        Stream.of(first, second, third)
            .flatMap(e -> StreamSupport.stream(e.explodeWindows().spliterator(), false))
            .collect(
                Collectors.groupingBy(
                    e -> KV.of(e.getValue().getKey(), Iterables.getOnlyElement(e.getWindows()))));

    List<String> result = new ArrayList<>();
    for (Map.Entry<KV<String, BoundedWindow>, List<WindowedValue<KV<String, Integer>>>> e :
        groupByKeyAndWindow.entrySet()) {

      SparkCombineFn.WindowedAccumulator<KV<String, Integer>, Integer, Long, ?> combiner = null;
      for (WindowedValue<KV<String, Integer>> v : e.getValue()) {
        if (combiner == null) {
          combiner = sparkCombineFn.createCombiner(v);
        } else {
          combiner.add(v, sparkCombineFn);
        }
      }
      WindowedValue<Long> combined = Iterables.getOnlyElement(combiner.extractOutput());
      result.add(combined.getValue() + ":" + combined.getTimestamp().getMillis());
    }

    assertUnorderedEquals(
        Lists.newArrayList("3:999", "5:1999", "5:2999", "2:3999", "1:5999", "1:6999", "1:7999"),
        result);
  }

  @Test
  public void testGlobalWindowMergeAccumulatorsWithEarliestCombiner() throws Exception {
    SparkCombineFn<KV<String, Integer>, Integer, Long, Long> sparkCombineFn =
        SparkCombineFn.keyed(
            combineFn,
            opts,
            Collections.emptyMap(),
            WindowingStrategy.globalDefault().withTimestampCombiner(TimestampCombiner.EARLIEST));

    Instant ts = BoundedWindow.TIMESTAMP_MIN_VALUE;
    WindowedValue<KV<String, Integer>> first = input("key", 1, ts);
    WindowedValue<KV<String, Integer>> second = input("key", 2, ts);
    WindowedValue<KV<String, Integer>> third = input("key", 3, ts);
    WindowedValue<Long> accumulator = WindowedValue.valueInGlobalWindow(0L);
    SparkCombineFn.SingleWindowWindowedAccumulator<KV<String, Integer>, Integer, Long> acc1 =
        SparkCombineFn.SingleWindowWindowedAccumulator.create(KV::getValue, accumulator);
    SparkCombineFn.SingleWindowWindowedAccumulator<KV<String, Integer>, Integer, Long> acc2 =
        SparkCombineFn.SingleWindowWindowedAccumulator.create(KV::getValue, accumulator);
    SparkCombineFn.SingleWindowWindowedAccumulator<KV<String, Integer>, Integer, Long> acc3 =
        SparkCombineFn.SingleWindowWindowedAccumulator.create(KV::getValue, accumulator);
    acc1.add(first, sparkCombineFn);
    acc2.add(second, sparkCombineFn);
    acc3.merge(acc1, sparkCombineFn);
    acc3.merge(acc2, sparkCombineFn);
    acc3.add(third, sparkCombineFn);
    assertEquals(6, (long) Iterables.getOnlyElement(sparkCombineFn.extractOutput(acc3)).getValue());
  }

  private static Combine.CombineFn<Integer, Long, Long> getSumFn() {
    return new Combine.CombineFn<Integer, Long, Long>() {

      @Override
      public Long createAccumulator() {
        return 0L;
      }

      @Override
      public Long addInput(Long mutableAccumulator, Integer input) {
        return mutableAccumulator + input;
      }

      @Override
      public Long mergeAccumulators(Iterable<Long> accumulators) {
        return StreamSupport.stream(accumulators.spliterator(), false).mapToLong(e -> e).sum();
      }

      @Override
      public Long extractOutput(Long accumulator) {
        return accumulator;
      }
    };
  }

  private <K, V> WindowedValue<KV<K, V>> input(K key, V value, Instant timestamp) throws Exception {

    return input(key, value, timestamp, WindowingStrategy.globalDefault().getWindowFn());
  }

  private <K, V> WindowedValue<KV<K, V>> input(
      K key, V value, Instant timestamp, WindowFn<?, ?> windowFn) throws Exception {

    return inputValue(KV.of(key, value), timestamp, windowFn);
  }

  private <V> WindowedValue<V> inputValue(V value, Instant timestamp) throws Exception {
    return inputValue(value, timestamp, WindowingStrategy.globalDefault().getWindowFn());
  }

  private <V> WindowedValue<V> inputValue(V value, Instant timestamp, WindowFn<?, ?> windowFn)
      throws Exception {

    @SuppressWarnings("unchecked")
    WindowFn<V, BoundedWindow> cast = (WindowFn<V, BoundedWindow>) windowFn;
    return WindowedValue.of(
        value,
        timestamp,
        cast.assignWindows(assignContext(cast, value, timestamp)),
        PaneInfo.NO_FIRING);
  }

  <V> WindowFn<V, BoundedWindow>.AssignContext assignContext(
      WindowFn<V, BoundedWindow> windowFn, V value, Instant timestamp) {
    return windowFn.new AssignContext() {
      @Override
      public V element() {
        return value;
      }

      @Override
      public Instant timestamp() {
        return timestamp;
      }

      @Override
      public BoundedWindow window() {
        return GlobalWindow.INSTANCE;
      }
    };
  }

  private <T> void assertUnorderedEquals(List<T> actual, List<T> expected) {
    assertEquals(
        actual.stream().collect(Collectors.toSet()), expected.stream().collect(Collectors.toSet()));
  }
}
