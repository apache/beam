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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
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
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.CombineFnUtil;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;

/** Test suite for {@link SparkKeyedCombineFn}. */
public class SparkKeyedCombineFnTest {

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
  public void testGlobalCombineFn() throws Exception {
    SparkKeyedCombineFn<String, Integer, Long, Long> sparkCombineFn =
        new SparkKeyedCombineFn<>(
            combineFn, opts, Collections.emptyMap(), WindowingStrategy.globalDefault());

    WindowedValue<KV<String, Integer>> first = input("key", 1, Instant.now());
    WindowedValue<KV<String, Integer>> second = input("key", 2, Instant.now());
    WindowedValue<KV<String, Integer>> third = input("key", 3, Instant.now());
    SparkKeyedCombineFn.WindowedAccumulator<Long> c1 = sparkCombineFn.createCombiner(first);
    SparkKeyedCombineFn.WindowedAccumulator<Long> c2 = sparkCombineFn.createCombiner(third);
    sparkCombineFn.mergeValue(c1, second);
    SparkKeyedCombineFn.WindowedAccumulator<Long> c3 = sparkCombineFn.mergeCombiners(c1, c2);
    assertEquals(6, (long) Iterables.getOnlyElement(sparkCombineFn.extractOutput(c3)).getValue());
  }

  @Test
  public void testSessionCombineFn() throws Exception {
    WindowingStrategy<Object, IntervalWindow> strategy =
        WindowingStrategy.of(Sessions.withGapDuration(Duration.millis(1000)));

    SparkKeyedCombineFn<String, Integer, Long, Long> sparkCombineFn =
        new SparkKeyedCombineFn<>(combineFn, opts, Collections.emptyMap(), strategy);

    Instant now = Instant.ofEpochMilli(0);
    WindowedValue<KV<String, Integer>> first =
        input("key", 1, now.plus(5000), strategy.getWindowFn());
    WindowedValue<KV<String, Integer>> second =
        input("key", 2, now.plus(1000), strategy.getWindowFn());
    WindowedValue<KV<String, Integer>> third =
        input("key", 3, now.plus(500), strategy.getWindowFn());
    SparkKeyedCombineFn.WindowedAccumulator<Long> c1 = sparkCombineFn.createCombiner(first);
    SparkKeyedCombineFn.WindowedAccumulator<Long> c2 = sparkCombineFn.createCombiner(third);
    sparkCombineFn.mergeValue(c1, second);
    SparkKeyedCombineFn.WindowedAccumulator<Long> c3 = sparkCombineFn.mergeCombiners(c1, c2);
    Iterable<WindowedValue<Long>> output = sparkCombineFn.extractOutput(c3);
    assertEquals(2, Iterables.size(output));
    List<String> format =
        StreamSupport.stream(output.spliterator(), false)
            .map(val -> val.getValue() + ":" + val.getTimestamp().getMillis())
            .collect(Collectors.toList());
    assertEquals(Lists.newArrayList("5:1999", "1:5999"), format);
  }

  @Test
  public void testSlidingCombineFn() throws Exception {
    WindowingStrategy<Object, IntervalWindow> strategy =
        WindowingStrategy.of(SlidingWindows.of(Duration.millis(3000)).every(Duration.millis(1000)));

    SparkKeyedCombineFn<String, Integer, Long, Long> sparkCombineFn =
        new SparkKeyedCombineFn<>(combineFn, opts, Collections.emptyMap(), strategy);

    Instant now = Instant.ofEpochMilli(0);
    WindowedValue<KV<String, Integer>> first =
        input("key", 1, now.plus(5000), strategy.getWindowFn());
    WindowedValue<KV<String, Integer>> second =
        input("key", 2, now.plus(1500), strategy.getWindowFn());
    WindowedValue<KV<String, Integer>> third =
        input("key", 3, now.plus(500), strategy.getWindowFn());
    SparkKeyedCombineFn.WindowedAccumulator<Long> c1 = sparkCombineFn.createCombiner(first);
    SparkKeyedCombineFn.WindowedAccumulator<Long> c2 = sparkCombineFn.createCombiner(third);
    sparkCombineFn.mergeValue(c1, second);
    SparkKeyedCombineFn.WindowedAccumulator<Long> c3 = sparkCombineFn.mergeCombiners(c1, c2);
    Iterable<WindowedValue<Long>> output = sparkCombineFn.extractOutput(c3);
    assertEquals(7, Iterables.size(output));
    List<String> format =
        StreamSupport.stream(output.spliterator(), false)
            .map(val -> val.getValue() + ":" + val.getTimestamp().getMillis())
            .collect(Collectors.toList());
    assertEquals(
        Lists.newArrayList("3:999", "5:1999", "5:2999", "2:3999", "1:5999", "1:6999", "1:7999"),
        format);
  }

  private static Combine.CombineFn<Integer, Long, Long> getSumFn() {
    return new Combine.CombineFn<Integer, Long, Long>() {

      @Override
      public Long createAccumulator() {
        return 0L;
      }

      @Override
      public Long addInput(Long mutableAccumulator, Integer input) {
        System.err.println("Summing " + mutableAccumulator + ", " + input);
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

  private WindowedValue<KV<String, Integer>> input(String key, int value, Instant timestamp)
      throws Exception {

    return input(key, value, timestamp, WindowingStrategy.globalDefault().getWindowFn());
  }

  private WindowedValue<KV<String, Integer>> input(
      String key, int value, Instant timestamp, WindowFn<?, ?> windowFn) throws Exception {

    @SuppressWarnings("unchecked")
    WindowFn<KV<String, Integer>, BoundedWindow> cast =
        (WindowFn<KV<String, Integer>, BoundedWindow>) windowFn;
    KV<String, Integer> kv = KV.of(key, value);
    return WindowedValue.of(
        kv, timestamp, cast.assignWindows(assignContext(cast, kv, timestamp)), PaneInfo.NO_FIRING);
  }

  WindowFn<KV<String, Integer>, BoundedWindow>.AssignContext assignContext(
      WindowFn<KV<String, Integer>, BoundedWindow> windowFn,
      KV<String, Integer> value,
      Instant timestamp) {
    return windowFn.new AssignContext() {
      @Override
      public KV<String, Integer> element() {
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
}
