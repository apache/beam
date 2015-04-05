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

package com.google.cloud.dataflow.sdk.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.coders.BigEndianLongCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Sessions;
import com.google.cloud.dataflow.sdk.transforms.windowing.SlidingWindows;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Unit tests for {@link GroupAlsoByWindowsDoFn}. */
@RunWith(JUnit4.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class GroupAlsoByWindowsDoFnTest {
  ExecutionContext execContext;
  CounterSet counters;
  TupleTag<KV<String, Iterable<String>>> outputTag;

  @Before public void setUp() {
    execContext = new DirectModeExecutionContext();
    counters = new CounterSet();
    outputTag = new TupleTag<>();
  }

  @Test public void testEmpty() throws Exception {
    DoFnRunner<KV<String, Iterable<WindowedValue<String>>>,
        KV<String, Iterable<String>>, List> runner =
        makeRunner(WindowingStrategy.of(FixedWindows.<String>of(Duration.millis(10))));

    runner.startBundle();

    runner.finishBundle();

    List<KV<String, Iterable<String>>> result = runner.getReceiver(outputTag);

    assertEquals(0, result.size());
  }

  @Test public void testFixedWindows() throws Exception {
    DoFnRunner<KV<String, Iterable<WindowedValue<String>>>,
        KV<String, Iterable<String>>, List> runner =
        makeRunner(WindowingStrategy.of(FixedWindows.<String>of(Duration.millis(10))));

    runner.startBundle();

    runner.processElement(WindowedValue.valueInEmptyWindows(
        KV.of("k", (Iterable<WindowedValue<String>>) Arrays.asList(
            WindowedValue.of(
                "v1",
                new Instant(1),
                Arrays.asList(window(0, 10))),
            WindowedValue.of(
                "v2",
                new Instant(2),
                Arrays.asList(window(0, 10))),
            WindowedValue.of(
                "v3",
                new Instant(13),
                Arrays.asList(window(10, 20)))))));

    runner.finishBundle();

    List<WindowedValue<KV<String, Iterable<String>>>> result = runner.getReceiver(outputTag);

    assertEquals(2, result.size());

    WindowedValue<KV<String, Iterable<String>>> item0 = result.get(0);
    assertEquals("k", item0.getValue().getKey());
    assertThat(item0.getValue().getValue(), Matchers.containsInAnyOrder("v1", "v2"));
    assertEquals(new Instant(1), item0.getTimestamp());
    assertThat(item0.getWindows(),
        Matchers.contains(window(0, 10)));

    WindowedValue<KV<String, Iterable<String>>> item1 = result.get(1);
    assertEquals("k", item1.getValue().getKey());
    assertThat(item1.getValue().getValue(), Matchers.contains("v3"));
    assertEquals(new Instant(13), item1.getTimestamp());
    assertThat(item1.getWindows(),
        Matchers.contains(window(10, 20)));
  }

  @Test public void testSlidingWindows() throws Exception {
    DoFnRunner<KV<String, Iterable<WindowedValue<String>>>,
        KV<String, Iterable<String>>, List> runner =
        makeRunner(WindowingStrategy.of(
            SlidingWindows.<String>of(Duration.millis(20)).every(Duration.millis(10))));

    runner.startBundle();

    runner.processElement(WindowedValue.valueInEmptyWindows(
        KV.of("k", (Iterable<WindowedValue<String>>) Arrays.asList(
            WindowedValue.of(
                "v1",
                new Instant(5),
                Arrays.asList(window(-10, 10), window(0, 20))),
            WindowedValue.of(
                "v2",
                new Instant(15),
                Arrays.asList(window(0, 20), window(10, 30)))))));

    runner.finishBundle();

    List<WindowedValue<KV<String, Iterable<String>>>> result = runner.getReceiver(outputTag);

    assertEquals(3, result.size());

    WindowedValue<KV<String, Iterable<String>>> item0 = result.get(0);
    assertEquals("k", item0.getValue().getKey());
    assertThat(item0.getValue().getValue(), Matchers.contains("v1"));
    assertEquals(new Instant(5), item0.getTimestamp());
    assertThat(item0.getWindows(),
        Matchers.contains(window(-10, 10)));

    WindowedValue<KV<String, Iterable<String>>> item1 = result.get(1);
    assertEquals("k", item1.getValue().getKey());
    assertThat(item1.getValue().getValue(), Matchers.containsInAnyOrder("v1", "v2"));
    assertEquals(new Instant(5), item1.getTimestamp());
    assertThat(item1.getWindows(),
        Matchers.contains(window(0, 20)));

    WindowedValue<KV<String, Iterable<String>>> item2 = result.get(2);
    assertEquals("k", item2.getValue().getKey());
    assertThat(item2.getValue().getValue(), Matchers.contains("v2"));
    assertEquals(new Instant(15), item2.getTimestamp());
    assertThat(item2.getWindows(),
        Matchers.contains(window(10, 30)));
  }

  @Test public void testDiscontiguousWindows() throws Exception {
    DoFnRunner<KV<String, Iterable<WindowedValue<String>>>,
        KV<String, Iterable<String>>, List> runner =
        makeRunner(WindowingStrategy.of(FixedWindows.<String>of(Duration.millis(10))));

    runner.startBundle();

    runner.processElement(WindowedValue.valueInEmptyWindows(
        KV.of("k", (Iterable<WindowedValue<String>>) Arrays.asList(
            WindowedValue.of(
                "v1",
                new Instant(1),
                Arrays.asList(window(0, 5))),
            WindowedValue.of(
                "v2",
                new Instant(4),
                Arrays.asList(window(1, 5))),
            WindowedValue.of(
                "v3",
                new Instant(4),
                Arrays.asList(window(0, 5)))))));

    runner.finishBundle();

    List<WindowedValue<KV<String, Iterable<String>>>> result = runner.getReceiver(outputTag);

    assertEquals(2, result.size());

    WindowedValue<KV<String, Iterable<String>>> item0 = result.get(0);
    assertEquals("k", item0.getValue().getKey());
    assertThat(item0.getValue().getValue(), Matchers.containsInAnyOrder("v1", "v3"));
    assertEquals(new Instant(1), item0.getTimestamp());
    assertThat(item0.getWindows(),
        Matchers.contains(window(0, 5)));

    WindowedValue<KV<String, Iterable<String>>> item1 = result.get(1);
    assertEquals("k", item1.getValue().getKey());
    assertThat(item1.getValue().getValue(), Matchers.contains("v2"));
    assertEquals(new Instant(4), item1.getTimestamp());
    assertThat(item1.getWindows(),
        Matchers.contains(window(1, 5)));
  }

  @Test public void testSessions() throws Exception {
    DoFnRunner<KV<String, Iterable<WindowedValue<String>>>,
        KV<String, Iterable<String>>, List> runner =
        makeRunner(WindowingStrategy.of(Sessions.<String>withGapDuration(Duration.millis(10))));

    runner.startBundle();

    runner.processElement(WindowedValue.valueInEmptyWindows(
        KV.of("k", (Iterable<WindowedValue<String>>) Arrays.asList(
            WindowedValue.of(
                "v1",
                new Instant(0),
                Arrays.asList(window(0, 10))),
            WindowedValue.of(
                "v2",
                new Instant(5),
                Arrays.asList(window(5, 15))),
            WindowedValue.of(
                "v3",
                new Instant(15),
                Arrays.asList(window(15, 25)))))));

    runner.finishBundle();

    List<WindowedValue<KV<String, Iterable<String>>>> result = runner.getReceiver(outputTag);

    assertEquals(2, result.size());

    WindowedValue<KV<String, Iterable<String>>> item0 = result.get(0);
    assertEquals("k", item0.getValue().getKey());
    assertThat(item0.getValue().getValue(), Matchers.containsInAnyOrder("v1", "v2"));
    assertEquals(new Instant(0), item0.getTimestamp());
    assertThat(item0.getWindows(),
        Matchers.contains(window(0, 15)));

    WindowedValue<KV<String, Iterable<String>>> item1 = result.get(1);
    assertEquals("k", item1.getValue().getKey());
    assertThat(item1.getValue().getValue(), Matchers.contains("v3"));
    assertEquals(new Instant(15), item1.getTimestamp());
    assertThat(item1.getWindows(),
        Matchers.contains(window(15, 25)));
  }

  @Test public void testSessionsCombine() throws Exception {
    CombineFn<Long, ?, Long> combineFn = new Sum.SumLongFn();
    DoFnRunner<KV<String, Iterable<WindowedValue<Long>>>,
        KV<String, Long>, List> runner =
        makeRunner(WindowingStrategy.of(Sessions.<String>withGapDuration(Duration.millis(10))),
                   combineFn.<String>asKeyedFn());
    runner.startBundle();

    runner.processElement(WindowedValue.valueInEmptyWindows(
        KV.of("k", (Iterable<WindowedValue<Long>>) Arrays.asList(
            WindowedValue.of(
                1L,
                new Instant(0),
                Arrays.asList(window(0, 10))),
            WindowedValue.of(
                2L,
                new Instant(5),
                Arrays.asList(window(5, 15))),
            WindowedValue.of(
                4L,
                new Instant(15),
                Arrays.asList(window(15, 25)))))));

    runner.finishBundle();

    List<WindowedValue<KV<String, Long>>> result = runner.getReceiver(outputTag);

    assertEquals(2, result.size());

    WindowedValue<KV<String, Long>> item0 = result.get(0);
    assertEquals("k", item0.getValue().getKey());
    assertEquals(3L, item0.getValue().getValue().longValue());
    assertEquals(new Instant(0), item0.getTimestamp());
    assertThat(item0.getWindows(), Matchers.contains(window(0, 15)));

    WindowedValue<KV<String, Long>> item1 = result.get(1);
    assertEquals("k", item1.getValue().getKey());
    assertEquals(4L, item1.getValue().getValue().longValue());
    assertEquals(new Instant(15), item1.getTimestamp());
    assertThat(item1.getWindows(), Matchers.contains(window(15, 25)));
  }

  private DoFnRunner<KV<String, Iterable<WindowedValue<String>>>,
      KV<String, Iterable<String>>, List> makeRunner(
          WindowingStrategy<? super String, IntervalWindow> windowingStrategy) {
    GroupAlsoByWindowsDoFn<String, String, Iterable<String>, IntervalWindow> fn =
        GroupAlsoByWindowsDoFn.createForIterable(windowingStrategy, StringUtf8Coder.of());
    return makeRunner(windowingStrategy, fn);
  }

  private DoFnRunner<KV<String, Iterable<WindowedValue<Long>>>,
      KV<String, Long>, List> makeRunner(
        WindowingStrategy<? super String, IntervalWindow> windowingStrategy,
        KeyedCombineFn<String, Long, ?, Long> combineFn) {
    GroupAlsoByWindowsDoFn<String, Long, Long, IntervalWindow> fn =
        GroupAlsoByWindowsDoFn.create(
            windowingStrategy, combineFn, StringUtf8Coder.of(), BigEndianLongCoder.of());

    return makeRunner(windowingStrategy, fn);
  }

  private <VI, VO> DoFnRunner<KV<String, Iterable<WindowedValue<VI>>>,
    KV<String, VO>, List> makeRunner(
        WindowingStrategy<? super String, IntervalWindow> windowingStrategy,
        GroupAlsoByWindowsDoFn<String, VI, VO, IntervalWindow> fn) {
    return
        DoFnRunner.createWithListOutputs(
            PipelineOptionsFactory.create(),
            fn,
            PTuple.empty(),
            (TupleTag<KV<String, VO>>) (TupleTag) outputTag,
            new ArrayList<TupleTag<?>>(),
            execContext.createStepContext("merge"),
            counters.getAddCounterMutator(),
            windowingStrategy);
  }

  private BoundedWindow window(long start, long end) {
    return new IntervalWindow(new Instant(start), new Instant(end));
  }
}
