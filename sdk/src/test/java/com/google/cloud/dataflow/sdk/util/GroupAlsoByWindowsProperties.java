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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.TestUtils.KvMatcher;
import com.google.cloud.dataflow.sdk.WindowMatchers;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.transforms.windowing.Sessions;
import com.google.cloud.dataflow.sdk.transforms.windowing.SlidingWindows;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.collect.ImmutableList;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Properties of {@link GroupAlsoByWindowsDoFn}.
 *
 * <p>Some properties may not hold of some implementations, due to restrictions on the context
 * in which the implementation is applicable. For example,
 * {@link GroupAlsoByWindowsViaIteratorsDoFn} does not support merging window functions.
 */
public class GroupAlsoByWindowsProperties {

  /**
   * A factory of {@link GroupAlsoByWindowsDoFn} so that the various properties can provide
   * the appropriate windowing strategy under test.
   */
  public interface GroupAlsoByWindowsDoFnFactory<K, InputT, OutputT> {
    <W extends BoundedWindow> GroupAlsoByWindowsDoFn<K, InputT, OutputT, W>
    forStrategy(WindowingStrategy<?, W> strategy);
  }

  /**
   * Tests that for empty input and the given {@link WindowingStrategy}, the provided GABW
   * implementation produces no output.
   *
   * <p>The input type is deliberately left as a wildcard, since it is not relevant.
   */
  public static <K, InputT, OutputT> void emptyInputEmptyOutput(
      GroupAlsoByWindowsDoFnFactory<K, InputT, OutputT> gabwFactory)
          throws Exception {

    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)));
    TupleTag<KV<K, OutputT>> outputTag = new TupleTag<>();
    DoFnRunner.ListOutputManager outputManager = new DoFnRunner.ListOutputManager();

    DoFnRunner<?, KV<K, OutputT>> runner =
        makeRunner(
            gabwFactory.forStrategy(windowingStrategy),
            windowingStrategy,
            outputTag,
            outputManager);

    runner.startBundle();

    runner.finishBundle();

    List<WindowedValue<KV<K, OutputT>>> result = outputManager.getOutput(outputTag);

    assertEquals(0, result.size());
  }

  /**
   * Tests that for a simple sequence of elements on the same key, the given GABW implementation
   * correctly groups them according to fixed windows.
   *
   * <p>The notable specialized property of this input is that each element occurs in a single
   * window.
   */
  public static void groupsElementsIntoFixedWindows(
      GroupAlsoByWindowsDoFnFactory<String, String, Iterable<String>> gabwFactory)
          throws Exception {

    TupleTag<KV<String, Iterable<String>>> outputTag = new TupleTag<>();
    DoFnRunner.ListOutputManager outputManager = new DoFnRunner.ListOutputManager();
    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)));

    DoFnRunner<KV<String, Iterable<WindowedValue<String>>>, KV<String, Iterable<String>>> runner =
        makeRunner(
            gabwFactory.forStrategy(windowingStrategy),
            windowingStrategy,
            outputTag,
            outputManager);

    runner.startBundle();

    runner.processElement(WindowedValue.valueInEmptyWindows(
        KV.of("k", (Iterable<WindowedValue<String>>) Arrays.asList(
            WindowedValue.of(
                "v1",
                new Instant(1),
                Arrays.asList(window(0, 10)),
                PaneInfo.NO_FIRING),
            WindowedValue.of(
                "v2",
                new Instant(2),
                Arrays.asList(window(0, 10)),
                PaneInfo.NO_FIRING),
            WindowedValue.of(
                "v3",
                new Instant(13),
                Arrays.asList(window(10, 20)),
                PaneInfo.NO_FIRING)))));

    runner.finishBundle();

    List<WindowedValue<KV<String, Iterable<String>>>> result = outputManager.getOutput(outputTag);

    assertEquals(2, result.size());

    WindowedValue<KV<String, Iterable<String>>> item0 = result.get(0);
    assertEquals("k", item0.getValue().getKey());
    assertThat(item0.getValue().getValue(), containsInAnyOrder("v1", "v2"));
    assertEquals(new Instant(1), item0.getTimestamp());
    assertThat(item0.getWindows(), contains(window(0, 10)));

    WindowedValue<KV<String, Iterable<String>>> item1 = result.get(1);
    assertEquals("k", item1.getValue().getKey());
    assertThat(item1.getValue().getValue(), contains("v3"));
    assertEquals(new Instant(13), item1.getTimestamp());
    assertThat(item1.getWindows(),
        contains(window(10, 20)));
  }

  /**
   * Tests that for a simple sequence of elements on the same key, the given GABW implementation
   * correctly groups them into sliding windows.
   *
   * <p>In the input here, each element occurs in multiple windows.
   */
  public static void groupsElementsIntoSlidingWindows(
      GroupAlsoByWindowsDoFnFactory<String, String, Iterable<String>> gabwFactory)
          throws Exception {

    TupleTag<KV<String, Iterable<String>>> outputTag = new TupleTag<>();
    DoFnRunner.ListOutputManager outputManager = new DoFnRunner.ListOutputManager();
    WindowingStrategy<?, IntervalWindow> windowingStrategy = WindowingStrategy.of(
        SlidingWindows.of(Duration.millis(20)).every(Duration.millis(10)));

    DoFnRunner<KV<String, Iterable<WindowedValue<String>>>, KV<String, Iterable<String>>> runner =
        makeRunner(
            gabwFactory.forStrategy(windowingStrategy),
            windowingStrategy,
            outputTag,
            outputManager);

    runner.startBundle();

    runner.processElement(WindowedValue.valueInEmptyWindows(
        KV.of("k", (Iterable<WindowedValue<String>>) Arrays.asList(
            WindowedValue.of(
                "v1",
                new Instant(5),
                Arrays.asList(window(-10, 10), window(0, 20)),
                PaneInfo.NO_FIRING),
            WindowedValue.of(
                "v2",
                new Instant(15),
                Arrays.asList(window(0, 20), window(10, 30)),
                PaneInfo.NO_FIRING)))));

    runner.finishBundle();

    List<WindowedValue<KV<String, Iterable<String>>>> result = outputManager.getOutput(outputTag);

    assertEquals(3, result.size());

    WindowedValue<KV<String, Iterable<String>>> item0 = result.get(0);
    assertEquals("k", item0.getValue().getKey());
    assertThat(item0.getValue().getValue(), contains("v1"));
    assertEquals(new Instant(5), item0.getTimestamp());
    assertThat(item0.getWindows(),
        contains(window(-10, 10)));

    WindowedValue<KV<String, Iterable<String>>> item1 = result.get(1);
    assertEquals("k", item1.getValue().getKey());
    assertThat(item1.getValue().getValue(), containsInAnyOrder("v1", "v2"));
    assertEquals(new Instant(10), item1.getTimestamp());
    assertThat(item1.getWindows(),
        contains(window(0, 20)));

    WindowedValue<KV<String, Iterable<String>>> item2 = result.get(2);
    assertEquals("k", item2.getValue().getKey());
    assertThat(item2.getValue().getValue(), contains("v2"));
    assertEquals(new Instant(20), item2.getTimestamp());
    assertThat(item2.getWindows(),
        contains(window(10, 30)));
  }

  /**
   * Tests that for a simple sequence of elements on the same key, the given GABW implementation
   * correctly groups and combines them according to sliding windows.
   *
   * <p>In the input here, each element occurs in multiple windows.
   */
  public static void combinesElementsInSlidingWindows(
      GroupAlsoByWindowsDoFnFactory<String, Long, Long> gabwFactory,
      CombineFn<Long, ?, Long> combineFn)
          throws Exception {

    TupleTag<KV<String, Long>> outputTag = new TupleTag<>();
    DoFnRunner.ListOutputManager outputManager = new DoFnRunner.ListOutputManager();
    WindowingStrategy<?, IntervalWindow> windowingStrategy = WindowingStrategy.of(
        SlidingWindows.of(Duration.millis(20)).every(Duration.millis(10)));

    DoFnRunner<KV<String, Iterable<WindowedValue<Long>>>, KV<String, Long>> runner =
        makeRunner(
            gabwFactory.forStrategy(windowingStrategy),
            windowingStrategy,
            outputTag,
            outputManager);

    runner.startBundle();

    runner.processElement(WindowedValue.valueInEmptyWindows(
        KV.of("k", (Iterable<WindowedValue<Long>>) Arrays.asList(
            WindowedValue.of(
                1L,
                new Instant(5),
                Arrays.asList(window(-10, 10), window(0, 20)),
                PaneInfo.NO_FIRING),
            WindowedValue.of(
                2L,
                new Instant(15),
                Arrays.asList(window(0, 20), window(10, 30)),
                PaneInfo.NO_FIRING),
            WindowedValue.of(
                4L,
                new Instant(18),
                Arrays.asList(window(0, 20), window(10, 30)),
                PaneInfo.NO_FIRING)))));

    runner.finishBundle();

    List<WindowedValue<KV<String, Long>>> result = outputManager.getOutput(outputTag);

    assertEquals(3, result.size());

    assertThat(result, contains(
        WindowMatchers.isSingleWindowedValue(
            KvMatcher.isKv(
                equalTo("k"),
                equalTo(combineFn.apply(ImmutableList.of(1L)))),
            5,   // aggregate timestamp
            -10, // window start
            10), // window end
        WindowMatchers.isSingleWindowedValue(
            KvMatcher.isKv(
                equalTo("k"),
                equalTo(combineFn.apply(ImmutableList.of(1L, 2L, 4L)))),
            10,  // aggregate timestamp
            0,   // window start
            20), // window end
        WindowMatchers.isSingleWindowedValue(
            KvMatcher.isKv(
                equalTo("k"),
                equalTo(combineFn.apply(ImmutableList.of(2L, 4L)))),
            20, // aggregate timestamp
            10, // window start
            30))); // window end
  }

  /**
   * Tests that the given GABW implementation correctly groups elements that fall into overlapping
   * windows that are not merged.
   */
  public static void groupsIntoOverlappingNonmergingWindows(
      GroupAlsoByWindowsDoFnFactory<String, String, Iterable<String>> gabwFactory)
          throws Exception {

    TupleTag<KV<String, Iterable<String>>> outputTag = new TupleTag<>();
    DoFnRunner.ListOutputManager outputManager = new DoFnRunner.ListOutputManager();
    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)));

    DoFnRunner<KV<String, Iterable<WindowedValue<String>>>, KV<String, Iterable<String>>> runner =
        makeRunner(
            gabwFactory.forStrategy(windowingStrategy),
            windowingStrategy,
            outputTag,
            outputManager);

    runner.startBundle();

    runner.processElement(WindowedValue.valueInEmptyWindows(
        KV.of("k", (Iterable<WindowedValue<String>>) Arrays.asList(
            WindowedValue.of(
                "v1",
                new Instant(1),
                Arrays.asList(window(0, 5)),
                PaneInfo.NO_FIRING),
            WindowedValue.of(
                "v2",
                new Instant(4),
                Arrays.asList(window(1, 5)),
                PaneInfo.NO_FIRING),
            WindowedValue.of(
                "v3",
                new Instant(4),
                Arrays.asList(window(0, 5)),
                PaneInfo.NO_FIRING)))));

    runner.finishBundle();

    List<WindowedValue<KV<String, Iterable<String>>>> result = outputManager.getOutput(outputTag);

    assertEquals(2, result.size());

    WindowedValue<KV<String, Iterable<String>>> item0 = result.get(0);
    assertEquals("k", item0.getValue().getKey());
    assertThat(item0.getValue().getValue(), containsInAnyOrder("v1", "v3"));
    assertEquals(new Instant(1), item0.getTimestamp());
    assertThat(item0.getWindows(),
        contains(window(0, 5)));

    WindowedValue<KV<String, Iterable<String>>> item1 = result.get(1);
    assertEquals("k", item1.getValue().getKey());
    assertThat(item1.getValue().getValue(), contains("v2"));
    assertEquals(new Instant(4), item1.getTimestamp());
    assertThat(item1.getWindows(),
        contains(window(1, 5)));
  }

  /**
   * Tests that the given GABW implementation correctly groups elements into merged sessions.
   */
  public static void groupsElementsInMergedSessions(
      GroupAlsoByWindowsDoFnFactory<String, String, Iterable<String>> gabwFactory)
          throws Exception {

    TupleTag<KV<String, Iterable<String>>> outputTag = new TupleTag<>();
    DoFnRunner.ListOutputManager outputManager = new DoFnRunner.ListOutputManager();
    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(Sessions.withGapDuration(Duration.millis(10)));

    DoFnRunner<KV<String, Iterable<WindowedValue<String>>>, KV<String, Iterable<String>>> runner =
        makeRunner(
            gabwFactory.forStrategy(windowingStrategy),
            windowingStrategy,
            outputTag,
            outputManager);

    runner.startBundle();

    runner.processElement(WindowedValue.valueInEmptyWindows(
        KV.of("k", (Iterable<WindowedValue<String>>) Arrays.asList(
            WindowedValue.of(
                "v1",
                new Instant(0),
                Arrays.asList(window(0, 10)),
                PaneInfo.NO_FIRING),
            WindowedValue.of(
                "v2",
                new Instant(5),
                Arrays.asList(window(5, 15)),
                PaneInfo.NO_FIRING),
            WindowedValue.of(
                "v3",
                new Instant(15),
                Arrays.asList(window(15, 25)),
                PaneInfo.NO_FIRING)))));

    runner.finishBundle();

    List<WindowedValue<KV<String, Iterable<String>>>> result = outputManager.getOutput(outputTag);

    assertEquals(2, result.size());

    WindowedValue<KV<String, Iterable<String>>> item0 = result.get(0);
    assertEquals("k", item0.getValue().getKey());
    assertThat(item0.getValue().getValue(), containsInAnyOrder("v1", "v2"));
    assertEquals(new Instant(0), item0.getTimestamp());
    assertThat(item0.getWindows(),
        contains(window(0, 15)));

    WindowedValue<KV<String, Iterable<String>>> item1 = result.get(1);
    assertEquals("k", item1.getValue().getKey());
    assertThat(item1.getValue().getValue(), contains("v3"));
    assertEquals(new Instant(15), item1.getTimestamp());
    assertThat(item1.getWindows(),
        contains(window(15, 25)));
  }

  /**
   * Tests that the given {@link GroupAlsoByWindowsDoFn} implementation combines elements per
   * session window correctly according to the provided {@link CombineFn}.
   */
  public static void combinesElementsPerSession(
      GroupAlsoByWindowsDoFnFactory<String, Long, Long> gabwFactory,
      CombineFn<Long, ?, Long> combineFn)
          throws Exception {

    TupleTag<KV<String, Long>> outputTag = new TupleTag<>();
    DoFnRunner.ListOutputManager outputManager = new DoFnRunner.ListOutputManager();
    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(Sessions.withGapDuration(Duration.millis(10)));

    DoFnRunner<KV<String, Iterable<WindowedValue<Long>>>, KV<String, Long>> runner =
        makeRunner(
            gabwFactory.forStrategy(windowingStrategy),
            windowingStrategy,
            outputTag,
            outputManager);

    runner.startBundle();

    runner.processElement(WindowedValue.valueInEmptyWindows(
        KV.of("k", (Iterable<WindowedValue<Long>>) Arrays.asList(
            WindowedValue.of(
                1L,
                new Instant(0),
                Arrays.asList(window(0, 10)),
                PaneInfo.NO_FIRING),
            WindowedValue.of(
                2L,
                new Instant(5),
                Arrays.asList(window(5, 15)),
                PaneInfo.NO_FIRING),
            WindowedValue.of(
                4L,
                new Instant(15),
                Arrays.asList(window(15, 25)),
                PaneInfo.NO_FIRING)))));

    runner.finishBundle();

    List<WindowedValue<KV<String, Long>>> result = outputManager.getOutput(outputTag);

    assertThat(result, contains(
        WindowMatchers.isSingleWindowedValue(
            KvMatcher.isKv(
                equalTo("k"),
                equalTo(combineFn.apply(ImmutableList.of(1L, 2L)))),
            0, // aggregate timestamp
            0, // window start
            15), // window end
        WindowMatchers.isSingleWindowedValue(
            KvMatcher.isKv(
                equalTo("k"),
                equalTo(combineFn.apply(ImmutableList.of(4L)))),
            15, // aggregate timestamp
            15, // window start
            25))); // window end
  }

  private static <K, InputT, OutputT, W extends BoundedWindow>
  DoFnRunner<KV<K, Iterable<WindowedValue<InputT>>>, KV<K, OutputT>>
      makeRunner(
          GroupAlsoByWindowsDoFn<K, InputT, OutputT, W> fn,
          WindowingStrategy<?, W> windowingStrategy,
          TupleTag<KV<K, OutputT>> outputTag,
          DoFnRunner.OutputManager outputManager) {

    ExecutionContext executionContext = DirectModeExecutionContext.create();
    CounterSet counters = new CounterSet();

    return DoFnRunner.create(
        PipelineOptionsFactory.create(),
        fn,
        NullSideInputReader.empty(),
        outputManager,
        outputTag,
        new ArrayList<TupleTag<?>>(),
        executionContext.getOrCreateStepContext("GABWStep", "GABWTransform", null),
        counters.getAddCounterMutator(),
        windowingStrategy);
  }

  private static BoundedWindow window(long start, long end) {
    return new IntervalWindow(new Instant(start), new Instant(end));
  }
}
