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
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.TestUtils.KvMatcher;
import com.google.cloud.dataflow.sdk.WindowMatchers;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.OutputTimeFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.OutputTimeFns;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.transforms.windowing.Sessions;
import com.google.cloud.dataflow.sdk.transforms.windowing.SlidingWindows;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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

    List<?> result = runGABW(
        gabwFactory,
        windowingStrategy,
        (K) null, // key should never be used
        Collections.<WindowedValue<InputT>>emptyList());

    assertThat(result.size(), equalTo(0));
  }

  /**
   * Tests that for a simple sequence of elements on the same key, the given GABW implementation
   * correctly groups them according to fixed windows.
   */
  public static void groupsElementsIntoFixedWindows(
      GroupAlsoByWindowsDoFnFactory<String, String, Iterable<String>> gabwFactory)
          throws Exception {

    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)));

    List<WindowedValue<KV<String, Iterable<String>>>> result =
        runGABW(gabwFactory, windowingStrategy, "key",
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
                PaneInfo.NO_FIRING));

    assertThat(result.size(), equalTo(2));

    WindowedValue<KV<String, Iterable<String>>> item0 = result.get(0);
    assertThat(item0.getValue().getValue(), containsInAnyOrder("v1", "v2"));
    assertThat(item0.getTimestamp(), equalTo(new Instant(1)));
    assertThat(item0.getWindows(), contains(window(0, 10)));

    WindowedValue<KV<String, Iterable<String>>> item1 = result.get(1);
    assertThat(item1.getValue().getValue(), contains("v3"));
    assertThat(item1.getTimestamp(), equalTo(new Instant(13)));
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

    WindowingStrategy<?, IntervalWindow> windowingStrategy = WindowingStrategy.of(
        SlidingWindows.of(Duration.millis(20)).every(Duration.millis(10)));

    List<WindowedValue<KV<String, Iterable<String>>>> result =
        runGABW(gabwFactory, windowingStrategy, "key",
            WindowedValue.of(
                "v1",
                new Instant(5),
                Arrays.asList(window(-10, 10), window(0, 20)),
                PaneInfo.NO_FIRING),
            WindowedValue.of(
                "v2",
                new Instant(15),
                Arrays.asList(window(0, 20), window(10, 30)),
                PaneInfo.NO_FIRING));

    assertThat(result.size(), equalTo(3));

    WindowedValue<KV<String, Iterable<String>>> item0 = result.get(0);
    assertThat(item0.getValue().getValue(), contains("v1"));
    assertThat(item0.getTimestamp(), equalTo(new Instant(5)));
    assertThat(item0.getWindows(),
        contains(window(-10, 10)));

    WindowedValue<KV<String, Iterable<String>>> item1 = result.get(1);
    assertThat(item1.getValue().getValue(), containsInAnyOrder("v1", "v2"));
    assertThat(item1.getTimestamp(), equalTo(new Instant(10)));
    assertThat(item1.getWindows(),
        contains(window(0, 20)));

    WindowedValue<KV<String, Iterable<String>>> item2 = result.get(2);
    assertThat(item2.getValue().getValue(), contains("v2"));
    assertThat(item2.getTimestamp(), equalTo(new Instant(20)));
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

    WindowingStrategy<?, IntervalWindow> windowingStrategy = WindowingStrategy.of(
        SlidingWindows.of(Duration.millis(20)).every(Duration.millis(10)));

    List<WindowedValue<KV<String, Long>>> result =
        runGABW(gabwFactory, windowingStrategy, "k",
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
                PaneInfo.NO_FIRING));

    assertThat(result.size(), equalTo(3));

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

    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)));

    List<WindowedValue<KV<String, Iterable<String>>>> result =
        runGABW(gabwFactory, windowingStrategy, "key",
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
                PaneInfo.NO_FIRING));

    assertThat(result.size(), equalTo(2));

    WindowedValue<KV<String, Iterable<String>>> item0 = result.get(0);
    assertThat(item0.getValue().getValue(), containsInAnyOrder("v1", "v3"));
    assertThat(item0.getTimestamp(), equalTo(new Instant(1)));
    assertThat(item0.getWindows(),
        contains(window(0, 5)));

    WindowedValue<KV<String, Iterable<String>>> item1 = result.get(1);
    assertThat(item1.getValue().getValue(), contains("v2"));
    assertThat(item1.getTimestamp(), equalTo(new Instant(4)));
    assertThat(item1.getWindows(),
        contains(window(1, 5)));
  }

  /**
   * Tests that the given GABW implementation correctly groups elements into merged sessions.
   */
  public static void groupsElementsInMergedSessions(
      GroupAlsoByWindowsDoFnFactory<String, String, Iterable<String>> gabwFactory)
          throws Exception {

    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(Sessions.withGapDuration(Duration.millis(10)));

    List<WindowedValue<KV<String, Iterable<String>>>> result =
        runGABW(gabwFactory, windowingStrategy, "key",
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
                PaneInfo.NO_FIRING));

    assertThat(result.size(), equalTo(2));

    WindowedValue<KV<String, Iterable<String>>> item0 = result.get(0);
    assertThat(item0.getValue().getValue(), containsInAnyOrder("v1", "v2"));
    assertThat(item0.getTimestamp(), equalTo(new Instant(0)));
    assertThat(item0.getWindows(),
        contains(window(0, 15)));

    WindowedValue<KV<String, Iterable<String>>> item1 = result.get(1);
    assertThat(item1.getValue().getValue(), contains("v3"));
    assertThat(item1.getTimestamp(), equalTo(new Instant(15)));
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

    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(Sessions.withGapDuration(Duration.millis(10)));

    List<WindowedValue<KV<String, Long>>> result =
        runGABW(gabwFactory, windowingStrategy, "k",
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
                PaneInfo.NO_FIRING));

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

  /**
   * Tests that for a simple sequence of elements on the same key, the given GABW implementation
   * correctly groups them according to fixed windows and also sets the output timestamp
   * according to the policy {@link OutputTimeFns#outputAtEndOfWindow()}.
   */
  public static void groupsElementsIntoFixedWindowsWithEndOfWindowTimestamp(
      GroupAlsoByWindowsDoFnFactory<String, String, Iterable<String>> gabwFactory)
          throws Exception {

    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)))
        .withOutputTimeFn(OutputTimeFns.outputAtEndOfWindow());

    List<WindowedValue<KV<String, Iterable<String>>>> result =
        runGABW(gabwFactory, windowingStrategy, "key",
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
                PaneInfo.NO_FIRING));

    assertThat(result.size(), equalTo(2));

    WindowedValue<KV<String, Iterable<String>>> item0 = result.get(0);
    assertThat(item0.getValue().getValue(), containsInAnyOrder("v1", "v2"));
    assertThat(item0.getTimestamp(), equalTo(window(0, 10).maxTimestamp()));
    assertThat(item0.getTimestamp(),
        equalTo(Iterables.getOnlyElement(item0.getWindows()).maxTimestamp()));

    WindowedValue<KV<String, Iterable<String>>> item1 = result.get(1);
    assertThat(item1.getValue().getValue(), contains("v3"));
    assertThat(item1.getTimestamp(), equalTo(window(10, 20).maxTimestamp()));
    assertThat(item1.getTimestamp(),
        equalTo(Iterables.getOnlyElement(item1.getWindows()).maxTimestamp()));
  }

  /**
   * Tests that for a simple sequence of elements on the same key, the given GABW implementation
   * correctly groups them according to fixed windows and also sets the output timestamp
   * according to a custom {@link OutputTimeFn}.
   */
  public static void groupsElementsIntoFixedWindowsWithCustomTimestamp(
      GroupAlsoByWindowsDoFnFactory<String, String, Iterable<String>> gabwFactory)
      throws Exception {
    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)))
            .withOutputTimeFn(new OutputTimeFn.Defaults<IntervalWindow>() {
              @Override
              public Instant assignOutputTime(Instant inputTimestamp, IntervalWindow window) {
                return inputTimestamp.isBefore(window.maxTimestamp())
                    ? inputTimestamp.plus(1) : window.maxTimestamp();
              }

              @Override
              public Instant combine(Instant outputTime, Instant otherOutputTime) {
                return outputTime.isBefore(otherOutputTime) ? outputTime : otherOutputTime;
              }

              @Override
              public boolean dependsOnlyOnEarliestInputTimestamp() {
                return true;
              }
            });

    List<WindowedValue<KV<String, Iterable<String>>>> result = runGABW(gabwFactory,
        windowingStrategy, "key",
        WindowedValue.of("v1", new Instant(1), Arrays.asList(window(0, 10)), PaneInfo.NO_FIRING),
        WindowedValue.of("v2", new Instant(2), Arrays.asList(window(0, 10)), PaneInfo.NO_FIRING),
        WindowedValue.of("v3", new Instant(13), Arrays.asList(window(10, 20)), PaneInfo.NO_FIRING));

    assertThat(result.size(), equalTo(2));

    WindowedValue<KV<String, Iterable<String>>> item0 = result.get(0);
    assertThat(item0.getValue().getValue(), containsInAnyOrder("v1", "v2"));
    assertThat(item0.getWindows(), contains(window(0, 10)));
    assertThat(item0.getTimestamp(), equalTo(new Instant(2)));

    WindowedValue<KV<String, Iterable<String>>> item1 = result.get(1);
    assertThat(item1.getValue().getValue(), contains("v3"));
    assertThat(item1.getWindows(), contains(window(10, 20)));
    assertThat(item1.getTimestamp(), equalTo(new Instant(14)));
  }

  /**
   * Tests that for a simple sequence of elements on the same key, the given GABW implementation
   * correctly groups them according to fixed windows and also sets the output timestamp
   * according to the policy {@link OutputTimeFns#outputAtLatestInputTimestamp()}.
   */
  public static void groupsElementsIntoFixedWindowsWithLatestTimestamp(
      GroupAlsoByWindowsDoFnFactory<String, String, Iterable<String>> gabwFactory)
          throws Exception {

    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)))
        .withOutputTimeFn(OutputTimeFns.outputAtLatestInputTimestamp());

    List<WindowedValue<KV<String, Iterable<String>>>> result =
        runGABW(gabwFactory, windowingStrategy, "k",
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
                PaneInfo.NO_FIRING));

    assertThat(result.size(), equalTo(2));

    WindowedValue<KV<String, Iterable<String>>> item0 = result.get(0);
    assertThat(item0.getValue().getValue(), containsInAnyOrder("v1", "v2"));
    assertThat(item0.getWindows(), contains(window(0, 10)));
    assertThat(item0.getTimestamp(), equalTo(new Instant(2)));

    WindowedValue<KV<String, Iterable<String>>> item1 = result.get(1);
    assertThat(item1.getValue().getValue(), contains("v3"));
    assertThat(item1.getWindows(), contains(window(10, 20)));
    assertThat(item1.getTimestamp(), equalTo(new Instant(13)));
  }

  /**
   * Tests that the given GABW implementation correctly groups elements into merged sessions
   * with output timestamps at the end of the merged window.
   */
  public static void groupsElementsInMergedSessionsWithEndOfWindowTimestamp(
      GroupAlsoByWindowsDoFnFactory<String, String, Iterable<String>> gabwFactory)
          throws Exception {

    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(Sessions.withGapDuration(Duration.millis(10)))
            .withOutputTimeFn(OutputTimeFns.outputAtEndOfWindow());

    List<WindowedValue<KV<String, Iterable<String>>>> result =
        runGABW(gabwFactory, windowingStrategy, "k",
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
                PaneInfo.NO_FIRING));

    assertThat(result.size(), equalTo(2));

    WindowedValue<KV<String, Iterable<String>>> item0 = result.get(0);
    assertThat(item0.getValue().getValue(), containsInAnyOrder("v1", "v2"));
    assertThat(item0.getWindows(), contains(window(0, 15)));
    assertThat(item0.getTimestamp(),
        equalTo(Iterables.getOnlyElement(item0.getWindows()).maxTimestamp()));

    WindowedValue<KV<String, Iterable<String>>> item1 = result.get(1);
    assertThat(item1.getValue().getValue(), contains("v3"));
    assertThat(item1.getWindows(), contains(window(15, 25)));
    assertThat(item1.getTimestamp(),
        equalTo(Iterables.getOnlyElement(item1.getWindows()).maxTimestamp()));
  }

  /**
   * Tests that the given GABW implementation correctly groups elements into merged sessions
   * with output timestamps at the end of the merged window.
   */
  public static void groupsElementsInMergedSessionsWithLatestTimestamp(
      GroupAlsoByWindowsDoFnFactory<String, String, Iterable<String>> gabwFactory)
          throws Exception {

    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(Sessions.withGapDuration(Duration.millis(10)))
            .withOutputTimeFn(OutputTimeFns.outputAtLatestInputTimestamp());

    List<WindowedValue<KV<String, Iterable<String>>>> result =
        runGABW(gabwFactory, windowingStrategy, "k",
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
                PaneInfo.NO_FIRING));

    assertThat(result.size(), equalTo(2));

    WindowedValue<KV<String, Iterable<String>>> item0 = result.get(0);
    assertThat(item0.getValue().getValue(), containsInAnyOrder("v1", "v2"));
    assertThat(item0.getWindows(), contains(window(0, 15)));
    assertThat(item0.getTimestamp(), equalTo(new Instant(5)));

    WindowedValue<KV<String, Iterable<String>>> item1 = result.get(1);
    assertThat(item1.getValue().getValue(), contains("v3"));
    assertThat(item1.getWindows(), contains(window(15, 25)));
    assertThat(item1.getTimestamp(), equalTo(new Instant(15)));
  }

  /**
   * Tests that the given {@link GroupAlsoByWindowsDoFn} implementation combines elements per
   * session window correctly according to the provided {@link CombineFn}.
   */
  public static void combinesElementsPerSessionWithEndOfWindowTimestamp(
      GroupAlsoByWindowsDoFnFactory<String, Long, Long> gabwFactory,
      CombineFn<Long, ?, Long> combineFn)
          throws Exception {

    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(Sessions.withGapDuration(Duration.millis(10)))
        .withOutputTimeFn(OutputTimeFns.outputAtEndOfWindow());


    List<WindowedValue<KV<String, Long>>> result =
        runGABW(gabwFactory, windowingStrategy, "k",
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
                PaneInfo.NO_FIRING));

    assertThat(result.size(), equalTo(2));

    WindowedValue<KV<String, Long>> item0 = result.get(0);
    assertThat(item0.getValue().getValue(), equalTo(combineFn.apply(ImmutableList.of(1L, 2L))));
    assertThat(item0.getWindows(), contains(window(0, 15)));
    assertThat(item0.getTimestamp(),
        equalTo(Iterables.getOnlyElement(item0.getWindows()).maxTimestamp()));

    WindowedValue<KV<String, Long>> item1 = result.get(1);
    assertThat(item1.getValue().getValue(), equalTo(combineFn.apply(ImmutableList.of(4L))));
    assertThat(item1.getWindows(), contains(window(15, 25)));
    assertThat(item1.getTimestamp(),
        equalTo(Iterables.getOnlyElement(item1.getWindows()).maxTimestamp()));
  }

  @SafeVarargs
  private static <K, InputT, OutputT, W extends BoundedWindow>
  List<WindowedValue<KV<K, OutputT>>> runGABW(
      GroupAlsoByWindowsDoFnFactory<K, InputT, OutputT> gabwFactory,
      WindowingStrategy<?, W> windowingStrategy,
      K key,
      WindowedValue<InputT>... values) {
    return runGABW(gabwFactory, windowingStrategy, key, Arrays.asList(values));
  }

  private static <K, InputT, OutputT, W extends BoundedWindow>
  List<WindowedValue<KV<K, OutputT>>> runGABW(
      GroupAlsoByWindowsDoFnFactory<K, InputT, OutputT> gabwFactory,
      WindowingStrategy<?, W> windowingStrategy,
      K key,
      Collection<WindowedValue<InputT>> values) {

    TupleTag<KV<K, OutputT>> outputTag = new TupleTag<>();
    DoFnRunnerBase.ListOutputManager outputManager = new DoFnRunnerBase.ListOutputManager();

    DoFnRunner<KV<K, Iterable<WindowedValue<InputT>>>, KV<K, OutputT>> runner =
        makeRunner(
            gabwFactory.forStrategy(windowingStrategy),
            windowingStrategy,
            outputTag,
            outputManager);

    runner.startBundle();

    if (values.size() > 0) {
      runner.processElement(WindowedValue.valueInEmptyWindows(
          KV.of(key, (Iterable<WindowedValue<InputT>>) values)));
    }

    runner.finishBundle();

    List<WindowedValue<KV<K, OutputT>>> result = outputManager.getOutput(outputTag);

    // Sanity check for corruption
    for (WindowedValue<KV<K, OutputT>> elem : result) {
      assertThat(elem.getValue().getKey(), equalTo(key));
    }

    return result;
  }

  private static <K, InputT, OutputT, W extends BoundedWindow>
  DoFnRunner<KV<K, Iterable<WindowedValue<InputT>>>, KV<K, OutputT>>
      makeRunner(
          GroupAlsoByWindowsDoFn<K, InputT, OutputT, W> fn,
          WindowingStrategy<?, W> windowingStrategy,
          TupleTag<KV<K, OutputT>> outputTag,
          DoFnRunners.OutputManager outputManager) {

    ExecutionContext executionContext = DirectModeExecutionContext.create();
    CounterSet counters = new CounterSet();

    return DoFnRunners.simpleRunner(
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
