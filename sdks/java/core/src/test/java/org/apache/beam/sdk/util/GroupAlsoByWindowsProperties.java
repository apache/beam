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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFns;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Properties of {@link GroupAlsoByWindowsDoFn}.
 *
 * <p>Some properties may not hold of some implementations, due to restrictions on the context
 * in which the implementation is applicable. For example, some {@code GroupAlsoByWindows} may not
 * support merging windows.
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

    DoFnTester<KV<K, Iterable<WindowedValue<InputT>>>, KV<K, OutputT>> result = runGABW(
        gabwFactory,
        windowingStrategy,
        (K) null, // key should never be used
        Collections.<WindowedValue<InputT>>emptyList());

    assertThat(result.peekOutputElements(), hasSize(0));
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

    DoFnTester<KV<String, Iterable<WindowedValue<String>>>, KV<String, Iterable<String>>> result =
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

    assertThat(result.peekOutputElements(), hasSize(2));

    TimestampedValue<KV<String, Iterable<String>>> item0 =
        Iterables.getOnlyElement(result.peekOutputElementsInWindow(window(0, 10)));
    assertThat(item0.getValue().getValue(), containsInAnyOrder("v1", "v2"));
    assertThat(item0.getTimestamp(), equalTo(window(0, 10).maxTimestamp()));

    TimestampedValue<KV<String, Iterable<String>>> item1 =
        Iterables.getOnlyElement(result.peekOutputElementsInWindow(window(10, 20)));
    assertThat(item1.getValue().getValue(), contains("v3"));
    assertThat(item1.getTimestamp(), equalTo(window(10, 20).maxTimestamp()));
  }

  /**
   * Tests that for a simple sequence of elements on the same key, the given GABW implementation
   * correctly groups them into sliding windows.
   *
   * <p>In the input here, each element occurs in multiple windows.
   */
  public static void groupsElementsIntoSlidingWindowsWithMinTimestamp(
      GroupAlsoByWindowsDoFnFactory<String, String, Iterable<String>> gabwFactory)
          throws Exception {

    WindowingStrategy<?, IntervalWindow> windowingStrategy = WindowingStrategy.of(
        SlidingWindows.of(Duration.millis(20)).every(Duration.millis(10)))
        .withOutputTimeFn(OutputTimeFns.outputAtEarliestInputTimestamp());

    DoFnTester<KV<String, Iterable<WindowedValue<String>>>, KV<String, Iterable<String>>> result =
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

    assertThat(result.peekOutputElements(), hasSize(3));

    TimestampedValue<KV<String, Iterable<String>>> item0 =
        Iterables.getOnlyElement(result.peekOutputElementsInWindow(window(-10, 10)));
    assertThat(item0.getValue().getValue(), contains("v1"));
    assertThat(item0.getTimestamp(), equalTo(new Instant(5)));

    TimestampedValue<KV<String, Iterable<String>>> item1 =
        Iterables.getOnlyElement(result.peekOutputElementsInWindow(window(0, 20)));
    assertThat(item1.getValue().getValue(), containsInAnyOrder("v1", "v2"));
    assertThat(item1.getTimestamp(), equalTo(new Instant(10)));

    TimestampedValue<KV<String, Iterable<String>>> item2 =
        Iterables.getOnlyElement(result.peekOutputElementsInWindow(window(10, 30)));
    assertThat(item2.getValue().getValue(), contains("v2"));
    assertThat(item2.getTimestamp(), equalTo(new Instant(20)));
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

    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(SlidingWindows.of(Duration.millis(20)).every(Duration.millis(10)))
            .withOutputTimeFn(OutputTimeFns.outputAtEarliestInputTimestamp());

    DoFnTester<KV<String, Iterable<WindowedValue<Long>>>, KV<String, Long>> result =
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

    assertThat(result.peekOutputElements(), hasSize(3));

    TimestampedValue<KV<String, Long>> item0 =
        Iterables.getOnlyElement(result.peekOutputElementsInWindow(window(-10, 10)));
    assertThat(item0.getValue().getKey(), equalTo("k"));
    assertThat(item0.getValue().getValue(), equalTo(combineFn.apply(ImmutableList.of(1L))));
    assertThat(item0.getTimestamp(), equalTo(new Instant(5L)));

    TimestampedValue<KV<String, Long>> item1 =
        Iterables.getOnlyElement(result.peekOutputElementsInWindow(window(0, 20)));
    assertThat(item1.getValue().getKey(), equalTo("k"));
    assertThat(item1.getValue().getValue(), equalTo(combineFn.apply(ImmutableList.of(1L, 2L, 4L))));
    assertThat(item1.getTimestamp(), equalTo(new Instant(5L)));

    TimestampedValue<KV<String, Long>> item2 =
        Iterables.getOnlyElement(result.peekOutputElementsInWindow(window(10, 30)));
    assertThat(item2.getValue().getKey(), equalTo("k"));
    assertThat(item2.getValue().getValue(), equalTo(combineFn.apply(ImmutableList.of(2L, 4L))));
    assertThat(item2.getTimestamp(), equalTo(new Instant(15L)));
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

    DoFnTester<KV<String, Iterable<WindowedValue<String>>>, KV<String, Iterable<String>>> result =
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

    assertThat(result.peekOutputElements(), hasSize(2));

    TimestampedValue<KV<String, Iterable<String>>> item0 =
        Iterables.getOnlyElement(result.peekOutputElementsInWindow(window(0, 5)));
    assertThat(item0.getValue().getValue(), containsInAnyOrder("v1", "v3"));
    assertThat(item0.getTimestamp(), equalTo(window(1, 5).maxTimestamp()));

    TimestampedValue<KV<String, Iterable<String>>> item1 =
        Iterables.getOnlyElement(result.peekOutputElementsInWindow(window(1, 5)));
    assertThat(item1.getValue().getValue(), contains("v2"));
    assertThat(item1.getTimestamp(), equalTo(window(0, 5).maxTimestamp()));
  }

  /**
   * Tests that the given GABW implementation correctly groups elements into merged sessions.
   */
  public static void groupsElementsInMergedSessions(
      GroupAlsoByWindowsDoFnFactory<String, String, Iterable<String>> gabwFactory)
          throws Exception {

    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(Sessions.withGapDuration(Duration.millis(10)));

    DoFnTester<KV<String, Iterable<WindowedValue<String>>>, KV<String, Iterable<String>>> result =
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

    assertThat(result.peekOutputElements(), hasSize(2));

    TimestampedValue<KV<String, Iterable<String>>> item0 =
        Iterables.getOnlyElement(result.peekOutputElementsInWindow(window(0, 15)));
    assertThat(item0.getValue().getValue(), containsInAnyOrder("v1", "v2"));
    assertThat(item0.getTimestamp(), equalTo(window(0, 15).maxTimestamp()));

    TimestampedValue<KV<String, Iterable<String>>> item1 =
        Iterables.getOnlyElement(result.peekOutputElementsInWindow(window(15, 25)));
    assertThat(item1.getValue().getValue(), contains("v3"));
    assertThat(item1.getTimestamp(), equalTo(window(15, 25).maxTimestamp()));
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

    DoFnTester<KV<String, Iterable<WindowedValue<Long>>>, KV<String, Long>> result =
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

    assertThat(result.peekOutputElements(), hasSize(2));

    TimestampedValue<KV<String, Long>> item0 =
        Iterables.getOnlyElement(result.peekOutputElementsInWindow(window(0, 15)));
    assertThat(item0.getValue().getKey(), equalTo("k"));
    assertThat(item0.getValue().getValue(), equalTo(combineFn.apply(ImmutableList.of(1L, 2L))));
    assertThat(item0.getTimestamp(), equalTo(window(0, 15).maxTimestamp()));

    TimestampedValue<KV<String, Long>> item1 =
        Iterables.getOnlyElement(result.peekOutputElementsInWindow(window(15, 25)));
    assertThat(item1.getValue().getKey(), equalTo("k"));
    assertThat(item1.getValue().getValue(), equalTo(combineFn.apply(ImmutableList.of(4L))));
    assertThat(item1.getTimestamp(), equalTo(window(15, 25).maxTimestamp()));
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

    DoFnTester<KV<String, Iterable<WindowedValue<String>>>, KV<String, Iterable<String>>> result =
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

    assertThat(result.peekOutputElements(), hasSize(2));

    TimestampedValue<KV<String, Iterable<String>>> item0 =
        Iterables.getOnlyElement(result.peekOutputElementsInWindow(window(0, 10)));
    assertThat(item0.getValue().getValue(), containsInAnyOrder("v1", "v2"));
    assertThat(item0.getTimestamp(), equalTo(window(0, 10).maxTimestamp()));

    TimestampedValue<KV<String, Iterable<String>>> item1 =
        Iterables.getOnlyElement(result.peekOutputElementsInWindow(window(10, 20)));
    assertThat(item1.getValue().getValue(), contains("v3"));
    assertThat(item1.getTimestamp(), equalTo(window(10, 20).maxTimestamp()));
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

    DoFnTester<KV<String, Iterable<WindowedValue<String>>>, KV<String, Iterable<String>>> result =
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

    assertThat(result.peekOutputElements(), hasSize(2));

    TimestampedValue<KV<String, Iterable<String>>> item0 =
        Iterables.getOnlyElement(result.peekOutputElementsInWindow(window(0, 10)));
    assertThat(item0.getValue().getValue(), containsInAnyOrder("v1", "v2"));
    assertThat(item0.getTimestamp(), equalTo(new Instant(2)));

    TimestampedValue<KV<String, Iterable<String>>> item1 =
        Iterables.getOnlyElement(result.peekOutputElementsInWindow(window(10, 20)));
    assertThat(item1.getValue().getValue(), contains("v3"));
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

    DoFnTester<KV<String, Iterable<WindowedValue<String>>>, KV<String, Iterable<String>>> result =
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

    assertThat(result.peekOutputElements(), hasSize(2));

    TimestampedValue<KV<String, Iterable<String>>> item0 =
        Iterables.getOnlyElement(result.peekOutputElementsInWindow(window(0, 15)));
    assertThat(item0.getValue().getValue(), containsInAnyOrder("v1", "v2"));
    assertThat(item0.getTimestamp(), equalTo(window(0, 15).maxTimestamp()));

    TimestampedValue<KV<String, Iterable<String>>> item1 =
        Iterables.getOnlyElement(result.peekOutputElementsInWindow(window(15, 25)));
    assertThat(item1.getValue().getValue(), contains("v3"));
    assertThat(item1.getTimestamp(), equalTo(window(15, 25).maxTimestamp()));
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

    BoundedWindow unmergedWindow = window(15, 25);
    DoFnTester<KV<String, Iterable<WindowedValue<String>>>, KV<String, Iterable<String>>> result =
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
                Arrays.asList(unmergedWindow),
                PaneInfo.NO_FIRING));

    assertThat(result.peekOutputElements(), hasSize(2));

    BoundedWindow mergedWindow = window(0, 15);
    TimestampedValue<KV<String, Iterable<String>>> item0 =
        Iterables.getOnlyElement(result.peekOutputElementsInWindow(mergedWindow));
    assertThat(item0.getValue().getValue(), containsInAnyOrder("v1", "v2"));
    assertThat(item0.getTimestamp(), equalTo(new Instant(5)));

    TimestampedValue<KV<String, Iterable<String>>> item1 =
        Iterables.getOnlyElement(result.peekOutputElementsInWindow(unmergedWindow));
    assertThat(item1.getValue().getValue(), contains("v3"));
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

    BoundedWindow secondWindow = window(15, 25);
    DoFnTester<?, KV<String, Long>> result =
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
                Arrays.asList(secondWindow),
                PaneInfo.NO_FIRING));

    assertThat(result.peekOutputElements(), hasSize(2));

    BoundedWindow firstResultWindow = window(0, 15);
    TimestampedValue<KV<String, Long>> item0 =
        Iterables.getOnlyElement(result.peekOutputElementsInWindow(firstResultWindow));
    assertThat(item0.getValue().getValue(), equalTo(combineFn.apply(ImmutableList.of(1L, 2L))));
    assertThat(item0.getTimestamp(), equalTo(firstResultWindow.maxTimestamp()));

    TimestampedValue<KV<String, Long>> item1 =
        Iterables.getOnlyElement(result.peekOutputElementsInWindow(secondWindow));
    assertThat(item1.getValue().getValue(), equalTo(combineFn.apply(ImmutableList.of(4L))));
    assertThat(item1.getTimestamp(),
        equalTo(secondWindow.maxTimestamp()));
  }

  @SafeVarargs
  private static <K, InputT, OutputT, W extends BoundedWindow>
  DoFnTester<KV<K, Iterable<WindowedValue<InputT>>>, KV<K, OutputT>> runGABW(
      GroupAlsoByWindowsDoFnFactory<K, InputT, OutputT> gabwFactory,
      WindowingStrategy<?, W> windowingStrategy,
      K key,
      WindowedValue<InputT>... values) throws Exception {
    return runGABW(gabwFactory, windowingStrategy, key, Arrays.asList(values));
  }

  private static <K, InputT, OutputT, W extends BoundedWindow>
  DoFnTester<KV<K, Iterable<WindowedValue<InputT>>>, KV<K, OutputT>> runGABW(
      GroupAlsoByWindowsDoFnFactory<K, InputT, OutputT> gabwFactory,
      WindowingStrategy<?, W> windowingStrategy,
      K key,
      Collection<WindowedValue<InputT>> values) throws Exception {

    TupleTag<KV<K, OutputT>> outputTag = new TupleTag<>();
    DoFnRunnerBase.ListOutputManager outputManager = new DoFnRunnerBase.ListOutputManager();

    DoFnTester<KV<K, Iterable<WindowedValue<InputT>>>, KV<K, OutputT>> tester =
        DoFnTester.of(gabwFactory.forStrategy(windowingStrategy));
    tester.startBundle();
    tester.processElement(KV.<K, Iterable<WindowedValue<InputT>>>of(key, values));
    tester.finishBundle();

    // Sanity check for corruption
    for (KV<K, OutputT> elem : tester.peekOutputElements()) {
      assertThat(elem.getKey(), equalTo(key));
    }

    return tester;
  }

  private static BoundedWindow window(long start, long end) {
    return new IntervalWindow(new Instant(start), new Instant(end));
  }

}
