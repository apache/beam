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
package org.apache.beam.runners.core;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

import com.google.common.base.Predicate;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Properties of {@link GroupAlsoByWindowsDoFn}.
 *
 * <p>Some properties may not hold of some implementations, due to restrictions on the context in
 * which the implementation is applicable. For example, some {@code GroupAlsoByWindows} may not
 * support merging windows.
 */
public class GroupAlsoByWindowsProperties {

  /**
   * A factory of {@link GroupAlsoByWindowsDoFn} so that the various properties can provide the
   * appropriate windowing strategy under test.
   */
  public interface GroupAlsoByWindowsDoFnFactory<K, InputT, OutputT> {
    <W extends BoundedWindow> GroupAlsoByWindowsDoFn<K, InputT, OutputT, W> forStrategy(
        WindowingStrategy<?, W> strategy, StateInternalsFactory<K> stateInternalsFactory);
  }

  /**
   * Tests that for empty input and the given {@link WindowingStrategy}, the provided GABW
   * implementation produces no output.
   *
   * <p>The input type is deliberately left as a wildcard, since it is not relevant.
   */
  public static <K, InputT, OutputT> void emptyInputEmptyOutput(
      GroupAlsoByWindowsDoFnFactory<K, InputT, OutputT> gabwFactory) throws Exception {

    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)));

    // This key should never actually be used, though it is eagerly passed to the
    // StateInternalsFactory so must be non-null
    @SuppressWarnings("unchecked")
    K fakeKey = (K) "this key should never be used";

    List<WindowedValue<KV<K, OutputT>>> result =
        runGABW(
            gabwFactory,
            windowingStrategy,
            fakeKey,
            Collections.<WindowedValue<InputT>>emptyList());

    assertThat(result, hasSize(0));
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
        runGABW(
            gabwFactory,
            windowingStrategy,
            "key",
            WindowedValue.of(
                "v1", new Instant(1), Arrays.asList(window(0, 10)), PaneInfo.NO_FIRING),
            WindowedValue.of(
                "v2", new Instant(2), Arrays.asList(window(0, 10)), PaneInfo.NO_FIRING),
            WindowedValue.of(
                "v3", new Instant(13), Arrays.asList(window(10, 20)), PaneInfo.NO_FIRING));

    assertThat(result, hasSize(2));

    TimestampedValue<KV<String, Iterable<String>>> item0 =
        getOnlyElementInWindow(result, window(0, 10));
    assertThat(item0.getValue().getValue(), containsInAnyOrder("v1", "v2"));
    assertThat(item0.getTimestamp(), equalTo(window(0, 10).maxTimestamp()));

    TimestampedValue<KV<String, Iterable<String>>> item1 =
        getOnlyElementInWindow(result, window(10, 20));
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

    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(SlidingWindows.of(Duration.millis(20)).every(Duration.millis(10)))
            .withTimestampCombiner(TimestampCombiner.EARLIEST);

    List<WindowedValue<KV<String, Iterable<String>>>> result =
        runGABW(
            gabwFactory,
            windowingStrategy,
            "key",
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

    assertThat(result, hasSize(3));

    TimestampedValue<KV<String, Iterable<String>>> item0 =
        getOnlyElementInWindow(result, window(-10, 10));
    assertThat(item0.getValue().getValue(), contains("v1"));
    assertThat(item0.getTimestamp(), equalTo(new Instant(5)));

    TimestampedValue<KV<String, Iterable<String>>> item1 =
        getOnlyElementInWindow(result, window(0, 20));
    assertThat(item1.getValue().getValue(), containsInAnyOrder("v1", "v2"));
    // Timestamp adjusted by WindowFn to exceed the end of the prior sliding window
    assertThat(item1.getTimestamp(), equalTo(new Instant(10)));

    TimestampedValue<KV<String, Iterable<String>>> item2 =
        getOnlyElementInWindow(result, window(10, 30));
    assertThat(item2.getValue().getValue(), contains("v2"));
    // Timestamp adjusted by WindowFn to exceed the end of the prior sliding window
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
            .withTimestampCombiner(TimestampCombiner.EARLIEST);

    List<WindowedValue<KV<String, Long>>> result =
        runGABW(
            gabwFactory,
            windowingStrategy,
            "k",
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

    assertThat(result, hasSize(3));

    TimestampedValue<KV<String, Long>> item0 = getOnlyElementInWindow(result, window(-10, 10));
    assertThat(item0.getValue().getKey(), equalTo("k"));
    assertThat(item0.getValue().getValue(), equalTo(combineFn.apply(ImmutableList.of(1L))));
    assertThat(item0.getTimestamp(), equalTo(new Instant(5L)));

    TimestampedValue<KV<String, Long>> item1 = getOnlyElementInWindow(result, window(0, 20));
    assertThat(item1.getValue().getKey(), equalTo("k"));
    assertThat(item1.getValue().getValue(), equalTo(combineFn.apply(ImmutableList.of(1L, 2L, 4L))));
    // Timestamp adjusted by WindowFn to exceed the end of the prior sliding window
    assertThat(item1.getTimestamp(), equalTo(new Instant(10L)));

    TimestampedValue<KV<String, Long>> item2 = getOnlyElementInWindow(result, window(10, 30));
    assertThat(item2.getValue().getKey(), equalTo("k"));
    assertThat(item2.getValue().getValue(), equalTo(combineFn.apply(ImmutableList.of(2L, 4L))));
    // Timestamp adjusted by WindowFn to exceed the end of the prior sliding window
    assertThat(item2.getTimestamp(), equalTo(new Instant(20L)));
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
        runGABW(
            gabwFactory,
            windowingStrategy,
            "key",
            WindowedValue.of("v1", new Instant(1), Arrays.asList(window(0, 5)), PaneInfo.NO_FIRING),
            WindowedValue.of("v2", new Instant(4), Arrays.asList(window(1, 5)), PaneInfo.NO_FIRING),
            WindowedValue.of(
                "v3", new Instant(4), Arrays.asList(window(0, 5)), PaneInfo.NO_FIRING));

    assertThat(result, hasSize(2));

    TimestampedValue<KV<String, Iterable<String>>> item0 =
        getOnlyElementInWindow(result, window(0, 5));
    assertThat(item0.getValue().getValue(), containsInAnyOrder("v1", "v3"));
    assertThat(item0.getTimestamp(), equalTo(window(1, 5).maxTimestamp()));

    TimestampedValue<KV<String, Iterable<String>>> item1 =
        getOnlyElementInWindow(result, window(1, 5));
    assertThat(item1.getValue().getValue(), contains("v2"));
    assertThat(item1.getTimestamp(), equalTo(window(0, 5).maxTimestamp()));
  }

  /** Tests that the given GABW implementation correctly groups elements into merged sessions. */
  public static void groupsElementsInMergedSessions(
      GroupAlsoByWindowsDoFnFactory<String, String, Iterable<String>> gabwFactory)
      throws Exception {

    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(Sessions.withGapDuration(Duration.millis(10)));

    List<WindowedValue<KV<String, Iterable<String>>>> result =
        runGABW(
            gabwFactory,
            windowingStrategy,
            "key",
            WindowedValue.of(
                "v1", new Instant(0), Arrays.asList(window(0, 10)), PaneInfo.NO_FIRING),
            WindowedValue.of(
                "v2", new Instant(5), Arrays.asList(window(5, 15)), PaneInfo.NO_FIRING),
            WindowedValue.of(
                "v3", new Instant(15), Arrays.asList(window(15, 25)), PaneInfo.NO_FIRING));

    assertThat(result, hasSize(2));

    TimestampedValue<KV<String, Iterable<String>>> item0 =
        getOnlyElementInWindow(result, window(0, 15));
    assertThat(item0.getValue().getValue(), containsInAnyOrder("v1", "v2"));
    assertThat(item0.getTimestamp(), equalTo(window(0, 15).maxTimestamp()));

    TimestampedValue<KV<String, Iterable<String>>> item1 =
        getOnlyElementInWindow(result, window(15, 25));
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

    List<WindowedValue<KV<String, Long>>> result =
        runGABW(
            gabwFactory,
            windowingStrategy,
            "k",
            WindowedValue.of(1L, new Instant(0), Arrays.asList(window(0, 10)), PaneInfo.NO_FIRING),
            WindowedValue.of(2L, new Instant(5), Arrays.asList(window(5, 15)), PaneInfo.NO_FIRING),
            WindowedValue.of(
                4L, new Instant(15), Arrays.asList(window(15, 25)), PaneInfo.NO_FIRING));

    assertThat(result, hasSize(2));

    TimestampedValue<KV<String, Long>> item0 = getOnlyElementInWindow(result, window(0, 15));
    assertThat(item0.getValue().getKey(), equalTo("k"));
    assertThat(item0.getValue().getValue(), equalTo(combineFn.apply(ImmutableList.of(1L, 2L))));
    assertThat(item0.getTimestamp(), equalTo(window(0, 15).maxTimestamp()));

    TimestampedValue<KV<String, Long>> item1 = getOnlyElementInWindow(result, window(15, 25));
    assertThat(item1.getValue().getKey(), equalTo("k"));
    assertThat(item1.getValue().getValue(), equalTo(combineFn.apply(ImmutableList.of(4L))));
    assertThat(item1.getTimestamp(), equalTo(window(15, 25).maxTimestamp()));
  }

  /**
   * Tests that for a simple sequence of elements on the same key, the given GABW implementation
   * correctly groups them according to fixed windows and also sets the output timestamp according
   * to the policy {@link TimestampCombiner#END_OF_WINDOW}.
   */
  public static void groupsElementsIntoFixedWindowsWithEndOfWindowTimestamp(
      GroupAlsoByWindowsDoFnFactory<String, String, Iterable<String>> gabwFactory)
      throws Exception {

    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)))
            .withTimestampCombiner(TimestampCombiner.END_OF_WINDOW);

    List<WindowedValue<KV<String, Iterable<String>>>> result =
        runGABW(
            gabwFactory,
            windowingStrategy,
            "key",
            WindowedValue.of(
                "v1", new Instant(1), Arrays.asList(window(0, 10)), PaneInfo.NO_FIRING),
            WindowedValue.of(
                "v2", new Instant(2), Arrays.asList(window(0, 10)), PaneInfo.NO_FIRING),
            WindowedValue.of(
                "v3", new Instant(13), Arrays.asList(window(10, 20)), PaneInfo.NO_FIRING));

    assertThat(result, hasSize(2));

    TimestampedValue<KV<String, Iterable<String>>> item0 =
        getOnlyElementInWindow(result, window(0, 10));
    assertThat(item0.getValue().getValue(), containsInAnyOrder("v1", "v2"));
    assertThat(item0.getTimestamp(), equalTo(window(0, 10).maxTimestamp()));

    TimestampedValue<KV<String, Iterable<String>>> item1 =
        getOnlyElementInWindow(result, window(10, 20));
    assertThat(item1.getValue().getValue(), contains("v3"));
    assertThat(item1.getTimestamp(), equalTo(window(10, 20).maxTimestamp()));
  }

  /**
   * Tests that for a simple sequence of elements on the same key, the given GABW implementation
   * correctly groups them according to fixed windows and also sets the output timestamp according
   * to the policy {@link TimestampCombiner#LATEST}.
   */
  public static void groupsElementsIntoFixedWindowsWithLatestTimestamp(
      GroupAlsoByWindowsDoFnFactory<String, String, Iterable<String>> gabwFactory)
      throws Exception {

    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)))
            .withTimestampCombiner(TimestampCombiner.LATEST);

    List<WindowedValue<KV<String, Iterable<String>>>> result =
        runGABW(
            gabwFactory,
            windowingStrategy,
            "k",
            WindowedValue.of(
                "v1", new Instant(1), Arrays.asList(window(0, 10)), PaneInfo.NO_FIRING),
            WindowedValue.of(
                "v2", new Instant(2), Arrays.asList(window(0, 10)), PaneInfo.NO_FIRING),
            WindowedValue.of(
                "v3", new Instant(13), Arrays.asList(window(10, 20)), PaneInfo.NO_FIRING));

    assertThat(result, hasSize(2));

    TimestampedValue<KV<String, Iterable<String>>> item0 =
        getOnlyElementInWindow(result, window(0, 10));
    assertThat(item0.getValue().getValue(), containsInAnyOrder("v1", "v2"));
    assertThat(item0.getTimestamp(), equalTo(new Instant(2)));

    TimestampedValue<KV<String, Iterable<String>>> item1 =
        getOnlyElementInWindow(result, window(10, 20));
    assertThat(item1.getValue().getValue(), contains("v3"));
    assertThat(item1.getTimestamp(), equalTo(new Instant(13)));
  }

  /**
   * Tests that the given GABW implementation correctly groups elements into merged sessions with
   * output timestamps at the end of the merged window.
   */
  public static void groupsElementsInMergedSessionsWithEndOfWindowTimestamp(
      GroupAlsoByWindowsDoFnFactory<String, String, Iterable<String>> gabwFactory)
      throws Exception {

    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(Sessions.withGapDuration(Duration.millis(10)))
            .withTimestampCombiner(TimestampCombiner.END_OF_WINDOW);

    List<WindowedValue<KV<String, Iterable<String>>>> result =
        runGABW(
            gabwFactory,
            windowingStrategy,
            "k",
            WindowedValue.of(
                "v1", new Instant(0), Arrays.asList(window(0, 10)), PaneInfo.NO_FIRING),
            WindowedValue.of(
                "v2", new Instant(5), Arrays.asList(window(5, 15)), PaneInfo.NO_FIRING),
            WindowedValue.of(
                "v3", new Instant(15), Arrays.asList(window(15, 25)), PaneInfo.NO_FIRING));

    assertThat(result, hasSize(2));

    TimestampedValue<KV<String, Iterable<String>>> item0 =
        getOnlyElementInWindow(result, window(0, 15));
    assertThat(item0.getValue().getValue(), containsInAnyOrder("v1", "v2"));
    assertThat(item0.getTimestamp(), equalTo(window(0, 15).maxTimestamp()));

    TimestampedValue<KV<String, Iterable<String>>> item1 =
        getOnlyElementInWindow(result, window(15, 25));
    assertThat(item1.getValue().getValue(), contains("v3"));
    assertThat(item1.getTimestamp(), equalTo(window(15, 25).maxTimestamp()));
  }

  /**
   * Tests that the given GABW implementation correctly groups elements into merged sessions with
   * output timestamps at the end of the merged window.
   */
  public static void groupsElementsInMergedSessionsWithLatestTimestamp(
      GroupAlsoByWindowsDoFnFactory<String, String, Iterable<String>> gabwFactory)
      throws Exception {

    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(Sessions.withGapDuration(Duration.millis(10)))
            .withTimestampCombiner(TimestampCombiner.LATEST);

    BoundedWindow unmergedWindow = window(15, 25);
    List<WindowedValue<KV<String, Iterable<String>>>> result =
        runGABW(
            gabwFactory,
            windowingStrategy,
            "k",
            WindowedValue.of(
                "v1", new Instant(0), Arrays.asList(window(0, 10)), PaneInfo.NO_FIRING),
            WindowedValue.of(
                "v2", new Instant(5), Arrays.asList(window(5, 15)), PaneInfo.NO_FIRING),
            WindowedValue.of(
                "v3", new Instant(15), Arrays.asList(unmergedWindow), PaneInfo.NO_FIRING));

    assertThat(result, hasSize(2));

    BoundedWindow mergedWindow = window(0, 15);
    TimestampedValue<KV<String, Iterable<String>>> item0 =
        getOnlyElementInWindow(result, mergedWindow);
    assertThat(item0.getValue().getValue(), containsInAnyOrder("v1", "v2"));
    assertThat(item0.getTimestamp(), equalTo(new Instant(5)));

    TimestampedValue<KV<String, Iterable<String>>> item1 =
        getOnlyElementInWindow(result, unmergedWindow);
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
            .withTimestampCombiner(TimestampCombiner.END_OF_WINDOW);

    BoundedWindow secondWindow = window(15, 25);
    List<WindowedValue<KV<String, Long>>> result =
        runGABW(
            gabwFactory,
            windowingStrategy,
            "k",
            WindowedValue.of(1L, new Instant(0), Arrays.asList(window(0, 10)), PaneInfo.NO_FIRING),
            WindowedValue.of(2L, new Instant(5), Arrays.asList(window(5, 15)), PaneInfo.NO_FIRING),
            WindowedValue.of(4L, new Instant(15), Arrays.asList(secondWindow), PaneInfo.NO_FIRING));

    assertThat(result, hasSize(2));

    BoundedWindow firstResultWindow = window(0, 15);
    TimestampedValue<KV<String, Long>> item0 = getOnlyElementInWindow(result, firstResultWindow);
    assertThat(item0.getValue().getValue(), equalTo(combineFn.apply(ImmutableList.of(1L, 2L))));
    assertThat(item0.getTimestamp(), equalTo(firstResultWindow.maxTimestamp()));

    TimestampedValue<KV<String, Long>> item1 = getOnlyElementInWindow(result, secondWindow);
    assertThat(item1.getValue().getValue(), equalTo(combineFn.apply(ImmutableList.of(4L))));
    assertThat(item1.getTimestamp(), equalTo(secondWindow.maxTimestamp()));
  }

  @SafeVarargs
  private static <K, InputT, OutputT, W extends BoundedWindow>
      List<WindowedValue<KV<K, OutputT>>> runGABW(
          GroupAlsoByWindowsDoFnFactory<K, InputT, OutputT> gabwFactory,
          WindowingStrategy<?, W> windowingStrategy,
          K key,
          WindowedValue<InputT>... values)
          throws Exception {
    return runGABW(gabwFactory, windowingStrategy, key, Arrays.asList(values));
  }

  private static <K, InputT, OutputT, W extends BoundedWindow>
      List<WindowedValue<KV<K, OutputT>>> runGABW(
          GroupAlsoByWindowsDoFnFactory<K, InputT, OutputT> gabwFactory,
          WindowingStrategy<?, W> windowingStrategy,
          K key,
          Collection<WindowedValue<InputT>> values)
          throws Exception {

    final StateInternalsFactory<K> stateInternalsCache = new CachingStateInternalsFactory<K>();

    List<WindowedValue<KV<K, OutputT>>> output =
        processElement(
            gabwFactory.forStrategy(windowingStrategy, stateInternalsCache),
            KV.<K, Iterable<WindowedValue<InputT>>>of(key, values));

    // Sanity check for corruption
    for (WindowedValue<KV<K, OutputT>> value : output) {
      assertThat(value.getValue().getKey(), equalTo(key));
    }

    return output;
  }

  private static BoundedWindow window(long start, long end) {
    return new IntervalWindow(new Instant(start), new Instant(end));
  }

  private static final class CachingStateInternalsFactory<K> implements StateInternalsFactory<K> {
    private final LoadingCache<K, StateInternals> stateInternalsCache;

    private CachingStateInternalsFactory() {
      this.stateInternalsCache = CacheBuilder.newBuilder().build(new StateInternalsLoader<K>());
    }

    @Override
    @SuppressWarnings("unchecked")
    public StateInternals stateInternalsForKey(K key) {
      try {
        return stateInternalsCache.get(key);
      } catch (Exception exc) {
        throw new RuntimeException(exc);
      }
    }
  }

  private static class StateInternalsLoader<K> extends CacheLoader<K, StateInternals> {
    @Override
    public StateInternals load(K key) throws Exception {
      return InMemoryStateInternals.forKey(key);
    }
  }

  private static <K, InputT, OutputT, W extends BoundedWindow>
      List<WindowedValue<KV<K, OutputT>>> processElement(
          GroupAlsoByWindowsDoFn<K, InputT, OutputT, W> fn,
          KV<K, Iterable<WindowedValue<InputT>>> element)
          throws Exception {
    TestProcessContext<K, InputT, OutputT, W> c = new TestProcessContext<>(fn, element);
    fn.processElement(c);
    return c.getOutput();
  }

  private static <K, OutputT> TimestampedValue<KV<K, OutputT>> getOnlyElementInWindow(
      List<WindowedValue<KV<K, OutputT>>> output, final BoundedWindow window) {
    WindowedValue<KV<K, OutputT>> res =
        Iterables.getOnlyElement(
            Iterables.filter(
                output,
                new Predicate<WindowedValue<KV<K, OutputT>>>() {
                  @Override
                  public boolean apply(@Nullable WindowedValue<KV<K, OutputT>> input) {
                    return input.getWindows().contains(window);
                  }
                }));
    return TimestampedValue.of(res.getValue(), res.getTimestamp());
  }

  /**
   * A {@link GroupAlsoByWindowsDoFn.ProcessContext} providing just enough context for a {@link
   * GroupAlsoByWindowsDoFn} - namely, information about the element and output via {@link
   * WindowingInternals}, but no side inputs/outputs and no normal output.
   */
  private static class TestProcessContext<K, InputT, OutputT, W extends BoundedWindow>
      extends GroupAlsoByWindowsDoFn<K, InputT, OutputT, W>.ProcessContext {
    private final PipelineOptions options = PipelineOptionsFactory.create();
    private final KV<K, Iterable<WindowedValue<InputT>>> element;
    private final List<WindowedValue<KV<K, OutputT>>> output = new ArrayList<>();

    private TestProcessContext(
        GroupAlsoByWindowsDoFn<K, InputT, OutputT, W> fn,
        KV<K, Iterable<WindowedValue<InputT>>> element) {
      fn.super();
      this.element = element;
    }

    @Override
    public KV<K, Iterable<WindowedValue<InputT>>> element() {
      return element;
    }

    @Override
    public Instant timestamp() {
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    @Override
    public BoundedWindow window() {
      return GlobalWindow.INSTANCE;
    }

    @Override
    public PaneInfo pane() {
      return PaneInfo.NO_FIRING;
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      throw new UnsupportedOperationException();
    }

    @Override
    public WindowingInternals<KV<K, Iterable<WindowedValue<InputT>>>, KV<K, OutputT>>
        windowingInternals() {
      return new WindowingInternals<KV<K, Iterable<WindowedValue<InputT>>>, KV<K, OutputT>>() {
        @Override
        public void outputWindowedValue(
            KV<K, OutputT> output,
            Instant timestamp,
            Collection<? extends BoundedWindow> windows,
            PaneInfo pane) {
          TestProcessContext.this.output.add(WindowedValue.of(output, timestamp, windows, pane));
        }

        @Override
        public <AdditionalOutputT> void outputWindowedValue(
            TupleTag<AdditionalOutputT> tag,
            AdditionalOutputT output,
            Instant timestamp,
            Collection<? extends BoundedWindow> windows,
            PaneInfo pane) {
          throw new UnsupportedOperationException();
        }

        @Override
        public StateInternals stateInternals() {
          throw new UnsupportedOperationException();
        }

        @Override
        public TimerInternals timerInternals() {
          throw new UnsupportedOperationException();
        }

        @Override
        public Collection<? extends BoundedWindow> windows() {
          return ImmutableList.of(GlobalWindow.INSTANCE);
        }

        @Override
        public PaneInfo pane() {
          return PaneInfo.NO_FIRING;
        }

        @Override
        public <T> T sideInput(PCollectionView<T> view, BoundedWindow sideInputWindow) {
          throw new UnsupportedOperationException();
        }
      };
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return options;
    }

    @Override
    public void output(KV<K, OutputT> output) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void outputWithTimestamp(KV<K, OutputT> output, Instant timestamp) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      throw new UnsupportedOperationException();
    }

    public List<WindowedValue<KV<K, OutputT>>> getOutput() {
      return output;
    }
  }
}
