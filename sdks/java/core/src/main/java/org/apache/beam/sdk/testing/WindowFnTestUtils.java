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
package org.apache.beam.sdk.testing;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Ordering;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;

/** A utility class for testing {@link WindowFn}s. */
public class WindowFnTestUtils {

  /** Creates a Set of elements to be used as expected output in {@link #runWindowFn}. */
  public static Set<String> set(long... timestamps) {
    Set<String> result = new HashSet<>();
    for (long timestamp : timestamps) {
      result.add(timestampValue(timestamp));
    }
    return result;
  }

  /**
   * Runs the {@link WindowFn} over the provided input, returning a map of windows to the timestamps
   * in those windows.
   */
  public static <T, W extends BoundedWindow> Map<W, Set<String>> runWindowFn(
      WindowFn<T, W> windowFn, List<Long> timestamps) throws Exception {
    List<TimestampedValue<T>> timestampedValues = new ArrayList<>();
    for (Long timestamp : timestamps) {
      timestampedValues.add(TimestampedValue.of((T) null, new Instant(timestamp)));
    }
    return runWindowFnWithValue(windowFn, timestampedValues);
  }

  /**
   * Runs the {@link WindowFn} over the provided input, returning a map of windows to the timestamps
   * in those windows. This version allows to pass a list of {@link TimestampedValue} in case the
   * values are used to assign windows.
   */
  public static <T, W extends BoundedWindow> Map<W, Set<String>> runWindowFnWithValue(
      WindowFn<T, W> windowFn, List<TimestampedValue<T>> timestampedValues) throws Exception {

    final TestWindowSet<W, String> windowSet = new TestWindowSet<>();
    for (final TimestampedValue<T> element : timestampedValues) {
      for (W window : assignedWindowsWithValue(windowFn, element)) {
        windowSet.put(window, timestampValue(element.getTimestamp().getMillis()));
      }
      TestMergeContext<T, W> mergeContext = new TestMergeContext<>(windowSet, windowFn);
      windowFn.mergeWindows(mergeContext);
    }
    Map<W, Set<String>> actual = new HashMap<>();
    for (W window : windowSet.windows()) {
      actual.put(window, windowSet.get(window));
    }
    return actual;
  }

  /** runs {@link WindowFn#assignWindows(WindowFn.AssignContext)}. */
  public static <T, W extends BoundedWindow> Collection<W> assignedWindows(
      WindowFn<T, W> windowFn, long timestamp) throws Exception {
    return assignedWindowsWithValue(
        windowFn, TimestampedValue.of((T) null, new Instant(timestamp)));
  }

  /**
   * runs {@link WindowFn#assignWindows(WindowFn.AssignContext)}. This version allows passing a
   * {@link TimestampedValue} in case the value is needed to assign windows.
   */
  public static <T, W extends BoundedWindow> Collection<W> assignedWindowsWithValue(
      WindowFn<T, W> windowFn, TimestampedValue<T> timestampedValue) throws Exception {
    TestAssignContext<T, W> assignContext = new TestAssignContext<>(timestampedValue, windowFn);
    return windowFn.assignWindows(assignContext);
  }

  private static String timestampValue(long timestamp) {
    return "T" + new Instant(timestamp);
  }

  /** Test implementation of AssignContext. */
  private static class TestAssignContext<T, W extends BoundedWindow>
      extends WindowFn<T, W>.AssignContext {
    private TimestampedValue<T> timestampedValue;

    public TestAssignContext(TimestampedValue<T> timestampedValue, WindowFn<T, W> windowFn) {
      windowFn.super();
      this.timestampedValue = timestampedValue;
    }

    @Override
    public T element() {
      return timestampedValue.getValue();
    }

    @Override
    public Instant timestamp() {
      return timestampedValue.getTimestamp();
    }

    @Override
    public BoundedWindow window() {
      return GlobalWindow.INSTANCE;
    }
  }

  /** Test implementation of MergeContext. */
  private static class TestMergeContext<T, W extends BoundedWindow>
      extends WindowFn<T, W>.MergeContext {
    private TestWindowSet<W, ?> windowSet;

    public TestMergeContext(TestWindowSet<W, ?> windowSet, WindowFn<T, W> windowFn) {
      windowFn.super();
      this.windowSet = windowSet;
    }

    @Override
    public Collection<W> windows() {
      return windowSet.windows();
    }

    @Override
    public void merge(Collection<W> toBeMerged, W mergeResult) {
      windowSet.merge(toBeMerged, mergeResult);
    }
  }

  /**
   * A WindowSet useful for testing WindowFns that simply collects the placed elements into
   * multisets.
   */
  private static class TestWindowSet<W extends BoundedWindow, V> {

    private Map<W, Set<V>> elements = new HashMap<>();

    public void put(W window, V value) {
      Set<V> all = elements.computeIfAbsent(window, k -> new HashSet<>());
      all.add(value);
    }

    public void merge(Collection<W> otherWindows, W window) {
      if (otherWindows.isEmpty()) {
        return;
      }
      Set<V> merged = new HashSet<>();
      if (elements.containsKey(window) && !otherWindows.contains(window)) {
        merged.addAll(elements.get(window));
      }
      for (W w : otherWindows) {
        if (!elements.containsKey(w)) {
          throw new IllegalArgumentException("Tried to merge a non-existent window:" + w);
        }
        merged.addAll(elements.get(w));
        elements.remove(w);
      }
      elements.put(window, merged);
    }

    public Collection<W> windows() {
      return elements.keySet();
    }

    // For testing.

    public Set<V> get(W window) {
      return elements.get(window);
    }
  }

  /**
   * Assigns the given {@code timestamp} to windows using the specified {@code windowFn}, and
   * verifies that result of {@code windowFn.getOutputTimestamp} for each window is within the
   * proper bound.
   */
  public static <T, W extends BoundedWindow> void validateNonInterferingOutputTimes(
      WindowFn<T, W> windowFn, long timestamp) throws Exception {
    validateNonInterferingOutputTimesWithValue(
        windowFn, TimestampedValue.of((T) null, new Instant(timestamp)));
  }
  /**
   * Assigns the given {@code timestampedValue} to windows using the specified {@code windowFn}, and
   * verifies that result of {@code windowFn.getOutputTimestamp} for each window is within the
   * proper bound. This version allows passing a {@link TimestampedValue} in case the value is
   * needed to assign windows.
   */
  public static <T, W extends BoundedWindow> void validateNonInterferingOutputTimesWithValue(
      WindowFn<T, W> windowFn, TimestampedValue<T> timestampedValue) throws Exception {
    Collection<W> windows = assignedWindowsWithValue(windowFn, timestampedValue);

    Instant instant = timestampedValue.getTimestamp();
    for (W window : windows) {
      Instant outputTimestamp = windowFn.getOutputTime(instant, window);
      assertFalse(
          "getOutputTime must be greater than or equal to input timestamp",
          outputTimestamp.isBefore(instant));
      assertFalse(
          "getOutputTime must be less than or equal to the max timestamp",
          outputTimestamp.isAfter(window.maxTimestamp()));
    }
  }

  /**
   * Assigns the given {@code timestamp} to windows using the specified {@code windowFn}, and
   * verifies that result of {@link WindowFn#getOutputTime windowFn.getOutputTime} for later windows
   * (as defined by {@code maxTimestamp} won't prevent the watermark from passing the end of earlier
   * windows.
   *
   * <p>This verifies that overlapping windows don't interfere at all. Depending on the {@code
   * windowFn} this may be stricter than desired.
   */
  public static <T, W extends BoundedWindow> void validateGetOutputTimestamp(
      WindowFn<T, W> windowFn, long timestamp) throws Exception {
    validateGetOutputTimestampWithValue(
        windowFn, TimestampedValue.of((T) null, new Instant(timestamp)));
  }

  /**
   * Assigns the given {@code timestampedValue} to windows using the specified {@code windowFn}, and
   * verifies that result of {@link WindowFn#getOutputTime windowFn.getOutputTime} for later windows
   * (as defined by {@code maxTimestamp} won't prevent the watermark from passing the end of earlier
   * windows.
   *
   * <p>This verifies that overlapping windows don't interfere at all. Depending on the {@code
   * windowFn} this may be stricter than desired. This version allows passing a {@link
   * TimestampedValue} in case the value is needed to assign windows.
   */
  public static <T, W extends BoundedWindow> void validateGetOutputTimestampWithValue(
      WindowFn<T, W> windowFn, TimestampedValue<T> timestampedValue) throws Exception {
    Collection<W> windows = assignedWindowsWithValue(windowFn, timestampedValue);
    List<W> sortedWindows = new ArrayList<>(windows);
    sortedWindows.sort(Comparator.comparing(BoundedWindow::maxTimestamp));

    Instant instant = timestampedValue.getTimestamp();
    Instant endOfPrevious = null;
    for (W window : sortedWindows) {
      Instant outputTimestamp = windowFn.getOutputTime(instant, window);
      if (endOfPrevious == null) {
        // If this is the first window, the output timestamp can be anything, as long as it is in
        // the valid range.
        assertFalse(
            "getOutputTime must be greater than or equal to input timestamp",
            outputTimestamp.isBefore(instant));
        assertFalse(
            "getOutputTime must be less than or equal to the max timestamp",
            outputTimestamp.isAfter(window.maxTimestamp()));
      } else {
        // If this is a later window, the output timestamp must be after the end of the previous
        // window
        assertTrue(
            "getOutputTime must be greater than the end of the previous window",
            outputTimestamp.isAfter(endOfPrevious));
        assertFalse(
            "getOutputTime must be less than or equal to the max timestamp",
            outputTimestamp.isAfter(window.maxTimestamp()));
      }
      endOfPrevious = window.maxTimestamp();
    }
  }

  /**
   * Verifies that later-ending merged windows from any of the timestamps hold up output of
   * earlier-ending windows, using the provided {@link WindowFn} and {@link TimestampCombiner}.
   *
   * <p>Given a list of lists of timestamps, where each list is expected to merge into a single
   * window with end times in ascending order, assigns and merges windows for each list (as though
   * each were a separate key/user session). Then combines each timestamp in the list according to
   * the provided {@link TimestampCombiner}.
   *
   * <p>Verifies that a overlapping windows do not hold each other up via the watermark.
   */
  public static <T, W extends IntervalWindow> void validateGetOutputTimestamps(
      WindowFn<T, W> windowFn,
      TimestampCombiner timestampCombiner,
      List<List<Long>> timestampsPerWindow)
      throws Exception {

    List<List<TimestampedValue<T>>> timestampValuesPerWindow = new ArrayList<>();
    for (List<Long> timestamps : timestampsPerWindow) {
      List<TimestampedValue<T>> timestampedValues = new ArrayList<>();
      for (Long timestamp : timestamps) {
        TimestampedValue<T> tv = TimestampedValue.of(null, new Instant(timestamp));
        timestampedValues.add(tv);
      }
      timestampValuesPerWindow.add(timestampedValues);
    }
    validateGetOutputTimestampsWithValue(windowFn, timestampCombiner, timestampValuesPerWindow);
  }

  /**
   * Verifies that later-ending merged windows from any of the timestampValues hold up output of
   * earlier-ending windows, using the provided {@link WindowFn} and {@link TimestampCombiner}.
   *
   * <p>Given a list of lists of timestampValues, where each list is expected to merge into a single
   * window with end times in ascending order, assigns and merges windows for each list (as though
   * each were a separate key/user session). Then combines each timestamp in the list according to
   * the provided {@link TimestampCombiner}.
   *
   * <p>Verifies that a overlapping windows do not hold each other up via the watermark. This
   * version allows passing {@link TimestampedValue} in case the value is needed to assign windows.
   */
  public static <T, W extends IntervalWindow> void validateGetOutputTimestampsWithValue(
      WindowFn<T, W> windowFn,
      TimestampCombiner timestampCombiner,
      List<List<TimestampedValue<T>>> timestampValuesPerWindow)
      throws Exception {

    // Assign windows to each timestamp, then merge them, storing the merged windows in
    // a list in corresponding order to timestampValuesPerWindow
    final List<W> windows = new ArrayList<>();
    for (List<TimestampedValue<T>> timestampValuesForWindow : timestampValuesPerWindow) {
      final Set<W> windowsToMerge = new HashSet<>();

      for (TimestampedValue<T> element : timestampValuesForWindow) {
        windowsToMerge.addAll(assignedWindowsWithValue(windowFn, element));
      }

      windowFn.mergeWindows(
          windowFn.new MergeContext() {
            @Override
            public Collection<W> windows() {
              return windowsToMerge;
            }

            @Override
            public void merge(Collection<W> toBeMerged, W mergeResult) throws Exception {
              windows.add(mergeResult);
            }
          });
    }

    // Map every list of input timestampValues timestamps to an output timestamp
    final List<Instant> combinedOutputTimestamps = new ArrayList<>();
    for (int i = 0; i < timestampValuesPerWindow.size(); ++i) {
      List<TimestampedValue<T>> timestampValuesForWindow = timestampValuesPerWindow.get(i);
      W window = windows.get(i);

      List<Instant> outputInstants = new ArrayList<>();
      for (TimestampedValue<T> element : timestampValuesForWindow) {
        outputInstants.add(
            assignOutputTime(timestampCombiner, new Instant(element.getTimestamp()), window));
      }

      combinedOutputTimestamps.add(combineOutputTimes(timestampCombiner, outputInstants));
    }

    // Consider windows in increasing order of max timestamp; ensure the output timestamp is after
    // the max timestamp of the previous
    @Nullable W earlierEndingWindow = null;
    for (int i = 0; i < windows.size(); ++i) {
      W window = windows.get(i);
      ReadableInstant outputTimestamp = combinedOutputTimestamps.get(i);

      if (earlierEndingWindow != null) {
        assertThat(
            outputTimestamp, greaterThan((ReadableInstant) earlierEndingWindow.maxTimestamp()));
      }

      earlierEndingWindow = window;
    }
  }

  private static Instant assignOutputTime(
      TimestampCombiner timestampCombiner, Instant inputTimestamp, BoundedWindow window) {
    switch (timestampCombiner) {
      case EARLIEST:
      case LATEST:
        return inputTimestamp;
      case END_OF_WINDOW:
        return window.maxTimestamp();
      default:
        throw new IllegalArgumentException(
            String.format("Unknown %s: %s", TimestampCombiner.class, timestampCombiner));
    }
  }

  private static Instant combineOutputTimes(
      TimestampCombiner timestampCombiner, Iterable<Instant> outputInstants) {
    checkArgument(
        !Iterables.isEmpty(outputInstants),
        "Cannot combine zero instants with %s",
        timestampCombiner);
    switch (timestampCombiner) {
      case EARLIEST:
        return Ordering.natural().min(outputInstants);
      case LATEST:
        return Ordering.natural().max(outputInstants);
      case END_OF_WINDOW:
        return outputInstants.iterator().next();
      default:
        throw new IllegalArgumentException(
            String.format("Unknown %s: %s", TimestampCombiner.class, timestampCombiner));
    }
  }
}
