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

package com.google.cloud.dataflow.sdk.testing;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;

import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A utility class for testing {@link WindowFn}s.
 */
public class WindowFnTestUtils {

  /**
   * Creates a Set of elements to be used as expected output in
   * {@link #runWindowFn}.
   */
  public static Set<String> set(long... timestamps) {
    Set<String> result = new HashSet<>();
    for (long timestamp : timestamps) {
      result.add(timestampValue(timestamp));
    }
    return result;
  }


  /**
   * Runs the {@link WindowFn} over the provided input, returning a map
   * of windows to the timestamps in those windows.
   */
  public static <T, W extends BoundedWindow> Map<W, Set<String>> runWindowFn(
      WindowFn<T, W> windowFn,
      List<Long> timestamps) throws Exception {

    final TestWindowSet<W, String> windowSet = new TestWindowSet<W, String>();
    for (final Long timestamp : timestamps) {
      for (W window : windowFn.assignWindows(
          new TestAssignContext<T, W>(new Instant(timestamp), windowFn))) {
        windowSet.put(window, timestampValue(timestamp));
      }
      windowFn.mergeWindows(new TestMergeContext<T, W>(windowSet, windowFn));
    }
    Map<W, Set<String>> actual = new HashMap<>();
    for (W window : windowSet.windows()) {
      actual.put(window, windowSet.get(window));
    }
    return actual;
  }

  public static <T, W extends BoundedWindow> Collection<W> assignedWindows(
      WindowFn<T, W> windowFn, long timestamp) throws Exception {
    return windowFn.assignWindows(new TestAssignContext<T, W>(new Instant(timestamp), windowFn));
  }

  private static String timestampValue(long timestamp) {
    return "T" + new Instant(timestamp);
  }

  /**
   * Test implementation of AssignContext.
   */
  private static class TestAssignContext<T, W extends BoundedWindow>
      extends WindowFn<T, W>.AssignContext {
    private Instant timestamp;

    public TestAssignContext(Instant timestamp, WindowFn<T, W> windowFn) {
      windowFn.super();
      this.timestamp = timestamp;
    }

    @Override
    public T element() {
      return null;
    }

    @Override
    public Instant timestamp() {
      return timestamp;
    }

    @Override
    public Collection<? extends BoundedWindow> windows() {
      return null;
    }
  }

  /**
   * Test implementation of MergeContext.
   */
  private static class TestMergeContext<T, W extends BoundedWindow>
    extends WindowFn<T, W>.MergeContext {
    private TestWindowSet<W, ?> windowSet;

    public TestMergeContext(
        TestWindowSet<W, ?> windowSet, WindowFn<T, W> windowFn) {
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
   * A WindowSet useful for testing WindowFns that simply
   * collects the placed elements into multisets.
   */
  private static class TestWindowSet<W extends BoundedWindow, V> {

    private Map<W, Set<V>> elements = new HashMap<>();

    public void put(W window, V value) {
      Set<V> all = elements.get(window);
      if (all == null) {
        all = new HashSet<>();
        elements.put(window, all);
      }
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
    Collection<W> windows = WindowFnTestUtils.<T, W>assignedWindows(windowFn, timestamp);

    Instant instant = new Instant(timestamp);
    for (W window : windows) {
      Instant outputTimestamp = windowFn.getOutputTime(instant, window);
      assertFalse("getOutputTime must be greater than or equal to input timestamp",
          outputTimestamp.isBefore(instant));
      assertFalse("getOutputTime must be less than or equal to the max timestamp",
          outputTimestamp.isAfter(window.maxTimestamp()));
    }
  }

  /**
   * Assigns the given {@code timestamp} to windows using the specified {@code windowFn}, and
   * verifies that result of {@code windowFn.getOutputTimestamp} for later windows (as defined by
   * {@code maxTimestamp} won't prevent the watermark from passing the end of earlier windows.
   *
   * <p> This verifies that overlapping windows don't interfere at all. Depending on the
   * {@code windowFn} this may be stricter than desired.
   */
  public static <T, W extends BoundedWindow> void validateGetOutputTimestamp(
      WindowFn<T, W> windowFn, long timestamp) throws Exception {
    Collection<W> windows = WindowFnTestUtils.<T, W>assignedWindows(windowFn, timestamp);
    List<W> sortedWindows = new ArrayList<>(windows);
    Collections.sort(sortedWindows, new Comparator<BoundedWindow>() {
      @Override
      public int compare(BoundedWindow o1, BoundedWindow o2) {
        return o1.maxTimestamp().compareTo(o2.maxTimestamp());
      }
    });

    Instant instant = new Instant(timestamp);
    Instant endOfPrevious = null;
    for (W window : sortedWindows) {
      Instant outputTimestamp = windowFn.getOutputTime(instant, window);
      if (endOfPrevious == null) {
        // If this is the first window, the output timestamp can be anything, as long as it is in
        // the valid range.
        assertFalse("getOutputTime must be greater than or equal to input timestamp",
            outputTimestamp.isBefore(instant));
        assertFalse("getOutputTime must be less than or equal to the max timestamp",
            outputTimestamp.isAfter(window.maxTimestamp()));
      } else {
        // If this is a later window, the output timestamp must be after the end of the previous
        // window
        assertTrue("getOutputTime must be greater than the end of the previous window",
            outputTimestamp.isAfter(endOfPrevious));
        assertFalse("getOutputTime must be less than or equal to the max timestamp",
            outputTimestamp.isAfter(window.maxTimestamp()));
      }
      endOfPrevious = window.maxTimestamp();
    }
  }
}
