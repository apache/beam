/*
 * Copyright (C) 2014 Google Inc.
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

import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;

import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.Collection;
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
   * A WindowSet useful for testing WindowFns which simply
   * collects the placed elements into multisets.
   */
  private static class TestWindowSet<W extends BoundedWindow, V> {

    private Map<W, Set<V>> elements = new HashMap<>();
    private List<Set<V>> emitted = new ArrayList<>();

    public void put(W window, V value) {
      Set<V> all = elements.get(window);
      if (all == null) {
        all = new HashSet<>();
        elements.put(window, all);
      }
      all.add(value);
    }

    public void remove(W window) {
      elements.remove(window);
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

    public void markCompleted(W window) {}

    public Collection<W> windows() {
      return elements.keySet();
    }

    public boolean contains(W window) {
      return elements.containsKey(window);
    }

    // For testing.

    public Set<V> get(W window) {
      return elements.get(window);
    }
  }
}
