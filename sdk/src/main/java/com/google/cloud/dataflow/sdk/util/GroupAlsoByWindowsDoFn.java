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

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresKeyedState;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.NonMergingWindowFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.base.Preconditions;

import org.joda.time.Instant;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * DoFn that merges windows and groups elements in those windows, optionally
 * combining values.
 *
 * @param <K> key type
 * @param <VI> input value element type
 * @param <VO> output value element type
 * @param <W> window type
 */
@SuppressWarnings("serial")
public abstract class GroupAlsoByWindowsDoFn<K, VI, VO, W extends BoundedWindow>
    extends DoFn<KV<K, Iterable<WindowedValue<VI>>>, KV<K, VO>>
    implements RequiresKeyedState {

  /**
   * Create a {@link GroupAlsoByWindowsDoFn} without a combine function. Depending on the
   * {@code windowFn} this will either use iterators or window sets to implement the grouping.
   *
   * @param windowFn The window function to use for grouping
   * @param inputCoder the input coder to use
   */
  public static <K, V, W extends BoundedWindow> GroupAlsoByWindowsDoFn<K, V, Iterable<V>, W>
  createForIterable(final WindowFn<?, W> windowFn, final Coder<V> inputCoder) {
    if (windowFn instanceof NonMergingWindowFn) {
      return new GroupAlsoByWindowsViaIteratorsDoFn<K, V, W>();
    } else {
      return new GABWViaWindowSetDoFn<K, V, Iterable<V>, W>(windowFn) {
        @Override
        AbstractWindowSet<K, V, Iterable<V>, W> createWindowSet(K key,
            DoFn<KV<K, Iterable<WindowedValue<V>>>, KV<K, Iterable<V>>>.ProcessContext context,
            BatchActiveWindowManager<W> activeWindowManager) throws Exception {
          return  new BufferingWindowSet<K, V, W>(key, windowFn, inputCoder,
              context, activeWindowManager);
        }
      };
    }
  }

  /**
   * Construct a {@link GroupAlsoByWindowsDoFn} using the {@code combineFn} if available.
   */
  public static <K, VI, VO, W extends BoundedWindow> GroupAlsoByWindowsDoFn<K, VI, VO, W>
  create(
      final WindowFn<?, W> windowFn,
      final KeyedCombineFn<K, VI, ?, VO> combineFn,
      final Coder<K> keyCoder,
      final Coder<VI> inputCoder) {
    Preconditions.checkNotNull(combineFn);
    return new GABWViaWindowSetDoFn<K, VI, VO, W>(windowFn) {
      @Override
      AbstractWindowSet<K, VI, VO, W> createWindowSet(
          K key,
          DoFn<KV<K, Iterable<WindowedValue<VI>>>, KV<K, VO>>.ProcessContext context,
          BatchActiveWindowManager<W> activeWindowManager) throws Exception {
        return CombiningWindowSet.create(
            key, windowFn, combineFn, keyCoder, inputCoder,
            context, activeWindowManager);
      }
    };
  }

  private abstract static class GABWViaWindowSetDoFn<K, VI, VO, W extends BoundedWindow>
     extends GroupAlsoByWindowsDoFn<K, VI, VO, W> {

    private WindowFn<Object, W> windowFn;

    public GABWViaWindowSetDoFn(WindowFn<?, W> windowFn) {
      @SuppressWarnings("unchecked")
      WindowFn<Object, W> noWildcard = (WindowFn<Object, W>) windowFn;
      this.windowFn = noWildcard;
    }

    abstract AbstractWindowSet<K, VI, VO, W> createWindowSet(
        K key,
        DoFn<KV<K, Iterable<WindowedValue<VI>>>, KV<K, VO>>.ProcessContext context,
        BatchActiveWindowManager<W> activeWindowManager)
        throws Exception;

    @Override
    public void processElement(
        DoFn<KV<K, Iterable<WindowedValue<VI>>>, KV<K, VO>>.ProcessContext c) throws Exception {
      K key = c.element().getKey();
      BatchActiveWindowManager<W> activeWindowManager = new BatchActiveWindowManager<>();
      AbstractWindowSet<K, VI, ?, W> windowSet = createWindowSet(key, c, activeWindowManager);

      for (WindowedValue<VI> e : c.element().getValue()) {
        for (BoundedWindow window : e.getWindows()) {
          @SuppressWarnings("unchecked")
          W w = (W) window;
          windowSet.put(w, e.getValue());
        }
        windowFn.mergeWindows(
            new AbstractWindowSet.WindowMergeContext<Object, W>(windowSet, windowFn));

        maybeOutputWindows(activeWindowManager, windowSet, e.getTimestamp());
      }

      maybeOutputWindows(activeWindowManager, windowSet, null);

      windowSet.flush();
    }

    /**
     * Outputs any windows that are complete, with their corresponding elemeents.
     * If there are potentially complete windows, try merging windows first.
     */
    private void maybeOutputWindows(
        BatchActiveWindowManager<W> activeWindowManager,
        AbstractWindowSet<?, ?, ?, W> windowSet,
        Instant nextTimestamp) throws Exception {
      if (activeWindowManager.hasMoreWindows()
          && (nextTimestamp == null
              || activeWindowManager.nextTimestamp().isBefore(nextTimestamp))) {
        // There is at least one window ready to emit.  Merge now in case that window should be
        // merged into a not yet completed one.
        windowFn.mergeWindows(
           new AbstractWindowSet.WindowMergeContext<Object, W>(windowSet, windowFn));
      }

      while (activeWindowManager.hasMoreWindows()
          && (nextTimestamp == null
              || activeWindowManager.nextTimestamp().isBefore(nextTimestamp))) {
        W window = activeWindowManager.getWindow();
        Preconditions.checkState(windowSet.contains(window));
        windowSet.markCompleted(window);
      }
    }
  }

  private static class BatchActiveWindowManager<W extends BoundedWindow>
      implements AbstractWindowSet.ActiveWindowManager<W> {
    // Sort the windows by their end timestamps so that we can efficiently
    // ask for the next window that will be completed.
    PriorityQueue<W> windows = new PriorityQueue<>(11, new Comparator<W>() {
          @Override
          public int compare(W w1, W w2) {
            return w1.maxTimestamp().compareTo(w2.maxTimestamp());
          }
        });

    @Override
    public void addWindow(W window) {
      windows.add(window);
    }

    @Override
    public void removeWindow(W window) {
      windows.remove(window);
    }

    /**
     * Returns whether there are more windows.
     */
    public boolean hasMoreWindows() {
      return windows.peek() != null;
    }

    /**
     * Returns the timestamp of the next window.
     */
    public Instant nextTimestamp() {
      return windows.peek().maxTimestamp();
    }

    /**
     * Returns the next window.
     */
    public W getWindow() {
      return windows.peek();
    }
  }
}
