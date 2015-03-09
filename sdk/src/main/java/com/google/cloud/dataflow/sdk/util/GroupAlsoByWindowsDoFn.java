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
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.NonMergingWindowFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.common.PeekingReiterator;
import com.google.cloud.dataflow.sdk.util.common.Reiterable;
import com.google.cloud.dataflow.sdk.util.common.Reiterator;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
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
    extends DoFn<KV<K, Iterable<WindowedValue<VI>>>, KV<K, VO>> {

  /**
   * Create a {@link GroupAlsoByWindowsDoFn} without a combine function. Depending on the
   * {@code windowFn} this will either use iterators or window sets to implement the grouping.
   *
   * @param windowFn The window function to use for grouping
   * @param inputCoder the input coder to use
   */
  public static <K, V, W extends BoundedWindow> GroupAlsoByWindowsDoFn<K, V, Iterable<V>, W>
  create(final WindowFn<?, W> windowFn, final Coder<V> inputCoder) {
    if (windowFn instanceof NonMergingWindowFn) {
      return new GABWViaIteratorsDoFn<K, V, W>();
    } else {
      return new GABWViaWindowSetDoFn<K, V, Iterable<V>, W>(windowFn) {
        @Override
        AbstractWindowSet<K, V, Iterable<V>, W> createWindowSet(K key,
            DoFnProcessContext<KV<K, Iterable<WindowedValue<V>>>, KV<K, Iterable<V>>> context,
            BatchActiveWindowManager<W> activeWindowManager) throws Exception {
          return  new BufferingWindowSet<K, V, W>(key, windowFn, inputCoder,
              context, activeWindowManager);
        }
      };
    }
  }

  /**
   * Create a {@link GroupAlsoByWindowsDoFn} using the specified combineFn.
   */
  private static <K, VA, W extends BoundedWindow> GroupAlsoByWindowsDoFn<K, VA, VA, W>
  createCombine(final WindowFn<?, W> windowFn,
      final KeyedCombineFn<K, ?, VA, ?> combineFn,
      final Coder<VA> inputCoder) {
    return new GABWViaWindowSetDoFn<K, VA, VA, W>(windowFn) {
    @Override
    AbstractWindowSet<K, VA, VA, W> createWindowSet(K key,
        DoFnProcessContext<KV<K, Iterable<WindowedValue<VA>>>, KV<K, VA>> context,
        BatchActiveWindowManager<W> activeWindowManager) throws Exception {
        return new CombiningWindowSet<K, VA, W>(
            key, windowFn, combineFn, inputCoder, context, activeWindowManager);
      }
    };
  }

  /**
   * Construct a {@link GroupAlsoByWindowsDoFn} using the {@code combineFn} if available.
   */
  public static <K, VI, VO, W extends BoundedWindow> GroupAlsoByWindowsDoFn<K, VI, VO, W>
  create(WindowFn<?, W> windowFn, KeyedCombineFn<K, ?, VI, ?> combineFn,
      Coder<VI> inputCoder) {
    if (combineFn == null) {
      // Without combineFn, it should be the case that VO = Iterable<VI>, so this is safe
      @SuppressWarnings("unchecked")
      GroupAlsoByWindowsDoFn<K, VI, VO, W> fn =
          (GroupAlsoByWindowsDoFn<K, VI, VO, W>) create(windowFn, inputCoder);
      return fn;
    } else {
      // With a combineFn, then VI = VO, and we'll use those as the type of the accumulator
      @SuppressWarnings("unchecked")
      GroupAlsoByWindowsDoFn<K, VI, VO, W> fn =
          (GroupAlsoByWindowsDoFn<K, VI, VO, W>) createCombine(windowFn, combineFn, inputCoder);
      return fn;
    }
  }

  private static class GABWViaIteratorsDoFn<K, V, W extends BoundedWindow>
      extends GroupAlsoByWindowsDoFn<K, V, Iterable<V>, W> {

    @Override
    public void processElement(ProcessContext c) throws Exception {
      @SuppressWarnings({"unchecked", "rawtypes"})
      DoFnProcessContext<?, KV<K, Iterable<V>>> internal = (DoFnProcessContext) c;

      K key = c.element().getKey();
      Iterable<WindowedValue<V>> value = c.element().getValue();
      PeekingReiterator<WindowedValue<V>> iterator;

      if (value instanceof Collection) {
        iterator = new PeekingReiterator<>(new ListReiterator<WindowedValue<V>>(
            new ArrayList<WindowedValue<V>>((Collection<WindowedValue<V>>) value), 0));
      } else if (value instanceof Reiterable) {
        iterator = new PeekingReiterator<>(((Reiterable<WindowedValue<V>>) value).iterator());
      } else {
        throw new IllegalArgumentException(
            "Input to GroupAlsoByWindowsDoFn must be a Collection or Reiterable");
      }

      // This ListMultimap is a map of window maxTimestamps to the list of active
      // windows with that maxTimestamp.
      ListMultimap<Instant, BoundedWindow> windows = ArrayListMultimap.create();

      while (iterator.hasNext()) {
        WindowedValue<V> e = iterator.peek();
        for (BoundedWindow window : e.getWindows()) {
          // If this window is not already in the active set, emit a new WindowReiterable
          // corresponding to this window, starting at this element in the input Reiterable.
          if (!windows.containsEntry(window.maxTimestamp(), window)) {
            // Iterating through the WindowReiterable may advance iterator as an optimization
            // for as long as it detects that there are no new windows.
            windows.put(window.maxTimestamp(), window);
            internal.outputWindowedValue(
                KV.of(key, (Iterable<V>) new WindowReiterable<V>(iterator, window)),
                window.maxTimestamp(),
                Arrays.asList(window));
          }
        }
        // Copy the iterator in case the next DoFn cached its version of the iterator instead
        // of immediately iterating through it.
        // And, only advance the iterator if the consuming operation hasn't done so.
        iterator = iterator.copy();
        if (iterator.hasNext() && iterator.peek() == e) {
          iterator.next();
        }

        // Remove all windows with maxTimestamp behind the current timestamp.
        Iterator<Instant> windowIterator = windows.keys().iterator();
        while (windowIterator.hasNext()
            && windowIterator.next().isBefore(e.getTimestamp())) {
          windowIterator.remove();
        }
      }
    }
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
        DoFnProcessContext<KV<K, Iterable<WindowedValue<VI>>>, KV<K, VO>> context,
        BatchActiveWindowManager<W> activeWindowManager)
        throws Exception;

    @Override
    public void processElement(
        DoFn<KV<K, Iterable<WindowedValue<VI>>>, KV<K, VO>>.ProcessContext c) throws Exception {
      @SuppressWarnings("unchecked")
      DoFnProcessContext<KV<K, Iterable<WindowedValue<VI>>>, KV<K, VO>> context =
          (DoFnProcessContext<KV<K, Iterable<WindowedValue<VI>>>, KV<K, VO>>) c;
      processElementViaWindowSet(context);
    }

    public void processElementViaWindowSet(
        DoFnProcessContext<KV<K, Iterable<WindowedValue<VI>>>, KV<K, VO>> context)
            throws Exception {
      K key = context.element().getKey();
      BatchActiveWindowManager<W> activeWindowManager = new BatchActiveWindowManager<>();
      AbstractWindowSet<K, VI, ?, W> windowSet =
          createWindowSet(key, context, activeWindowManager);

      for (WindowedValue<VI> e : context.element().getValue()) {
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
        if (windowSet.contains(window)) {
          windowSet.markCompleted(window);
        }
      }
    }
  }

  /**
   * {@link Reiterable} representing a view of all elements in a base
   * {@link Reiterator} that are in a given window.
   */
  private static class WindowReiterable<V> implements Reiterable<V> {
    private PeekingReiterator<WindowedValue<V>> baseIterator;
    private BoundedWindow window;

    public WindowReiterable(
        PeekingReiterator<WindowedValue<V>> baseIterator, BoundedWindow window) {
      this.baseIterator = baseIterator;
      this.window = window;
    }

    @Override
    public Reiterator<V> iterator() {
      // We don't copy the baseIterator when creating the first WindowReiterator
      // so that the WindowReiterator can advance the baseIterator.  We have to
      // make a copy afterwards so that future calls to iterator() will start
      // at the right spot.
      Reiterator<V> result = new WindowReiterator<V>(baseIterator, window);
      baseIterator = baseIterator.copy();
      return result;
    }

    @Override
    public String toString() {
      StringBuilder result = new StringBuilder();
      result.append("WR{");
      for (V v : this) {
        result.append(v.toString()).append(',');
      }
      result.append("}");
      return result.toString();
    }
  }

  /**
   * The {@link Reiterator} used by
   * {@link com.google.cloud.dataflow.sdk.util.GroupAlsoByWindowsDoFn.WindowReiterable}.
   */
  private static class WindowReiterator<V> implements Reiterator<V> {
    private PeekingReiterator<WindowedValue<V>> iterator;
    private BoundedWindow window;

    public WindowReiterator(PeekingReiterator<WindowedValue<V>> iterator, BoundedWindow window) {
      this.iterator = iterator;
      this.window = window;
    }

    @Override
    public Reiterator<V> copy() {
      return new WindowReiterator<V>(iterator.copy(), window);
    }

    @Override
    public boolean hasNext() {
      skipToValidElement();
      return (iterator.hasNext() && iterator.peek().getWindows().contains(window));
    }

    @Override
    public V next() {
      skipToValidElement();
      WindowedValue<V> next = iterator.next();
      if (!next.getWindows().contains(window)) {
        throw new NoSuchElementException("No next item in window");
      }
      return next.getValue();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    /**
     * Moves the underlying iterator forward until it either points to the next
     * element in the correct window, or is past the end of the window.
     */
    private void skipToValidElement() {
      while (iterator.hasNext()) {
        WindowedValue<V> peek = iterator.peek();
        if (peek.getTimestamp().isAfter(window.maxTimestamp())) {
          // We are past the end of this window, so there can't be any more
          // elements in this iterator.
          break;
        }
        if (!(peek.getWindows().size() == 1 && peek.getWindows().contains(window))) {
          // We have reached new windows; we need to copy the iterator so we don't
          // keep advancing the outer loop in processElement.
          iterator = iterator.copy();
        }
        if (!peek.getWindows().contains(window)) {
          // The next element is not in the right window: skip it.
          iterator.next();
        } else {
          // The next element is in the right window.
          break;
        }
      }
    }
  }

  /**
   * {@link Reiterator} that wraps a {@link List}.
   */
  private static class ListReiterator<T> implements Reiterator<T> {
    private List<T> list;
    private int index;

    public ListReiterator(List<T> list, int index) {
      this.list = list;
      this.index = index;
    }

    @Override
    public T next() {
      return list.get(index++);
    }

    @Override
    public boolean hasNext() {
      return index < list.size();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Reiterator<T> copy() {
      return new ListReiterator<T>(list, index);
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
