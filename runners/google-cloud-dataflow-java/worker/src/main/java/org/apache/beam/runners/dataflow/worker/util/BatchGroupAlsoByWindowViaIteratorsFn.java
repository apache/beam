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
package org.apache.beam.runners.dataflow.worker.util;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.PeekingReiterator;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterable;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterator;
import org.apache.beam.sdk.util.common.Reiterable;
import org.apache.beam.sdk.util.common.Reiterator;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ListMultimap;
import org.joda.time.Instant;

/**
 * {@link BatchGroupAlsoByWindowFn} that uses reiterators to handle non-merging window functions
 * with the default triggering strategy.
 *
 * @param <K> key type
 * @param <V> value element type
 * @param <W> window type
 */
class BatchGroupAlsoByWindowViaIteratorsFn<K, V, W extends BoundedWindow>
    extends BatchGroupAlsoByWindowFn<K, V, Iterable<V>> {
  private final WindowingStrategy<?, W> strategy;

  public static boolean isSupported(WindowingStrategy<?, ?> strategy) {
    if (!strategy.getWindowFn().isNonMerging()) {
      return false;
    }

    // It must be possible to compute the output timestamp of a pane from the input timestamp
    // of the element with the earliest input timestamp.
    if (!strategy.getTimestampCombiner().dependsOnlyOnEarliestTimestamp()
        && !strategy.getTimestampCombiner().dependsOnlyOnWindow()) {
      return false;
    }

    return true;
  }

  public BatchGroupAlsoByWindowViaIteratorsFn(WindowingStrategy<?, W> strategy) {
    checkArgument(
        BatchGroupAlsoByWindowViaIteratorsFn.isSupported(strategy),
        "%s does not support merging "
            + "or any TimestampCombiner where both dependsOnlyOnEarliestTimestamp() and "
            + "dependsOnlyOnWindow() are false, found in windowing strategy: %s",
        getClass(),
        strategy);
    this.strategy = strategy;
  }

  @Override
  @SuppressWarnings("ReferenceEquality")
  public void processElement(
      KV<K, Iterable<WindowedValue<V>>> element,
      PipelineOptions options,
      StepContext stepContext,
      SideInputReader sideInputReader,
      OutputWindowedValue<KV<K, Iterable<V>>> output)
      throws Exception {
    K key = element.getKey();
    // This iterable is required to be in order of increasing timestamps
    Iterable<WindowedValue<V>> value = element.getValue();
    PeekingReiterator<WindowedValue<V>> iterator;

    if (value instanceof Collection) {
      iterator =
          new PeekingReiterator<>(
              new ListReiterator<WindowedValue<V>>(
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
          // This window was produced by strategy.getWindowFn()
          @SuppressWarnings("unchecked")
          W typedWindow = (W) window;
          // Iterating through the WindowReiterable may advance iterator as an optimization
          // for as long as it detects that there are no new windows.
          windows.put(window.maxTimestamp(), window);
          output.outputWindowedValue(
              KV.of(key, (Iterable<V>) new WindowReiterable<V>(iterator, window)),
              strategy
                  .getTimestampCombiner()
                  .assign(
                      typedWindow,
                      strategy.getWindowFn().getOutputTime(e.getTimestamp(), typedWindow)),
              Arrays.asList(window),
              PaneInfo.ON_TIME_AND_ONLY_FIRING);
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
      while (windowIterator.hasNext() && windowIterator.next().isBefore(e.getTimestamp())) {
        windowIterator.remove();
      }
    }
  }

  /**
   * {@link Reiterable} representing a view of all elements in a base {@link Reiterator} that are in
   * a given window.
   */
  private static class WindowReiterable<V>
      extends ElementByteSizeObservableIterable<V, WindowReiterator<V>> implements Reiterable<V> {
    private PeekingReiterator<WindowedValue<V>> baseIterator;
    private BoundedWindow window;

    public WindowReiterable(
        PeekingReiterator<WindowedValue<V>> baseIterator, BoundedWindow window) {
      this.baseIterator = baseIterator;
      this.window = window;
    }

    @Override
    public WindowReiterator<V> iterator() {
      return createIterator();
    }

    @Override
    protected WindowReiterator<V> createIterator() {
      // We don't copy the baseIterator when creating the first WindowReiterator
      // so that the WindowReiterator can advance the baseIterator.  We have to
      // make a copy afterwards so that future calls to iterator() will start
      // at the right spot.
      WindowReiterator<V> result = new WindowReiterator<V>(baseIterator, window);
      baseIterator = baseIterator.copy();
      return result;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).addValue(Iterables.toString(this)).toString();
    }
  }

  /**
   * The {@link Reiterator} used by {@link BatchGroupAlsoByWindowViaIteratorsFn.WindowReiterable}.
   */
  private static class WindowReiterator<V> extends ElementByteSizeObservableIterator<V>
      implements Reiterator<V> {
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
     * Moves the underlying iterator forward until it either points to the next element in the
     * correct window, or is past the end of the window.
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

  /** {@link Reiterator} that wraps a {@link List}. */
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
}
