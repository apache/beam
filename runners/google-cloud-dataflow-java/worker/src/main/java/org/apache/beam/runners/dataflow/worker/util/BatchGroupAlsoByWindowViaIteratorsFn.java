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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.runners.core.PeekingReiterator;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValueReceiver;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterable;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterator;
import org.apache.beam.sdk.util.common.Reiterable;
import org.apache.beam.sdk.util.common.Reiterator;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ListMultimap;
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
    if (strategy.needsMerge()) {
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
      WindowedValueReceiver<KV<K, Iterable<V>>> output)
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

    // Inline single-window slot avoids allocating ArrayListMultimap and its key iterator for
    // GlobalWindows and FixedWindows (where at most one window is active at a time).
    BoundedWindow singleActiveWindow = null;
    long singleActiveMaxTsMillis = 0L;
    ListMultimap<Instant, BoundedWindow> windows = null;

    while (iterator.hasNext()) {
      WindowedValue<V> e = iterator.peek();
      long elemTsMillis = e.getTimestamp().getMillis();
      // Since elements arrive in non-decreasing timestamp order, a single active window whose
      // maxTimestamp is already before elemTsMillis has expired and cannot match this or any
      // subsequent element; clear it now so non-overlapping windows (e.g. FixedWindows) never
      // promote to ArrayListMultimap.
      if (windows == null && singleActiveWindow != null && singleActiveMaxTsMillis < elemTsMillis) {
        singleActiveWindow = null;
      }
      if (e instanceof WindowedValues.SingleWindowedValue) {
        BoundedWindow window = ((WindowedValues.SingleWindowedValue) e).getWindow();
        boolean alreadyActive;
        long windowMaxTsMillis;
        if (windows == null) {
          alreadyActive = singleActiveWindow != null && singleActiveWindow.equals(window);
          if (!alreadyActive) {
            windowMaxTsMillis = maxTimestampMillis(window);
            if (singleActiveWindow == null) {
              singleActiveWindow = window;
              singleActiveMaxTsMillis = windowMaxTsMillis;
            } else {
              windows = ArrayListMultimap.create();
              windows.put(new Instant(singleActiveMaxTsMillis), singleActiveWindow);
              windows.put(new Instant(windowMaxTsMillis), window);
              singleActiveWindow = null;
            }
          } else {
            windowMaxTsMillis = singleActiveMaxTsMillis;
          }
        } else {
          Instant maxTs = window.maxTimestamp();
          windowMaxTsMillis = maxTs.getMillis();
          alreadyActive = windows.containsEntry(maxTs, window);
          if (!alreadyActive) {
            windows.put(maxTs, window);
          }
        }
        if (!alreadyActive) {
          @SuppressWarnings("unchecked")
          W typedWindow = (W) window;
          output.output(
              WindowedValues.of(
                  KV.of(
                      key,
                      (Iterable<V>) new WindowReiterable<V>(iterator, window, windowMaxTsMillis)),
                  strategy.getTimestampCombiner().assign(typedWindow, e.getTimestamp()),
                  window,
                  PaneInfo.ON_TIME_AND_ONLY_FIRING));
        }
      } else {
        for (BoundedWindow window : e.getWindows()) {
          // If this window is not already in the active set, emit a new WindowReiterable
          // corresponding to this window, starting at this element in the input Reiterable.
          boolean alreadyActive;
          long windowMaxTsMillis;
          if (windows == null) {
            alreadyActive = singleActiveWindow != null && singleActiveWindow.equals(window);
            if (!alreadyActive) {
              windowMaxTsMillis = maxTimestampMillis(window);
              if (singleActiveWindow == null) {
                singleActiveWindow = window;
                singleActiveMaxTsMillis = windowMaxTsMillis;
              } else {
                windows = ArrayListMultimap.create();
                windows.put(new Instant(singleActiveMaxTsMillis), singleActiveWindow);
                windows.put(new Instant(windowMaxTsMillis), window);
                singleActiveWindow = null;
              }
            } else {
              windowMaxTsMillis = singleActiveMaxTsMillis;
            }
          } else {
            Instant maxTs = window.maxTimestamp();
            windowMaxTsMillis = maxTs.getMillis();
            alreadyActive = windows.containsEntry(maxTs, window);
            if (!alreadyActive) {
              windows.put(maxTs, window);
            }
          }
          if (!alreadyActive) {
            // This window was produced by strategy.getWindowFn()
            @SuppressWarnings("unchecked")
            W typedWindow = (W) window;
            // Iterating through the WindowReiterable may advance iterator as an optimization
            // for as long as it detects that there are no new windows.
            output.output(
                WindowedValues.of(
                    KV.of(
                        key,
                        (Iterable<V>) new WindowReiterable<V>(iterator, window, windowMaxTsMillis)),
                    strategy.getTimestampCombiner().assign(typedWindow, e.getTimestamp()),
                    window,
                    PaneInfo.ON_TIME_AND_ONLY_FIRING));
          }
        }
      }
      if (!iterator.hasNext()) {
        break;
      }
      // Copy the iterator in case the next DoFn cached its version of the iterator instead
      // of immediately iterating through it.
      // And, only advance the iterator if the consuming operation hasn't done so.
      iterator = iterator.copy();
      if (iterator.hasNext() && iterator.peek() == e) {
        iterator.next();
      }

      // Remove all windows with maxTimestamp behind the current timestamp.
      if (windows == null) {
        if (singleActiveWindow != null && singleActiveMaxTsMillis < elemTsMillis) {
          singleActiveWindow = null;
        }
      } else {
        Iterator<Instant> windowIterator = windows.keys().iterator();
        while (windowIterator.hasNext() && windowIterator.next().isBefore(e.getTimestamp())) {
          windowIterator.remove();
        }
      }
    }
  }

  private static long maxTimestampMillis(BoundedWindow window) {
    if (window instanceof org.apache.beam.sdk.transforms.windowing.IntervalWindow) {
      return ((org.apache.beam.sdk.transforms.windowing.IntervalWindow) window).end().getMillis()
          - 1L;
    }
    return window.maxTimestamp().getMillis();
  }

  /**
   * {@link Reiterable} representing a view of all elements in a base {@link Reiterator} that are in
   * a given window.
   */
  private static class WindowReiterable<V>
      extends ElementByteSizeObservableIterable<V, WindowReiterator<V>> implements Reiterable<V> {
    private PeekingReiterator<WindowedValue<V>> baseIterator;
    private final BoundedWindow window;
    private final long windowMaxTimestampMillis;

    public WindowReiterable(
        PeekingReiterator<WindowedValue<V>> baseIterator,
        BoundedWindow window,
        long windowMaxTimestampMillis) {
      this.baseIterator = baseIterator;
      this.window = window;
      this.windowMaxTimestampMillis = windowMaxTimestampMillis;
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
      WindowReiterator<V> result =
          new WindowReiterator<V>(baseIterator, window, windowMaxTimestampMillis, false, false);
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
    private final BoundedWindow window;
    private final long windowMaxTimestampMillis;
    private boolean positioned;
    private boolean hasValid;

    private WindowReiterator(
        PeekingReiterator<WindowedValue<V>> iterator,
        BoundedWindow window,
        long windowMaxTimestampMillis,
        boolean positioned,
        boolean hasValid) {
      this.iterator = iterator;
      this.window = window;
      this.windowMaxTimestampMillis = windowMaxTimestampMillis;
      this.positioned = positioned;
      this.hasValid = hasValid;
    }

    @Override
    public Reiterator<V> copy() {
      return new WindowReiterator<V>(
          iterator.copy(), window, windowMaxTimestampMillis, positioned, hasValid);
    }

    @Override
    public boolean hasNext() {
      if (!positioned) {
        hasValid = skipToValidElement();
        positioned = true;
      }
      return hasValid;
    }

    @Override
    public V next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No next item in window");
      }
      positioned = false;
      return iterator.next().getValue();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    /**
     * Moves the underlying iterator forward until it either points to the next element in the
     * correct window, or is past the end of the window.
     */
    @SuppressWarnings("ReferenceEquality")
    private boolean skipToValidElement() {
      while (iterator.hasNext()) {
        WindowedValue<V> peek = iterator.peek();
        if (peek.getTimestamp().getMillis() > windowMaxTimestampMillis) {
          // We are past the end of this window, so there can't be any more
          // elements in this iterator.
          return false;
        }
        if (peek instanceof WindowedValues.SingleWindowedValue) {
          BoundedWindow peekWindow = ((WindowedValues.SingleWindowedValue) peek).getWindow();
          if (peekWindow == window || window.equals(peekWindow)) {
            return true;
          }
          // We have reached a new window; copy the iterator so we don't keep advancing
          // the outer loop in processElement.
          iterator = iterator.copy();
          iterator.next();
        } else {
          Collection<? extends BoundedWindow> peekWindows = peek.getWindows();
          boolean contains = peekWindows.contains(window);
          if (!(peekWindows.size() == 1 && contains)) {
            // We have reached new windows; we need to copy the iterator so we don't
            // keep advancing the outer loop in processElement.
            iterator = iterator.copy();
          }
          if (!contains) {
            // The next element is not in the right window: skip it.
            iterator.next();
          } else {
            // The next element is in the right window.
            return true;
          }
        }
      }
      return false;
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
