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
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.beam.runners.core.PeekingReiterator;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
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
    PeekingReiterator<WindowedValue<V>> iterator = createPeekingIterator(value);

    // Inline single-window slot avoids allocating any active-window collection for GlobalWindows
    // and non-overlapping FixedWindows (where at most one window is active at a time).
    BoundedWindow singleActiveWindow = null;
    long singleActiveMaxTsMillis = 0L;
    @Nullable ActiveWindows activeWindows = null;

    while (iterator.hasNext()) {
      WindowedValue<V> e = iterator.peek();
      Instant elemTs = e.getTimestamp();
      long elemTsMillis = elemTs.getMillis();
      boolean needsCopy = false;
      BoundedWindow singleWindow = null;
      Collection<? extends BoundedWindow> multiWindows = null;
      if (e instanceof WindowedValues.SingleWindowedValue) {
        singleWindow = checkNotNull(((WindowedValues.SingleWindowedValue) e).getWindow());
      } else {
        Collection<? extends BoundedWindow> elemWindows = e.getWindows();
        if (elemWindows.size() == 1) {
          singleWindow = checkNotNull(elemWindows.iterator().next());
        } else if (!elemWindows.isEmpty()) {
          multiWindows = elemWindows;
        }
      }

      if (singleWindow != null) {
        if (activeWindows == null) {
          if (singleActiveWindow == null) {
            long maxTsMillis = maxTimestampMillis(singleWindow);
            singleActiveWindow = singleWindow;
            singleActiveMaxTsMillis = maxTsMillis;
            needsCopy = emitWindow(key, singleWindow, maxTsMillis, elemTs, iterator, output);
          } else if (singleActiveWindow != singleWindow
              && !singleActiveWindow.equals(singleWindow)) {
            long maxTsMillis = maxTimestampMillis(singleWindow);
            if (singleActiveMaxTsMillis < elemTsMillis) {
              // Prior active window expired before elemTs and didn't match this window; replace
              // in place so non-overlapping windows (e.g. FixedWindows) never allocate
              // ActiveWindows.
              singleActiveWindow = singleWindow;
              singleActiveMaxTsMillis = maxTsMillis;
            } else {
              activeWindows =
                  promoteToActiveWindows(
                      singleActiveWindow, singleActiveMaxTsMillis, singleWindow, maxTsMillis);
              singleActiveWindow = null;
            }
            needsCopy = emitWindow(key, singleWindow, maxTsMillis, elemTs, iterator, output);
          }
        } else {
          needsCopy =
              emitIfAbsentInActiveWindows(
                  key, singleWindow, elemTs, iterator, activeWindows, output);
        }
      } else if (multiWindows != null) {
        activeWindows =
            ensureActiveWindows(activeWindows, singleActiveWindow, singleActiveMaxTsMillis);
        singleActiveWindow = null;
        needsCopy =
            processMultipleWindows(key, multiWindows, elemTs, iterator, activeWindows, output);
      }
      if (!iterator.hasNext()) {
        break;
      }
      boolean sameElement = iterator.peek() == e;
      if (needsCopy) {
        // An emitted WindowReiterable or its first WindowReiterator still aliases iterator.
        // Detach before advancing so delayed/partial consumers remain at the right position.
        iterator = iterator.copy();
      }
      if (sameElement) {
        iterator.next();
      }

      // Remove all windows with maxTimestamp behind the current element's timestamp.
      if (activeWindows == null) {
        if (singleActiveWindow != null && singleActiveMaxTsMillis < elemTsMillis) {
          singleActiveWindow = null;
        }
      } else {
        activeWindows.expireBefore(elemTsMillis);
      }
    }
  }

  private static <V> PeekingReiterator<WindowedValue<V>> createPeekingIterator(
      Iterable<WindowedValue<V>> value) {
    if (value instanceof Collection) {
      return new PeekingReiterator<>(
          new ListReiterator<WindowedValue<V>>(
              new ArrayList<WindowedValue<V>>((Collection<WindowedValue<V>>) value), 0));
    } else if (value instanceof Reiterable) {
      return new PeekingReiterator<>(((Reiterable<WindowedValue<V>>) value).iterator());
    } else {
      throw new IllegalArgumentException(
          "Input to GroupAlsoByWindowsDoFn must be a Collection or Reiterable");
    }
  }

  private boolean emitIfAbsentInActiveWindows(
      K key,
      BoundedWindow window,
      Instant elemTs,
      PeekingReiterator<WindowedValue<V>> iterator,
      ActiveWindows activeWindows,
      WindowedValueReceiver<KV<K, Iterable<V>>> output)
      throws Exception {
    if (activeWindows.contains(window)) {
      return false;
    }
    long maxTsMillis = maxTimestampMillis(window);
    activeWindows.add(window, maxTsMillis);
    return emitWindow(key, window, maxTsMillis, elemTs, iterator, output);
  }

  private boolean emitWindow(
      K key,
      BoundedWindow window,
      long windowMaxTsMillis,
      Instant elemTs,
      PeekingReiterator<WindowedValue<V>> iterator,
      WindowedValueReceiver<KV<K, Iterable<V>>> output)
      throws Exception {
    // This window was produced by strategy.getWindowFn()
    @SuppressWarnings("unchecked")
    W typedWindow = (W) window;
    // Iterating through the WindowReiterable may advance iterator as an optimization
    // for as long as it detects that there are no new windows.
    WindowReiterable<V> reiterable = new WindowReiterable<>(iterator, window, windowMaxTsMillis);
    output.output(
        WindowedValues.of(
            KV.of(key, (Iterable<V>) reiterable),
            strategy.getTimestampCombiner().assign(typedWindow, elemTs),
            window,
            PaneInfo.ON_TIME_AND_ONLY_FIRING));
    return reiterable.isParentAliased();
  }

  private static ActiveWindows promoteToActiveWindows(
      BoundedWindow existingWindow,
      long existingMaxTsMillis,
      BoundedWindow newWindow,
      long newMaxTsMillis) {
    ActiveWindows active = new ActiveWindows();
    active.add(existingWindow, existingMaxTsMillis);
    active.add(newWindow, newMaxTsMillis);
    return active;
  }

  private static ActiveWindows ensureActiveWindows(
      @Nullable ActiveWindows activeWindows,
      @Nullable BoundedWindow singleActiveWindow,
      long singleActiveMaxTsMillis) {
    if (activeWindows != null) {
      return activeWindows;
    }
    ActiveWindows created = new ActiveWindows();
    if (singleActiveWindow != null) {
      created.add(singleActiveWindow, singleActiveMaxTsMillis);
    }
    return created;
  }

  private boolean processMultipleWindows(
      K key,
      Collection<? extends BoundedWindow> elemWindows,
      Instant elemTs,
      PeekingReiterator<WindowedValue<V>> iterator,
      ActiveWindows activeWindows,
      WindowedValueReceiver<KV<K, Iterable<V>>> output)
      throws Exception {
    boolean needsCopy = false;
    for (BoundedWindow window : elemWindows) {
      if (!activeWindows.contains(window)) {
        long maxTsMillis = maxTimestampMillis(window);
        activeWindows.add(window, maxTsMillis);
        if (emitWindow(key, window, maxTsMillis, elemTs, iterator, output)) {
          needsCopy = true;
        }
      }
    }
    return needsCopy;
  }

  private static long maxTimestampMillis(BoundedWindow window) {
    // Exact class check avoids bypassing overridden maxTimestamp() on IntervalWindow subclasses,
    // and subtractExact preserves IntervalWindow.maxTimestamp()'s ArithmeticException at MIN_VALUE.
    if (window.getClass() == IntervalWindow.class) {
      return Math.subtractExact(((IntervalWindow) window).end().getMillis(), 1L);
    }
    return window.maxTimestamp().getMillis();
  }

  /**
   * Multi-window active set used only when more than one window is active concurrently (e.g. {@code
   * SlidingWindows}). Membership lookup via {@link HashSet#contains} avoids allocating {@link
   * Instant}/{@link org.joda.time.Duration} objects per element, and {@link PriorityQueue}
   * guarantees exact min-timestamp expiry ordering.
   */
  private static final class ActiveWindows {
    private static final class WindowExpiry {
      final BoundedWindow window;
      final long maxTimestampMillis;

      WindowExpiry(BoundedWindow window, long maxTimestampMillis) {
        this.window = window;
        this.maxTimestampMillis = maxTimestampMillis;
      }
    }

    private static final Comparator<WindowExpiry> BY_MAX_TIMESTAMP =
        Comparator.comparingLong(e -> e.maxTimestampMillis);

    private final Set<BoundedWindow> active = new HashSet<>();
    private final PriorityQueue<WindowExpiry> byMaxTimestamp =
        new PriorityQueue<>(BY_MAX_TIMESTAMP);

    boolean contains(BoundedWindow window) {
      return active.contains(window);
    }

    void add(BoundedWindow window, long maxTimestampMillis) {
      if (active.add(window)) {
        byMaxTimestamp.add(new WindowExpiry(window, maxTimestampMillis));
      }
    }

    void expireBefore(long timestampMillis) {
      WindowExpiry head;
      while ((head = byMaxTimestamp.peek()) != null && head.maxTimestampMillis < timestampMillis) {
        byMaxTimestamp.poll();
        active.remove(head.window);
      }
    }
  }

  /**
   * {@link Reiterable} representing a view of all elements in a base {@link Reiterator} that are in
   * a given window.
   */
  private static final class WindowReiterable<V>
      extends ElementByteSizeObservableIterable<V, WindowReiterator<V>> implements Reiterable<V> {
    private PeekingReiterator<WindowedValue<V>> baseIterator;
    private final BoundedWindow window;
    private final long windowMaxTimestampMillis;
    private boolean sharedWithParent = true;
    // Remains true while either this reiterable or its first WindowReiterator still shares
    // processElement()'s un-copied PeekingReiterator. Updated directly by the first
    // WindowReiterator instead of holding a WindowReiterator reference here so C2 escape
    // analysis can still scalar-replace the iterator inside eager consumers.
    boolean parentAliased = true;

    public WindowReiterable(
        PeekingReiterator<WindowedValue<V>> baseIterator,
        BoundedWindow window,
        long windowMaxTimestampMillis) {
      this.baseIterator = baseIterator;
      this.window = window;
      this.windowMaxTimestampMillis = windowMaxTimestampMillis;
    }

    boolean isParentAliased() {
      return parentAliased;
    }

    @Override
    public WindowReiterator<V> iterator() {
      return createIterator();
    }

    @Override
    protected WindowReiterator<V> createIterator() {
      // We don't copy the baseIterator when creating the first WindowReiterator
      // so that the WindowReiterator can advance the baseIterator. We have to
      // make a copy afterwards so that future calls to iterator() will start
      // at the right spot (and subsequent iterators are already detached from
      // the parent processElement() iterator).
      WindowReiterator<V> result =
          new WindowReiterator<V>(
              baseIterator,
              window,
              windowMaxTimestampMillis,
              false,
              false,
              sharedWithParent ? this : null);
      baseIterator = baseIterator.copy();
      sharedWithParent = false;
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
  private static final class WindowReiterator<V> extends ElementByteSizeObservableIterator<V>
      implements Reiterator<V> {
    private @Nullable PeekingReiterator<WindowedValue<V>> iterator;
    private final BoundedWindow window;
    private final long windowMaxTimestampMillis;
    private boolean positioned;
    private boolean hasValid;
    // Non-null only for the first WindowReiterator while its iterator still aliases the parent
    // PeekingReiterator from processElement() (or a shared sibling WindowReiterable). Cleared on
    // the first detach or exhaustion so we copy at most once per window and hold zero reference
    // after draining.
    private @Nullable WindowReiterable<V> parentAliasOwner;

    private WindowReiterator(
        @Nullable PeekingReiterator<WindowedValue<V>> iterator,
        BoundedWindow window,
        long windowMaxTimestampMillis,
        boolean positioned,
        boolean hasValid,
        @Nullable WindowReiterable<V> parentAliasOwner) {
      this.iterator = iterator;
      this.window = window;
      this.windowMaxTimestampMillis = windowMaxTimestampMillis;
      this.positioned = positioned;
      this.hasValid = hasValid;
      this.parentAliasOwner = parentAliasOwner;
    }

    @Override
    public Reiterator<V> copy() {
      PeekingReiterator<WindowedValue<V>> iter = this.iterator;
      if (iter == null) {
        return new WindowReiterator<V>(null, window, windowMaxTimestampMillis, true, false, null);
      }
      return new WindowReiterator<V>(
          iter.copy(), window, windowMaxTimestampMillis, positioned, hasValid, null);
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
      PeekingReiterator<WindowedValue<V>> iter = this.iterator;
      if (iter == null) {
        throw new NoSuchElementException("No next item in window");
      }
      positioned = false;
      return iter.next().getValue();
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
      PeekingReiterator<WindowedValue<V>> iter = this.iterator;
      if (iter == null) {
        return false;
      }
      while (iter.hasNext()) {
        WindowedValue<V> peek = iter.peek();
        if (peek.getTimestamp().getMillis() > windowMaxTimestampMillis) {
          // We are past the end of this window, so there can't be any more
          // elements in this iterator.
          break;
        }
        if (peek instanceof WindowedValues.SingleWindowedValue) {
          BoundedWindow peekWindow = ((WindowedValues.SingleWindowedValue) peek).getWindow();
          if (peekWindow == window || window.equals(peekWindow)) {
            return true;
          }
          // We have reached a new window; detach once from processElement()'s iterator.
          WindowReiterable<V> owner = parentAliasOwner;
          if (owner != null) {
            iter = iter.copy();
            this.iterator = iter;
            owner.parentAliased = false;
            parentAliasOwner = null;
          }
          iter.next();
        } else {
          Collection<? extends BoundedWindow> peekWindows = peek.getWindows();
          boolean contains = peekWindows.contains(window);
          WindowReiterable<V> owner = parentAliasOwner;
          if (owner != null && (!contains || peekWindows.size() != 1)) {
            // We have reached a different or multi-window element; detach once from
            // processElement()'s iterator.
            iter = iter.copy();
            this.iterator = iter;
            owner.parentAliased = false;
            parentAliasOwner = null;
          }
          if (!contains) {
            // The next element is not in the right window: skip it.
            iter.next();
          } else {
            // The next element is in the right window.
            return true;
          }
        }
      }
      this.iterator = null;
      WindowReiterable<V> owner = parentAliasOwner;
      if (owner != null) {
        owner.parentAliased = false;
        parentAliasOwner = null;
      }
      return false;
    }
  }

  /** {@link Reiterator} that wraps a {@link List}. */
  private static final class ListReiterator<T> implements Reiterator<T> {
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
