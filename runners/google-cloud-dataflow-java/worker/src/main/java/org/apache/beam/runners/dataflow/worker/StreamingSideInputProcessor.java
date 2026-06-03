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
package org.apache.beam.runners.dataflow.worker;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Helper class for handling elements blocked on side inputs. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class StreamingSideInputProcessor<InputT, W extends BoundedWindow> {
  private final StreamingSideInputFetcher<InputT, W> sideInputFetcher;

  public StreamingSideInputProcessor(StreamingSideInputFetcher<InputT, W> sideInputFetcher) {
    this.sideInputFetcher = sideInputFetcher;
  }

  /**
   * Handle's startBundle. If there are unblocked elements, process them and then return the set of
   * windows that were unblocked.
   */
  Iterator<WindowedValue<InputT>> tryUnblockElements() {
    sideInputFetcher.prefetchBlockedMap();

    // Find the set of ready windows.
    Set<W> readyWindows = sideInputFetcher.getReadyWindows();

    Iterable<BagState<WindowedValue<InputT>>> elementsBags =
        sideInputFetcher.prefetchElements(readyWindows);

    // Return a lazy iterator to the released elements.
    Iterator<WindowedValue<InputT>> releasedElements =
        new DestructivePagingIterator(elementsBags, readyWindows);

    return releasedElements;
  }

  Iterator<TimerInternals.TimerData> tryUnblockTimers() {
    sideInputFetcher.prefetchBlockedMap();

    // Find the set of ready windows.
    Set<W> readyWindows = sideInputFetcher.getReadyWindows();

    Iterable<BagState<TimerInternals.TimerData>> timerBags =
        sideInputFetcher.prefetchTimers(readyWindows);

    // Return a lazy iterator to the released timers.
    Iterator<TimerInternals.TimerData> releasedTimers =
        new DestructivePagingIterator(timerBags, readyWindows);

    return releasedTimers;
  }

  void handleFinishBundle() {
    sideInputFetcher.persist();
  }

  /*
  Handle process element. Runs the elements that have an available side input, and buffers elements for which the
  side input is blocked. Returns the list of elements that are unblocked and should be processed.
   */
  Iterator<? extends WindowedValue<InputT>> handleProcessElement(
      WindowedValue<InputT> compressedElem) {
    // Note: We could write this as a three-line stream expression, but side effects are discouraged
    // in Java streams.
    return Iterators.filter(
        compressedElem.explodeWindows().iterator(),
        (WindowedValue<InputT> e) -> !sideInputFetcher.storeIfBlocked(e));
  }

  <K> KeyedWorkItem<K, InputT> handleProcessKeyedWorkItem(
      WindowedValue<KeyedWorkItem<K, InputT>> elem) {
    List<WindowedValue<InputT>> readyInputs =
        Lists.newArrayList(
            Iterables.filter(
                elem.getValue().elementsIterable(),
                input -> !sideInputFetcher.storeIfBlocked(input)));

    List<TimerInternals.TimerData> readyTimers =
        Lists.newArrayList(
            Iterables.filter(
                elem.getValue().timersIterable(),
                timer -> !sideInputFetcher.storeIfBlocked(timer)));
    KeyedWorkItem<K, InputT> keyedWorkItem =
        KeyedWorkItems.workItem(elem.getValue().key(), readyTimers, readyInputs);
    return keyedWorkItem;
  }

  void handleProcessTimer(TimerInternals.TimerData timer) {
    // We must call this to ensure the side-input is cached for the timer. However since a user
    // timer can only
    // be set via element processing (or another timer) in the same window, the window should be
    // unblocked once
    // we get here.
    Preconditions.checkState(!sideInputFetcher.storeIfBlocked(timer));
  }

  // This is a destructive iterator - it clears
  // the bags after reading them. Bags can be paged in from the service, so we try to avoid
  // materializing the whole
  // bag into memory here.
  private class DestructivePagingIterator<T> implements Iterator<T> {
    private final Set<W> readyWindows;
    Iterator<BagState<T>> bagsIterator;
    @Nullable Iterator<T> currentBagElements;
    @Nullable BagState<T> currentBag;

    public DestructivePagingIterator(Iterable<BagState<T>> elementsBags, Set<W> readyWindows) {
      this.readyWindows = readyWindows;
      bagsIterator = elementsBags.iterator();
    }

    @Override
    public boolean hasNext() {
      do {
        if (currentBagElements == null || !currentBagElements.hasNext()) {
          if (!advanceBag()) {
            // We're done iterating - release the blocked windows.
            sideInputFetcher.releaseBlockedWindows(readyWindows);
            return false;
          }
        }
      } while (!org.apache.beam.sdk.util.Preconditions.checkStateNotNull(currentBagElements)
          .hasNext());
      return true;
    }

    boolean advanceBag() {
      // Once we finish reading a bag, clear it.
      clearCurrentBag();
      if (bagsIterator.hasNext()) {
        currentBag = bagsIterator.next();
        currentBagElements = currentBag.read().iterator();
        return true;
      } else {
        return false;
      }
    }

    void clearCurrentBag() {
      if (currentBag != null) {
        currentBag.clear();
        currentBag = null;
        currentBagElements = null;
      }
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return org.apache.beam.sdk.util.Preconditions.checkStateNotNull(currentBagElements).next();
    }
  }
}
