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
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
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

/** Helper class for handling elements blocked on side inputs. */
@SuppressWarnings("nullness" // TODO(https://github.com/apache/beam/issues/20497)
)
class StreamingSideInputProcessor<InputT, W extends BoundedWindow> {
  private final StreamingSideInputFetcher<InputT, W> sideInputFetcher;

  public StreamingSideInputProcessor(StreamingSideInputFetcher<InputT, W> sideInputFetcher) {
    this.sideInputFetcher = sideInputFetcher;
  }

  /**
   * Handle's startBundle. If there are unblocked elements, process them and then return the set of
   * windows that were unblocked.
   */
  void tryUnblockElements(Consumer<Iterable<WindowedValue<InputT>>> consumer) {
    sideInputFetcher.prefetchBlockedMap();

    // Find the set of ready windows.
    Set<W> readyWindows = sideInputFetcher.getReadyWindows();

    Iterable<BagState<WindowedValue<InputT>>> elementBags =
        sideInputFetcher.prefetchElements(readyWindows);
    Iterable<WindowedValue<InputT>> releasedElements =
        Iterables.concat(Iterables.transform(elementBags, BagState::read));
    consumer.accept(releasedElements);
    elementBags.forEach(BagState::clear);
    sideInputFetcher.releaseBlockedWindows(readyWindows);
  }

  void tryUnblockElementsAndTimers(
      BiConsumer<Iterable<WindowedValue<InputT>>, Iterable<TimerInternals.TimerData>> consumer) {
    sideInputFetcher.prefetchBlockedMap();

    // Find the set of ready windows.
    Set<W> readyWindows = sideInputFetcher.getReadyWindows();

    Iterable<BagState<TimerInternals.TimerData>> timerBags =
        sideInputFetcher.prefetchTimers(readyWindows);
    Iterable<TimerInternals.TimerData> releasedTimers =
        Iterables.concat(
            Iterables.transform(sideInputFetcher.prefetchTimers(readyWindows), BagState::read));
    Iterable<BagState<WindowedValue<InputT>>> elementBags =
        sideInputFetcher.prefetchElements(readyWindows);
    Iterable<WindowedValue<InputT>> releasedElements =
        Iterables.concat(Iterables.transform(elementBags, BagState::read));

    consumer.accept(releasedElements, releasedTimers);
    timerBags.forEach(BagState::clear);
    elementBags.forEach(BagState::clear);
    sideInputFetcher.releaseBlockedWindows(readyWindows);
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

  <K> WindowedValue<KeyedWorkItem<K, InputT>> handleProcessKeyedWorkItem(
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

    return elem.withValue(keyedWorkItem);
  }

  void handleProcessTimer(TimerInternals.TimerData timer) {
    // We must call this to ensure the side-input is cached for the timer. However since a user
    // timer can only
    // be set via element processing (or another timer) in the same window, the window should be
    // unblocked once
    // we get here.
    Preconditions.checkState(!sideInputFetcher.storeIfBlocked(timer));
  }
}
