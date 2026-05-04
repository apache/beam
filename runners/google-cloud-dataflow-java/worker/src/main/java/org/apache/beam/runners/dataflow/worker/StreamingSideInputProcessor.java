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
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Helper class for handling elements blocked on side inputs. */
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

    // Return a lazy iterator to the released elements. This is a destructive iterator - it clears
    // the bags after reading them. Bags can be paged in from the service, so we try to avoid
    // materializing the whole
    // bag into memory here.
    Iterator<WindowedValue<InputT>> releasedElements =
        new Iterator<WindowedValue<InputT>>() {
          Iterator<BagState<WindowedValue<InputT>>> bagsIterator = elementsBags.iterator();
          @Nullable Iterator<WindowedValue<InputT>> currentBagElements;
          @Nullable BagState<WindowedValue<InputT>> currentBag;

          @Override
          public boolean hasNext() {
            do {
              if (currentBagElements == null || !currentBagElements.hasNext()) {
                if (!advanceBag()) {
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
          public WindowedValue<InputT> next() {
            return org.apache.beam.sdk.util.Preconditions.checkStateNotNull(currentBagElements)
                .next();
          }
        };

    sideInputFetcher.releaseBlockedWindows(readyWindows);
    return releasedElements;
  }

  void handleFinishBundle() {
    sideInputFetcher.persist();
  }

  /*
  Handle process element. Runs the elements that have an available side input, and buffers elements for which the
  side input is blocked. Returns the list of elements that are unblocked and should be processed.
   */
  Iterable<WindowedValue<InputT>> handleProcessElement(WindowedValue<InputT> compressedElem) {
    List<WindowedValue<InputT>> unblockedElements = Lists.newArrayList();
    for (WindowedValue<InputT> exploded : compressedElem.explodeWindows()) {
      if (!sideInputFetcher.storeIfBlocked(exploded)) {
        unblockedElements.add(exploded);
      }
    }
    return unblockedElements;
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
