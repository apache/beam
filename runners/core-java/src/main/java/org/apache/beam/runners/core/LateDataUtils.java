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
package org.apache.beam.runners.core;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowTracing;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;


/**
 * Utils to handle late data.
 */
public class LateDataUtils {

  /**
   * Returns an {@code Iterable<WindowedValue<InputT>>} that only contains non-late input elements.
   */
  public static <K, V> Iterable<WindowedValue<V>> dropExpiredWindows(
      final K key,
      Iterable<WindowedValue<V>> elements,
      final TimerInternals timerInternals,
      final WindowingStrategy<?, ?> windowingStrategy,
      final Aggregator<Long, Long> droppedDueToLateness) {
    return FluentIterable.from(elements)
        .transformAndConcat(
            // Explode windows to filter out expired ones
            new Function<WindowedValue<V>, Iterable<WindowedValue<V>>>() {
              @Override
              public Iterable<WindowedValue<V>> apply(@Nullable WindowedValue<V> input) {
                if (input == null) {
                  return null;
                }
                return input.explodeWindows();
              }
            })
        .filter(
            new Predicate<WindowedValue<V>>() {
              @Override
              public boolean apply(@Nullable WindowedValue<V> input) {
                if (input == null) {
                  // drop null elements.
                  return false;
                }
                BoundedWindow window = Iterables.getOnlyElement(input.getWindows());
                boolean expired =
                    window
                        .maxTimestamp()
                        .plus(windowingStrategy.getAllowedLateness())
                        .isBefore(timerInternals.currentInputWatermarkTime());
                if (expired) {
                  // The element is too late for this window.
                  droppedDueToLateness.addValue(1L);
                  WindowTracing.debug(
                      "GroupAlsoByWindow: Dropping element at {} for key: {}; "
                          + "window: {} since it is too far behind inputWatermark: {}",
                      input.getTimestamp(),
                      key,
                      window,
                      timerInternals.currentInputWatermarkTime());
                }
                // Keep the element if the window is not expired.
                return !expired;
              }
            });
  }
}
