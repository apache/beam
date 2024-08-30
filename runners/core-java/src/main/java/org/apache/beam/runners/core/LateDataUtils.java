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

import org.apache.beam.runners.core.metrics.CounterCell;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowTracing;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** Utils to handle late data. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness",
  "keyfor"
}) // TODO(https://github.com/apache/beam/issues/20497)
public class LateDataUtils {
  private LateDataUtils() {}

  /**
   * Return when {@code window} should be garbage collected. If the window's expiration time is on
   * or after the end of the global window, it will be truncated to the end of the global window.
   */
  public static Instant garbageCollectionTime(
      BoundedWindow window, WindowingStrategy windowingStrategy) {
    return garbageCollectionTime(window, windowingStrategy.getAllowedLateness());
  }

  /**
   * Return when {@code window} should be garbage collected. If the window's expiration time is on
   * or after the end of the global window, it will be truncated to the end of the global window.
   */
  public static Instant garbageCollectionTime(BoundedWindow window, Duration allowedLateness) {

    // If the end of the window + allowed lateness is beyond the "end of time" aka the end of the
    // global window, then we truncate it. The conditional is phrased like it is because the
    // addition of EOW + allowed lateness might even overflow the maximum allowed Instant
    if (GlobalWindow.INSTANCE
        .maxTimestamp()
        .minus(allowedLateness)
        .isBefore(window.maxTimestamp())) {
      return GlobalWindow.INSTANCE.maxTimestamp();
    } else {
      return window.maxTimestamp().plus(allowedLateness);
    }
  }

  /**
   * Returns an {@code Iterable<WindowedValue<InputT>>} that only contains non-late input elements.
   */
  public static <K, V> Iterable<WindowedValue<V>> dropExpiredWindows(
      final K key,
      Iterable<WindowedValue<V>> elements,
      final TimerInternals timerInternals,
      final WindowingStrategy<?, ?> windowingStrategy,
      final CounterCell droppedDueToLateness) {
    return FluentIterable.from(elements)
        .transformAndConcat(
            // Explode windows to filter out expired ones
            input -> {
              if (input == null) {
                return null;
              }
              return input.explodeWindows();
            })
        .filter(
            input -> {
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
                droppedDueToLateness.inc();
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
            });
  }
}
