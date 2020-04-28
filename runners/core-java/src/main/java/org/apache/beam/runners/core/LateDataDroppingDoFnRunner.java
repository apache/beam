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

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowTracing;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.joda.time.Instant;

/**
 * A customized {@link DoFnRunner} that handles late data dropping for a {@link KeyedWorkItem} input
 * {@link DoFn}.
 *
 * <p>It expands windows before checking data lateness.
 *
 * <p>{@link KeyedWorkItem KeyedWorkItems} are always in empty windows.
 *
 * @param <K> key type
 * @param <InputT> input value element type
 * @param <OutputT> output value element type
 * @param <W> window type
 */
public class LateDataDroppingDoFnRunner<K, InputT, OutputT, W extends BoundedWindow>
    implements DoFnRunner<KeyedWorkItem<K, InputT>, KV<K, OutputT>> {
  private final DoFnRunner<KeyedWorkItem<K, InputT>, KV<K, OutputT>> doFnRunner;
  private final LateDataFilter lateDataFilter;

  public static final String DROPPED_DUE_TO_LATENESS = "droppedDueToLateness";

  public LateDataDroppingDoFnRunner(
      DoFnRunner<KeyedWorkItem<K, InputT>, KV<K, OutputT>> doFnRunner,
      WindowingStrategy<?, ?> windowingStrategy,
      TimerInternals timerInternals) {
    this.doFnRunner = doFnRunner;
    lateDataFilter = new LateDataFilter(windowingStrategy, timerInternals);
  }

  @Override
  public DoFn<KeyedWorkItem<K, InputT>, KV<K, OutputT>> getFn() {
    return doFnRunner.getFn();
  }

  @Override
  public void startBundle() {
    doFnRunner.startBundle();
  }

  @Override
  public void processElement(WindowedValue<KeyedWorkItem<K, InputT>> elem) {
    Iterable<WindowedValue<InputT>> nonLateElements =
        lateDataFilter.filter(elem.getValue().key(), elem.getValue().elementsIterable());
    KeyedWorkItem<K, InputT> keyedWorkItem =
        KeyedWorkItems.workItem(
            elem.getValue().key(), elem.getValue().timersIterable(), nonLateElements);
    doFnRunner.processElement(elem.withValue(keyedWorkItem));
  }

  @Override
  public <KeyT> void onTimer(
      String timerId,
      String timerFamilyId,
      KeyT key,
      BoundedWindow window,
      Instant timestamp,
      Instant outputTimestamp,
      TimeDomain timeDomain) {
    doFnRunner.onTimer(timerId, timerFamilyId, key, window, timestamp, outputTimestamp, timeDomain);
  }

  @Override
  public void finishBundle() {
    doFnRunner.finishBundle();
  }

  @Override
  public <KeyT> void onWindowExpiration(BoundedWindow window, Instant timestamp, KeyT key) {
    doFnRunner.onWindowExpiration(window, timestamp, key);
  }

  /** It filters late data in a {@link KeyedWorkItem}. */
  @VisibleForTesting
  static class LateDataFilter {
    private final WindowingStrategy<?, ?> windowingStrategy;
    private final TimerInternals timerInternals;
    private final Counter droppedDueToLateness;

    public LateDataFilter(
        WindowingStrategy<?, ?> windowingStrategy, TimerInternals timerInternals) {
      this.windowingStrategy = windowingStrategy;
      this.timerInternals = timerInternals;
      this.droppedDueToLateness =
          Metrics.counter(LateDataDroppingDoFnRunner.class, DROPPED_DUE_TO_LATENESS);
    }

    /**
     * Returns an {@code Iterable<WindowedValue<InputT>>} that only contains non-late input
     * elements.
     */
    public <K, InputT> Iterable<WindowedValue<InputT>> filter(
        final K key, Iterable<WindowedValue<InputT>> elements) {
      Iterable<Iterable<WindowedValue<InputT>>> windowsExpandedElements =
          StreamSupport.stream(elements.spliterator(), false)
              .map(
                  input ->
                      input.getWindows().stream()
                          .map(
                              window ->
                                  WindowedValue.of(
                                      input.getValue(),
                                      input.getTimestamp(),
                                      window,
                                      input.getPane()))
                          .collect(Collectors.toList()))
              .collect(Collectors.toList());
      Iterable<WindowedValue<InputT>> concatElements = Iterables.concat(windowsExpandedElements);

      // Bump the counter separately since we don't want multiple iterations to
      // increase it multiple times.
      for (WindowedValue<InputT> input : concatElements) {
        BoundedWindow window = Iterables.getOnlyElement(input.getWindows());
        if (canDropDueToExpiredWindow(window)) {
          // The element is too late for this window.
          droppedDueToLateness.inc();
          WindowTracing.debug(
              "{}: Dropping element at {} for key:{}; window:{} "
                  + "since too far behind inputWatermark:{}; outputWatermark:{}",
              LateDataFilter.class.getSimpleName(),
              input.getTimestamp(),
              key,
              window,
              timerInternals.currentInputWatermarkTime(),
              timerInternals.currentOutputWatermarkTime());
        }
      }

      // return nonLateElements
      return StreamSupport.stream(concatElements.spliterator(), false)
          .filter(
              input -> {
                BoundedWindow window = Iterables.getOnlyElement(input.getWindows());
                return !canDropDueToExpiredWindow(window);
              })
          .collect(Collectors.toList());
    }

    /** Is {@code window} expired w.r.t. the garbage collection watermark? */
    private boolean canDropDueToExpiredWindow(BoundedWindow window) {
      Instant inputWM = timerInternals.currentInputWatermarkTime();
      return LateDataUtils.garbageCollectionTime(window, windowingStrategy).isBefore(inputWM);
    }
  }
}
