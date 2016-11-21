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
package org.apache.beam.sdk.util.state;

import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

/**
 * Helpers for merging state.
 */
public class StateMerging {
  /**
   * Clear all state in {@code address} in all windows under merge (even result windows)
   * in {@code context}.
   */
  public static <K, StateT extends State, W extends BoundedWindow> void clear(
      MergingStateAccessor<K, W> context, StateTag<? super K, StateT> address) {
    for (StateT state : context.accessInEachMergingWindow(address).values()) {
      state.clear();
    }
  }

  /**
   * Prefetch all bag state in {@code address} across all windows under merge in
   * {@code context}, except for the bag state in the final state address window which we can
   * blindly append to.
   */
  public static <K, T, W extends BoundedWindow> void prefetchBags(
      MergingStateAccessor<K, W> context, StateTag<? super K, BagState<T>> address) {
    Map<W, BagState<T>> map = context.accessInEachMergingWindow(address);
    if (map.isEmpty()) {
      // Nothing to prefetch.
      return;
    }
    BagState<T> result = context.access(address);
    // Prefetch everything except what's already in result.
    for (BagState<T> source : map.values()) {
      if (!source.equals(result)) {
        prefetchRead(source);
      }
    }
  }

  /**
   * Merge all bag state in {@code address} across all windows under merge.
   */
  public static <K, T, W extends BoundedWindow> void mergeBags(
      MergingStateAccessor<K, W> context, StateTag<? super K, BagState<T>> address) {
    mergeBags(context.accessInEachMergingWindow(address).values(), context.access(address));
  }

  /**
   * Merge all bag state in {@code sources} (which may include {@code result}) into {@code result}.
   */
  public static <T, W extends BoundedWindow> void mergeBags(
      Collection<BagState<T>> sources, BagState<T> result) {
    if (sources.isEmpty()) {
      // Nothing to merge.
      return;
    }
    // Prefetch everything except what's already in result.
    List<ReadableState<Iterable<T>>> futures = new ArrayList<>(sources.size());
    for (BagState<T> source : sources) {
      if (!source.equals(result)) {
        prefetchRead(source);
        futures.add(source);
      }
    }
    if (futures.isEmpty()) {
      // Result already holds all the values.
      return;
    }
    // Transfer from sources to result.
    for (ReadableState<Iterable<T>> future : futures) {
      for (T element : future.read()) {
        result.add(element);
      }
    }
    // Clear sources except for result.
    for (BagState<T> source : sources) {
      if (!source.equals(result)) {
        source.clear();
      }
    }
  }

  /**
   * Prefetch all combining value state for {@code address} across all merging windows in {@code
   * context}.
   */
  public static <K, StateT extends CombiningState<?, ?>, W extends BoundedWindow> void
      prefetchCombiningValues(MergingStateAccessor<K, W> context,
          StateTag<? super K, StateT> address) {
    for (StateT state : context.accessInEachMergingWindow(address).values()) {
      prefetchRead(state);
    }
  }

  /**
   * Merge all value state in {@code address} across all merging windows in {@code context}.
   */
  public static <K, InputT, AccumT, OutputT, W extends BoundedWindow> void mergeCombiningValues(
      MergingStateAccessor<K, W> context,
      StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> address) {
    mergeCombiningValues(
        context.accessInEachMergingWindow(address).values(), context.access(address));
  }

  /**
   * Merge all value state from {@code sources} (which may include {@code result}) into
   * {@code result}.
   */
  public static <InputT, AccumT, OutputT, W extends BoundedWindow> void mergeCombiningValues(
      Collection<AccumulatorCombiningState<InputT, AccumT, OutputT>> sources,
      AccumulatorCombiningState<InputT, AccumT, OutputT> result) {
    if (sources.isEmpty()) {
      // Nothing to merge.
      return;
    }
    if (sources.size() == 1 && sources.contains(result)) {
      // Result already holds combined value.
      return;
    }
    // Prefetch.
    List<ReadableState<AccumT>> futures = new ArrayList<>(sources.size());
    for (AccumulatorCombiningState<InputT, AccumT, OutputT> source : sources) {
      prefetchRead(source);
    }
    // Read.
    List<AccumT> accumulators = new ArrayList<>(futures.size());
    for (AccumulatorCombiningState<InputT, AccumT, OutputT> source : sources) {
      accumulators.add(source.getAccum());
    }
    // Merge (possibly update and return one of the existing accumulators).
    AccumT merged = result.mergeAccumulators(accumulators);
    // Clear sources.
    for (AccumulatorCombiningState<InputT, AccumT, OutputT> source : sources) {
      source.clear();
    }
    // Update result.
    result.addAccum(merged);
  }

  /**
   * Prefetch all watermark state for {@code address} across all merging windows in
   * {@code context}.
   */
  public static <K, W extends BoundedWindow> void prefetchWatermarks(
      MergingStateAccessor<K, W> context,
      StateTag<? super K, WatermarkHoldState<W>> address) {
    Map<W, WatermarkHoldState<W>> map = context.accessInEachMergingWindow(address);
    WatermarkHoldState<W> result = context.access(address);
    if (map.isEmpty()) {
      // Nothing to prefetch.
      return;
    }
    if (map.size() == 1 && map.values().contains(result)
        && result.getOutputTimeFn().dependsOnlyOnEarliestInputTimestamp()) {
      // Nothing to change.
      return;
    }
    if (result.getOutputTimeFn().dependsOnlyOnWindow()) {
      // No need to read existing holds.
      return;
    }
    // Prefetch.
    for (WatermarkHoldState<W> source : map.values()) {
      prefetchRead(source);
    }
  }

  private static void prefetchRead(ReadableState<?> source) {
    source.readLater();
  }

  /**
   * Merge all watermark state in {@code address} across all merging windows in {@code context},
   * where the final merge result window is {@code mergeResult}.
   */
  public static <K, W extends BoundedWindow> void mergeWatermarks(
      MergingStateAccessor<K, W> context,
      StateTag<? super K, WatermarkHoldState<W>> address,
      W mergeResult) {
    mergeWatermarks(
        context.accessInEachMergingWindow(address).values(), context.access(address), mergeResult);
  }

  /**
   * Merge all watermark state in {@code sources} (which must include {@code result} if non-empty)
   * into {@code result}, where the final merge result window is {@code mergeResult}.
   */
  public static <W extends BoundedWindow> void mergeWatermarks(
      Collection<WatermarkHoldState<W>> sources, WatermarkHoldState<W> result,
      W resultWindow) {
    if (sources.isEmpty()) {
      // Nothing to merge.
      return;
    }
    if (sources.size() == 1 && sources.contains(result)
        && result.getOutputTimeFn().dependsOnlyOnEarliestInputTimestamp()) {
      // Nothing to merge.
      return;
    }
    if (result.getOutputTimeFn().dependsOnlyOnWindow()) {
      // Clear sources.
      for (WatermarkHoldState<W> source : sources) {
        source.clear();
      }
      // Update directly from window-derived hold.
      Instant hold = result.getOutputTimeFn().assignOutputTime(
          BoundedWindow.TIMESTAMP_MIN_VALUE, resultWindow);
      checkState(hold.isAfter(BoundedWindow.TIMESTAMP_MIN_VALUE));
      result.add(hold);
    } else {
      // Prefetch.
      List<ReadableState<Instant>> futures = new ArrayList<>(sources.size());
      for (WatermarkHoldState<W> source : sources) {
        futures.add(source);
      }
      // Read.
      List<Instant> outputTimesToMerge = new ArrayList<>(sources.size());
      for (ReadableState<Instant> future : futures) {
        Instant sourceOutputTime = future.read();
        if (sourceOutputTime != null) {
          outputTimesToMerge.add(sourceOutputTime);
        }
      }
      // Clear sources.
      for (WatermarkHoldState<W> source : sources) {
        source.clear();
      }
      if (!outputTimesToMerge.isEmpty()) {
        // Merge and update.
        result.add(result.getOutputTimeFn().merge(resultWindow, outputTimesToMerge));
      }
    }
  }
}
