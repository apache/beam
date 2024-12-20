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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.GroupingState;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.TimestampedValue;

/** Helpers for merging state. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class StateMerging {
  /**
   * Clear all state in {@code address} in all windows under merge (even result windows) in {@code
   * context}.
   */
  public static <K, StateT extends State, W extends BoundedWindow> void clear(
      MergingStateAccessor<K, W> context, StateTag<StateT> address) {
    for (StateT state : context.accessInEachMergingWindow(address).values()) {
      state.clear();
    }
  }

  /**
   * Prefetch all bag state in {@code address} across all windows under merge in {@code context},
   * except for the bag state in the final state address window which we can blindly append to.
   */
  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT") // just prefetch calls to readLater
  public static <K, T, W extends BoundedWindow> void prefetchBags(
      MergingStateAccessor<K, W> context, StateTag<BagState<T>> address) {
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

  /** Merge all bag state in {@code address} across all windows under merge. */
  public static <K, T, W extends BoundedWindow> void mergeBags(
      MergingStateAccessor<K, W> context, StateTag<BagState<T>> address) {
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

  public static <K, T, W extends BoundedWindow> void mergeOrderedLists(
      MergingStateAccessor<K, W> context, StateTag<OrderedListState<T>> address) {
    mergeOrderedLists(context.accessInEachMergingWindow(address).values(), context.access(address));
  }

  public static <T, W extends BoundedWindow> void mergeOrderedLists(
      Collection<OrderedListState<T>> sources, OrderedListState<T> result) {
    if (sources.isEmpty()) {
      // Nothing to merge.
      return;
    }
    // Prefetch everything except what's already in result.
    final List<ReadableState<Iterable<TimestampedValue<T>>>> futures =
        new ArrayList<>(sources.size());
    for (OrderedListState<T> source : sources) {
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
    for (ReadableState<Iterable<TimestampedValue<T>>> future : futures) {
      for (TimestampedValue<T> timestampedValue : future.read()) {
        result.add(timestampedValue);
      }
    }
    // Clear sources except for result.
    for (OrderedListState<T> source : sources) {
      if (!source.equals(result)) {
        source.clear();
      }
    }
  }

  /** Merge all set state in {@code address} across all windows under merge. */
  public static <K, T, W extends BoundedWindow> void mergeSets(
      MergingStateAccessor<K, W> context, StateTag<SetState<T>> address) {
    mergeSets(context.accessInEachMergingWindow(address).values(), context.access(address));
  }

  /**
   * Merge all set state in {@code sources} (which may include {@code result}) into {@code result}.
   */
  public static <T, W extends BoundedWindow> void mergeSets(
      Collection<SetState<T>> sources, SetState<T> result) {
    if (sources.isEmpty()) {
      // Nothing to merge.
      return;
    }
    // Prefetch everything except what's already in result.
    List<ReadableState<Iterable<T>>> futures = new ArrayList<>(sources.size());
    for (SetState<T> source : sources) {
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
    for (SetState<T> source : sources) {
      if (!source.equals(result)) {
        source.clear();
      }
    }
  }

  /**
   * Prefetch all combining value state for {@code address} across all merging windows in {@code
   * context}.
   */
  public static <K, StateT extends GroupingState<?, ?>, W extends BoundedWindow>
      void prefetchCombiningValues(MergingStateAccessor<K, W> context, StateTag<StateT> address) {
    for (StateT state : context.accessInEachMergingWindow(address).values()) {
      prefetchRead(state);
    }
  }

  /** Merge all value state in {@code address} across all merging windows in {@code context}. */
  public static <K, InputT, AccumT, OutputT, W extends BoundedWindow> void mergeCombiningValues(
      MergingStateAccessor<K, W> context,
      StateTag<CombiningState<InputT, AccumT, OutputT>> address) {
    mergeCombiningValues(
        context.accessInEachMergingWindow(address).values(), context.access(address));
  }

  /**
   * Merge all value state from {@code sources} (which may include {@code result}) into {@code
   * result}.
   */
  public static <InputT, AccumT, OutputT, W extends BoundedWindow> void mergeCombiningValues(
      Collection<CombiningState<InputT, AccumT, OutputT>> sources,
      CombiningState<InputT, AccumT, OutputT> result) {
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
    for (CombiningState<InputT, AccumT, OutputT> source : sources) {
      prefetchRead(source);
    }
    // Read.
    List<AccumT> accumulators = new ArrayList<>(futures.size());
    for (CombiningState<InputT, AccumT, OutputT> source : sources) {
      accumulators.add(source.getAccum());
    }
    // Merge (possibly update and return one of the existing accumulators).
    AccumT merged = result.mergeAccumulators(accumulators);
    // Clear sources.
    for (CombiningState<InputT, AccumT, OutputT> source : sources) {
      source.clear();
    }
    // Update result.
    result.addAccum(merged);
  }

  private static void prefetchRead(ReadableState<?> source) {
    source.readLater();
  }
}
