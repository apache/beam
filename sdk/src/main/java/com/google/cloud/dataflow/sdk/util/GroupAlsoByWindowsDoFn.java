/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.base.Preconditions;

/**
 * DoFn that merges windows and groups elements in those windows, optionally
 * combining values.
 *
 * @param <K> key type
 * @param <InputT> input value element type
 * @param <OutputT> output value element type
 * @param <W> window type
 */
@SystemDoFnInternal
public abstract class GroupAlsoByWindowsDoFn<K, InputT, OutputT, W extends BoundedWindow>
    extends DoFn<KV<K, Iterable<WindowedValue<InputT>>>, KV<K, OutputT>> {

  /**
   * Create a {@link GroupAlsoByWindowsDoFn} without a combine function. Depending on the
   * {@code windowFn} this will either use iterators or window sets to implement the grouping.
   *
   * @param windowingStrategy The window function and trigger to use for grouping
   * @param inputCoder the input coder to use
   */
  public static <K, V, W extends BoundedWindow> GroupAlsoByWindowsDoFn<K, V, Iterable<V>, W>
  createForIterable(WindowingStrategy<?, W> windowingStrategy, Coder<V> inputCoder) {
    @SuppressWarnings("unchecked")
    WindowingStrategy<Object, W> noWildcard = (WindowingStrategy<Object, W>) windowingStrategy;

    return GroupAlsoByWindowsViaIteratorsDoFn.isSupported(windowingStrategy)
        ? new GroupAlsoByWindowsViaIteratorsDoFn<K, V, W>(windowingStrategy)
        : new GroupAlsoByWindowsViaOutputBufferDoFn<>(
            noWildcard,
            SystemReduceFn.<K, V, W>buffering(inputCoder));
  }

  /**
   * Construct a {@link GroupAlsoByWindowsDoFn} using the {@code combineFn} if available.
   */
  public static <K, InputT, AccumT, OutputT, W extends BoundedWindow>
  GroupAlsoByWindowsDoFn<K, InputT, OutputT, W>
  create(
      final WindowingStrategy<?, W> windowingStrategy,
      final AppliedCombineFn<K, InputT, AccumT, OutputT> combineFn,
      final Coder<K> keyCoder) {
    Preconditions.checkNotNull(combineFn);

    @SuppressWarnings("unchecked")
    WindowingStrategy<Object, W> noWildcard = (WindowingStrategy<Object, W>) windowingStrategy;
    return GroupAlsoByWindowsAndCombineDoFn.isSupported(windowingStrategy)
        ? new GroupAlsoByWindowsAndCombineDoFn<>(noWildcard.getWindowFn(), combineFn.getFn())
        : new GroupAlsoByWindowsViaOutputBufferDoFn<>(
            noWildcard,
            SystemReduceFn.<K, InputT, AccumT, OutputT, W>combining(keyCoder, combineFn));
  }
}
