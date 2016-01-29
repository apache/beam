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

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.AppliedCombineFn;
import com.google.cloud.dataflow.sdk.util.GroupAlsoByWindowViaWindowSetDoFn;
import com.google.cloud.dataflow.sdk.util.KeyedWorkItem;
import com.google.cloud.dataflow.sdk.util.SystemDoFnInternal;
import com.google.cloud.dataflow.sdk.util.SystemReduceFn;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.base.Preconditions;

/**
 * DoFn that merges windows and groups elements in those windows.
 *
 * @param <K> key type
 * @param <InputT> input value element type
 * @param <OutputT> output value element type
 * @param <W> window type
 */
@SystemDoFnInternal
public abstract class StreamingGroupAlsoByWindowsDoFn<K, InputT, OutputT, W extends BoundedWindow>
    extends DoFn<KeyedWorkItem<K, InputT>, KV<K, OutputT>> {
  public static <K, InputT, AccumT, OutputT, W extends BoundedWindow>
      DoFn<KeyedWorkItem<K, InputT>, KV<K, OutputT>> create(
          final WindowingStrategy<?, W> windowingStrategy,
          final AppliedCombineFn<K, InputT, AccumT, OutputT> combineFn,
          final Coder<K> keyCoder) {
    Preconditions.checkNotNull(combineFn);
    DoFn<KeyedWorkItem<K, InputT>, KV<K, OutputT>> fn =
        GroupAlsoByWindowViaWindowSetDoFn.create(
            windowingStrategy,
            SystemReduceFn.<K, InputT, AccumT, OutputT, W>combining(keyCoder, combineFn));
    return fn;
  }

  public static <K, V, W extends BoundedWindow>
      DoFn<KeyedWorkItem<K, V>, KV<K, Iterable<V>>> createForIterable(
          final WindowingStrategy<?, W> windowingStrategy, final Coder<V> inputCoder) {
    // If the windowing strategy indicates we're doing a reshuffle, use the special-path.
    if (StreamingGroupAlsoByWindowsReshuffleDoFn.isReshuffle(windowingStrategy)) {
      return new StreamingGroupAlsoByWindowsReshuffleDoFn<>();
    } else {
      return GroupAlsoByWindowViaWindowSetDoFn.create(
          windowingStrategy, SystemReduceFn.<K, V, W>buffering(inputCoder));
    }
  }
}
