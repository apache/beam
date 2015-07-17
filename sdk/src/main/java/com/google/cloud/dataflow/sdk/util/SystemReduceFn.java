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
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.state.MergeableState;
import com.google.cloud.dataflow.sdk.util.state.StateContents;
import com.google.cloud.dataflow.sdk.util.state.StateTag;
import com.google.cloud.dataflow.sdk.util.state.StateTags;

/**
 * {@link ReduceFn} implementing the default reduction behaviors of {@link GroupByKey}.
 *
 * @param <K> The type of key being processed.
 * @param <InputT> The type of values associated with the key.
 * @param <OutputT> The output type that will be produced for each key.
 * @param <W> The type of windows this operates on.
 */
abstract class SystemReduceFn<K, InputT, OutputT, W extends BoundedWindow>
    extends ReduceFn<K, InputT, OutputT, W> {

  private static final long serialVersionUID = 0L;
  private static final String BUFFER_NAME = "__buffer";

  /**
   * Create a {@link SystemReduceFn} that buffers all of the input values in persistent state,
   * and produces an {@code Iterable<T>}.
   */
  public static <K, T, W extends BoundedWindow> ReduceFn<K, T, Iterable<T>, W> buffering(
      final Coder<T> inputCoder) {
    return new SystemReduceFn<K, T, Iterable<T>, W>() {
      private static final long serialVersionUID = 0L;

      @Override
      protected StateTag<? extends MergeableState<T, Iterable<T>>> bufferTag(K key) {
        return StateTags.bag(BUFFER_NAME, inputCoder);
      }
    };
  }

  /**
   * Create a {@link SystemReduceFn} that combines all of the input values using a
   * {@link CombineFn}.
   */
  public static
  <K, InputT, OutputT, W extends BoundedWindow> ReduceFn<K, InputT, OutputT, W> combining(
      final Coder<K> keyCoder, final Coder<InputT> inputCoder,
      final KeyedCombineFn<K, InputT, ?, OutputT> combineFn) {
    return new SystemReduceFn<K, InputT, OutputT, W>() {
      private static final long serialVersionUID = 0L;

      @Override
      protected StateTag<? extends MergeableState<InputT, OutputT>> bufferTag(K key) {
        return StateTags.<InputT, OutputT>combiningValue(
            BUFFER_NAME, inputCoder, combineFn.forKey(key, keyCoder));
      }
    };
  }

  protected abstract StateTag<? extends MergeableState<InputT, OutputT>> bufferTag(K key);

  @Override
  public void processValue(ProcessValueContext c) throws Exception {
    c.state().access(bufferTag(c.key())).add(c.value());
  }

  @Override
  public void onMerge(OnMergeContext c) throws Exception {
    // All of the state used by SystemReduceFn is mergeable. Rather than eagerly reading it in
    // to perform the merge here, we wait until the output is desired, and combine the values
    // from all the source windows at that point.
  }

  @Override
  public void onTrigger(OnTriggerContext c) throws Exception {
    MergeableState<InputT, OutputT> buffer =
        c.state().accessAcrossMergedWindows(bufferTag(c.key()));
    StateContents<OutputT> output = buffer.get();
    if (!buffer.isEmpty().read()) {
      c.output(output.read());
    }
  }

  @Override
  public void clearState(Context c) throws Exception {
    c.state().accessAcrossMergedWindows(bufferTag(c.key())).clear();
  }
}
