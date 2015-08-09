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
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.state.BagState;
import com.google.cloud.dataflow.sdk.util.state.CombiningValueState;
import com.google.cloud.dataflow.sdk.util.state.MergeableState;
import com.google.cloud.dataflow.sdk.util.state.StateContents;
import com.google.cloud.dataflow.sdk.util.state.StateTag;
import com.google.cloud.dataflow.sdk.util.state.StateTags;

import java.io.Serializable;

/**
 * {@link ReduceFn} implementing the default reduction behaviors of {@link GroupByKey}.
 *
 * @param <K> The type of key being processed.
 * @param <InputT> The type of values associated with the key.
 * @param <OutputT> The output type that will be produced for each key.
 * @param <W> The type of windows this operates on.
 */
class SystemReduceFn<K, InputT, OutputT, W extends BoundedWindow>
    extends ReduceFn<K, InputT, OutputT, W> {

  private static final long serialVersionUID = 0L;
  private static final String BUFFER_NAME = "buf";

  /**
   * Factory that produces {@link SystemReduceFn} instances for specific keys.
   *
   * @param <K> The type of key being processed.
   * @param <InputT> The type of values associated with the key.
   * @param <OutputT> The output type that will be produced for each key.
   * @param <W> The type of windows this operates on.
   */
  public interface Factory<K, InputT, OutputT, W extends BoundedWindow> extends Serializable {
    ReduceFn<K, InputT, OutputT, W> create(K key);
  }

  /**
   * Create a factory that produces {@link SystemReduceFn} instances that that buffer all of the
   * input values in persistent state and produces an {@code Iterable<T>}.
   */
  public static <K, T, W extends BoundedWindow> Factory<K, T, Iterable<T>, W> buffering(
      final Coder<T> inputCoder) {
    final StateTag<BagState<T>> bufferTag =
        StateTags.makeSystemTagInternal(StateTags.bag(BUFFER_NAME, inputCoder));
    return new Factory<K, T, Iterable<T>, W>() {

      private static final long serialVersionUID = 0L;

      @Override
      public ReduceFn<K, T, Iterable<T>, W> create(K key) {
        return new SystemReduceFn<K, T, Iterable<T>, W>(bufferTag);
      }
    };
  }

  /**
   * Create a factory that produces {@link SystemReduceFn} instances that combine all of the input
   * values using a {@link CombineFn}.
   */
  public static
  <K, InputT, AccumT, OutputT, W extends BoundedWindow> Factory<K, InputT, OutputT, W> combining(
      final Coder<K> keyCoder, final AppliedCombineFn<K, InputT, AccumT, OutputT> combineFn) {
    return new Factory<K, InputT, OutputT, W>() {

      private static final long serialVersionUID = 0L;

      @Override
      public ReduceFn<K, InputT, OutputT, W> create(K key) {
        StateTag<CombiningValueState<InputT, OutputT>> bufferTag =
            StateTags.makeSystemTagInternal(StateTags.<InputT, AccumT, OutputT>combiningValue(
                BUFFER_NAME, combineFn.getAccumulatorCoder(),
                combineFn.getFn().forKey(key, keyCoder)));
        return new SystemReduceFn<K, InputT, OutputT, W>(bufferTag);
      }
    };
  }

  private StateTag<? extends MergeableState<InputT, OutputT>> bufferTag;

  public SystemReduceFn(StateTag<? extends MergeableState<InputT, OutputT>> bufferTag) {
    this.bufferTag = bufferTag;
  }

  @Override
  public void processValue(ProcessValueContext c) throws Exception {
    c.state().access(bufferTag).add(c.value());
  }

  @Override
  public void onMerge(OnMergeContext c) throws Exception {
    // All of the state used by SystemReduceFn is mergeable. Rather than eagerly reading it in
    // to perform the merge here, we wait until the output is desired, and combine the values
    // from all the source windows at that point.
  }

  @Override
  public void prefetchOnTrigger(com.google.cloud.dataflow.sdk.util.ReduceFn.StateContext c) {
    c.accessAcrossMergedWindows(bufferTag).get();
  }

  @Override
  public void onTrigger(OnTriggerContext c) throws Exception {
    c.output(c.state().accessAcrossMergedWindows(bufferTag).get().read());
  }

  @Override
  public void clearState(Context c) throws Exception {
    c.state().accessAcrossMergedWindows(bufferTag).clear();
  }

  @Override
  public StateContents<Boolean> isEmpty(StateContext state) {
    return state.accessAcrossMergedWindows(bufferTag).isEmpty();
  }
}
