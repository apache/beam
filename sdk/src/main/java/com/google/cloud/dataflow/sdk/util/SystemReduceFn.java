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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.transforms.CombineWithContext.RequiresContextInternal;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.state.BagState;
import com.google.cloud.dataflow.sdk.util.state.CombiningValueState;
import com.google.cloud.dataflow.sdk.util.state.CombiningValueStateInternal;
import com.google.cloud.dataflow.sdk.util.state.MergingStateContext;
import com.google.cloud.dataflow.sdk.util.state.StateContents;
import com.google.cloud.dataflow.sdk.util.state.StateContext;
import com.google.cloud.dataflow.sdk.util.state.StateMerging;
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
public abstract class SystemReduceFn<K, InputT, AccumT, OutputT, W extends BoundedWindow>
    extends ReduceFn<K, InputT, OutputT, W> {
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
    final StateTag<Object, BagState<T>> bufferTag =
        StateTags.makeSystemTagInternal(StateTags.bag(BUFFER_NAME, inputCoder));
    return new Factory<K, T, Iterable<T>, W>() {
      @Override
      public ReduceFn<K, T, Iterable<T>, W> create(K key) {
        return new SystemReduceFn<K, T, Iterable<T>, Iterable<T>, W>(bufferTag) {
          @Override
          public void prefetchOnMerge(MergingStateContext<K, W> state) throws Exception {
            StateMerging.prefetchBags(state, bufferTag);
          }

          @Override
          public void onMerge(OnMergeContext c) throws Exception {
            StateMerging.mergeBags(c.state(), bufferTag);
          }
        };
      }
    };
  }

  /**
   * Create a factory that produces {@link SystemReduceFn} instances that combine all of the input
   * values using a {@link CombineFn}.
   */
  public static <K, InputT, AccumT, OutputT, W extends BoundedWindow> Factory<K, InputT, OutputT, W>
      combining(
          final Coder<K> keyCoder, final AppliedCombineFn<K, InputT, AccumT, OutputT> combineFn) {
    checkArgument(
        !(combineFn.getFn() instanceof RequiresContextInternal),
        "Combiner lifting is not supported for combine functions with contexts: %s",
        combineFn.getFn().getClass().getName());
    return new Factory<K, InputT, OutputT, W>() {
      @Override
      public ReduceFn<K, InputT, OutputT, W> create(K key) {
        final StateTag<Object, CombiningValueStateInternal<InputT, AccumT, OutputT>> bufferTag =
            StateTags.makeSystemTagInternal(StateTags.<InputT, AccumT, OutputT>combiningValue(
                BUFFER_NAME, combineFn.getAccumulatorCoder(),
                (CombineFn<InputT, AccumT, OutputT>) combineFn.getFn().forKey(key, keyCoder)));
        return new SystemReduceFn<K, InputT, AccumT, OutputT, W>(bufferTag) {
          @Override
          public void prefetchOnMerge(MergingStateContext<K, W> state) throws Exception {
            StateMerging.prefetchCombiningValues(state, bufferTag);
          }

          @Override
          public void onMerge(OnMergeContext c) throws Exception {
            StateMerging.mergeCombiningValues(c.state(), bufferTag);
          }
        };
      }
    };
  }

  private StateTag<? super K, ? extends CombiningValueState<InputT, OutputT>> bufferTag;

  public SystemReduceFn(
      StateTag<? super K, ? extends CombiningValueState<InputT, OutputT>> bufferTag) {
    this.bufferTag = bufferTag;
  }

  @Override
  public void processValue(ProcessValueContext c) throws Exception {
    c.state().access(bufferTag).add(c.value());
  }

  @Override
  public void prefetchOnTrigger(StateContext<K> state) {
    state.access(bufferTag).get();
  }

  @Override
  public void onTrigger(OnTriggerContext c) throws Exception {
    c.output(c.state().access(bufferTag).get().read());
  }

  @Override
  public void clearState(Context c) throws Exception {
    c.state().access(bufferTag).clear();
  }

  @Override
  public StateContents<Boolean> isEmpty(StateContext<K> state) {
    return state.access(bufferTag).isEmpty();
  }
}
