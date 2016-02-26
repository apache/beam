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
package com.google.cloud.dataflow.sdk.util.state;

import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.annotations.Experimental.Kind;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn;
import com.google.cloud.dataflow.sdk.transforms.CombineWithContext.KeyedCombineFnWithContext;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.OutputTimeFn;

import java.io.IOException;
import java.io.Serializable;

/**
 * An address for persistent state. This includes a unique identifier for the location, the
 * information necessary to encode the value, and details about the intended access pattern.
 *
 * <p>State can be thought of as a sparse table, with each {@code StateTag} defining a column
 * that has cells of type {@code StateT}.
 *
 * <p>Currently, this can only be used in a step immediately following a {@link GroupByKey}.
 *
 * @param <K> The type of key that must be used with the state tag. Contravariant: methods should
 *            accept values of type {@code KeyedStateTag<? super K, StateT>}.
 * @param <StateT> The type of state being tagged.
 */
@Experimental(Kind.STATE)
public interface StateTag<K, StateT extends State> extends Serializable {

  /**
   * Visitor for binding a {@link StateTag} and to the associated {@link State}.
   *
   * @param <K> the type of key this binder embodies.
   */
  public interface StateBinder<K> {
    <T> ValueState<T> bindValue(StateTag<? super K, ValueState<T>> address, Coder<T> coder);

    <T> BagState<T> bindBag(StateTag<? super K, BagState<T>> address, Coder<T> elemCoder);

    <InputT, AccumT, OutputT> AccumulatorCombiningState<InputT, AccumT, OutputT>
    bindCombiningValue(
        StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> address,
        Coder<AccumT> accumCoder, CombineFn<InputT, AccumT, OutputT> combineFn);

    <InputT, AccumT, OutputT> AccumulatorCombiningState<InputT, AccumT, OutputT>
    bindKeyedCombiningValue(
        StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> address,
        Coder<AccumT> accumCoder, KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn);

    <InputT, AccumT, OutputT> AccumulatorCombiningState<InputT, AccumT, OutputT>
    bindKeyedCombiningValueWithContext(
        StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> address,
        Coder<AccumT> accumCoder,
        KeyedCombineFnWithContext<? super K, InputT, AccumT, OutputT> combineFn);

    /**
     * Bind to a watermark {@link StateTag}.
     *
     * <p>This accepts the {@link OutputTimeFn} that dictates how watermark hold timestamps
     * added to the returned {@link WatermarkHoldState} are to be combined.
     */
    <W extends BoundedWindow> WatermarkHoldState<W> bindWatermark(
        StateTag<? super K, WatermarkHoldState<W>> address,
        OutputTimeFn<? super W> outputTimeFn);
  }

  /** Append the UTF-8 encoding of this tag to the given {@link Appendable}. */
  void appendTo(Appendable sb) throws IOException;

  /**
   * Returns the user-provided name of this state cell.
   */
  String getId();

  /**
   * Use the {@code binder} to create an instance of {@code StateT} appropriate for this address.
   */
  StateT bind(StateBinder<? extends K> binder);
}
