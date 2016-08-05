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

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Combine.KeyedCombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.KeyedCombineFnWithContext;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;

import java.io.IOException;
import java.io.Serializable;

/**
 * A specification of a persistent state cell. This includes information necessary to encode the
 * value and details about the intended access pattern.
 *
 * @param <K> The type of key that must be used with the state tag. Contravariant: methods should
 *            accept values of type {@code StateSpec<? super K, StateT>}.
 * @param <StateT> The type of state being described.
 */
@Experimental(Kind.STATE)
public interface StateSpec<K, StateT extends State> extends Serializable {

  /**
   * Visitor for binding a {@link StateSpec} and to the associated {@link State}.
   *
   * @param <K> the type of key this binder embodies.
   */
  public interface StateBinder<K> {
    <T> ValueState<T> bindValue(StateSpec<? super K, ValueState<T>> spec, Coder<T> coder);

    <T> BagState<T> bindBag(StateSpec<? super K, BagState<T>> spec, Coder<T> elemCoder);

    <InputT, AccumT, OutputT> AccumulatorCombiningState<InputT, AccumT, OutputT>
    bindCombiningValue(
        StateSpec<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> spec,
        Coder<AccumT> accumCoder, CombineFn<InputT, AccumT, OutputT> combineFn);

    <InputT, AccumT, OutputT> AccumulatorCombiningState<InputT, AccumT, OutputT>
    bindKeyedCombiningValue(
        StateSpec<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> spec,
        Coder<AccumT> accumCoder, KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn);

    <InputT, AccumT, OutputT> AccumulatorCombiningState<InputT, AccumT, OutputT>
    bindKeyedCombiningValueWithContext(
        StateSpec<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> spec,
        Coder<AccumT> accumCoder,
        KeyedCombineFnWithContext<? super K, InputT, AccumT, OutputT> combineFn);

    /**
     * Bind to a watermark {@link StateSpec}.
     *
     * <p>This accepts the {@link OutputTimeFn} that dictates how watermark hold timestamps
     * added to the returned {@link WatermarkHoldState} are to be combined.
     */
    <W extends BoundedWindow> WatermarkHoldState<W> bindWatermark(
        StateSpec<? super K, WatermarkHoldState<W>> spec,
        OutputTimeFn<? super W> outputTimeFn);
  }

  /**
   * Use the {@code binder} to create an instance of {@code StateT} appropriate for this address.
   */
  StateT bind(StateBinder<? extends K> binder);
}
