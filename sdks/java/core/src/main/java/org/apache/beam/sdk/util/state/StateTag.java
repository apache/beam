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

import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Combine.KeyedCombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.KeyedCombineFnWithContext;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;

/**
 * An address and specification for a persistent state cell. This includes a unique identifier for
 * the location, the information necessary to encode the value, and details about the intended
 * access pattern.
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

  /** Append the UTF-8 encoding of this tag to the given {@link Appendable}. */
  void appendTo(Appendable sb) throws IOException;

  /**
   * An identifier for the state cell that this tag references.
   */
  String getId();

  /**
   * The specification for the state stored in the referenced cell.
   */
  StateSpec<K, StateT> getSpec();

  /**
   * Bind this state tag. See {@link StateSpec#bind}.
   *
   * @deprecated Use the {@link StateSpec#bind} method via {@link #getSpec} for now.
   */
  @Deprecated
  StateT bind(StateBinder<? extends K> binder);

  /**
   * Visitor for binding a {@link StateSpec} and to the associated {@link State}.
   *
   * @param <K> the type of key this binder embodies.
   * @deprecated for migration only; runners should reference the top level {@link StateBinder}
   * and move towards {@link StateSpec} rather than {@link StateTag}.
   */
  @Deprecated
  public interface StateBinder<K> {
    <T> ValueState<T> bindValue(StateTag<? super K, ValueState<T>> spec, Coder<T> coder);

    <T> BagState<T> bindBag(StateTag<? super K, BagState<T>> spec, Coder<T> elemCoder);

    <InputT, AccumT, OutputT> AccumulatorCombiningState<InputT, AccumT, OutputT> bindCombiningValue(
        StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> spec,
        Coder<AccumT> accumCoder,
        CombineFn<InputT, AccumT, OutputT> combineFn);

    <InputT, AccumT, OutputT>
    AccumulatorCombiningState<InputT, AccumT, OutputT> bindKeyedCombiningValue(
        StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> spec,
        Coder<AccumT> accumCoder,
        KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn);

    <InputT, AccumT, OutputT>
    AccumulatorCombiningState<InputT, AccumT, OutputT> bindKeyedCombiningValueWithContext(
        StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> spec,
        Coder<AccumT> accumCoder,
        KeyedCombineFnWithContext<? super K, InputT, AccumT, OutputT>
            combineFn);

    /**
     * Bind to a watermark {@link StateSpec}.
     *
     * <p>This accepts the {@link OutputTimeFn} that dictates how watermark hold timestamps added to
     * the returned {@link WatermarkHoldState} are to be combined.
     */
    <W extends BoundedWindow> WatermarkHoldState<W> bindWatermark(
        StateTag<? super K, WatermarkHoldState<W>> spec,
        OutputTimeFn<? super W> outputTimeFn);
  }
}
