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

import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.MultimapState;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;

/**
 * An address and specification for a persistent state cell. This includes a unique identifier for
 * the location, the information necessary to encode the value, and details about the intended
 * access pattern.
 *
 * <p>State can be thought of as a sparse table, with each {@code StateTag} defining a column that
 * has cells of type {@code StateT}.
 *
 * <p>Currently, this can only be used in a step immediately following a {@link GroupByKey}.
 *
 * @param <StateT> The type of state being tagged.
 */
public interface StateTag<StateT extends State> extends Serializable {

  /** Append the UTF-8 encoding of this tag to the given {@link Appendable}. */
  void appendTo(Appendable sb) throws IOException;

  /** An identifier for the state cell that this tag references. */
  String getId();

  /** The specification for the state stored in the referenced cell. */
  StateSpec<StateT> getSpec();

  /**
   * Bind this state tag. See {@link StateSpec#bind}.
   *
   * @deprecated Use the {@link StateSpec#bind} method via {@link #getSpec} for now.
   */
  @Deprecated
  StateT bind(StateBinder binder);

  /**
   * Visitor for binding a {@link StateSpec} and to the associated {@link State}.
   *
   * @deprecated for migration only; runners should reference the top level {@link
   *     org.apache.beam.sdk.state.StateBinder} and move towards {@link StateSpec} rather than
   *     {@link StateTag}.
   */
  @Deprecated
  public interface StateBinder {
    <T> ValueState<T> bindValue(StateTag<ValueState<T>> spec, Coder<T> coder);

    <T> BagState<T> bindBag(StateTag<BagState<T>> spec, Coder<T> elemCoder);

    <T> SetState<T> bindSet(StateTag<SetState<T>> spec, Coder<T> elemCoder);

    <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(
        StateTag<MapState<KeyT, ValueT>> spec,
        Coder<KeyT> mapKeyCoder,
        Coder<ValueT> mapValueCoder);

    <KeyT, ValueT> MultimapState<KeyT, ValueT> bindMultimap(
        StateTag<MultimapState<KeyT, ValueT>> spec, Coder<KeyT> keyCoder, Coder<ValueT> valueCoder);

    <T> OrderedListState<T> bindOrderedList(StateTag<OrderedListState<T>> spec, Coder<T> elemCoder);

    <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT> bindCombiningValue(
        StateTag<CombiningState<InputT, AccumT, OutputT>> spec,
        Coder<AccumT> accumCoder,
        CombineFn<InputT, AccumT, OutputT> combineFn);

    <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT> bindCombiningValueWithContext(
        StateTag<CombiningState<InputT, AccumT, OutputT>> spec,
        Coder<AccumT> accumCoder,
        CombineFnWithContext<InputT, AccumT, OutputT> combineFn);

    /**
     * Bind to a watermark {@link StateSpec}.
     *
     * <p>This accepts the {@link TimestampCombiner} that dictates how watermark hold timestamps
     * added to the returned {@link WatermarkHoldState} are to be combined.
     */
    WatermarkHoldState bindWatermark(
        StateTag<WatermarkHoldState> spec, TimestampCombiner timestampCombiner);
  }
}
