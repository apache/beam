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
package org.apache.beam.sdk.state;

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;

/**
 * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
 *
 * <p>Visitor for binding a {@link StateSpec} and to the associated {@link State}.
 */
@Internal
public interface StateBinder {
  <T> ValueState<T> bindValue(String id, StateSpec<ValueState<T>> spec, Coder<T> coder);

  <T> BagState<T> bindBag(String id, StateSpec<BagState<T>> spec, Coder<T> elemCoder);

  <T> SetState<T> bindSet(String id, StateSpec<SetState<T>> spec, Coder<T> elemCoder);

  <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(
      String id,
      StateSpec<MapState<KeyT, ValueT>> spec,
      Coder<KeyT> mapKeyCoder,
      Coder<ValueT> mapValueCoder);

  <T> OrderedListState<T> bindOrderedList(
      String id, StateSpec<OrderedListState<T>> spec, Coder<T> elemCoder);

  <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT> bindCombining(
      String id,
      StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
      Coder<AccumT> accumCoder,
      Combine.CombineFn<InputT, AccumT, OutputT> combineFn);

  <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT> bindCombiningWithContext(
      String id,
      StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
      Coder<AccumT> accumCoder,
      CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn);

  /**
   * Bind to a watermark {@link StateSpec}.
   *
   * <p>This accepts the {@link TimestampCombiner} that dictates how watermark hold timestamps added
   * to the returned {@link WatermarkHoldState} are to be combined.
   */
  WatermarkHoldState bindWatermark(
      String id, StateSpec<WatermarkHoldState> spec, TimestampCombiner timestampCombiner);
}
