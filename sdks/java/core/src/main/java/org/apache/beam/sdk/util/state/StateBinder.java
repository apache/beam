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

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;

/**
 * Visitor for binding a {@link StateSpec} and to the associated {@link State}.
 *
 * @param <K> the type of key this binder embodies.
 */
public interface StateBinder<K> {
  <T> ValueState<T> bindValue(String id, StateSpec<? super K, ValueState<T>> spec, Coder<T> coder);

  <T> BagState<T> bindBag(String id, StateSpec<? super K, BagState<T>> spec, Coder<T> elemCoder);

  <T> SetState<T> bindSet(String id, StateSpec<? super K, SetState<T>> spec, Coder<T> elemCoder);

  <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(
      String id, StateSpec<? super K, MapState<KeyT, ValueT>> spec,
      Coder<KeyT> mapKeyCoder, Coder<ValueT> mapValueCoder);

  <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT> bindCombining(
      String id,
      StateSpec<? super K, CombiningState<InputT, AccumT, OutputT>> spec,
      Coder<AccumT> accumCoder,
      Combine.CombineFn<InputT, AccumT, OutputT> combineFn);

  <InputT, AccumT, OutputT>
  CombiningState<InputT, AccumT, OutputT> bindKeyedCombining(
          String id,
          StateSpec<? super K, CombiningState<InputT, AccumT, OutputT>> spec,
          Coder<AccumT> accumCoder,
          Combine.KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn);

  <InputT, AccumT, OutputT>
  CombiningState<InputT, AccumT, OutputT> bindKeyedCombiningWithContext(
          String id,
          StateSpec<? super K, CombiningState<InputT, AccumT, OutputT>> spec,
          Coder<AccumT> accumCoder,
          CombineWithContext.KeyedCombineFnWithContext<? super K, InputT, AccumT, OutputT>
              combineFn);

  /**
   * Bind to a watermark {@link StateSpec}.
   *
   * <p>This accepts the {@link OutputTimeFn} that dictates how watermark hold timestamps added to
   * the returned {@link WatermarkHoldState} are to be combined.
   */
  <W extends BoundedWindow> WatermarkHoldState<W> bindWatermark(
      String id,
      StateSpec<? super K, WatermarkHoldState<W>> spec,
      OutputTimeFn<? super W> outputTimeFn);
}
