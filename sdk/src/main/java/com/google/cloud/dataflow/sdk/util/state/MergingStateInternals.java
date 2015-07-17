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

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.util.state.StateTag.StateBinder;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract implementation of {@link StateInternals} that provides {@link #mergedState} in terms of
 * {@link #state}.
 */
public abstract class MergingStateInternals implements StateInternals {

  @Override
  public <T extends MergeableState<?, ?>> T mergedState(
      final Iterable<StateNamespace> sourceNamespaces,
      final StateNamespace resultNamespace, StateTag<T> address) {
    return address.bind(new StateBinder() {
      @Override
      public <T> ValueState<T> bindValue(StateTag<ValueState<T>> address, Coder<T> coder) {
        throw new IllegalStateException(
            "Value is not mergable. Should not be passed to mergedState");
      }

      @Override
      public <T> BagState<T> bindBag(StateTag<BagState<T>> address, Coder<T> elemCoder) {
        List<BagState<T>> sources = new ArrayList<>();
        for (StateNamespace sourceNamespace : sourceNamespaces) {
          // Skip adding the result namespace for now.
          if (!sourceNamespace.equals(resultNamespace)) {
            sources.add(state(sourceNamespace, address));
          }
        }

        BagState<T> results = state(resultNamespace, address);
        sources.add(results);
        return new MergedBag<>(sources, results);
      }

      @Override
      public <InputT, AccumT, OutputT>
      CombiningValueStateInternal<InputT, AccumT, OutputT> bindCombiningValue(
          StateTag<CombiningValueStateInternal<InputT, AccumT, OutputT>> address,
          Coder<AccumT> accumCoder, CombineFn<InputT, AccumT, OutputT> combineFn) {
        List<CombiningValueStateInternal<InputT, AccumT, OutputT>> sources = new ArrayList<>();
        for (StateNamespace sourceNamespace : sourceNamespaces) {
          // Skip adding the result namespace for now.
          if (!sourceNamespace.equals(resultNamespace)) {
            sources.add(state(sourceNamespace, address));
          }
        }
        CombiningValueStateInternal<InputT, AccumT, OutputT> result =
            state(resultNamespace, address);
        sources.add(result);
        return new MergedCombiningValue<>(sources, result, combineFn);
      }

      @Override
      public <T> WatermarkStateInternal bindWatermark(
          StateTag<WatermarkStateInternal> address) {
        List<WatermarkStateInternal> sources = new ArrayList<>();
        for (StateNamespace sourceNamespace : sourceNamespaces) {
          // Skip adding the result namespace for now.
          if (!sourceNamespace.equals(resultNamespace)) {
            sources.add(state(sourceNamespace, address));
          }
        }
        WatermarkStateInternal result = state(resultNamespace, address);
        sources.add(result);
        return new MergedWatermarkStateInternal(sources, result);
      }
    });
  }
}
