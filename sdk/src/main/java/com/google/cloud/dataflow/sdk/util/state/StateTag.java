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
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;

import java.io.Serializable;

/**
 * An address for persistent state. This includes a unique identifier for the location, the
 * information necessary to encode the value, and details about the intended access pattern.
 *
 * <p> State can be thought of as a sparse table, with each {@code StateTag} defining a column
 * that has cells of type {@code StateT}.
 *
 * <p> Currently, this can only be used in a step immediately following a {@link GroupByKey}.
 *
 * @param <StateT> The type of state being tagged.
 */
@Experimental(Kind.STATE)
public interface StateTag<StateT extends State> extends Serializable {

  /**
   * Visitor for binding a {@link StateTag} and to the associated {@link State}.
   */
  public interface StateBinder {
    <T> ValueState<T> bindValue(StateTag<ValueState<T>> address, Coder<T> coder);

    <T> BagState<T> bindBag(StateTag<BagState<T>> address, Coder<T> elemCoder);

    <InputT, AccumT, OutputT> CombiningValueStateInternal<InputT, AccumT, OutputT>
    bindCombiningValue(
        StateTag<CombiningValueStateInternal<InputT, AccumT, OutputT>> address,
        Coder<AccumT> accumCoder, CombineFn<InputT, AccumT, OutputT> combineFn);

    <T> WatermarkStateInternal bindWatermark(StateTag<WatermarkStateInternal> address);
  }

  /**
   * Returns the identifier for this state cell.
   */
  String getId();

  /**
   * Use the {@code binder} to create an instance of {@code StateT} appropriate for this address.
   */
  StateT bind(StateBinder binder);
}
