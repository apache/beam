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

import com.google.cloud.dataflow.sdk.coders.CannotProvideCoderException;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn;

import java.io.Serializable;

/**
 * A {@link KeyedCombineFn} with a fixed accumulator coder. This is created from a specific
 * application of the {@link KeyedCombineFn}.
 *
 *  <p>Because the {@code AccumT} may reference {@code InputT}, the specific {@code Coder<AccumT>}
 *  may depend on the {@code Coder<InputT>}.
 *
 * @param <K> type of keys
 * @param <InputT> type of input values
 * @param <AccumT> type of mutable accumulator values
 * @param <OutputT> type of output values
 */
public class AppliedCombineFn<K, InputT, AccumT, OutputT> implements Serializable {

  private static final long serialVersionUID = 0L;

  private final KeyedCombineFn<K, InputT, AccumT, OutputT> fn;
  private final Coder<AccumT> accumulatorCoder;

  private AppliedCombineFn(
      KeyedCombineFn<K, InputT, AccumT, OutputT> fn, Coder<AccumT> accumulatorCoder) {
    this.fn = fn;
    this.accumulatorCoder = accumulatorCoder;
  }

  public static <K, InputT, AccumT, OutputT> AppliedCombineFn<K, InputT, AccumT, OutputT>
  withAccumulatorCoder(KeyedCombineFn<? super K, ? super InputT, AccumT, OutputT> fn,
      Coder<AccumT> accumCoder) {
    // Casting down the K and InputT is safe because they're only used as inputs.
    @SuppressWarnings("unchecked")
    KeyedCombineFn<K, InputT, AccumT, OutputT> clonedFn =
        (KeyedCombineFn<K, InputT, AccumT, OutputT>) SerializableUtils.clone(fn);
    return new AppliedCombineFn<>(clonedFn, accumCoder);
  }

  public static <K, InputT, AccumT, OutputT> AppliedCombineFn<K, InputT, AccumT, OutputT>
  withInputCoder(KeyedCombineFn<? super K, ? super InputT, AccumT, OutputT> fn,
      CoderRegistry registry, KvCoder<K, InputT> kvCoder) {
    // Casting down the K and InputT is safe because they're only used as inputs.
    @SuppressWarnings("unchecked")
    KeyedCombineFn<K, InputT, AccumT, OutputT> clonedFn =
        (KeyedCombineFn<K, InputT, AccumT, OutputT>) SerializableUtils.clone(fn);
    try {
      Coder<AccumT> accumulatorCoder = clonedFn.getAccumulatorCoder(
          registry, kvCoder.getKeyCoder(), kvCoder.getValueCoder());
      return new AppliedCombineFn<>(clonedFn, accumulatorCoder);
    } catch (CannotProvideCoderException e) {
      throw new IllegalStateException("Could not determine coder for accumulator", e);
    }
  }

  public KeyedCombineFn<K, InputT, AccumT, OutputT> getFn() {
    return fn;
  }

  public Coder<AccumT> getAccumulatorCoder() {
    return accumulatorCoder;
  }
}
