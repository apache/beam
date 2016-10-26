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
package org.apache.beam.sdk.util;

import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.CombineFnBase.PerKeyCombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.KeyedCombineFnWithContext;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * A {@link KeyedCombineFnWithContext} with a fixed accumulator coder. This is created from a
 * specific application of the {@link KeyedCombineFnWithContext}.
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

  private final PerKeyCombineFn<K, InputT, AccumT, OutputT> fn;
  private final Coder<AccumT> accumulatorCoder;

  private final Iterable<PCollectionView<?>> sideInputViews;
  private final KvCoder<K, InputT> kvCoder;
  private final WindowingStrategy<?, ?> windowingStrategy;

  private AppliedCombineFn(PerKeyCombineFn<K, InputT, AccumT, OutputT> fn,
      Coder<AccumT> accumulatorCoder, Iterable<PCollectionView<?>> sideInputViews,
      KvCoder<K, InputT> kvCoder, WindowingStrategy<?, ?> windowingStrategy) {
    this.fn = fn;
    this.accumulatorCoder = accumulatorCoder;
    this.sideInputViews = sideInputViews;
    this.kvCoder = kvCoder;
    this.windowingStrategy = windowingStrategy;
  }

  public static <K, InputT, AccumT, OutputT> AppliedCombineFn<K, InputT, AccumT, OutputT>
      withAccumulatorCoder(
          PerKeyCombineFn<? super K, ? super InputT, AccumT, OutputT> fn,
          Coder<AccumT> accumCoder) {
    return withAccumulatorCoder(fn, accumCoder, null, null, null);
  }

  public static <K, InputT, AccumT, OutputT> AppliedCombineFn<K, InputT, AccumT, OutputT>
      withAccumulatorCoder(
          PerKeyCombineFn<? super K, ? super InputT, AccumT, OutputT> fn,
          Coder<AccumT> accumCoder, Iterable<PCollectionView<?>> sideInputViews,
          KvCoder<K, InputT> kvCoder, WindowingStrategy<?, ?> windowingStrategy) {
    // Casting down the K and InputT is safe because they're only used as inputs.
    @SuppressWarnings("unchecked")
    PerKeyCombineFn<K, InputT, AccumT, OutputT> clonedFn =
        (PerKeyCombineFn<K, InputT, AccumT, OutputT>) SerializableUtils.clone(fn);
    return create(clonedFn, accumCoder, sideInputViews, kvCoder, windowingStrategy);
  }

  @VisibleForTesting
  public static <K, InputT, AccumT, OutputT> AppliedCombineFn<K, InputT, AccumT, OutputT>
      withInputCoder(PerKeyCombineFn<? super K, ? super InputT, AccumT, OutputT> fn,
          CoderRegistry registry, KvCoder<K, InputT> kvCoder) {
    return withInputCoder(fn, registry, kvCoder, null, null);
  }

  public static <K, InputT, AccumT, OutputT> AppliedCombineFn<K, InputT, AccumT, OutputT>
      withInputCoder(PerKeyCombineFn<? super K, ? super InputT, AccumT, OutputT> fn,
          CoderRegistry registry, KvCoder<K, InputT> kvCoder,
          Iterable<PCollectionView<?>> sideInputViews, WindowingStrategy<?, ?> windowingStrategy) {
    // Casting down the K and InputT is safe because they're only used as inputs.
    @SuppressWarnings("unchecked")
    PerKeyCombineFn<K, InputT, AccumT, OutputT> clonedFn =
        (PerKeyCombineFn<K, InputT, AccumT, OutputT>) SerializableUtils.clone(fn);
    try {
      Coder<AccumT> accumulatorCoder = clonedFn.getAccumulatorCoder(
          registry, kvCoder.getKeyCoder(), kvCoder.getValueCoder());
      return create(clonedFn, accumulatorCoder, sideInputViews, kvCoder, windowingStrategy);
    } catch (CannotProvideCoderException e) {
      throw new IllegalStateException("Could not determine coder for accumulator", e);
    }
  }

  private static <K, InputT, AccumT, OutputT> AppliedCombineFn<K, InputT, AccumT, OutputT> create(
      PerKeyCombineFn<K, InputT, AccumT, OutputT> fn,
      Coder<AccumT> accumulatorCoder, Iterable<PCollectionView<?>> sideInputViews,
      KvCoder<K, InputT> kvCoder, WindowingStrategy<?, ?> windowingStrategy) {
    return new AppliedCombineFn<>(
        fn, accumulatorCoder, sideInputViews, kvCoder, windowingStrategy);
  }

  public PerKeyCombineFn<K, InputT, AccumT, OutputT> getFn() {
    return fn;
  }

  public Iterable<PCollectionView<?>> getSideInputViews() {
    return sideInputViews;
  }

  public Coder<AccumT> getAccumulatorCoder() {
    return accumulatorCoder;
  }

  public KvCoder<K, InputT> getKvCoder() {
    return kvCoder;
  }

  public WindowingStrategy<?, ?> getWindowingStrategy() {
    return windowingStrategy;
  }
}
