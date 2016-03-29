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
package com.google.cloud.dataflow.sdk.transforms;

import com.google.cloud.dataflow.sdk.coders.CannotProvideCoderException;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn;
import com.google.cloud.dataflow.sdk.transforms.CombineWithContext.CombineFnWithContext;
import com.google.cloud.dataflow.sdk.transforms.CombineWithContext.KeyedCombineFnWithContext;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;

/**
 * This class contains the shared interfaces and abstract classes for different types of combine
 * functions.
 *
 * <p>Users should not implement or extend them directly.
 */
public class CombineFnBase {
  /**
   * A {@code GloballyCombineFn<InputT, AccumT, OutputT>} specifies how to combine a
   * collection of input values of type {@code InputT} into a single
   * output value of type {@code OutputT}.  It does this via one or more
   * intermediate mutable accumulator values of type {@code AccumT}.
   *
   * <p>Do not implement this interface directly.
   * Extends {@link CombineFn} and {@link CombineFnWithContext} instead.
   *
   * @param <InputT> type of input values
   * @param <AccumT> type of mutable accumulator values
   * @param <OutputT> type of output values
   */
  public interface GlobalCombineFn<InputT, AccumT, OutputT> extends Serializable {

    /**
     * Returns the {@code Coder} to use for accumulator {@code AccumT}
     * values, or null if it is not able to be inferred.
     *
     * <p>By default, uses the knowledge of the {@code Coder} being used
     * for {@code InputT} values and the enclosing {@code Pipeline}'s
     * {@code CoderRegistry} to try to infer the Coder for {@code AccumT}
     * values.
     *
     * <p>This is the Coder used to send data through a communication-intensive
     * shuffle step, so a compact and efficient representation may have
     * significant performance benefits.
     */
    public Coder<AccumT> getAccumulatorCoder(CoderRegistry registry, Coder<InputT> inputCoder)
        throws CannotProvideCoderException;

    /**
     * Returns the {@code Coder} to use by default for output
     * {@code OutputT} values, or null if it is not able to be inferred.
     *
     * <p>By default, uses the knowledge of the {@code Coder} being
     * used for input {@code InputT} values and the enclosing
     * {@code Pipeline}'s {@code CoderRegistry} to try to infer the
     * Coder for {@code OutputT} values.
     */
    public Coder<OutputT> getDefaultOutputCoder(CoderRegistry registry, Coder<InputT> inputCoder)
        throws CannotProvideCoderException;

    /**
     * Returns the error message for not supported default values in Combine.globally().
     */
    public String getIncompatibleGlobalWindowErrorMessage();

    /**
     * Returns the default value when there are no values added to the accumulator.
     */
    public OutputT defaultValue();

    /**
     * Converts this {@code GloballyCombineFn} into an equivalent
     * {@link PerKeyCombineFn} that ignores the keys passed to it and
     * combines the values according to this {@code GloballyCombineFn}.
     *
     * @param <K> the type of the (ignored) keys
     */
    public <K> PerKeyCombineFn<K, InputT, AccumT, OutputT> asKeyedFn();
  }

  /**
   * A {@code PerKeyCombineFn<K, InputT, AccumT, OutputT>} specifies how to combine
   * a collection of input values of type {@code InputT}, associated with
   * a key of type {@code K}, into a single output value of type
   * {@code OutputT}.  It does this via one or more intermediate mutable
   * accumulator values of type {@code AccumT}.
   *
   * <p>Do not implement this interface directly.
   * Extends {@link KeyedCombineFn} and {@link KeyedCombineFnWithContext} instead.
   *
   * @param <K> type of keys
   * @param <InputT> type of input values
   * @param <AccumT> type of mutable accumulator values
   * @param <OutputT> type of output values
   */
  public interface PerKeyCombineFn<K, InputT, AccumT, OutputT> extends Serializable {
    /**
     * Returns the {@code Coder} to use for accumulator {@code AccumT}
     * values, or null if it is not able to be inferred.
     *
     * <p>By default, uses the knowledge of the {@code Coder} being
     * used for {@code K} keys and input {@code InputT} values and the
     * enclosing {@code Pipeline}'s {@code CoderRegistry} to try to
     * infer the Coder for {@code AccumT} values.
     *
     * <p>This is the Coder used to send data through a communication-intensive
     * shuffle step, so a compact and efficient representation may have
     * significant performance benefits.
     */
    public Coder<AccumT> getAccumulatorCoder(CoderRegistry registry, Coder<K> keyCoder,
        Coder<InputT> inputCoder) throws CannotProvideCoderException;

    /**
     * Returns the {@code Coder} to use by default for output
     * {@code OutputT} values, or null if it is not able to be inferred.
     *
     * <p>By default, uses the knowledge of the {@code Coder} being
     * used for {@code K} keys and input {@code InputT} values and the
     * enclosing {@code Pipeline}'s {@code CoderRegistry} to try to
     * infer the Coder for {@code OutputT} values.
     */
    public Coder<OutputT> getDefaultOutputCoder(CoderRegistry registry, Coder<K> keyCoder,
        Coder<InputT> inputCoder) throws CannotProvideCoderException;

    /**
     * Returns the a regular {@link GlobalCombineFn} that operates on a specific key.
     */
    public abstract GlobalCombineFn<InputT, AccumT, OutputT> forKey(
        final K key, final Coder<K> keyCoder);
  }

  /**
   * An abstract {@link GlobalCombineFn} base class shared by
   * {@link CombineFn} and {@link CombineFnWithContext}.
   *
   * <p>Do not extend this class directly.
   * Extends {@link CombineFn} and {@link CombineFnWithContext} instead.
   *
   * @param <InputT> type of input values
   * @param <AccumT> type of mutable accumulator values
   * @param <OutputT> type of output values
   */
  abstract static class AbstractGlobalCombineFn<InputT, AccumT, OutputT>
      implements GlobalCombineFn<InputT, AccumT, OutputT>, Serializable {
    private static final String INCOMPATIBLE_GLOBAL_WINDOW_ERROR_MESSAGE =
        "Default values are not supported in Combine.globally() if the output "
        + "PCollection is not windowed by GlobalWindows. Instead, use "
        + "Combine.globally().withoutDefaults() to output an empty PCollection if the input "
        + "PCollection is empty, or Combine.globally().asSingletonView() to get the default "
        + "output of the CombineFn if the input PCollection is empty.";

    @Override
    public Coder<AccumT> getAccumulatorCoder(CoderRegistry registry, Coder<InputT> inputCoder)
        throws CannotProvideCoderException {
      return registry.getDefaultCoder(getClass(), AbstractGlobalCombineFn.class,
          ImmutableMap.<Type, Coder<?>>of(getInputTVariable(), inputCoder), getAccumTVariable());
    }

    @Override
    public Coder<OutputT> getDefaultOutputCoder(CoderRegistry registry, Coder<InputT> inputCoder)
        throws CannotProvideCoderException {
      return registry.getDefaultCoder(getClass(), AbstractGlobalCombineFn.class,
          ImmutableMap.<Type, Coder<?>>of(getInputTVariable(), inputCoder, getAccumTVariable(),
              this.getAccumulatorCoder(registry, inputCoder)),
          getOutputTVariable());
    }

    @Override
    public String getIncompatibleGlobalWindowErrorMessage() {
      return INCOMPATIBLE_GLOBAL_WINDOW_ERROR_MESSAGE;
    }

    /**
     * Returns the {@link TypeVariable} of {@code InputT}.
     */
    public TypeVariable<?> getInputTVariable() {
      return (TypeVariable<?>)
          new TypeDescriptor<InputT>(AbstractGlobalCombineFn.class) {}.getType();
    }

    /**
     * Returns the {@link TypeVariable} of {@code AccumT}.
     */
    public TypeVariable<?> getAccumTVariable() {
      return (TypeVariable<?>)
          new TypeDescriptor<AccumT>(AbstractGlobalCombineFn.class) {}.getType();
    }

    /**
     * Returns the {@link TypeVariable} of {@code OutputT}.
     */
    public TypeVariable<?> getOutputTVariable() {
      return (TypeVariable<?>)
          new TypeDescriptor<OutputT>(AbstractGlobalCombineFn.class) {}.getType();
    }
  }

  /**
   * An abstract {@link PerKeyCombineFn} base class shared by
   * {@link KeyedCombineFn} and {@link KeyedCombineFnWithContext}.
   *
   * <p>Do not extends this class directly.
   * Extends {@link KeyedCombineFn} and {@link KeyedCombineFnWithContext} instead.
   *
   * @param <K> type of keys
   * @param <InputT> type of input values
   * @param <AccumT> type of mutable accumulator values
   * @param <OutputT> type of output values
   */
  abstract static class AbstractPerKeyCombineFn<K, InputT, AccumT, OutputT>
      implements PerKeyCombineFn<K, InputT, AccumT, OutputT> {
    @Override
    public Coder<AccumT> getAccumulatorCoder(CoderRegistry registry, Coder<K> keyCoder,
        Coder<InputT> inputCoder) throws CannotProvideCoderException {
      return registry.getDefaultCoder(getClass(), AbstractPerKeyCombineFn.class,
          ImmutableMap.<Type, Coder<?>>of(
              getKTypeVariable(), keyCoder, getInputTVariable(), inputCoder),
          getAccumTVariable());
    }

    @Override
    public Coder<OutputT> getDefaultOutputCoder(CoderRegistry registry, Coder<K> keyCoder,
        Coder<InputT> inputCoder) throws CannotProvideCoderException {
      return registry.getDefaultCoder(getClass(), AbstractPerKeyCombineFn.class,
          ImmutableMap.<Type, Coder<?>>of(getKTypeVariable(), keyCoder, getInputTVariable(),
              inputCoder, getAccumTVariable(),
              this.getAccumulatorCoder(registry, keyCoder, inputCoder)),
          getOutputTVariable());
    }

    /**
     * Returns the {@link TypeVariable} of {@code K}.
     */
    public TypeVariable<?> getKTypeVariable() {
      return (TypeVariable<?>) new TypeDescriptor<K>(AbstractPerKeyCombineFn.class) {}.getType();
    }

    /**
     * Returns the {@link TypeVariable} of {@code InputT}.
     */
    public TypeVariable<?> getInputTVariable() {
      return (TypeVariable<?>)
          new TypeDescriptor<InputT>(AbstractPerKeyCombineFn.class) {}.getType();
    }

    /**
     * Returns the {@link TypeVariable} of {@code AccumT}.
     */
    public TypeVariable<?> getAccumTVariable() {
      return (TypeVariable<?>)
          new TypeDescriptor<AccumT>(AbstractPerKeyCombineFn.class) {}.getType();
    }

    /**
     * Returns the {@link TypeVariable} of {@code OutputT}.
     */
    public TypeVariable<?> getOutputTVariable() {
      return (TypeVariable<?>)
          new TypeDescriptor<OutputT>(AbstractPerKeyCombineFn.class) {}.getType();
    }
  }
}
