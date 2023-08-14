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
package org.apache.beam.sdk.transforms;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
 *
 * <p>This class contains the shared interfaces and abstract classes for different types of combine
 * functions.
 *
 * <p>Users should not implement or extend them directly.
 */
@Internal
public class CombineFnBase {
  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>A {@code GloballyCombineFn<InputT, AccumT, OutputT>} specifies how to combine a collection
   * of input values of type {@code InputT} into a single output value of type {@code OutputT}. It
   * does this via one or more intermediate mutable accumulator values of type {@code AccumT}.
   *
   * <p>Do not implement this interface directly. Extends {@link CombineFn} and {@link
   * CombineFnWithContext} instead.
   *
   * @param <InputT> type of input values
   * @param <AccumT> type of mutable accumulator values
   * @param <OutputT> type of output values
   */
  @Internal
  public interface GlobalCombineFn<InputT, AccumT, OutputT> extends Serializable, HasDisplayData {

    /**
     * Returns the {@code Coder} to use for accumulator {@code AccumT} values, or null if it is not
     * able to be inferred.
     *
     * <p>By default, uses the knowledge of the {@code Coder} being used for {@code InputT} values
     * and the enclosing {@code Pipeline}'s {@code CoderRegistry} to try to infer the Coder for
     * {@code AccumT} values.
     *
     * <p>This is the Coder used to send data through a communication-intensive shuffle step, so a
     * compact and efficient representation may have significant performance benefits.
     */
    Coder<AccumT> getAccumulatorCoder(CoderRegistry registry, Coder<InputT> inputCoder)
        throws CannotProvideCoderException;

    /**
     * Returns the {@code Coder} to use by default for output {@code OutputT} values, or null if it
     * is not able to be inferred.
     *
     * <p>By default, uses the knowledge of the {@code Coder} being used for input {@code InputT}
     * values and the enclosing {@code Pipeline}'s {@code CoderRegistry} to try to infer the Coder
     * for {@code OutputT} values.
     */
    Coder<OutputT> getDefaultOutputCoder(CoderRegistry registry, Coder<InputT> inputCoder)
        throws CannotProvideCoderException;

    /** Returns the error message for not supported default values in Combine.globally(). */
    String getIncompatibleGlobalWindowErrorMessage();

    /** Returns the default value when there are no values added to the accumulator. */
    OutputT defaultValue();
  }

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>An abstract {@link GlobalCombineFn} base class shared by {@link CombineFn} and {@link
   * CombineFnWithContext}.
   *
   * <p>Do not extend this class directly. Extends {@link CombineFn} and {@link
   * CombineFnWithContext} instead.
   *
   * @param <InputT> type of input values
   * @param <AccumT> type of mutable accumulator values
   * @param <OutputT> type of output values
   */
  @Internal
  abstract static class AbstractGlobalCombineFn<InputT, AccumT, OutputT>
      implements GlobalCombineFn<InputT, AccumT, OutputT>, Serializable {
    private static final String INCOMPATIBLE_GLOBAL_WINDOW_ERROR_MESSAGE =
        "Default values are not supported in Combine.globally() if the input "
            + "PCollection is not windowed by GlobalWindows. Instead, use "
            + "Combine.globally().withoutDefaults() to output an empty PCollection if the input "
            + "PCollection is empty, or Combine.globally().asSingletonView() to get the default "
            + "output of the CombineFn if the input PCollection is empty.";

    @Override
    public Coder<AccumT> getAccumulatorCoder(CoderRegistry registry, Coder<InputT> inputCoder)
        throws CannotProvideCoderException {
      return registry.getCoder(
          getClass(),
          AbstractGlobalCombineFn.class,
          ImmutableMap.<Type, Coder<?>>of(getInputTVariable(), inputCoder),
          getAccumTVariable());
    }

    @Override
    public Coder<OutputT> getDefaultOutputCoder(CoderRegistry registry, Coder<InputT> inputCoder)
        throws CannotProvideCoderException {
      return registry.getCoder(
          getClass(),
          AbstractGlobalCombineFn.class,
          ImmutableMap.<Type, Coder<?>>of(
              getInputTVariable(),
              inputCoder,
              getAccumTVariable(),
              this.getAccumulatorCoder(registry, inputCoder)),
          getOutputTVariable());
    }

    @Override
    public String getIncompatibleGlobalWindowErrorMessage() {
      return INCOMPATIBLE_GLOBAL_WINDOW_ERROR_MESSAGE;
    }

    /** Returns the {@link TypeVariable} of {@code InputT}. */
    public TypeVariable<?> getInputTVariable() {
      return (TypeVariable<?>)
          new TypeDescriptor<InputT>(AbstractGlobalCombineFn.class) {}.getType();
    }

    /** Returns the {@link TypeVariable} of {@code AccumT}. */
    public TypeVariable<?> getAccumTVariable() {
      return (TypeVariable<?>)
          new TypeDescriptor<AccumT>(AbstractGlobalCombineFn.class) {}.getType();
    }

    /** Returns the {@link TypeVariable} of {@code OutputT}. */
    public TypeVariable<?> getOutputTVariable() {
      return (TypeVariable<?>)
          new TypeDescriptor<OutputT>(AbstractGlobalCombineFn.class) {}.getType();
    }

    /**
     * {@inheritDoc}
     *
     * <p>By default, does not register any display data. Implementors may override this method to
     * provide their own display data.
     */
    @Override
    public void populateDisplayData(DisplayData.Builder builder) {}
  }
}
