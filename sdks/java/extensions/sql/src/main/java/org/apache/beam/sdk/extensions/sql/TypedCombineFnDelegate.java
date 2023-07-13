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
package org.apache.beam.sdk.extensions.sql;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.TypeVariable;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A {@link Combine.CombineFn} delegating all relevant calls to given delegate. This is used to
 * create a type anonymous class for cases where the CombineFn is a generic class. The anonymous
 * class can then be used in a UDAF as
 *
 * <pre>
 *   .registerUdaf("UDAF", new TypedCombineFnDelegate<>(genericCombineFn) {})
 * </pre>
 *
 * @param <InputT> the type of input
 * @param <AccumT> the type of accumulator
 * @param <OutputT> the type of output
 */
public class TypedCombineFnDelegate<InputT, AccumT, OutputT>
    extends Combine.CombineFn<InputT, AccumT, OutputT> {

  private final Combine.CombineFn<InputT, AccumT, OutputT> delegate;

  protected TypedCombineFnDelegate(Combine.CombineFn<InputT, AccumT, OutputT> delegate) {
    this.delegate = delegate;
  }

  @Override
  public TypeDescriptor<OutputT> getOutputType() {
    return Optional.<TypeDescriptor<OutputT>>ofNullable(getGenericSuperTypeAtIndex(2))
        .orElse(delegate.getOutputType());
  }

  @Override
  public TypeDescriptor<InputT> getInputType() {
    return Optional.<TypeDescriptor<InputT>>ofNullable(getGenericSuperTypeAtIndex(0))
        .orElse(delegate.getInputType());
  }

  @Override
  public AccumT createAccumulator() {
    return delegate.createAccumulator();
  }

  @Override
  public AccumT addInput(AccumT mutableAccumulator, InputT input) {
    return delegate.addInput(mutableAccumulator, input);
  }

  @Override
  public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
    return delegate.mergeAccumulators(accumulators);
  }

  @Override
  public OutputT extractOutput(AccumT accumulator) {
    return delegate.extractOutput(accumulator);
  }

  @Override
  public AccumT compact(AccumT accumulator) {
    return delegate.compact(accumulator);
  }

  @Override
  public OutputT apply(Iterable<? extends InputT> inputs) {
    return delegate.apply(inputs);
  }

  @Override
  public OutputT defaultValue() {
    return delegate.defaultValue();
  }

  @Override
  public Coder<AccumT> getAccumulatorCoder(CoderRegistry registry, Coder<InputT> inputCoder)
      throws CannotProvideCoderException {
    return delegate.getAccumulatorCoder(registry, inputCoder);
  }

  @Override
  public Coder<OutputT> getDefaultOutputCoder(CoderRegistry registry, Coder<InputT> inputCoder)
      throws CannotProvideCoderException {
    return delegate.getDefaultOutputCoder(registry, inputCoder);
  }

  @Override
  public String getIncompatibleGlobalWindowErrorMessage() {
    return delegate.getIncompatibleGlobalWindowErrorMessage();
  }

  @Override
  public TypeVariable<?> getInputTVariable() {
    return delegate.getInputTVariable();
  }

  @Override
  public TypeVariable<?> getAccumTVariable() {
    return delegate.getAccumTVariable();
  }

  @Override
  public TypeVariable<?> getOutputTVariable() {
    return delegate.getOutputTVariable();
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    delegate.populateDisplayData(builder);
  }

  @SuppressWarnings("unchecked")
  @Nullable
  private <T> TypeDescriptor<T> getGenericSuperTypeAtIndex(int index) {
    Class<?> cls = Preconditions.checkArgumentNotNull(getClass());
    do {
      Class<?> superClass = cls.getSuperclass();
      if (superClass == null) {
        break;
      }
      if (superClass.equals(TypedCombineFnDelegate.class)) {
        @Nonnull
        ParameterizedType superType =
            (ParameterizedType) Preconditions.checkArgumentNotNull(cls.getGenericSuperclass());
        TypeDescriptor<T> candidate =
            (TypeDescriptor<T>) TypeDescriptor.of(superType.getActualTypeArguments()[index]);
        if (!(candidate instanceof TypeVariable)) {
          return candidate;
        }
      }
      cls = superClass;
    } while (true);
    return null;
  }
}
