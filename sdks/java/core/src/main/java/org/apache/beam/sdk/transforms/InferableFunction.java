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

import java.lang.reflect.Method;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link ProcessFunction} which is not a <i>functional interface</i>. Concrete subclasses allow
 * us to infer type information, which in turn aids {@link org.apache.beam.sdk.coders.Coder Coder}
 * inference.
 *
 * <p>See {@link SimpleFunction} for providing robust type information where a {@link
 * SerializableFunction} is required.
 */
public abstract class InferableFunction<InputT, OutputT>
    implements ProcessFunction<InputT, OutputT>, HasDisplayData {

  private final @Nullable ProcessFunction<InputT, OutputT> fn;

  protected InferableFunction() {
    this.fn = null;
    // A subclass must override apply if using this constructor. Check that via
    // reflection.
    try {
      Method methodThatMustBeOverridden =
          InferableFunction.class.getDeclaredMethod("apply", Object.class);
      Method methodOnSubclass = getClass().getMethod("apply", Object.class);

      if (methodOnSubclass.equals(methodThatMustBeOverridden)) {
        throw new IllegalStateException(
            "Subclass of InferableFunction must override 'apply' method"
                + " or pass a ProcessFunction to the constructor,"
                + " usually via a lambda or method reference.");
      }

    } catch (NoSuchMethodException exc) {
      throw new RuntimeException("Impossible state: missing 'apply' method entirely", exc);
    }
  }

  protected InferableFunction(ProcessFunction<InputT, OutputT> fn) {
    this.fn = fn;
  }

  @Override
  public OutputT apply(InputT input) throws Exception {
    return fn.apply(input);
  }

  public static <InputT, OutputT>
      InferableFunction<InputT, OutputT> fromProcessFunctionWithOutputType(
          ProcessFunction<InputT, OutputT> fn, TypeDescriptor<OutputT> outputType) {
    return new InferableFunctionWithOutputType<>(fn, outputType);
  }

  /**
   * Returns a {@link TypeDescriptor} capturing what is known statically about the input type of
   * this {@link InferableFunction} instance's most-derived class.
   *
   * <p>See {@link #getOutputTypeDescriptor} for more discussion.
   */
  public TypeDescriptor<InputT> getInputTypeDescriptor() {
    return new TypeDescriptor<InputT>(this) {};
  }

  /**
   * Returns a {@link TypeDescriptor} capturing what is known statically about the output type of
   * this {@link InferableFunction} instance's most-derived class.
   *
   * <p>In the normal case of a concrete {@link InferableFunction} subclass with no generic type
   * parameters of its own (including anonymous inner classes), this will be a complete non-generic
   * type, which is good for choosing a default output {@code Coder<OutputT>} for the output {@code
   * PCollection<OutputT>}.
   */
  public TypeDescriptor<OutputT> getOutputTypeDescriptor() {
    return new TypeDescriptor<OutputT>(this) {};
  }

  /**
   * {@inheritDoc}
   *
   * <p>By default, does not register any display data. Implementors may override this method to
   * provide their own display data.
   */
  @Override
  public void populateDisplayData(DisplayData.Builder builder) {}

  /**
   * A {@link InferableFunction} built from a {@link ProcessFunction}, having a known output type
   * that is explicitly set.
   */
  private static class InferableFunctionWithOutputType<InputT, OutputT>
      extends InferableFunction<InputT, OutputT> {

    private final TypeDescriptor<OutputT> outputType;

    public InferableFunctionWithOutputType(
        ProcessFunction<InputT, OutputT> fn, TypeDescriptor<OutputT> outputType) {
      super(fn);
      this.outputType = outputType;
    }

    @Override
    public TypeDescriptor<OutputT> getOutputTypeDescriptor() {
      return outputType;
    }
  }
}
