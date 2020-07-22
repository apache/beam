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
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link SerializableFunction} which is not a <i>functional interface</i>. Concrete subclasses
 * allow us to infer type information, which in turn aids {@link org.apache.beam.sdk.coders.Coder
 * Coder} inference.
 */
public abstract class SimpleFunction<InputT, OutputT> extends InferableFunction<InputT, OutputT>
    implements SerializableFunction<InputT, OutputT> {

  private final @Nullable SerializableFunction<InputT, OutputT> fn;

  protected SimpleFunction() {
    this.fn = null;
    // A subclass must override apply if using this constructor. Check that via
    // reflection.
    try {
      Method methodThatMustBeOverridden =
          SimpleFunction.class.getDeclaredMethod("apply", Object.class);
      Method methodOnSubclass = getClass().getMethod("apply", Object.class);

      if (methodOnSubclass.equals(methodThatMustBeOverridden)) {
        throw new IllegalStateException(
            "Subclass of SimpleFunction must override 'apply' method"
                + " or pass a SerializableFunction to the constructor,"
                + " usually via a lambda or method reference.");
      }

    } catch (NoSuchMethodException exc) {
      throw new RuntimeException("Impossible state: missing 'apply' method entirely", exc);
    }
  }

  protected SimpleFunction(SerializableFunction<InputT, OutputT> fn) {
    this.fn = fn;
  }

  @Override
  public OutputT apply(InputT input) {
    return fn.apply(input);
  }

  public static <InputT, OutputT>
      SimpleFunction<InputT, OutputT> fromSerializableFunctionWithOutputType(
          SerializableFunction<InputT, OutputT> fn, TypeDescriptor<OutputT> outputType) {
    return new SimpleFunctionWithOutputType<>(fn, outputType);
  }

  /**
   * A {@link SimpleFunction} built from a {@link SerializableFunction}, having a known output type
   * that is explicitly set.
   */
  private static class SimpleFunctionWithOutputType<InputT, OutputT>
      extends SimpleFunction<InputT, OutputT> {

    private final TypeDescriptor<OutputT> outputType;

    public SimpleFunctionWithOutputType(
        SerializableFunction<InputT, OutputT> fn, TypeDescriptor<OutputT> outputType) {
      super(fn);
      this.outputType = outputType;
    }

    @Override
    public TypeDescriptor<OutputT> getOutputTypeDescriptor() {
      return outputType;
    }
  }
}
