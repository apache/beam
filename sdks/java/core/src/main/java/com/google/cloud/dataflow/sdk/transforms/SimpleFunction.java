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

import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

/**
 * A {@link SerializableFunction} which is not a <i>functional interface</i>.
 * Concrete subclasses allow us to infer type information, which in turn aids
 * {@link Coder} inference.
 */
public abstract class SimpleFunction<InputT, OutputT>
    implements SerializableFunction<InputT, OutputT> {

  /**
   * Returns a {@link TypeDescriptor} capturing what is known statically
   * about the input type of this {@code DoFn} instance's most-derived
   * class.
   *
   * <p>See {@link #getOutputTypeDescriptor} for more discussion.
   */
  public TypeDescriptor<InputT> getInputTypeDescriptor() {
    return new TypeDescriptor<InputT>(this) {};
  }

  /**
   * Returns a {@link TypeDescriptor} capturing what is known statically
   * about the output type of this {@code DoFn} instance's
   * most-derived class.
   *
   * <p>In the normal case of a concrete {@code DoFn} subclass with
   * no generic type parameters of its own (including anonymous inner
   * classes), this will be a complete non-generic type, which is good
   * for choosing a default output {@code Coder<OutputT>} for the output
   * {@code PCollection<OutputT>}.
   */
  public TypeDescriptor<OutputT> getOutputTypeDescriptor() {
    return new TypeDescriptor<OutputT>(this) {};
  }
}
