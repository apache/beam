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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.dataflow.sdk.values.PCollection;

import java.lang.reflect.Method;

/**
 * A {@link DoFn} that represents a FlatMap operation. {@code FlatMapFn} is applied to each
 * element of the input {@link PCollection} and outputs each element in the result of the
 * application. No behavior is provided in startBundle and finishBundle. A {@link
 * SerializableFunction} can be provided to specify the behavior of {@link #apply}.
 *
 * @param <InputT> the type of input element
 * @param <OutputT> the type of output element
 */
public abstract class FlatMapFn<InputT, OutputT> extends DoFn<InputT, OutputT> {
  private static final long serialVersionUID = 0L;
  private final SerializableFunction<InputT, Iterable<OutputT>> fn;

  /**
   * Creates a new {@code FlatMapFn}. {@link #apply} must be overridden for this FlatMapDoFn to
   * function properly.
   */
  protected FlatMapFn() {
    this.fn = null;

    if (!applyOverriden()) {
      throw new IllegalStateException("Didn't find an override for FlatMapFn#apply(InputT). "
          + "Apply must be overridden to use the no-arg FlatMapFn constructor.");
    }
  }

  /**
   * Creates a new {@code FlatMapFn} that applies the {@link SerializableFunction} to its inputs.
   */
  protected FlatMapFn(SerializableFunction<InputT, Iterable<OutputT>> fn) {
    this.fn = checkNotNull(fn, "null SerializableFunction provided to FlatMapFn constructor");

    if (applyOverriden()) {
      throw new IllegalStateException("Found an override of FlatMapFn#apply(InputT). "
          + "FlatMapFn#apply(InputT) cannot be overriden if a SerializableFunction is provided.");
    }
  }

  private boolean applyOverriden() {
    try {
      Method m = getClass().getMethod("apply", Object.class);
      if (m.getDeclaringClass().equals(FlatMapFn.class)) {
        return false;
      }
      return true;
    } catch (NoSuchMethodException e) {
      throw new AssertionError(
          "NoSuchMethodException encountered for method apply() in FlatMapFn "
          + "but FlatMapFn declares apply()",
          e);
    }
  }

  @Override
  public final void startBundle(Context c) throws Exception {}

  @Override
  public final void processElement(DoFn<InputT, OutputT>.ProcessContext c) throws Exception {
    for (OutputT output : apply(c.element())) {
      c.output(output);
    }
  }

  @Override
  public final void finishBundle(Context c) throws Exception {}

  /**
   * Applies this FlatMapFn to an input element, returning the elements to add to the output
   * {@link PCollection}.
   *
   * <p> If a {@link SerializableFunction} was not provided to this {@code FlatMapDoFn} when it was
   * created, this method must be overriden, or it will throw a {@link NullPointerException} when it
   * is invoked.
   */
  public Iterable<OutputT> apply(InputT input) {
    return fn.apply(input);
  }
}
