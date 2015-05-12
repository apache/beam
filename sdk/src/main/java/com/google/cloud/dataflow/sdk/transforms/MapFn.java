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
 * A {@link DoFn} that represents a simple Map operation. {@code MapDoFn} is applied to each element
 * of the input {@link PCollection} and outputs the result. No behavior is provided in startBundle
 * and finishBundle.  A {@link SerializableFunction} can be provided to specify the behavior of
 * {@link #apply}.
 *
 * @param <InputT> the type of input element
 * @param <OutputT> the type of output element
 */
public abstract class MapFn<InputT, OutputT> extends DoFn<InputT, OutputT> {
  private static final long serialVersionUID = 0L;
  private final SerializableFunction<InputT, OutputT> fn;

  /**
   * Creates a new {@code MapFn}. {@link #apply} must be overridden for this MapFn to function
   * properly.
   */
  protected MapFn() {
    this.fn = null;

    if (!applyOverriden()) {
      throw new IllegalStateException("Didn't find an override for MapFn#apply(InputT). "
          + "Apply must be overridden to use the no-arg MapFn constructor.");
    }
  }

  /**
   * Create a new {@code MapFn} that applies the provided {@link SerializableFunction} to its
   * inputs.
   */
  protected MapFn(SerializableFunction<InputT, OutputT> fn) {
    this.fn = checkNotNull(fn, "null SerializableFunction provided to MapFn constructor");

    if (applyOverriden()) {
      throw new IllegalStateException("Found an override of MapFn#apply(InputT). "
          + "MapFn#apply(InputT) cannot be overriden if a SerializableFunction is provided.");
    }
  }

  private boolean applyOverriden() {
    try {
      Method m = getClass().getMethod("apply", Object.class);
      if (m.getDeclaringClass().equals(MapFn.class)) {
        return false;
      }
      return true;
    } catch (NoSuchMethodException e) {
      // Generic apply is declared in this class
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
    c.output(apply(c.element()));
  }

  @Override
  public final void finishBundle(Context c) throws Exception {}

  /**
   * Applies this MapFn to an input element, returning the element to add to the output
   * {@link PCollection}.
   *
   * <p> If a {@link SerializableFunction} was not provided to this {@code MapFn} when it was
   * created, this method must be overriden, or it will throw a {@link NullPointerException} when it
   * is invoked.
   */
  public OutputT apply(InputT input) {
    return fn.apply(input);
  }
}
