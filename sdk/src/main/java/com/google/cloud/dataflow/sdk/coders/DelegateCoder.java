/*
 * Copyright (C) 2014 Google Inc.
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

package com.google.cloud.dataflow.sdk.coders;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * A {@code DelegateCoder<T, DT>} wraps a {@link Coder Coder<DT>} and
 * encodes/decodes values of type {@code T}s by converting
 * to/from {@code DT} and then encoding/decoding using the underlying
 * {@link Coder Coder<DT>}.
 *
 * <p> The conversions from {@code T} to {@code DT} and vice versa
 * must be supplied as {@link CodingFunction}, a serializable
 * function which may throw any {@code Exception}. If a thrown
 * exception is an instance of {@link CoderException} or
 * {@link IOException}, it will be re-thrown, otherwise it will be wrapped as
 * a {@link CoderException}.
 *
 * @param <T> The type of objects coded by this Coder.
 * @param <DT> The type of objects a {@code T} will be converted to for coding.
 */
public class DelegateCoder<T, DT> extends CustomCoder<T> {

  /**
   * A {@code CodingFunction<Input, Output>} is a serializable function
   * from {@code Input} to {@code Output} that
   * may throw any {@code Exception}.
   */
  public static interface CodingFunction<Input, Output> extends Serializable {
     public abstract Output apply(Input input) throws Exception;
  }

  public static <T, DT> DelegateCoder<T, DT> of(Coder<DT> coder,
      CodingFunction<T, DT> toFn,
      CodingFunction<DT, T> fromFn) {
    return new DelegateCoder<T, DT>(coder, toFn, fromFn);
  }

  @Override
  public void encode(T value, OutputStream outStream, Context context)
      throws CoderException, IOException {
    coder.encode(applyAndWrapExceptions(toFn, value), outStream, context);
  }

  @Override
  public T decode(InputStream inStream, Context context) throws CoderException, IOException {
    return applyAndWrapExceptions(fromFn, coder.decode(inStream, context));
  }

  @Override
  public boolean isDeterministic() {
    return coder.isDeterministic();
  }

  @Override
  public String toString() {
    return "DelegateCoder(" + coder + ")";
  }

  /////////////////////////////////////////////////////////////////////////////

  private <Input, Output> Output applyAndWrapExceptions(
      CodingFunction<Input, Output> fn,
      Input input) throws CoderException, IOException {
    try {
      return fn.apply(input);
    } catch (IOException exc) {
      throw exc;
    } catch (Exception exc) {
      throw new CoderException(exc);
    }
  }

  private final Coder<DT> coder;
  private final CodingFunction<T, DT> toFn;
  private final CodingFunction<DT, T> fromFn;

  protected DelegateCoder(Coder<DT> coder,
      CodingFunction<T, DT> toFn,
      CodingFunction<DT, T> fromFn) {
    this.coder = coder;
    this.fromFn = fromFn;
    this.toFn = toFn;
  }
}
