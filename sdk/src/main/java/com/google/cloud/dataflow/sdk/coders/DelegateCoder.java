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

package com.google.cloud.dataflow.sdk.coders;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 * A {@code DelegateCoder<T, IntermediateT>} wraps a {@link Coder} for {@code IntermediateT} and
 * encodes/decodes values of type {@code T}s by converting
 * to/from {@code IntermediateT} and then encoding/decoding using the underlying
 * {@code Coder<IntermediateT>}.
 *
 * <p> The conversions from {@code T} to {@code IntermediateT} and vice versa
 * must be supplied as {@link CodingFunction}, a serializable
 * function that may throw any {@code Exception}. If a thrown
 * exception is an instance of {@link CoderException} or
 * {@link IOException}, it will be re-thrown, otherwise it will be wrapped as
 * a {@link CoderException}.
 *
 * @param <T> The type of objects coded by this Coder.
 * @param <IntermediateT> The type of objects a {@code T} will be converted to for coding.
 */
public class DelegateCoder<T, IntermediateT> extends CustomCoder<T> {
  private static final long serialVersionUID = 0;

  /**
   * A {@code CodingFunction<InputT, OutputT>} is a serializable function
   * from {@code InputT} to {@code OutputT} that
   * may throw any {@code Exception}.
   */
  public static interface CodingFunction<InputT, OutputT> extends Serializable {
     public abstract OutputT apply(InputT input) throws Exception;
  }

  public static <T, IntermediateT> DelegateCoder<T, IntermediateT> of(Coder<IntermediateT> coder,
      CodingFunction<T, IntermediateT> toFn,
      CodingFunction<IntermediateT, T> fromFn) {
    return new DelegateCoder<T, IntermediateT>(coder, toFn, fromFn);
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

  /**
   * Returns the coder used to encode/decode the intermediate values produced/consumed by the
   * coding functions of this {@code DelegateCoder}.
   */
  public Coder<IntermediateT> getCoder() {
    return coder;
  }

  /**
   * A delegate coder is deterministic if the underlying coder is deterministic.
   * For this to be safe, the intermediate {@code CodingFunction<T, IntermediateT>} must
   * also be deterministic.
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    coder.verifyDeterministic();
  }

  @Override
  public Object structuralValue(T value) throws Exception {
    return coder.structuralValue(toFn.apply(value));
  }

  @Override
  public String toString() {
    return "DelegateCoder(" + coder + ")";
  }

  /**
   * The encoding id for the binary format of the delegate coder is a combination of the underlying
   * coder class and its encoding id.
   *
   * <p>Note that this omits any description of the coding functions. These should be modified with
   * care.
   */
  @Override
  public String getEncodingId() {
    return delegateEncodingId(coder.getClass(), coder.getEncodingId());
  }

  @Override
  public Collection<String> getAllowedEncodings() {
    List<String> allowedEncodings = Lists.newArrayList();
    for (String allowedEncoding : coder.getAllowedEncodings()) {
      allowedEncodings.add(delegateEncodingId(coder.getClass(), allowedEncoding));
    }
    return allowedEncodings;
  }

  private String delegateEncodingId(Class<?> delegateClass, String encodingId) {
    return String.format("%s:%s", delegateClass.getName(), encodingId);
  }

  /////////////////////////////////////////////////////////////////////////////

  private <InputT, OutputT> OutputT applyAndWrapExceptions(
      CodingFunction<InputT, OutputT> fn,
      InputT input) throws CoderException, IOException {
    try {
      return fn.apply(input);
    } catch (IOException exc) {
      throw exc;
    } catch (Exception exc) {
      throw new CoderException(exc);
    }
  }

  private final Coder<IntermediateT> coder;
  private final CodingFunction<T, IntermediateT> toFn;
  private final CodingFunction<IntermediateT, T> fromFn;

  protected DelegateCoder(Coder<IntermediateT> coder,
      CodingFunction<T, IntermediateT> toFn,
      CodingFunction<IntermediateT, T> fromFn) {
    this.coder = coder;
    this.fromFn = fromFn;
    this.toFn = toFn;
  }
}
