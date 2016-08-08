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
package org.apache.beam.sdk.coders;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 * A {@code DelegateCoder<T, IntermediateT>} wraps a {@link Coder} for {@code IntermediateT} and
 * encodes/decodes values of type {@code T} by converting
 * to/from {@code IntermediateT} and then encoding/decoding using the underlying
 * {@code Coder<IntermediateT>}.
 *
 * <p>The conversions from {@code T} to {@code IntermediateT} and vice versa
 * must be supplied as {@link CodingFunction}, a serializable
 * function that may throw any {@code Exception}. If a thrown
 * exception is an instance of {@link CoderException} or
 * {@link IOException}, it will be re-thrown, otherwise it will be wrapped as
 * a {@link CoderException}.
 *
 * @param <T> The type of objects coded by this Coder.
 * @param <IntermediateT> The type of objects a {@code T} will be converted to for coding.
 */
public final class DelegateCoder<T, IntermediateT> extends CustomCoder<T> {
  /**
   * A {@link DelegateCoder.CodingFunction CodingFunction&lt;InputT, OutputT&gt;} is a serializable
   * function from {@code InputT} to {@code OutputT} that may throw any {@link Exception}.
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
   * {@inheritDoc}
   *
   * @throws NonDeterministicException when the underlying coder's {@code verifyDeterministic()}
   *         throws a {@link Coder.NonDeterministicException}. For this to be safe, the
   *         intermediate {@code CodingFunction<T, IntermediateT>} must also be deterministic.
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    coder.verifyDeterministic();
  }

  /**
   * {@inheritDoc}
   *
   * @return a structural for a value of type {@code T} obtained by first converting to
   *         {@code IntermediateT} and then obtaining a structural value according to the underlying
   *         coder.
   */
  @Override
  public Object structuralValue(T value) throws Exception {
    return coder.structuralValue(toFn.apply(value));
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    DelegateCoder<?, ?> that = (DelegateCoder<?, ?>) o;
    return Objects.equal(this.coder, that.coder)
        && Objects.equal(this.toFn, that.toFn)
        && Objects.equal(this.fromFn, that.fromFn);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.coder, this.toFn, this.fromFn);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("coder", coder)
        .add("toFn", toFn)
        .add("fromFn", fromFn)
        .toString();
  }

  /**
   * {@inheritDoc}
   *
   * @return a {@link String} composed from the underlying coder class name and its encoding id.
   *         Note that this omits any description of the coding functions. These should be modified
   *         with care.
   */
  @Override
  public String getEncodingId() {
    return delegateEncodingId(coder.getClass(), coder.getEncodingId());
  }

  /**
   * {@inheritDoc}
   *
   * @return allowed encodings which are composed from the underlying coder class and its allowed
   *         encoding ids. Note that this omits any description of the coding functions. These
   *         should be modified with care.
   */
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
