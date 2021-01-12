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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@code DelegateCoder<T, IntermediateT>} wraps a {@link Coder} for {@code IntermediateT} and
 * encodes/decodes values of type {@code T} by converting to/from {@code IntermediateT} and then
 * encoding/decoding using the underlying {@code Coder<IntermediateT>}.
 *
 * <p>The conversions from {@code T} to {@code IntermediateT} and vice versa must be supplied as
 * {@link CodingFunction}, a serializable function that may throw any {@code Exception}. If a thrown
 * exception is an instance of {@link CoderException} or {@link IOException}, it will be re-thrown,
 * otherwise it will be wrapped as a {@link CoderException}.
 *
 * @param <T> The type of objects coded by this Coder.
 * @param <IntermediateT> The type of objects a {@code T} will be converted to for coding.
 */
public final class DelegateCoder<T, IntermediateT> extends CustomCoder<T> {
  /**
   * A {@link DelegateCoder.CodingFunction CodingFunction&lt;InputT, OutputT&gt;} is a serializable
   * function from {@code InputT} to {@code OutputT} that may throw any {@link Exception}.
   */
  public interface CodingFunction<InputT, OutputT> extends Serializable {
    OutputT apply(InputT input) throws Exception;
  }

  public static <T, IntermediateT> DelegateCoder<T, IntermediateT> of(
      Coder<IntermediateT> coder,
      CodingFunction<T, IntermediateT> toFn,
      CodingFunction<IntermediateT, T> fromFn) {
    return of(coder, toFn, fromFn, null);
  }

  public static <T, IntermediateT> DelegateCoder<T, IntermediateT> of(
      Coder<IntermediateT> coder,
      CodingFunction<T, IntermediateT> toFn,
      CodingFunction<IntermediateT, T> fromFn,
      @Nullable TypeDescriptor<T> typeDescriptor) {
    return new DelegateCoder<>(coder, toFn, fromFn, typeDescriptor);
  }

  @Override
  public void encode(T value, OutputStream outStream) throws CoderException, IOException {
    encode(value, outStream, Context.NESTED);
  }

  @Override
  public void encode(T value, OutputStream outStream, Context context)
      throws CoderException, IOException {
    coder.encode(applyAndWrapExceptions(toFn, value), outStream, context);
  }

  @Override
  public T decode(InputStream inStream) throws CoderException, IOException {
    return decode(inStream, Context.NESTED);
  }

  @Override
  public T decode(InputStream inStream, Context context) throws CoderException, IOException {
    return applyAndWrapExceptions(fromFn, coder.decode(inStream, context));
  }

  /**
   * Returns the coder used to encode/decode the intermediate values produced/consumed by the coding
   * functions of this {@code DelegateCoder}.
   */
  public Coder<IntermediateT> getCoder() {
    return coder;
  }

  /**
   * {@inheritDoc}
   *
   * @throws NonDeterministicException when the underlying coder's {@code verifyDeterministic()}
   *     throws a {@link Coder.NonDeterministicException}. For this to be safe, the intermediate
   *     {@code CodingFunction<T, IntermediateT>} must also be deterministic.
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    coder.verifyDeterministic();
  }

  /**
   * {@inheritDoc}
   *
   * @return a structural for a value of type {@code T} obtained by first converting to {@code
   *     IntermediateT} and then obtaining a structural value according to the underlying coder.
   */
  @Override
  public Object structuralValue(T value) {
    try {
      IntermediateT intermediate = toFn.apply(value);
      return coder.structuralValue(intermediate);
    } catch (Exception exn) {
      throw new IllegalArgumentException(
          "Unable to encode element '" + value + "' with coder '" + this + "'.", exn);
    }
  }

  @Override
  public boolean equals(@Nullable Object o) {
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

  @Override
  public TypeDescriptor<T> getEncodedTypeDescriptor() {
    if (typeDescriptor == null) {
      return super.getEncodedTypeDescriptor();
    }
    return typeDescriptor;
  }

  private String delegateEncodingId(Class<?> delegateClass, String encodingId) {
    return String.format("%s:%s", delegateClass.getName(), encodingId);
  }

  /////////////////////////////////////////////////////////////////////////////

  private <InputT, OutputT> OutputT applyAndWrapExceptions(
      CodingFunction<InputT, OutputT> fn, InputT input) throws CoderException, IOException {
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

  // null unless the user explicitly provides a TypeDescriptor.
  // If null, then the machinery from the superclass (StructuredCoder) will be used
  // to try to deduce a good type descriptor.
  private final @Nullable TypeDescriptor<T> typeDescriptor;

  protected DelegateCoder(
      Coder<IntermediateT> coder,
      CodingFunction<T, IntermediateT> toFn,
      CodingFunction<IntermediateT, T> fromFn,
      @Nullable TypeDescriptor<T> typeDescriptor) {
    this.coder = coder;
    this.fromFn = fromFn;
    this.toFn = toFn;
    this.typeDescriptor = typeDescriptor;
  }
}
