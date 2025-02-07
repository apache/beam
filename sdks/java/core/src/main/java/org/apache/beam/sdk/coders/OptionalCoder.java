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
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;

/**
 * A {@link OptionalCoder} encodes optional values of type {@code T} using a nested {@code
 * Coder<T>}. {@link OptionalCoder} uses exactly 1 byte per entry to indicate whether the value is
 * empty, then adds the encoding of the inner coder for non-empty values.
 *
 * @param <T> the type of the values being transcoded
 */
public class OptionalCoder<T> extends StructuredCoder<Optional<T>> {
  public static <T> OptionalCoder<T> of(Coder<T> valueCoder) {
    return new OptionalCoder<>(valueCoder);
  }

  /////////////////////////////////////////////////////////////////////////////

  private final NullableCoder<T> nullableCoder;

  private OptionalCoder(Coder<T> valueCoder) {
    this.nullableCoder = NullableCoder.of(valueCoder);
  }

  /** Returns the inner {@link Coder} wrapped by this {@link OptionalCoder} instance. */
  public Coder<T> getValueCoder() {
    return nullableCoder.getValueCoder();
  }

  @Override
  public void encode(Optional<T> value, OutputStream outStream) throws IOException, CoderException {
    nullableCoder.encode(value.orElse(null), outStream);
  }

  @Override
  public Optional<T> decode(InputStream inStream) throws IOException, CoderException {
    return Optional.ofNullable(nullableCoder.decode(inStream));
  }

  @Override
  public List<Coder<T>> getCoderArguments() {
    return nullableCoder.getCoderArguments();
  }

  /**
   * {@code OptionalCoder} is deterministic if the nested {@code Coder} is.
   *
   * <p>{@inheritDoc}
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    nullableCoder.verifyDeterministic();
  }

  /**
   * {@code OptionalCoder} is consistent with equals if the nested {@code Coder} is.
   *
   * <p>{@inheritDoc}
   */
  @Override
  public boolean consistentWithEquals() {
    return nullableCoder.consistentWithEquals();
  }

  @Override
  public Object structuralValue(Optional<T> value) {
    return nullableCoder.structuralValue(value.orElse(null));
  }

  /**
   * Overridden to short-circuit the default {@code StructuredCoder} behavior of encoding and
   * counting the bytes. The size is known (1 byte) when {@code value} is {@code null}, otherwise
   * the size is 1 byte plus the size of nested {@code Coder}'s encoding of {@code value}.
   *
   * <p>{@inheritDoc}
   */
  @Override
  public void registerByteSizeObserver(Optional<T> value, ElementByteSizeObserver observer)
      throws Exception {
    nullableCoder.registerByteSizeObserver(value.orElse(null), observer);
  }

  /**
   * Overridden to short-circuit the default {@code StructuredCoder} behavior of encoding and
   * counting the bytes. The size is known (1 byte) when {@code value} is {@code null}, otherwise
   * the size is 1 byte plus the size of nested {@code Coder}'s encoding of {@code value}.
   *
   * <p>{@inheritDoc}
   */
  @Override
  protected long getEncodedElementByteSize(Optional<T> value) throws Exception {
    return nullableCoder.getEncodedElementByteSize(value.orElse(null));
  }

  /**
   * {@code OptionalCoder} is cheap if {@code valueCoder} is cheap.
   *
   * <p>{@inheritDoc}
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(Optional<T> value) {
    return nullableCoder.isRegisterByteSizeObserverCheap(value.orElse(null));
  }

  @Override
  public TypeDescriptor<Optional<T>> getEncodedTypeDescriptor() {
    return new TypeDescriptor<Optional<T>>() {}.where(
        new TypeParameter<T>() {}, getValueCoder().getEncodedTypeDescriptor());
  }
}
