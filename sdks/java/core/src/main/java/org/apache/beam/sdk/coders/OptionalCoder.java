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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

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

  private final Coder<T> valueCoder;
  private static final int ENCODE_EMPTY = 0;
  private static final int ENCODE_PRESENT = 1;

  private OptionalCoder(Coder<T> valueCoder) {
    this.valueCoder = valueCoder;
  }

  /** Returns the inner {@link Coder} wrapped by this {@link OptionalCoder} instance. */
  public Coder<T> getValueCoder() {
    return valueCoder;
  }

  @Override
  public void encode(Optional<T> value, OutputStream outStream) throws IOException, CoderException {
    if (!value.isPresent()) {
      outStream.write(ENCODE_EMPTY);
    } else {
      outStream.write(ENCODE_PRESENT);
      valueCoder.encode(value.get(), outStream);
    }
  }

  @Override
  public Optional<T> decode(InputStream inStream) throws IOException, CoderException {
    int b = inStream.read();
    if (b == ENCODE_EMPTY) {
      return Optional.empty();
    } else if (b != ENCODE_PRESENT) {
      throw new CoderException(
          String.format(
              "OptionalCoder expects either a byte valued %s (empty) or %s (present), got %s",
              ENCODE_EMPTY, ENCODE_PRESENT, b));
    }
    T value = checkNotNull(valueCoder.decode(inStream), "cannot decode a null");
    return Optional.of(value);
  }

  @Override
  public List<Coder<T>> getCoderArguments() {
    return ImmutableList.of(valueCoder);
  }

  /**
   * {@code OptionalCoder} is deterministic if the nested {@code Coder} is.
   *
   * <p>{@inheritDoc}
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    verifyDeterministic(this, "Value coder must be deterministic", valueCoder);
  }

  /**
   * {@code OptionalCoder} is consistent with equals if the nested {@code Coder} is.
   *
   * <p>{@inheritDoc}
   */
  @Override
  public boolean consistentWithEquals() {
    return valueCoder.consistentWithEquals();
  }

  @Override
  public Object structuralValue(Optional<T> value) {
    return value.map(valueCoder::structuralValue);
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
    observer.update(1);
    if (value.isPresent()) {
      valueCoder.registerByteSizeObserver(value.get(), observer);
    }
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
    if (!value.isPresent()) {
      return 1;
    }

    if (valueCoder instanceof StructuredCoder) {
      // If valueCoder is a StructuredCoder then we can ask it directly for the encoded size of
      // the value, adding 1 byte to count the null indicator.
      return 1 + valueCoder.getEncodedElementByteSize(value.get());
    }

    // If value is not a StructuredCoder then fall back to the default StructuredCoder behavior
    // of encoding and counting the bytes. The encoding will include the null indicator byte.
    return super.getEncodedElementByteSize(value);
  }

  /**
   * {@code OptionalCoder} is cheap if {@code valueCoder} is cheap.
   *
   * <p>{@inheritDoc}
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(Optional<T> value) {
    return value.map(valueCoder::isRegisterByteSizeObserverCheap).orElse(true);
  }

  @Override
  public TypeDescriptor<Optional<T>> getEncodedTypeDescriptor() {
    return new TypeDescriptor<Optional<T>>() {}.where(
        new TypeParameter<T>() {}, valueCoder.getEncodedTypeDescriptor());
  }
}
