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
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link NullableCoder} encodes nullable values of type {@code T} using a nested {@code Coder<T>}
 * that does not tolerate {@code null} values. {@link NullableCoder} uses exactly 1 byte per entry
 * to indicate whether the value is {@code null}, then adds the encoding of the inner coder for
 * non-null values.
 *
 * @param <T> the type of the values being transcoded
 */
public class NullableCoder<T> extends StructuredCoder<T> {
  public static <T> NullableCoder<T> of(Coder<T> valueCoder) {
    if (valueCoder instanceof NullableCoder) {
      return (NullableCoder<T>) valueCoder;
    }
    return new NullableCoder<>(valueCoder);
  }

  /////////////////////////////////////////////////////////////////////////////

  private final Coder<T> valueCoder;
  private static final int ENCODE_NULL = 0;
  private static final int ENCODE_PRESENT = 1;

  private NullableCoder(Coder<T> valueCoder) {
    this.valueCoder = valueCoder;
  }

  /** Returns the inner {@link Coder} wrapped by this {@link NullableCoder} instance. */
  public Coder<T> getValueCoder() {
    return valueCoder;
  }

  @Override
  public void encode(@Nullable T value, OutputStream outStream) throws IOException, CoderException {
    encode(value, outStream, Context.NESTED);
  }

  @Override
  public void encode(@Nullable T value, OutputStream outStream, Context context)
      throws IOException, CoderException {
    if (value == null) {
      outStream.write(ENCODE_NULL);
    } else {
      outStream.write(ENCODE_PRESENT);
      valueCoder.encode(value, outStream, context);
    }
  }

  @Override
  public T decode(InputStream inStream) throws IOException, CoderException {
    return decode(inStream, Context.NESTED);
  }

  @Override
  public @Nullable T decode(InputStream inStream, Context context)
      throws IOException, CoderException {
    int b = inStream.read();
    if (b == ENCODE_NULL) {
      return null;
    } else if (b != ENCODE_PRESENT) {
      throw new CoderException(
          String.format(
              "NullableCoder expects either a byte valued %s (null) or %s (present), got %s",
              ENCODE_NULL, ENCODE_PRESENT, b));
    }
    return valueCoder.decode(inStream, context);
  }

  @Override
  public List<Coder<T>> getCoderArguments() {
    return ImmutableList.of(valueCoder);
  }

  /**
   * {@code NullableCoder} is deterministic if the nested {@code Coder} is.
   *
   * <p>{@inheritDoc}
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    verifyDeterministic(this, "Value coder must be deterministic", valueCoder);
  }

  /**
   * {@code NullableCoder} is consistent with equals if the nested {@code Coder} is.
   *
   * <p>{@inheritDoc}
   */
  @Override
  public boolean consistentWithEquals() {
    return valueCoder.consistentWithEquals();
  }

  @Override
  public Object structuralValue(@Nullable T value) {
    if (value == null) {
      return Optional.absent();
    }
    return Optional.of(valueCoder.structuralValue(value));
  }

  /**
   * Overridden to short-circuit the default {@code StructuredCoder} behavior of encoding and
   * counting the bytes. The size is known (1 byte) when {@code value} is {@code null}, otherwise
   * the size is 1 byte plus the size of nested {@code Coder}'s encoding of {@code value}.
   *
   * <p>{@inheritDoc}
   */
  @Override
  public void registerByteSizeObserver(@Nullable T value, ElementByteSizeObserver observer)
      throws Exception {
    observer.update(1);
    if (value != null) {
      valueCoder.registerByteSizeObserver(value, observer);
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
  protected long getEncodedElementByteSize(@Nullable T value) throws Exception {
    if (value == null) {
      return 1;
    }

    if (valueCoder instanceof StructuredCoder) {
      // If valueCoder is a StructuredCoder then we can ask it directly for the encoded size of
      // the value, adding 1 byte to count the null indicator.
      return 1 + ((StructuredCoder<T>) valueCoder).getEncodedElementByteSize(value);
    }

    // If value is not a StructuredCoder then fall back to the default StructuredCoder behavior
    // of encoding and counting the bytes. The encoding will include the null indicator byte.
    return super.getEncodedElementByteSize(value);
  }

  /**
   * {@code NullableCoder} is cheap if {@code valueCoder} is cheap.
   *
   * <p>{@inheritDoc}
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(@Nullable T value) {
    if (value == null) {
      return true;
    }
    return valueCoder.isRegisterByteSizeObserverCheap(value);
  }

  @Override
  public TypeDescriptor<T> getEncodedTypeDescriptor() {
    return valueCoder.getEncodedTypeDescriptor();
  }
}
