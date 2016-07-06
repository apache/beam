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

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.sdk.util.PropertyNames;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import javax.annotation.Nullable;

/**
 * A {@link NullableCoder} encodes nullable values of type {@code T} using a nested
 * {@code Coder<T>} that does not tolerate {@code null} values. {@link NullableCoder} uses
 * exactly 1 byte per entry to indicate whether the value is {@code null}, then adds the encoding
 * of the inner coder for non-null values.
 *
 * @param <T> the type of the values being transcoded
 */
public class NullableCoder<T> extends StandardCoder<T> {
  public static <T> NullableCoder<T> of(Coder<T> valueCoder) {
    if (valueCoder instanceof NullableCoder) {
      return (NullableCoder<T>) valueCoder;
    }
    return new NullableCoder<>(valueCoder);
  }

  @JsonCreator
  public static NullableCoder<?> of(
      @JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
      List<Coder<?>> components) {
    checkArgument(components.size() == 1, "Expecting 1 components, got %s", components.size());
    return of(components.get(0));
  }

  /////////////////////////////////////////////////////////////////////////////

  private final Coder<T> valueCoder;
  private static final int ENCODE_NULL = 0;
  private static final int ENCODE_PRESENT = 1;

  private NullableCoder(Coder<T> valueCoder) {
    this.valueCoder = valueCoder;
  }

  @Override
  public void encode(@Nullable T value, OutputStream outStream, Context context)
      throws IOException, CoderException  {
    if (value == null) {
      outStream.write(ENCODE_NULL);
    } else {
      outStream.write(ENCODE_PRESENT);
      valueCoder.encode(value, outStream, context.nested());
    }
  }

  @Override
  @Nullable
  public T decode(InputStream inStream, Context context) throws IOException, CoderException {
    int b = inStream.read();
    if (b == ENCODE_NULL) {
      return null;
    } else if (b != ENCODE_PRESENT) {
        throw new CoderException(String.format(
            "NullableCoder expects either a byte valued %s (null) or %s (present), got %s",
            ENCODE_NULL, ENCODE_PRESENT, b));
    }
    return valueCoder.decode(inStream, context.nested());
  }

  @Override
  public List<Coder<T>> getCoderArguments() {
    return ImmutableList.of(valueCoder);
  }

  /**
   * {@code NullableCoder} is deterministic if the nested {@code Coder} is.
   *
   * {@inheritDoc}
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    verifyDeterministic("Value coder must be deterministic", valueCoder);
  }

  /**
   * {@code NullableCoder} is consistent with equals if the nested {@code Coder} is.
   *
   * {@inheritDoc}
   */
  @Override
  public boolean consistentWithEquals() {
    return valueCoder.consistentWithEquals();
  }

  @Override
  public Object structuralValue(@Nullable T value) throws Exception {
    if (value == null) {
      return Optional.absent();
    }
    return Optional.of(valueCoder.structuralValue(value));
  }

  /**
   * Overridden to short-circuit the default {@code StandardCoder} behavior of encoding and
   * counting the bytes. The size is known (1 byte) when {@code value} is {@code null}, otherwise
   * the size is 1 byte plus the size of nested {@code Coder}'s encoding of {@code value}.
   *
   * {@inheritDoc}
   */
  @Override
  public void registerByteSizeObserver(
      @Nullable T value, ElementByteSizeObserver observer, Context context) throws Exception {
    observer.update(1);
    if (value != null) {
      valueCoder.registerByteSizeObserver(value, observer, context.nested());
    }
  }

  /**
   * Overridden to short-circuit the default {@code StandardCoder} behavior of encoding and
   * counting the bytes. The size is known (1 byte) when {@code value} is {@code null}, otherwise
   * the size is 1 byte plus the size of nested {@code Coder}'s encoding of {@code value}.
   *
   * {@inheritDoc}
   */
  @Override
  protected long getEncodedElementByteSize(@Nullable T value, Context context) throws Exception {
    if (value == null) {
      return 1;
    }

    if (valueCoder instanceof StandardCoder) {
      // If valueCoder is a StandardCoder then we can ask it directly for the encoded size of
      // the value, adding 1 byte to count the null indicator.
      return 1  + ((StandardCoder<T>) valueCoder)
          .getEncodedElementByteSize(value, context.nested());
    }

    // If value is not a StandardCoder then fall back to the default StandardCoder behavior
    // of encoding and counting the bytes. The encoding will include the null indicator byte.
    return super.getEncodedElementByteSize(value, context);
  }

  /**
   * {@code NullableCoder} is cheap if {@code valueCoder} is cheap.
   *
   * {@inheritDoc}
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(@Nullable T value, Context context) {
    return valueCoder.isRegisterByteSizeObserverCheap(value, context.nested());
  }
}
