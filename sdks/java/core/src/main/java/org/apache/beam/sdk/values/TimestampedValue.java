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
package org.apache.beam.sdk.values;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * An immutable pair of a value and a timestamp.
 *
 * <p>The timestamp of a value determines many properties, such as its assignment to windows and
 * whether the value is late (with respect to the watermark of a {@link PCollection}).
 *
 * @param <V> the type of the value
 */
public class TimestampedValue<V> {
  /**
   * Returns a new {@link TimestampedValue} with the {@link BoundedWindow#TIMESTAMP_MIN_VALUE
   * minimum timestamp}.
   */
  public static <V> TimestampedValue<V> atMinimumTimestamp(@Nullable V value) {
    return of(value, BoundedWindow.TIMESTAMP_MIN_VALUE);
  }

  /** Returns a new {@code TimestampedValue} with the given value and timestamp. */
  public static <V> TimestampedValue<V> of(@Nullable V value, Instant timestamp) {
    return new TimestampedValue<>(value, timestamp);
  }

  public V getValue() {
    return value;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  @Override
  public boolean equals(@Nullable Object other) {
    if (!(other instanceof TimestampedValue)) {
      return false;
    }
    TimestampedValue<?> that = (TimestampedValue<?>) other;
    return Objects.equals(value, that.value) && Objects.equals(timestamp, that.timestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, timestamp);
  }

  @Override
  public String toString() {
    return "TimestampedValue(" + value + ", " + timestamp + ")";
  }

  /////////////////////////////////////////////////////////////////////////////

  /** A {@link Coder} for {@link TimestampedValue}. */
  public static class TimestampedValueCoder<T> extends StructuredCoder<TimestampedValue<T>> {

    private final Coder<T> valueCoder;

    public static <T> TimestampedValueCoder<T> of(Coder<T> valueCoder) {
      return new TimestampedValueCoder<>(valueCoder);
    }

    @Override
    public Object structuralValue(TimestampedValue<T> value) {
      Object structuralValue = valueCoder.structuralValue(value.getValue());
      return TimestampedValue.of(structuralValue, value.getTimestamp());
    }

    @SuppressWarnings("unchecked")
    TimestampedValueCoder(Coder<T> valueCoder) {
      this.valueCoder = checkNotNull(valueCoder);
    }

    @Override
    public void encode(TimestampedValue<T> windowedElem, OutputStream outStream)
        throws IOException {
      valueCoder.encode(windowedElem.getValue(), outStream);
      InstantCoder.of().encode(windowedElem.getTimestamp(), outStream);
    }

    @Override
    public TimestampedValue<T> decode(InputStream inStream) throws IOException {
      T value = valueCoder.decode(inStream);
      Instant timestamp = InstantCoder.of().decode(inStream);
      return TimestampedValue.of(value, timestamp);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic(
          this, "TimestampedValueCoder requires a deterministic valueCoder", valueCoder);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Arrays.<Coder<?>>asList(valueCoder);
    }

    public Coder<T> getValueCoder() {
      return valueCoder;
    }

    @Override
    public TypeDescriptor<TimestampedValue<T>> getEncodedTypeDescriptor() {
      return new TypeDescriptor<TimestampedValue<T>>() {}.where(
          new TypeParameter<T>() {}, valueCoder.getEncodedTypeDescriptor());
    }

    @Override
    public List<? extends Coder<?>> getComponents() {
      return Collections.singletonList(valueCoder);
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  private final @Nullable V value;
  private final Instant timestamp;

  protected TimestampedValue(@Nullable V value, Instant timestamp) {
    checkNotNull(timestamp, "timestamp must be non-null");

    this.value = value;
    this.timestamp = timestamp;
  }
}
