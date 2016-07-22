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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StandardCoder;
import org.apache.beam.sdk.util.PropertyNames;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * An immutable pair of a value and a timestamp.
 *
 * <p>The timestamp of a value determines many properties, such as its assignment to
 * windows and whether the value is late (with respect to the watermark of a {@link PCollection}).
 *
 * @param <V> the type of the value
 */
public class TimestampedValue<V> {

  /**
   * Returns a new {@code TimestampedValue} with the given value and timestamp.
   */
  public static <V> TimestampedValue<V> of(V value, Instant timestamp) {
    return new TimestampedValue<>(value, timestamp);
  }

  public V getValue() {
    return value;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  @Override
  public boolean equals(Object other) {
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

  /**
   * A {@link Coder} for {@link TimestampedValue}.
   */
  public static class TimestampedValueCoder<T>
      extends StandardCoder<TimestampedValue<T>> {

    private final Coder<T> valueCoder;

    public static <T> TimestampedValueCoder<T> of(Coder<T> valueCoder) {
      return new TimestampedValueCoder<>(valueCoder);
    }

    @JsonCreator
    public static TimestampedValueCoder<?> of(
        @JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
        List<Object> components) {
      checkArgument(components.size() == 1,
                    "Expecting 1 component, got " + components.size());
      return of((Coder<?>) components.get(0));
    }

    @SuppressWarnings("unchecked")
    TimestampedValueCoder(Coder<T> valueCoder) {
      this.valueCoder = checkNotNull(valueCoder);
    }

    @Override
    public void encode(TimestampedValue<T> windowedElem,
                       OutputStream outStream,
                       Context context)
        throws IOException {
      valueCoder.encode(windowedElem.getValue(), outStream, context.nested());
      InstantCoder.of().encode(
          windowedElem.getTimestamp(), outStream, context);
    }

    @Override
    public TimestampedValue<T> decode(InputStream inStream, Context context)
        throws IOException {
      T value = valueCoder.decode(inStream, context.nested());
      Instant timestamp = InstantCoder.of().decode(inStream, context);
      return TimestampedValue.of(value, timestamp);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic(
          "TimestampedValueCoder requires a deterministic valueCoder",
          valueCoder);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Arrays.<Coder<?>>asList(valueCoder);
    }

    public static <T> List<Object> getInstanceComponents(TimestampedValue<T> exampleValue) {
      return Arrays.<Object>asList(exampleValue.getValue());
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  private final V value;
  private final Instant timestamp;

  protected TimestampedValue(V value, Instant timestamp) {
    this.value = value;
    this.timestamp = timestamp;
  }
}
