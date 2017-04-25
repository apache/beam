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
package org.apache.beam.sdk.util;

import com.google.common.base.MoreObjects;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Immutable struct containing a value as well as a unique id identifying the value.
 *
 * @param <ValueT> the underlying value type
 */
public class ValueWithRecordId<ValueT> {
  private final ValueT value;
  private final byte[] id;

  public ValueWithRecordId(ValueT value, byte[] id) {
    this.value = value;
    this.id = id;
  }

  public ValueT getValue() {
    return value;
  }

  public byte[] getId() {
    return id;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", id)
        .add("value", value)
        .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof ValueWithRecordId)) {
      return false;
    }
    ValueWithRecordId<?> otherRecord = (ValueWithRecordId<?>) other;
    return Objects.deepEquals(id, otherRecord.id)
        && Objects.deepEquals(value, otherRecord.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(Arrays.hashCode(id), value);
  }

  /**
   * A {@link Coder} for {@code ValueWithRecordId}, using a wrapped value {@code Coder}.
   */
  public static class ValueWithRecordIdCoder<ValueT>
      extends CustomCoder<ValueWithRecordId<ValueT>> {
    public static <ValueT> ValueWithRecordIdCoder<ValueT> of(Coder<ValueT> valueCoder) {
      return new ValueWithRecordIdCoder<>(valueCoder);
    }

    protected ValueWithRecordIdCoder(Coder<ValueT> valueCoder) {
      this.valueCoder = valueCoder;
      this.idCoder = ByteArrayCoder.of();
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Arrays.asList(valueCoder);
    }

    @Override
    public void encode(ValueWithRecordId<ValueT> value, OutputStream outStream, Context context)
        throws IOException {
      valueCoder.encode(value.value, outStream, context.nested());
      idCoder.encode(value.id, outStream, context);
    }

    @Override
    public ValueWithRecordId<ValueT> decode(InputStream inStream, Context context)
        throws IOException {
      return new ValueWithRecordId<ValueT>(
          valueCoder.decode(inStream, context.nested()),
          idCoder.decode(inStream, context));
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      valueCoder.verifyDeterministic();
    }

    public Coder<ValueT> getValueCoder() {
      return valueCoder;
    }

    Coder<ValueT> valueCoder;
    ByteArrayCoder idCoder;
  }

  /** {@link DoFn} to turn a {@code ValueWithRecordId<T>} back to the value {@code T}. */
  public static class StripIdsDoFn<T> extends DoFn<ValueWithRecordId<T>, T> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().getValue());
    }
  }
}
