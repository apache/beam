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
package org.apache.beam.runners.flink.translation.types;

import io.opentelemetry.context.Context;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import org.apache.beam.runners.flink.translation.wrappers.streaming.FlinkKeyUtils;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.CausedByDrain;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A {@link WindowedValue} of a {@link KV} that is kept in its encoded form until it is accessed.
 *
 * <p>Created by {@link LengthPrefixedCoderTypeSerializer} for records that Flink only moves around,
 * e.g. between the network, its sorter and spill files. While the value is untouched, the record is
 * serialized again by writing back its original bytes. The windowing information is only decoded
 * when it is accessed, and the grouping key is read from the encoded record without decoding the
 * value.
 *
 * <p>The first call to {@link #getValue()} decodes the value and drops the encoded record, as the
 * caller may mutate the returned value.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public final class LazyKvWindowedValue<K, V> implements WindowedValue<KV<K, V>> {

  /** Decodes the windowing information of a record, and none of its value. */
  private final WindowedValues.WindowedValueCoder<Void> headerCoder;

  private final KvCoder<K, V> kvCoder;

  /** The encoded record, or null once the value has been decoded. */
  private byte @Nullable [] encodedRecord;

  /** The windowing information and metadata, with a null value. Decoded on first access. */
  private @Nullable WindowedValue<Void> header;

  /**
   * Offset of the encoded {@link KV} in {@link #encodedRecord}, valid once the header is decoded.
   */
  private int valueOffset;

  private @Nullable KV<K, V> value;

  LazyKvWindowedValue(
      byte[] encodedRecord,
      WindowedValues.WindowedValueCoder<Void> headerCoder,
      KvCoder<K, V> kvCoder) {
    this.encodedRecord = encodedRecord;
    this.headerCoder = headerCoder;
    this.kvCoder = kvCoder;
  }

  /** The encoded record if the value was never decoded, null otherwise. */
  byte @Nullable [] encodedRecord() {
    return encodedRecord;
  }

  private WindowedValue<Void> header() {
    WindowedValue<Void> h = header;
    if (h == null) {
      byte[] bytes = encodedRecord;
      // Not java.io.ByteArrayInputStream: its synchronized reads take a lock per byte for coders
      // that decode byte by byte, such as AvroCoder.
      ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(bytes);
      try {
        // The value coder is a VoidCoder, so this stops right before the encoded KV.
        h = headerCoder.decode(in);
      } catch (IOException e) {
        throw new RuntimeException("Failed to decode windowing information", e);
      }
      valueOffset = bytes.length - in.available();
      header = h;
    }
    return h;
  }

  /** The key, encoded as by {@link FlinkKeyUtils#encodeKey}. */
  public byte[] getEncodedKey() {
    if (encodedRecord == null) {
      return FlinkKeyUtils.encodeKey(value.getKey(), kvCoder.getKeyCoder()).array();
    }
    header();
    byte[] bytes = encodedRecord;
    // KvCoder encodes the key first, the same way as FlinkKeyUtils.encodeKey.
    ByteArrayInputStreamWithPos in =
        new ByteArrayInputStreamWithPos(bytes, valueOffset, bytes.length);
    try {
      kvCoder.getKeyCoder().decode(in);
    } catch (IOException e) {
      throw new RuntimeException("Failed to decode key", e);
    }
    return Arrays.copyOfRange(bytes, valueOffset, bytes.length - in.available());
  }

  @Override
  public KV<K, V> getValue() {
    if (encodedRecord != null) {
      header();
      byte[] bytes = encodedRecord;
      try {
        // The value was encoded in the nested context, as part of the WindowedValue.
        value =
            kvCoder.decode(
                new ByteArrayInputStreamWithPos(bytes, valueOffset, bytes.length - valueOffset));
      } catch (IOException e) {
        throw new RuntimeException("Failed to decode value", e);
      }
      encodedRecord = null;
    }
    return value;
  }

  @Override
  public Instant getTimestamp() {
    return header().getTimestamp();
  }

  @Override
  public Collection<? extends BoundedWindow> getWindows() {
    return header().getWindows();
  }

  @Override
  public PaneInfo getPaneInfo() {
    return header().getPaneInfo();
  }

  @Override
  public @Nullable String getRecordId() {
    return header().getRecordId();
  }

  @Override
  public @Nullable Context getOpenTelemetryContext() {
    return header().getOpenTelemetryContext();
  }

  @Override
  public @Nullable Long getRecordOffset() {
    return header().getRecordOffset();
  }

  @Override
  public CausedByDrain causedByDrain() {
    return header().causedByDrain();
  }

  @Override
  public ValueKind getValueKind() {
    return header().getValueKind();
  }

  @Override
  public Iterable<? extends WindowedValue<KV<K, V>>> explodeWindows() {
    return decoded().explodeWindows();
  }

  @Override
  public <OtherT> WindowedValue<OtherT> withValue(OtherT newValue) {
    return header().withValue(newValue);
  }

  /** This value as a regular, fully decoded {@link WindowedValue}. */
  private WindowedValue<KV<K, V>> decoded() {
    return header().withValue(getValue());
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o instanceof LazyKvWindowedValue) {
      return decoded().equals(((LazyKvWindowedValue<?, ?>) o).decoded());
    }
    return o instanceof WindowedValue && decoded().equals(o);
  }

  @Override
  public int hashCode() {
    return decoded().hashCode();
  }

  @Override
  public String toString() {
    return decoded().toString();
  }
}
