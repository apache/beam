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
package org.apache.beam.runners.spark.structuredstreaming.translation.streaming;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.beam.runners.spark.structuredstreaming.translation.streaming.state.BeamStatefulProcessor;
import org.apache.beam.runners.spark.structuredstreaming.translation.streaming.state.BeamStatefulProcessorConfig;
import org.apache.beam.sdk.util.VarInt;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.TimeMode;

/**
 * The translator facing entry point of the Spark 4 stateful streaming bridge: it groups a keyed
 * dataset of raw Beam bytes by key and runs a {@link BeamStatefulProcessor} over it with Spark's
 * {@code transformWithState}.
 *
 * <p>Everything on the wire is {@code byte[]} and every Spark encoder involved is {@code
 * Encoders.BINARY()}. That is deliberate: it keeps Catalyst encoders, and therefore Catalyst
 * schemas for Beam types, entirely out of the stateful path.
 *
 * <h2>Input row encoding</h2>
 *
 * <p>Each element of the input {@code Dataset<byte[]>} is one Beam element, encoded as
 *
 * <pre>{@code
 * varint32(keyBytes.length) || keyBytes || windowedValueBytes
 * }</pre>
 *
 * where
 *
 * <ul>
 *   <li>{@code keyBytes} is the Beam key {@code K} encoded with {@code config.keyCoder()}. It is
 *       the Spark grouping key, so it must be a deterministic encoding, two elements of the same
 *       Beam key must produce byte-identical {@code keyBytes}.
 *   <li>{@code windowedValueBytes} is the {@code WindowedValue<V>} of the <b>value side only</b>,
 *       encoded with {@code config.inputValueCoder()}, that is {@code
 *       WindowedValues.getFullCoder(config.valueCoder(), config.windowCoder())}. The key is not
 *       repeated inside the payload, the operator re-attaches it.
 * </ul>
 *
 * <p>Use {@link #encodeInputRow(byte[], byte[])} to build such a row. There is no length prefix on
 * the payload, it simply runs to the end of the array.
 *
 * <h2>Output row encoding</h2>
 *
 * <p>Each element of the returned {@code Dataset<byte[]>} is one tagged Beam output, encoded as
 *
 * <pre>{@code
 * varint32(outputTagIndex) || windowedValueBytes
 * }</pre>
 *
 * where {@code outputTagIndex} is the index of the emitting {@link
 * org.apache.beam.sdk.values.TupleTag} in {@code config.outputTags()}, that is {@code 0} for the
 * main output tag and {@code 1..n} for {@code config.additionalOutputTags()} in their configured
 * order, and {@code windowedValueBytes} is the emitted {@code WindowedValue} encoded with {@code
 * config.outputCoderFor(tag)}. Use {@link #outputTagIndex(byte[])} and {@link
 * #outputPayload(byte[])} to take such a row apart, typically with one {@code filter} plus one
 * {@code map} per output tag.
 *
 * <p>For {@link BeamStatefulProcessorConfig.Mode#GROUP_ALSO_BY_WINDOW} there is only the main
 * output and its element type is {@code KV<K, Iterable<V>>}, so its configured output coder must be
 * {@code KvCoder.of(keyCoder, IterableCoder.of(valueCoder))}.
 *
 * <h2>Semantics</h2>
 *
 * <p>The operator always runs with {@code TimeMode.EventTime()} and {@code OutputMode.Append()}.
 * The input dataset must already carry an event time watermark declared upstream with {@code
 * withWatermark}, Spark forbids re-declaring it here.
 */
public final class TwsTransformFactory {

  private TwsTransformFactory() {}

  /**
   * Groups {@code keyedInput} by the Beam key embedded in every row and runs the configured Beam
   * transform over it inside Spark's {@code transformWithState}.
   *
   * @param keyedInput rows in the input encoding documented on this class
   * @param config what to run and how to decode the rows
   * @return rows in the output encoding documented on this class
   */
  public static Dataset<byte[]> transform(
      Dataset<byte[]> keyedInput, BeamStatefulProcessorConfig config) {
    return keyedInput
        .groupByKey((MapFunction<byte[], byte[]>) TwsTransformFactory::inputKey, Encoders.BINARY())
        .transformWithState(
            new BeamStatefulProcessor(config),
            TimeMode.EventTime(),
            OutputMode.Append(),
            Encoders.BINARY());
  }

  /** Builds an input row from the encoded Beam key and the encoded {@code WindowedValue}. */
  public static byte[] encodeInputRow(byte[] keyBytes, byte[] windowedValueBytes) {
    ByteArrayOutputStream out =
        new ByteArrayOutputStream(
            VarInt.getLength(keyBytes.length) + keyBytes.length + windowedValueBytes.length);
    try {
      VarInt.encode(keyBytes.length, out);
      out.write(keyBytes);
      out.write(windowedValueBytes);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to encode a transformWithState input row", e);
    }
    return out.toByteArray();
  }

  /** Extracts the encoded Beam key, the Spark grouping key, from an input row. */
  public static byte[] inputKey(byte[] row) {
    ByteArrayInputStream in = new ByteArrayInputStream(row);
    int keyLength = readVarInt(in);
    return readExactly(in, keyLength);
  }

  /** Extracts the encoded {@code WindowedValue} payload from an input row. */
  public static byte[] inputPayload(byte[] row) {
    ByteArrayInputStream in = new ByteArrayInputStream(row);
    int keyLength = readVarInt(in);
    skip(in, keyLength);
    return readExactly(in, in.available());
  }

  /** Builds an output row from the output tag index and the encoded {@code WindowedValue}. */
  public static byte[] encodeOutputRow(int outputTagIndex, byte[] windowedValueBytes) {
    ByteArrayOutputStream out =
        new ByteArrayOutputStream(VarInt.getLength(outputTagIndex) + windowedValueBytes.length);
    try {
      VarInt.encode(outputTagIndex, out);
      out.write(windowedValueBytes);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to encode a transformWithState output row", e);
    }
    return out.toByteArray();
  }

  /** Extracts the output tag index from an output row. */
  public static int outputTagIndex(byte[] row) {
    return readVarInt(new ByteArrayInputStream(row));
  }

  /** Extracts the encoded {@code WindowedValue} payload from an output row. */
  public static byte[] outputPayload(byte[] row) {
    ByteArrayInputStream in = new ByteArrayInputStream(row);
    readVarInt(in);
    return readExactly(in, in.available());
  }

  private static int readVarInt(ByteArrayInputStream in) {
    try {
      return VarInt.decodeInt(in);
    } catch (IOException e) {
      throw new UncheckedIOException("Malformed transformWithState row, bad length prefix", e);
    }
  }

  private static void skip(ByteArrayInputStream in, int length) {
    long skipped = in.skip(length);
    if (skipped != length) {
      throw new IllegalArgumentException(
          "Malformed transformWithState row, expected " + length + " more bytes");
    }
  }

  private static byte[] readExactly(ByteArrayInputStream in, int length) {
    byte[] bytes = new byte[length];
    int read = in.read(bytes, 0, length);
    if (read != length && length > 0) {
      throw new IllegalArgumentException(
          "Malformed transformWithState row, expected "
              + length
              + " bytes but only "
              + Math.max(read, 0)
              + " were available");
    }
    return bytes;
  }
}
