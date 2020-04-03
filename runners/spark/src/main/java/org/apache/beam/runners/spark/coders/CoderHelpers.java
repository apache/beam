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
package org.apache.beam.runners.spark.coders;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import org.apache.beam.runners.spark.util.ByteArray;
import org.apache.beam.sdk.coders.Coder;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.joda.time.Instant;
import scala.Tuple2;

/** Serialization utility class. */
public final class CoderHelpers {
  private CoderHelpers() {}

  /**
   * Utility method for serializing an object using the specified coder.
   *
   * @param value Value to serialize.
   * @param coder Coder to serialize with.
   * @param <T> type of value that is serialized
   * @return Byte array representing serialized object.
   */
  public static <T> byte[] toByteArray(T value, Coder<T> coder) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      coder.encode(value, baos);
    } catch (IOException e) {
      throw new IllegalStateException("Error encoding value: " + value, e);
    }
    return baos.toByteArray();
  }

  /**
   * Utility method for serializing an object using the specified coder, appending timestamp
   * representation. This is useful when sorting by timestamp
   *
   * @param value Value to serialize.
   * @param coder Coder to serialize with.
   * @param timestamp timestamp to be bundled into key's ByteArray representation
   * @param <T> type of value that is serialized
   * @return Byte array representing serialized object.
   */
  public static <T> byte[] toByteArrayWithTs(T value, Coder<T> coder, Instant timestamp) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      coder.encode(value, baos);
      ByteBuffer buf = ByteBuffer.allocate(8);
      buf.asLongBuffer().put(timestamp.getMillis());
      baos.write(buf.array());
    } catch (IOException e) {
      throw new IllegalStateException("Error encoding value: " + value, e);
    }
    return baos.toByteArray();
  }

  /**
   * Utility method for serializing a Iterable of values using the specified coder.
   *
   * @param values Values to serialize.
   * @param coder Coder to serialize with.
   * @param <T> type of value that is serialized
   * @return List of bytes representing serialized objects.
   */
  public static <T> List<byte[]> toByteArrays(Iterable<T> values, Coder<T> coder) {
    List<byte[]> res = new ArrayList<>();
    for (T value : values) {
      res.add(toByteArray(value, coder));
    }
    return res;
  }

  /**
   * Utility method for deserializing a byte array using the specified coder.
   *
   * @param serialized bytearray to be deserialized.
   * @param coder Coder to deserialize with.
   * @param <T> Type of object to be returned.
   * @return Deserialized object.
   */
  public static <T> T fromByteArray(byte[] serialized, Coder<T> coder) {
    ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
    try {
      return coder.decode(bais);
    } catch (IOException e) {
      throw new IllegalStateException("Error decoding bytes for coder: " + coder, e);
    }
  }

  /**
   * Utility method for deserializing a Iterable of byte arrays using the specified coder.
   *
   * @param serialized bytearrays to be deserialized.
   * @param coder Coder to deserialize with.
   * @param <T> Type of object to be returned.
   * @return Iterable of deserialized objects.
   */
  public static <T> Iterable<T> fromByteArrays(
      Collection<byte[]> serialized, final Coder<T> coder) {
    return serialized.stream()
        .map(bytes -> fromByteArray(checkNotNull(bytes, "Cannot decode null values."), coder))
        .collect(Collectors.toList());
  }

  /**
   * A function wrapper for converting an object to a bytearray.
   *
   * @param coder Coder to serialize with.
   * @param <T> The type of the object being serialized.
   * @return A function that accepts an object and returns its coder-serialized form.
   */
  public static <T> Function<T, byte[]> toByteFunction(final Coder<T> coder) {
    return t -> toByteArray(t, coder);
  }

  /**
   * A function wrapper for converting a byte array to an object.
   *
   * @param coder Coder to deserialize with.
   * @param <T> The type of the object being deserialized.
   * @return A function that accepts a byte array and returns its corresponding object.
   */
  public static <T> Function<byte[], T> fromByteFunction(final Coder<T> coder) {
    return bytes -> fromByteArray(bytes, coder);
  }

  /**
   * A function wrapper for converting a key-value pair to a byte array pair.
   *
   * @param keyCoder Coder to serialize keys.
   * @param valueCoder Coder to serialize values.
   * @param <K> The type of the key being serialized.
   * @param <V> The type of the value being serialized.
   * @return A function that accepts a key-value pair and returns a pair of byte arrays.
   */
  public static <K, V> PairFunction<Tuple2<K, V>, ByteArray, byte[]> toByteFunction(
      final Coder<K> keyCoder, final Coder<V> valueCoder) {
    return kv ->
        new Tuple2<>(
            new ByteArray(toByteArray(kv._1(), keyCoder)), toByteArray(kv._2(), valueCoder));
  }

  /**
   * A function wrapper for converting a key-value pair to a byte array pair, where the key in
   * resulting ByteArray contains (key, timestamp).
   *
   * @param keyCoder Coder to serialize keys.
   * @param valueCoder Coder to serialize values.
   * @param timestamp timestamp of the input Tuple2
   * @param <K> The type of the key being serialized.
   * @param <V> The type of the value being serialized.
   * @return A function that accepts a key-value pair and returns a pair of byte arrays.
   */
  public static <K, V> PairFunction<Tuple2<K, V>, ByteArray, byte[]> toByteFunctionWithTs(
      final Coder<K> keyCoder,
      final Coder<V> valueCoder,
      Function<Tuple2<K, V>, Instant> timestamp) {

    return kv ->
        new Tuple2<>(
            new ByteArray(toByteArrayWithTs(kv._1(), keyCoder, timestamp.call(kv))),
            toByteArray(kv._2(), valueCoder));
  }

  /**
   * A function for converting a byte array pair to a key-value pair.
   *
   * @param <K> The type of the key being deserialized.
   * @param <V> The type of the value being deserialized.
   */
  public static class FromByteFunction<K, V>
      implements PairFunction<Tuple2<ByteArray, byte[]>, K, V>,
          org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function<
              Tuple2<ByteArray, byte[]>, Tuple2<K, V>> {
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;

    /**
     * @param keyCoder Coder to deserialize keys.
     * @param valueCoder Coder to deserialize values.
     */
    public FromByteFunction(final Coder<K> keyCoder, final Coder<V> valueCoder) {
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
    }

    @Override
    public Tuple2<K, V> call(Tuple2<ByteArray, byte[]> tuple) {
      return new Tuple2<>(
          fromByteArray(tuple._1().getValue(), keyCoder), fromByteArray(tuple._2(), valueCoder));
    }

    @SuppressFBWarnings(
        value = "NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION",
        justification = "https://github.com/google/guava/issues/920")
    @Override
    public Tuple2<K, V> apply(@Nonnull Tuple2<ByteArray, byte[]> tuple) {
      return call(tuple);
    }
  }

  /**
   * A function wrapper for converting a byte array pair to a key-value pair, where values are
   * {@link Iterable}.
   *
   * @param keyCoder Coder to deserialize keys.
   * @param valueCoder Coder to deserialize values.
   * @param <K> The type of the key being deserialized.
   * @param <V> The type of the value being deserialized.
   * @return A function that accepts a pair of byte arrays and returns a key-value pair.
   */
  public static <K, V>
      PairFunction<Tuple2<ByteArray, Iterable<byte[]>>, K, Iterable<V>> fromByteFunctionIterable(
          final Coder<K> keyCoder, final Coder<V> valueCoder) {
    return tuple ->
        new Tuple2<>(
            fromByteArray(tuple._1().getValue(), keyCoder),
            StreamSupport.stream(tuple._2().spliterator(), false)
                .map(bytes -> fromByteArray(bytes, valueCoder))
                .collect(Collectors.toList()));
  }
}
