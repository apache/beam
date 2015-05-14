/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.dataflow.spark;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import com.google.cloud.dataflow.sdk.coders.Coder;
import org.apache.spark.api.java.function.Function;

/**
 * Serialization utility class.
 */
public final class CoderHelpers {
  private CoderHelpers() {
  }

  /**
   * Utility method for serializing an object using the specified coder.
   *
   * @param value Value to serialize.
   * @param coder Coder to serialize with.
   * @param <T> type of value that is serialized
   * @return Byte array representing serialized object.
   */
  static <T> byte[] toByteArray(T value, Coder<T> coder) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      coder.encode(value, baos, new Coder.Context(true));
    } catch (IOException e) {
      throw new IllegalStateException("Error encoding value: " + value, e);
    }
    return baos.toByteArray();
  }

  /**
   * Utility method for serializing a Iterable of values using the specified coder.
   *
   * @param values Values to serialize.
   * @param coder  Coder to serialize with.
   * @param <T> type of value that is serialized
   * @return List of bytes representing serialized objects.
   */
  static <T> List<byte[]> toByteArrays(Iterable<T> values, Coder<T> coder) {
    List<byte[]> res = new LinkedList<>();
    for (T value : values) {
      res.add(toByteArray(value, coder));
    }
    return res;
  }

  /**
   * Utility method for deserializing a byte array using the specified coder.
   *
   * @param serialized bytearray to be deserialized.
   * @param coder      Coder to deserialize with.
   * @param <T>        Type of object to be returned.
   * @return Deserialized object.
   */
  static <T> T fromByteArray(byte[] serialized, Coder<T> coder) {
    ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
    try {
      return coder.decode(bais, new Coder.Context(true));
    } catch (IOException e) {
      throw new IllegalStateException("Error decoding bytes for coder: " + coder, e);
    }
  }

  /**
   * A function wrapper for converting an object to a bytearray.
   *
   * @param coder Coder to serialize with.
   * @param <T>   The type of the object being serialized.
   * @return A function that accepts an object and returns its coder-serialized form.
   */
  static <T> Function<T, byte[]> toByteFunction(final Coder<T> coder) {
    return new Function<T, byte[]>() {
      @Override
      public byte[] call(T t) throws Exception {
        return toByteArray(t, coder);
      }
    };
  }

  /**
   * A function wrapper for converting a byte array to an object.
   *
   * @param coder Coder to deserialize with.
   * @param <T>   The type of the object being deserialized.
   * @return A function that accepts a byte array and returns its corresponding object.
   */
  static <T> Function<byte[], T> fromByteFunction(final Coder<T> coder) {
    return new Function<byte[], T>() {
      @Override
      public T call(byte[] bytes) throws Exception {
        return fromByteArray(bytes, coder);
      }
    };
  }
}
