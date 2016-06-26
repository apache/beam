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

import static org.apache.beam.sdk.util.CoderUtils.decodeFromByteArray;
import static org.apache.beam.sdk.util.CoderUtils.encodeToByteArray;

import static com.google.common.base.Preconditions.checkState;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;

import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Utilities for working with Serializables.
 */
public class SerializableUtils {
  /**
   * Serializes the argument into an array of bytes, and returns it.
   *
   * @throws IllegalArgumentException if there are errors when serializing
   */
  public static byte[] serializeToByteArray(Serializable value) {
    try {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      try (ObjectOutputStream oos = new ObjectOutputStream(new SnappyOutputStream(buffer))) {
        oos.writeObject(value);
      }
      return buffer.toByteArray();
    } catch (IOException exn) {
      throw new IllegalArgumentException(
          "unable to serialize " + value,
          exn);
    }
  }

  /**
   * Deserializes an object from the given array of bytes, e.g., as
   * serialized using {@link #serializeToByteArray}, and returns it.
   *
   * @throws IllegalArgumentException if there are errors when
   * deserializing, using the provided description to identify what
   * was being deserialized
   */
  public static Object deserializeFromByteArray(byte[] encodedValue,
      String description) {
    try {
      try (ObjectInputStream ois = new ObjectInputStream(
          new SnappyInputStream(new ByteArrayInputStream(encodedValue)))) {
        return ois.readObject();
      }
    } catch (IOException | ClassNotFoundException exn) {
      throw new IllegalArgumentException(
          "unable to deserialize " + description,
          exn);
    }
  }

  public static <T extends Serializable> T ensureSerializable(T value) {
    @SuppressWarnings("unchecked")
    T copy = (T) deserializeFromByteArray(serializeToByteArray(value),
        value.toString());
    return copy;
  }

  public static <T extends Serializable> T clone(T value) {
    @SuppressWarnings("unchecked")
    T copy = (T) deserializeFromByteArray(serializeToByteArray(value),
        value.toString());
    return copy;
  }

  /**
   * Serializes a Coder and verifies that it can be correctly deserialized.
   *
   * <p>Throws a RuntimeException if serialized Coder cannot be deserialized, or
   * if the deserialized instance is not equal to the original.
   *
   * @return the serialized Coder, as a {@link CloudObject}
   */
  public static CloudObject ensureSerializable(Coder<?> coder) {
    // Make sure that Coders are java serializable as well since
    // they are regularly captured within DoFn's.
    Coder<?> copy = (Coder<?>) ensureSerializable((Serializable) coder);

    CloudObject cloudObject = copy.asCloudObject();

    Coder<?> decoded;
    try {
      decoded = Serializer.deserialize(cloudObject, Coder.class);
    } catch (RuntimeException e) {
      throw new RuntimeException(
          String.format("Unable to deserialize Coder: %s. "
              + "Check that a suitable constructor is defined.  "
              + "See Coder for details.", coder), e
      );
    }
    checkState(coder.equals(decoded),
        "Coder not equal to original after serialization, indicating that the Coder may not "
        + "implement serialization correctly.  Before: %s, after: %s, cloud encoding: %s",
        coder, decoded, cloudObject);

    return cloudObject;
  }

  /**
   * Serializes an arbitrary T with the given {@code Coder<T>} and verifies
   * that it can be correctly deserialized.
   */
  public static <T> T ensureSerializableByCoder(
      Coder<T> coder, T value, String errorContext) {
      byte[] encodedValue;
      try {
        encodedValue = encodeToByteArray(coder, value);
      } catch (CoderException exn) {
        // TODO: Put in better element printing:
        // truncate if too long.
        throw new IllegalArgumentException(
            errorContext + ": unable to encode value "
            + value + " using " + coder,
            exn);
      }
      try {
        return decodeFromByteArray(coder, encodedValue);
      } catch (CoderException exn) {
        // TODO: Put in better encoded byte array printing:
        // use printable chars with escapes instead of codes, and
        // truncate if too long.
        throw new IllegalArgumentException(
            errorContext + ": unable to decode " + Arrays.toString(encodedValue)
            + ", encoding of value " + value + ", using " + coder,
            exn);
      }
  }
}
