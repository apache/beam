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
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.JsonMappingException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

/** Utilities for working with Serializables. */
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes"
})
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
      ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
      try {
        throw new IllegalArgumentException(
            "unable to serialize " + ow.writeValueAsString(value), exn);
      } catch (JsonProcessingException ex) {
        throw new IllegalArgumentException("unable to jsonify " + value, exn);
      }
    }
  }

  /**
   * Deserializes an object from the given array of bytes, e.g., as serialized using {@link
   * #serializeToByteArray}, and returns it.
   *
   * @throws IllegalArgumentException if there are errors when deserializing, using the provided
   *     description to identify what was being deserialized
   */
  public static Object deserializeFromByteArray(byte[] encodedValue, String description) {
    try {
      try (ObjectInputStream ois =
          new ContextualObjectInputStream(
              new SnappyInputStream(new ByteArrayInputStream(encodedValue)))) {
        return ois.readObject();
      }
    } catch (IOException | ClassNotFoundException exn) {
      throw new IllegalArgumentException("unable to deserialize " + description, exn);
    }
  }

  public static <T extends Serializable> T ensureSerializableRoundTrip(T value) {
    T copy = ensureSerializable(value);

    checkState(
        value.equals(copy),
        "Value not equal to original after serialization, indicating that its type may not "
            + "implement serialization or equals correctly.  Before: %s, after: %s",
        value,
        copy);

    return copy;
  }

  public static <T extends Serializable> T ensureSerializable(T value) {
    return clone(value);
  }

  public static <T extends Serializable> T clone(T value) {
    final Thread thread = Thread.currentThread();
    final ClassLoader tccl = thread.getContextClassLoader();
    ClassLoader loader = tccl;
    try {
      if (tccl.loadClass(value.getClass().getName()) != value.getClass()) {
        loader = value.getClass().getClassLoader();
      }
    } catch (final NoClassDefFoundError | ClassNotFoundException e) {
      loader = value.getClass().getClassLoader();
    }
    if (loader == null) {
      loader = tccl; // will likely fail but the best we can do
    }
    thread.setContextClassLoader(loader);
    @SuppressWarnings("unchecked")
    final T copy;
    try {
      copy = (T) deserializeFromByteArray(serializeToByteArray(value), value.toString());
    } finally {
      thread.setContextClassLoader(tccl);
    }
    return copy;
  }

  /**
   * Serializes a Coder and verifies that it can be correctly deserialized.
   *
   * <p>Throws a RuntimeException if serialized Coder cannot be deserialized, or if the deserialized
   * instance is not equal to the original.
   *
   * @return the deserialized Coder
   */
  public static Coder<?> ensureSerializable(Coder<?> coder) {
    // Make sure that Coders are java serializable as well since
    // they are regularly captured within DoFn's.
    Coder<?> copy = (Coder<?>) ensureSerializable((Serializable) coder);

    checkState(
        coder.equals(copy),
        "Coder not equal to original after serialization, indicating that the Coder may not "
            + "implement serialization correctly.  Before: %s, after: %s",
        coder,
        copy);

    return copy;
  }

  /**
   * Serializes an arbitrary T with the given {@code Coder<T>} and verifies that it can be correctly
   * deserialized.
   */
  public static <T> T ensureSerializableByCoder(Coder<T> coder, T value, String errorContext) {
    byte[] encodedValue;
    try {
      encodedValue = encodeToByteArray(coder, value);
    } catch (CoderException exn) {
      // TODO: Put in better element printing:
      // truncate if too long.
      throw new IllegalArgumentException(
          errorContext + ": unable to encode value " + value + " using " + coder, exn);
    }
    try {
      return decodeFromByteArray(coder, encodedValue);
    } catch (CoderException exn) {
      // TODO: Put in better encoded byte array printing:
      // use printable chars with escapes instead of codes, and
      // truncate if too long.
      throw new IllegalArgumentException(
          errorContext
              + ": unable to decode "
              + Arrays.toString(encodedValue)
              + ", encoding of value "
              + value
              + ", using "
              + coder,
          exn);
    }
  }

  private static final class ContextualObjectInputStream extends ObjectInputStream {
    private ContextualObjectInputStream(final InputStream in) throws IOException {
      super(in);
    }

    @Override
    protected Class<?> resolveClass(final ObjectStreamClass classDesc)
        throws IOException, ClassNotFoundException {
      // note: staying aligned on JVM default but can need class filtering here to avoid 0day issue
      final String n = classDesc.getName();
      final ClassLoader classloader = ReflectHelpers.findClassLoader();
      try {
        return Class.forName(n, false, classloader);
      } catch (final ClassNotFoundException e) {
        return super.resolveClass(classDesc);
      }
    }

    @Override
    protected Class resolveProxyClass(final String[] interfaces)
        throws IOException, ClassNotFoundException {
      final ClassLoader classloader = ReflectHelpers.findClassLoader();

      final Class[] cinterfaces = new Class[interfaces.length];
      for (int i = 0; i < interfaces.length; i++) {
        cinterfaces[i] = classloader.loadClass(interfaces[i]);
      }

      try {
        return Proxy.getProxyClass(classloader, cinterfaces);
      } catch (final IllegalArgumentException e) {
        throw new ClassNotFoundException(null, e);
      }
    }
  }
}
