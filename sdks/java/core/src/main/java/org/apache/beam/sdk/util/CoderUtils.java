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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.SoftReference;
import java.lang.reflect.ParameterizedType;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.BaseEncoding;

/** Utilities for working with Coders. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public final class CoderUtils {
  private CoderUtils() {} // Non-instantiable

  private static ThreadLocal<SoftReference<ExposedByteArrayOutputStream>> threadLocalOutputStream =
      new ThreadLocal<>();

  /** If true, a call to {@code encodeToByteArray} is already on the call stack. */
  private static ThreadLocal<Boolean> threadLocalOutputStreamInUse =
      ThreadLocal.withInitial(() -> false);

  /**
   * Encodes the given value using the specified Coder, and returns the encoded bytes.
   *
   * <p>This function is not reentrant; it should not be called from methods of the provided {@link
   * Coder}.
   */
  public static <T> byte[] encodeToByteArray(Coder<T> coder, T value) throws CoderException {
    return encodeToByteArray(coder, value, Coder.Context.OUTER);
  }

  public static <T> byte[] encodeToByteArray(Coder<T> coder, T value, Coder.Context context)
      throws CoderException {
    if (threadLocalOutputStreamInUse.get()) {
      // encodeToByteArray() is called recursively and the thread local stream is in use,
      // allocating a new one.
      ByteArrayOutputStream stream = new ExposedByteArrayOutputStream();
      encodeToSafeStream(coder, value, stream, context);
      return stream.toByteArray();
    } else {
      threadLocalOutputStreamInUse.set(true);
      try {
        ByteArrayOutputStream stream = getThreadLocalOutputStream();
        encodeToSafeStream(coder, value, stream, context);
        return stream.toByteArray();
      } finally {
        threadLocalOutputStreamInUse.set(false);
      }
    }
  }

  /**
   * Encodes {@code value} to the given {@code stream}, which should be a stream that never throws
   * {@code IOException}, such as {@code ByteArrayOutputStream} or {@link
   * ExposedByteArrayOutputStream}.
   */
  private static <T> void encodeToSafeStream(
      Coder<T> coder, T value, OutputStream stream, Coder.Context context) throws CoderException {
    try {
      coder.encode(value, new UnownedOutputStream(stream), context);
    } catch (IOException exn) {
      Throwables.propagateIfPossible(exn, CoderException.class);
      throw new IllegalArgumentException("Forbidden IOException when writing to OutputStream", exn);
    }
  }

  /** Decodes the given bytes using the specified Coder, and returns the resulting decoded value. */
  public static <T> T decodeFromByteArray(Coder<T> coder, byte[] encodedValue)
      throws CoderException {
    return decodeFromByteArray(coder, encodedValue, Coder.Context.OUTER);
  }

  public static <T> T decodeFromByteArray(
      Coder<T> coder, byte[] encodedValue, Coder.Context context) throws CoderException {
    try (ExposedByteArrayInputStream stream = new ExposedByteArrayInputStream(encodedValue)) {
      T result = decodeFromSafeStream(coder, stream, context);
      if (stream.available() != 0) {
        throw new CoderException(
            stream.available() + " unexpected extra bytes after decoding " + result);
      }
      return result;
    }
  }

  /**
   * Decodes a value from the given ByteString, validating that no bytes are remaining once decoded.
   */
  public static <T> T decodeFromByteString(Coder<T> coder, ByteString encodedValue)
      throws IOException {
    return decodeFromByteString(coder, encodedValue, Coder.Context.OUTER);
  }

  /**
   * Decodes a value from the given ByteString using a given context, validating that no bytes are
   * remaining once decoded.
   */
  public static <T> T decodeFromByteString(
      Coder<T> coder, ByteString encodedValue, Coder.Context context) throws IOException {
    InputStream stream = encodedValue.newInput();
    T result = coder.decode(stream, context);
    if (stream.available() != 0) {
      throw new CoderException(
          stream.available() + " unexpected extra bytes after decoding " + result);
    }
    return result;
  }

  /**
   * Decodes a value from the given {@code stream}, which should be a stream that never throws
   * {@code IOException}, such as {@code ByteArrayInputStream} or {@link
   * ExposedByteArrayInputStream}.
   */
  private static <T> T decodeFromSafeStream(
      Coder<T> coder, InputStream stream, Coder.Context context) throws CoderException {
    try {
      return coder.decode(new UnownedInputStream(stream), context);
    } catch (IOException exn) {
      Throwables.propagateIfPossible(exn, CoderException.class);
      throw new IllegalArgumentException(
          "Forbidden IOException when reading from InputStream", exn);
    }
  }

  private static ByteArrayOutputStream getThreadLocalOutputStream() {
    SoftReference<ExposedByteArrayOutputStream> refStream = threadLocalOutputStream.get();
    ExposedByteArrayOutputStream stream = refStream == null ? null : refStream.get();
    if (stream == null) {
      stream = new ExposedByteArrayOutputStream();
      threadLocalOutputStream.set(new SoftReference<>(stream));
    }
    stream.reset();
    return stream;
  }

  /**
   * Clones the given value by encoding and then decoding it with the specified Coder.
   *
   * <p>This function is not reentrant; it should not be called from methods of the provided {@link
   * Coder}.
   */
  public static <T> T clone(Coder<T> coder, T value) throws CoderException {
    return decodeFromByteArray(coder, encodeToByteArray(coder, value));
  }

  /**
   * Encodes the given value using the specified Coder, and returns the Base64 encoding of the
   * encoded bytes.
   *
   * @throws CoderException if there are errors during encoding.
   */
  public static <T> String encodeToBase64(Coder<T> coder, T value) throws CoderException {
    byte[] rawValue = encodeToByteArray(coder, value);
    return BaseEncoding.base64Url().omitPadding().encode(rawValue);
  }

  /** Parses a value from a base64-encoded String using the given coder. */
  public static <T> T decodeFromBase64(Coder<T> coder, String encodedValue) throws CoderException {
    return decodeFromSafeStream(
        coder,
        new ByteArrayInputStream(BaseEncoding.base64Url().omitPadding().decode(encodedValue)),
        Coder.Context.OUTER);
  }

  /**
   * If {@code coderType} is a subclass of {@code Coder<T>} for a specific type {@code T}, returns
   * {@code T.class}.
   */
  @SuppressWarnings({
    "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
    "unchecked"
  })
  public static TypeDescriptor getCodedType(TypeDescriptor coderDescriptor) {
    ParameterizedType coderType =
        (ParameterizedType) coderDescriptor.getSupertype(Coder.class).getType();
    return TypeDescriptor.of(coderType.getActualTypeArguments()[0]);
  }
}
