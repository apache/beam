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

import static org.apache.beam.sdk.util.Structs.addList;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.api.client.util.Base64;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.SoftReference;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.TypeVariable;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Utilities for working with Coders.
 */
public final class CoderUtils {
  private CoderUtils() {}  // Non-instantiable

  /** A mapping from well known coder types to their implementing classes. */
  private static final Map<String, Class<?>> WELL_KNOWN_CODER_TYPES =
      ImmutableMap.<String, Class<?>>builder()
      .put("kind:pair", KvCoder.class)
      .put("kind:stream", IterableCoder.class)
      .put("kind:global_window", GlobalWindow.Coder.class)
      .put("kind:interval_window", IntervalWindow.IntervalWindowCoder.class)
      .put("kind:length_prefix", LengthPrefixCoder.class)
      .put("kind:windowed_value", WindowedValue.FullWindowedValueCoder.class)
      .build();

  private static ThreadLocal<SoftReference<ExposedByteArrayOutputStream>>
      threadLocalOutputStream = new ThreadLocal<>();

  /**
   * If true, a call to {@code encodeToByteArray} is already on the call stack.
   */
  private static ThreadLocal<Boolean> threadLocalOutputStreamInUse = new ThreadLocal<Boolean>() {
    @Override
    protected Boolean initialValue() {
      return false;
    }
  };

  /**
   * Encodes the given value using the specified Coder, and returns
   * the encoded bytes.
   *
   * <p>This function is not reentrant; it should not be called from methods of the provided
   * {@link Coder}.
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
   * {@code IOException}, such as {@code ByteArrayOutputStream} or
   * {@link ExposedByteArrayOutputStream}.
   */
  private static <T> void encodeToSafeStream(
      Coder<T> coder, T value, OutputStream stream, Coder.Context context) throws CoderException {
    try {
      coder.encode(value, new UnownedOutputStream(stream), context);
    } catch (IOException exn) {
      Throwables.propagateIfPossible(exn, CoderException.class);
      throw new IllegalArgumentException(
          "Forbidden IOException when writing to OutputStream", exn);
    }
  }

  /**
   * Decodes the given bytes using the specified Coder, and returns
   * the resulting decoded value.
   */
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
   * Decodes a value from the given {@code stream}, which should be a stream that never throws
   * {@code IOException}, such as {@code ByteArrayInputStream} or
   * {@link ExposedByteArrayInputStream}.
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
   * <p>This function is not reentrant; it should not be called from methods of the provided
   * {@link Coder}.
   */
  public static <T> T clone(Coder<T> coder, T value) throws CoderException {
    return decodeFromByteArray(coder, encodeToByteArray(coder, value, Coder.Context.OUTER));
  }

  /**
   * Encodes the given value using the specified Coder, and returns the Base64 encoding of the
   * encoded bytes.
   *
   * @throws CoderException if there are errors during encoding.
   */
  public static <T> String encodeToBase64(Coder<T> coder, T value)
      throws CoderException {
    byte[] rawValue = encodeToByteArray(coder, value);
    return Base64.encodeBase64URLSafeString(rawValue);
  }

  /**
   * Parses a value from a base64-encoded String using the given coder.
   */
  public static <T> T decodeFromBase64(Coder<T> coder, String encodedValue) throws CoderException {
    return decodeFromSafeStream(
        coder, new ByteArrayInputStream(Base64.decodeBase64(encodedValue)), Coder.Context.OUTER);
  }

  /**
   * If {@code coderType} is a subclass of {@code Coder<T>} for a specific
   * type {@code T}, returns {@code T.class}.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static TypeDescriptor getCodedType(TypeDescriptor coderDescriptor) {
    ParameterizedType coderType =
        (ParameterizedType) coderDescriptor.getSupertype(Coder.class).getType();
    TypeDescriptor codedType = TypeDescriptor.of(coderType.getActualTypeArguments()[0]);
    return codedType;
  }

  public static CloudObject makeCloudEncoding(
      String type,
      CloudObject... componentSpecs) {
    CloudObject encoding = CloudObject.forClassName(type);
    if (componentSpecs.length > 0) {
      addList(encoding, PropertyNames.COMPONENT_ENCODINGS, componentSpecs);
    }
    return encoding;
  }

  /**
   * A {@link com.fasterxml.jackson.databind.Module} that adds the type
   * resolver needed for Coder definitions.
   */
  static final class Jackson2Module extends SimpleModule {
    /**
     * The Coder custom type resolver.
     *
     * <p>This resolver resolves coders. If the Coder ID is a particular
     * well-known identifier, it's replaced with the corresponding class.
     * All other Coder instances are resolved by class name, using the package
     * org.apache.beam.sdk.coders if there are no "."s in the ID.
     */
    private static final class Resolver extends TypeIdResolverBase {
      @SuppressWarnings("unused") // Used via @JsonTypeIdResolver annotation on Mixin
      public Resolver() {
        super(TypeFactory.defaultInstance().constructType(Coder.class),
            TypeFactory.defaultInstance());
      }

      @Override
      public JavaType typeFromId(DatabindContext context, String id) {
        Class<?> clazz = getClassForId(id);
        @SuppressWarnings("rawtypes")
        TypeVariable[] tvs = clazz.getTypeParameters();
        JavaType[] types = new JavaType[tvs.length];
        for (int lupe = 0; lupe < tvs.length; lupe++) {
          types[lupe] = TypeFactory.unknownType();
        }
        return _typeFactory.constructSimpleType(clazz, types);
      }

      private Class<?> getClassForId(String id) {
        try {
          if (id.contains(".")) {
            return Class.forName(id);
          }

          if (WELL_KNOWN_CODER_TYPES.containsKey(id)) {
            return WELL_KNOWN_CODER_TYPES.get(id);
          }

          // Otherwise, see if the ID is the name of a class in
          // org.apache.beam.sdk.coders.  We do this via creating
          // the class object so that class loaders have a chance to get
          // involved -- and since we need the class object anyway.
          return Class.forName(Coder.class.getPackage().getName() + "." + id);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException("Unable to convert coder ID " + id + " to class", e);
        }
      }

      @Override
      public String idFromValueAndType(Object o, Class<?> clazz) {
        return clazz.getName();
      }

      @Override
      public String idFromValue(Object o) {
        return o.getClass().getName();
      }

      @Override
      public JsonTypeInfo.Id getMechanism() {
        return JsonTypeInfo.Id.CUSTOM;
      }
    }

    /**
     * The mixin class defining how Coders are handled by the deserialization
     * {@link ObjectMapper}.
     *
     * <p>This is done via a mixin so that this resolver is <i>only</i> used
     * during deserialization requested by the Apache Beam SDK.
     */
    @JsonTypeIdResolver(Resolver.class)
    @JsonTypeInfo(use = Id.CUSTOM, include = As.PROPERTY, property = PropertyNames.OBJECT_TYPE_NAME)
    private static final class Mixin {}

    public Jackson2Module() {
      super("BeamCoders");
      setMixInAnnotation(Coder.class, Mixin.class);
    }
  }
}
