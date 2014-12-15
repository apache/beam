/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import static com.google.cloud.dataflow.sdk.util.Structs.addList;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoderBase;
import com.google.cloud.dataflow.sdk.coders.MapCoder;
import com.google.cloud.dataflow.sdk.coders.MapCoderBase;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.type.TypeFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.TypeVariable;

/**
 * Utilities for working with Coders.
 */
public final class CoderUtils {
  private CoderUtils() {}  // Non-instantiable

  /**
   * Coder class-name alias for a key-value type.
   */
  public static final String KIND_PAIR = "kind:pair";

  /**
   * Coder class-name alias for a stream type.
   */
  public static final String KIND_STREAM = "kind:stream";

  /**
   * Encodes the given value using the specified Coder, and returns
   * the encoded bytes.
   *
   * @throws CoderException if there are errors during encoding
   */
  public static <T> byte[] encodeToByteArray(Coder<T> coder, T value)
      throws CoderException {
    try {
      try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
        coder.encode(value, os, Coder.Context.OUTER);
        return os.toByteArray();
      }
    } catch (IOException exn) {
      throw new RuntimeException("unexpected IOException", exn);
    }
  }

  /**
   * Decodes the given bytes using the specified Coder, and returns
   * the resulting decoded value.
   *
   * @throws CoderException if there are errors during decoding
   */
  public static <T> T decodeFromByteArray(Coder<T> coder, byte[] encodedValue)
      throws CoderException {
    try {
      try (ByteArrayInputStream is = new ByteArrayInputStream(encodedValue)) {
        T result = coder.decode(is, Coder.Context.OUTER);
        if (is.available() != 0) {
          throw new CoderException(
              is.available() + " unexpected extra bytes after decoding " +
              result);
        }
        return result;
      }
    } catch (IOException exn) {
      throw new RuntimeException("unexpected IOException", exn);
    }
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
   * A {@link com.fasterxml.jackson.databind.module.Module} which adds the type
   * resolver needed for Coder definitions created by the Dataflow service.
   */
  @SuppressWarnings("serial")
  static final class Jackson2Module extends SimpleModule {
    /**
     * The Coder custom type resolver.
     * <p>
     * This resolver resolves coders.  If the Coder ID is a particular
     * well-known identifier supplied by the Dataflow service, it's replaced
     * with the corresponding class.  All other Coder instances are resolved
     * by class name, using the package com.google.cloud.dataflow.sdk.coders
     * if there are no "."s in the ID.
     */
    private static final class Resolver extends TypeIdResolverBase {
      public Resolver() {
        super(TypeFactory.defaultInstance().constructType(Coder.class),
            TypeFactory.defaultInstance());
      }

      @Override
      public JavaType typeFromId(String id) {
        Class<?> clazz = getClassForId(id);
        if (clazz == KvCoder.class) {
          clazz = KvCoderBase.class;
        }
        if (clazz == MapCoder.class) {
          clazz = MapCoderBase.class;
        }
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

          if (id.equals(KIND_STREAM)) {
            return IterableCoder.class;
          } else if (id.equals(KIND_PAIR)) {
            return KvCoder.class;
          }

          // Otherwise, see if the ID is the name of a class in
          // com.google.cloud.dataflow.sdk.coders.  We do this via creating
          // the class object so that class loaders have a chance to get
          // involved -- and since we need the class object anyway.
          return Class.forName("com.google.cloud.dataflow.sdk.coders." + id);
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
     * <p>
     * This is done via a mixin so that this resolver is <i>only</i> used
     * during deserialization requested by the Dataflow SDK.
     */
    @JsonTypeIdResolver(Resolver.class)
    @JsonTypeInfo(use = Id.CUSTOM, include = As.PROPERTY, property = PropertyNames.OBJECT_TYPE_NAME)
    private static final class Mixin {}

    public Jackson2Module() {
      super("DataflowCoders");
      setMixInAnnotation(Coder.class, Mixin.class);
    }
  }
}
