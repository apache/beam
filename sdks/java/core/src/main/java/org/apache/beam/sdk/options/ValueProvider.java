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
package org.apache.beam.sdk.options;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

/**
 * {@link ValueProvider} is an interface which abstracts the notion of
 * fetching a value that may or may not be currently available.  This can be
 * used to parameterize transforms that only read values in at runtime, for
 * example.
 */
@JsonSerialize(using = ValueProvider.Serializer.class)
@JsonDeserialize(using = ValueProvider.Deserializer.class)
public interface ValueProvider<T> {
  /**
   * Return the value wrapped by this {@link ValueProvider}.
   */
  T get();

  /**
   * Whether the contents of this {@link ValueProvider} is available to
   * routines that run at graph construction time.
   */
  boolean isAccessible();

  /**
   * {@link StaticValueProvider} is an implementation of {@link ValueProvider} that
   * allows for a static value to be provided.
   */
  public static class StaticValueProvider<T> implements ValueProvider<T>, Serializable {
    @Nullable
    private final T value;

    StaticValueProvider(@Nullable T value) {
      this.value = value;
    }

    /**
     * Creates a {@link StaticValueProvider} that wraps the provided value.
     */
    public static <T> StaticValueProvider<T> of(T value) {
      StaticValueProvider<T> factory = new StaticValueProvider<>(value);
      return factory;
    }

    @Override
    public T get() {
      return value;
    }

    @Override
    public boolean isAccessible() {
      return true;
    }
  }

  /**
   * {@link RuntimeValueProvider} is an implementation of {@link ValueProvider} that
   * allows for a value to be provided at execution time rather than at graph
   * construction time.
   *
   * <p>To enforce this contract, if there is no default, users must only call
   * {@link #get()} at execution time (after a call to {@link Pipeline#run}),
   * which will provide the value of {@code optionsMap}.
   */
  public static class RuntimeValueProvider<T> implements ValueProvider<T>, Serializable {
    private static ConcurrentHashMap<Long, PipelineOptions> optionsMap =
      new ConcurrentHashMap<>();

    private final Class<? extends PipelineOptions> klass;
    private final String methodName;
    @Nullable
    private final T defaultValue;
    private final Long optionsId;

    /**
     * Creates a {@link RuntimeValueProvider} that will query the provided
     * {@code optionsId} for a value.
     */
    RuntimeValueProvider(String methodName, Class<? extends PipelineOptions> klass,
                         Long optionsId) {
      this.methodName = methodName;
      this.klass = klass;
      this.defaultValue = null;
      this.optionsId = optionsId;
    }

    /**
     * Creates a {@link RuntimeValueProvider} that will query the provided
     * {@code optionsId} for a value, or use the default if no value is available.
     */
    RuntimeValueProvider(String methodName, Class<? extends PipelineOptions> klass,
      T defaultValue, Long optionsId) {
      this.methodName = methodName;
      this.klass = klass;
      this.defaultValue = defaultValue;
      this.optionsId = optionsId;
    }

    /**
     * Once set, all {@code RuntimeValueProviders} will return {@code true}
     * from {@code isAccessible()}. By default, the value is set when
     * deserializing {@link PipelineOptions}.
     */
    static void setRuntimeOptions(PipelineOptions runtimeOptions) {
      optionsMap.put(runtimeOptions.getOptionsId(), runtimeOptions);
    }

    @Override
    public T get() {
      PipelineOptions options = optionsMap.get(optionsId);
      if (options == null) {
        throw new RuntimeException("Not called from a runtime context.");
      }
      try {
        Method method = klass.getMethod(methodName);
        PipelineOptions methodOptions = options.as(klass);
        InvocationHandler handler = Proxy.getInvocationHandler(methodOptions);
        T value = ((ValueProvider<T>) handler.invoke(methodOptions, method, null)).get();
        return firstNonNull(value, defaultValue);
      } catch (Throwable e) {
        throw new RuntimeException("Unable to load runtime value.", e);
      }
    }

    @Override
    public boolean isAccessible() {
      PipelineOptions options = optionsMap.get(optionsId);
      return options != null;
    }
  }

  /**
   * Serializer for {@link ValueProvider}.
   */
  static class Serializer extends JsonSerializer<ValueProvider<?>> {
    @Override
    public void serialize(ValueProvider<?> value, JsonGenerator jgen,
                          SerializerProvider provider) throws IOException {
      if (value.isAccessible()) {
        jgen.writeObject(value.get());
      } else {
        jgen.writeNull();
      }
    }
  }

  /**
   * Deserializer for {@link ValueProvider}, which handles type marshalling.
   */
  static class Deserializer extends JsonDeserializer<ValueProvider<?>>
    implements ContextualDeserializer {

    private final JavaType innerType;

    // A 0-arg constructor is required by the compiler.
    Deserializer() {
      this.innerType = null;
    }

    Deserializer(JavaType innerType) {
      this.innerType = innerType;
    }

    @Override
    public JsonDeserializer<?> createContextual(DeserializationContext ctxt,
                                                BeanProperty property)
        throws JsonMappingException {
      checkNotNull(ctxt, "Null DeserializationContext.");
      JavaType type = checkNotNull(ctxt.getContextualType(), "Invalid type: %s", getClass());
      JavaType[] params = type.findTypeParameters(ValueProvider.class);
      if (params.length != 1) {
        throw new RuntimeException(
          "Unable to derive type for ValueProvider: " + type.toString());
      }
      JavaType param = params[0];
      return new Deserializer(param);
    }

    @Override
    public ValueProvider<?> deserialize(JsonParser jp, DeserializationContext ctxt)
        throws IOException, JsonProcessingException {
      JsonDeserializer dser = ctxt.findRootValueDeserializer(
        checkNotNull(innerType, "Invalid %s: innerType is null. Serialization error?", getClass()));
      Object o = dser.deserialize(jp, ctxt);
      return StaticValueProvider.of(o);
    }
  }
}
