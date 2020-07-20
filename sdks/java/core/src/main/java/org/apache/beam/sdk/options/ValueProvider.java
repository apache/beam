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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link ValueProvider} abstracts the notion of fetching a value that may or may not be currently
 * available.
 *
 * <p>This can be used to parameterize transforms that only read values in at runtime, for example.
 *
 * <p>A common task is to create a {@link PCollection} containing the value of this {@link
 * ValueProvider} regardless of whether it's accessible at construction time or not. For that, use
 * {@link Create#ofProvider}.
 *
 * <p>For unit-testing a transform against a {@link ValueProvider} that only becomes available at
 * runtime, use {@link org.apache.beam.sdk.testing.TestPipeline#newProvider}.
 */
@JsonSerialize(using = ValueProvider.Serializer.class)
@JsonDeserialize(using = ValueProvider.Deserializer.class)
public interface ValueProvider<T> extends Serializable {
  /**
   * Returns the runtime value wrapped by this {@link ValueProvider} in case it is {@link
   * #isAccessible}, otherwise fails.
   */
  T get();

  /** Whether the contents of this {@link ValueProvider} is currently available via {@link #get}. */
  boolean isAccessible();

  /**
   * {@link StaticValueProvider} is an implementation of {@link ValueProvider} that allows for a
   * static value to be provided.
   */
  class StaticValueProvider<T> implements ValueProvider<T>, Serializable {
    private final @Nullable T value;

    StaticValueProvider(@Nullable T value) {
      this.value = value;
    }

    /** Creates a {@link StaticValueProvider} that wraps the provided value. */
    public static <T> StaticValueProvider<T> of(T value) {
      return new StaticValueProvider<>(value);
    }

    @Override
    public T get() {
      return value;
    }

    @Override
    public boolean isAccessible() {
      return true;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof StaticValueProvider
          && Objects.equals(value, ((StaticValueProvider) other).value);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(value);
    }
  }

  /**
   * {@link NestedValueProvider} is an implementation of {@link ValueProvider} that allows for
   * wrapping another {@link ValueProvider} object.
   */
  class NestedValueProvider<T, X> implements ValueProvider<T>, Serializable {

    private final ValueProvider<X> value;
    private final SerializableFunction<X, T> translator;
    private transient volatile T cachedValue;

    NestedValueProvider(ValueProvider<X> value, SerializableFunction<X, T> translator) {
      this.value = checkNotNull(value);
      this.translator = checkNotNull(translator);
    }

    /** Creates a {@link NestedValueProvider} that wraps the provided value. */
    public static <T, X> NestedValueProvider<T, X> of(
        ValueProvider<X> value, SerializableFunction<X, T> translator) {
      return new NestedValueProvider<>(value, translator);
    }

    @Override
    public T get() {
      if (cachedValue == null) {
        cachedValue = translator.apply(value.get());
      }
      return cachedValue;
    }

    @Override
    public boolean isAccessible() {
      return value.isAccessible();
    }

    /** Returns the property name associated with this provider. */
    public String propertyName() {
      if (value instanceof RuntimeValueProvider) {
        return ((RuntimeValueProvider) value).propertyName();
      } else if (value instanceof NestedValueProvider) {
        return ((NestedValueProvider) value).propertyName();
      } else {
        throw new RuntimeException(
            "Only a RuntimeValueProvider or a NestedValueProvider can supply"
                + " a property name.");
      }
    }

    @Override
    public String toString() {
      if (isAccessible()) {
        return String.valueOf(get());
      }
      return MoreObjects.toStringHelper(this)
          .add("value", value)
          .add("translator", translator.getClass().getSimpleName())
          .toString();
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof NestedValueProvider
          && Objects.equals(value, ((NestedValueProvider) other).value)
          && Objects.equals(translator, ((NestedValueProvider) other).translator);
    }

    @Override
    public int hashCode() {
      return Objects.hash(value, translator);
    }
  }

  /**
   * {@link RuntimeValueProvider} is an implementation of {@link ValueProvider} that allows for a
   * value to be provided at execution time rather than at graph construction time.
   *
   * <p>To enforce this contract, if there is no default, users must only call {@link #get()} at
   * execution time (after a call to {@link org.apache.beam.sdk.Pipeline#run}), which will provide
   * the value of {@code optionsMap}.
   */
  class RuntimeValueProvider<T> implements ValueProvider<T>, Serializable {
    private static ConcurrentHashMap<Long, PipelineOptions> optionsMap = new ConcurrentHashMap<>();

    private final Class<? extends PipelineOptions> klass;
    private final String methodName;
    private final String propertyName;
    private final @Nullable T defaultValue;
    private final Long optionsId;

    /**
     * Creates a {@link RuntimeValueProvider} that will query the provided {@code optionsId} for a
     * value.
     */
    RuntimeValueProvider(
        String methodName,
        String propertyName,
        Class<? extends PipelineOptions> klass,
        Long optionsId) {
      this.methodName = methodName;
      this.propertyName = propertyName;
      this.klass = klass;
      this.defaultValue = null;
      this.optionsId = optionsId;
    }

    /**
     * Creates a {@link RuntimeValueProvider} that will query the provided {@code optionsId} for a
     * value, or use the default if no value is available.
     */
    RuntimeValueProvider(
        String methodName,
        String propertyName,
        Class<? extends PipelineOptions> klass,
        T defaultValue,
        Long optionsId) {
      this.methodName = methodName;
      this.propertyName = propertyName;
      this.klass = klass;
      this.defaultValue = defaultValue;
      this.optionsId = optionsId;
    }

    /**
     * Once set, all {@code RuntimeValueProviders} will return {@code true} from {@code
     * isAccessible()}. By default, the value is set when deserializing {@link PipelineOptions}.
     */
    static void setRuntimeOptions(PipelineOptions runtimeOptions) {
      optionsMap.put(runtimeOptions.getOptionsId(), runtimeOptions);
    }

    @Override
    public T get() {
      PipelineOptions options = optionsMap.get(optionsId);
      if (options == null) {
        throw new IllegalStateException(
            "Value only available at runtime, but accessed from a non-runtime context: " + this);
      }
      try {
        Method method = klass.getMethod(methodName);
        PipelineOptions methodOptions = options.as(klass);
        InvocationHandler handler = Proxy.getInvocationHandler(methodOptions);
        @SuppressWarnings("unchecked")
        ValueProvider<T> result = (ValueProvider<T>) handler.invoke(methodOptions, method, null);
        // Two cases: If we have deserialized a new value from JSON, it will
        // be wrapped in a StaticValueProvider, which we can provide here.  If
        // not, there was no JSON value, and we return the default, whether or
        // not it is null.
        if (result instanceof StaticValueProvider) {
          return result.get();
        }
        return defaultValue;
      } catch (Throwable e) {
        throw new RuntimeException("Unable to load runtime value.", e);
      }
    }

    @Override
    public boolean isAccessible() {
      return optionsMap.get(optionsId) != null;
    }

    /** Returns the property name that corresponds to this provider. */
    public String propertyName() {
      return propertyName;
    }

    @Override
    public String toString() {
      if (isAccessible()) {
        return String.valueOf(get());
      }
      return MoreObjects.toStringHelper(this)
          .add("propertyName", propertyName)
          .add("default", defaultValue)
          .toString();
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof RuntimeValueProvider
          && Objects.equals(klass, ((RuntimeValueProvider) other).klass)
          && Objects.equals(methodName, ((RuntimeValueProvider) other).methodName)
          && Objects.equals(propertyName, ((RuntimeValueProvider) other).propertyName)
          && Objects.equals(defaultValue, ((RuntimeValueProvider) other).defaultValue)
          && Objects.equals(optionsId, ((RuntimeValueProvider) other).optionsId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(klass, methodName, propertyName, defaultValue, optionsId);
    }
  }

  /** <b>For internal use only; no backwards compatibility guarantees.</b> */
  @Internal
  class Serializer extends JsonSerializer<ValueProvider<?>> {
    @Override
    public void serialize(ValueProvider<?> value, JsonGenerator jgen, SerializerProvider provider)
        throws IOException {
      if (value.isAccessible()) {
        jgen.writeObject(value.get());
      } else {
        jgen.writeNull();
      }
    }
  }

  /** <b>For internal use only; no backwards compatibility guarantees.</b> */
  @Internal
  class Deserializer extends JsonDeserializer<ValueProvider<?>> implements ContextualDeserializer {

    private final JavaType innerType;

    // A 0-arg constructor is required by the compiler.
    Deserializer() {
      this.innerType = null;
    }

    Deserializer(JavaType innerType) {
      this.innerType = innerType;
    }

    @Override
    public JsonDeserializer<?> createContextual(DeserializationContext ctxt, BeanProperty property)
        throws JsonMappingException {
      checkNotNull(ctxt, "Null DeserializationContext.");
      JavaType type = checkNotNull(ctxt.getContextualType(), "Invalid type: %s", getClass());
      JavaType[] params = type.findTypeParameters(ValueProvider.class);
      if (params.length != 1) {
        throw new RuntimeException("Unable to derive type for ValueProvider: " + type.toString());
      }
      JavaType param = params[0];
      return new Deserializer(param);
    }

    @Override
    public ValueProvider<?> deserialize(JsonParser jp, DeserializationContext ctxt)
        throws IOException, JsonProcessingException {
      JsonDeserializer dser =
          ctxt.findRootValueDeserializer(
              checkNotNull(
                  innerType, "Invalid %s: innerType is null. Serialization error?", getClass()));
      Object o = dser.deserialize(jp, ctxt);
      return StaticValueProvider.of(o);
    }
  }
}
