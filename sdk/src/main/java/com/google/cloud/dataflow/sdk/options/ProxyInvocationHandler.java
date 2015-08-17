/*
 * Copyright (C) 2015 Google Inc.
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

package com.google.cloud.dataflow.sdk.options;

import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory.JsonIgnorePredicate;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory.Registration;
import com.google.cloud.dataflow.sdk.util.InstanceBuilder;
import com.google.cloud.dataflow.sdk.util.common.ReflectHelpers;
import com.google.common.base.Defaults;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ClassToInstanceMap;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.MutableClassToInstanceMap;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Represents and {@link InvocationHandler} for a {@link Proxy}. The invocation handler uses bean
 * introspection of the proxy class to store and retrieve values based off of the property name.
 *
 * <p>Unset properties use the {@code @Default} metadata on the getter to return values. If there
 * is no {@code @Default} annotation on the getter, then a <a
 * href="https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html">default</a> as
 * per the Java Language Specification for the expected return type is returned.
 *
 * <p>In addition to the getter/setter pairs, this proxy invocation handler supports
 * {@link Object#equals(Object)}, {@link Object#hashCode()}, {@link Object#toString()} and
 * {@link PipelineOptions#as(Class)}.
 */
@ThreadSafe
class ProxyInvocationHandler implements InvocationHandler {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  /**
   * No two instances of this class are considered equivalent hence we generate a random hash code
   * between 0 and {@link Integer#MAX_VALUE}.
   */
  private final int hashCode = (int) (Math.random() * Integer.MAX_VALUE);
  private final Set<Class<? extends PipelineOptions>> knownInterfaces;
  private final ClassToInstanceMap<PipelineOptions> interfaceToProxyCache;
  private final Map<String, Object> options;
  private final Map<String, JsonNode> jsonOptions;
  private final Map<String, String> gettersToPropertyNames;
  private final Map<String, String> settersToPropertyNames;

  ProxyInvocationHandler(Map<String, Object> options) {
    this(options, Maps.<String, JsonNode>newHashMap());
  }

  private ProxyInvocationHandler(Map<String, Object> options, Map<String, JsonNode> jsonOptions) {
    this.options = options;
    this.jsonOptions = jsonOptions;
    this.knownInterfaces = new HashSet<>(PipelineOptionsFactory.getRegisteredOptions());
    gettersToPropertyNames = Maps.newHashMap();
    settersToPropertyNames = Maps.newHashMap();
    interfaceToProxyCache = MutableClassToInstanceMap.create();
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) {
    if (args == null && "toString".equals(method.getName())) {
      return toString();
    } else if (args != null && args.length == 1 && "equals".equals(method.getName())) {
      return equals(args[0]);
    } else if (args == null && "hashCode".equals(method.getName())) {
      return hashCode();
    } else if (args != null && "as".equals(method.getName()) && args[0] instanceof Class) {
      @SuppressWarnings("unchecked")
      Class<? extends PipelineOptions> clazz = (Class<? extends PipelineOptions>) args[0];
      return as(clazz);
    } else if (args != null && "cloneAs".equals(method.getName()) && args[0] instanceof Class) {
      @SuppressWarnings("unchecked")
      Class<? extends PipelineOptions> clazz = (Class<? extends PipelineOptions>) args[0];
      return cloneAs(proxy, clazz);
    }
    String methodName = method.getName();
    synchronized (this) {
      if (gettersToPropertyNames.keySet().contains(methodName)) {
        String propertyName = gettersToPropertyNames.get(methodName);
        if (!options.containsKey(propertyName)) {
          // Lazy bind the default to the method.
          Object value = jsonOptions.containsKey(propertyName)
              ? getValueFromJson(propertyName, method)
              : getDefault((PipelineOptions) proxy, method);
          options.put(propertyName, value);
        }
        return options.get(propertyName);
      } else if (settersToPropertyNames.containsKey(methodName)) {
        options.put(settersToPropertyNames.get(methodName), args[0]);
        return Void.TYPE;
      }
    }
    throw new RuntimeException("Unknown method [" + method + "] invoked with args ["
        + Arrays.toString(args) + "].");
  }

  /**
   * Backing implementation for {@link PipelineOptions#as(Class)}.
   *
   * @param iface The interface that the returned object needs to implement.
   * @return An object that implements the interface <T>.
   */
  synchronized <T extends PipelineOptions> T as(Class<T> iface) {
    Preconditions.checkNotNull(iface);
    Preconditions.checkArgument(iface.isInterface());
    if (!interfaceToProxyCache.containsKey(iface)) {
      Registration<T> registration =
          PipelineOptionsFactory.validateWellFormed(iface, knownInterfaces);
      List<PropertyDescriptor> propertyDescriptors = registration.getPropertyDescriptors();
      Class<T> proxyClass = registration.getProxyClass();
      gettersToPropertyNames.putAll(generateGettersToPropertyNames(propertyDescriptors));
      settersToPropertyNames.putAll(generateSettersToPropertyNames(propertyDescriptors));
      knownInterfaces.add(iface);
      interfaceToProxyCache.putInstance(iface,
          InstanceBuilder.ofType(proxyClass)
              .fromClass(proxyClass)
              .withArg(InvocationHandler.class, this)
              .build());
    }
    return interfaceToProxyCache.getInstance(iface);
  }

  /**
   * Backing implementation for {@link PipelineOptions#cloneAs(Class)}.
   *
   * @return A copy of the PipelineOptions.
   */
  synchronized <T extends PipelineOptions> T cloneAs(Object proxy, Class<T> iface) {
    PipelineOptions clonedOptions;
    try {
      clonedOptions = MAPPER.readValue(MAPPER.writeValueAsBytes(proxy), PipelineOptions.class);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to serialize the pipeline options to JSON.", e);
    }
    for (Class<? extends PipelineOptions> knownIface : knownInterfaces) {
      clonedOptions.as(knownIface);
    }
    return clonedOptions.as(iface);
  }

  /**
   * Returns true if the other object is a ProxyInvocationHandler or is a Proxy object and has the
   * same ProxyInvocationHandler as this.
   *
   * @param obj The object to compare against this.
   * @return true iff the other object is a ProxyInvocationHandler or is a Proxy object and has the
   *         same ProxyInvocationHandler as this.
   */
  @Override
  public boolean equals(Object obj) {
    return obj != null && ((obj instanceof ProxyInvocationHandler && this == obj)
        || (Proxy.isProxyClass(obj.getClass()) && this == Proxy.getInvocationHandler(obj)));
  }

  /**
   * Each instance of this ProxyInvocationHandler is unique and has a random hash code.
   *
   * @return A hash code that was generated randomly.
   */
  @Override
  public int hashCode() {
    return hashCode;
  }

  /**
   * This will output all the currently set values. This is a relatively costly function
   * as it will call {@code toString()} on each object that has been set and format
   * the results in a readable format.
   *
   * @return A pretty printed string representation of this.
   */
  @Override
  public synchronized String toString() {
    SortedMap<String, Object> sortedOptions = new TreeMap<>();
    // Add the options that we received from deserialization
    sortedOptions.putAll(jsonOptions);
    // Override with any programmatically set options.
    sortedOptions.putAll(options);

    StringBuilder b = new StringBuilder();
    b.append("Current Settings:\n");
    for (Map.Entry<String, Object> entry : sortedOptions.entrySet()) {
      b.append("  " + entry.getKey() + ": " + entry.getValue() + "\n");
    }
    return b.toString();
  }

  /**
   * Uses a Jackson {@link ObjectMapper} to attempt type conversion.
   *
   * @param method The method whose return type you would like to return.
   * @param propertyName The name of the property that is being returned.
   * @return An object matching the return type of the method passed in.
   */
  private Object getValueFromJson(String propertyName, Method method) {
    try {
      JavaType type = MAPPER.getTypeFactory().constructType(method.getGenericReturnType());
      JsonNode jsonNode = jsonOptions.get(propertyName);
      return MAPPER.readValue(jsonNode.toString(), type);
    } catch (IOException e) {
      throw new RuntimeException("Unable to parse representation", e);
    }
  }

  /**
   * Returns a default value for the method based upon {@code @Default} metadata on the getter
   * to return values. If there is no {@code @Default} annotation on the getter, then a <a
   * href="https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html">default</a> as
   * per the Java Language Specification for the expected return type is returned.
   *
   * @param proxy The proxy object for which we are attempting to get the default.
   * @param method The getter method that was invoked.
   * @return The default value from an {@link Default} annotation if present, otherwise a default
   *         value as per the Java Language Specification.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private Object getDefault(PipelineOptions proxy, Method method) {
    for (Annotation annotation : method.getAnnotations()) {
      if (annotation instanceof Default.Class) {
        return ((Default.Class) annotation).value();
      } else if (annotation instanceof Default.String) {
        return ((Default.String) annotation).value();
      } else if (annotation instanceof Default.Boolean) {
        return ((Default.Boolean) annotation).value();
      } else if (annotation instanceof Default.Character) {
        return ((Default.Character) annotation).value();
      } else if (annotation instanceof Default.Byte) {
        return ((Default.Byte) annotation).value();
      } else if (annotation instanceof Default.Short) {
        return ((Default.Short) annotation).value();
      } else if (annotation instanceof Default.Integer) {
        return ((Default.Integer) annotation).value();
      } else if (annotation instanceof Default.Long) {
        return ((Default.Long) annotation).value();
      } else if (annotation instanceof Default.Float) {
        return ((Default.Float) annotation).value();
      } else if (annotation instanceof Default.Double) {
        return ((Default.Double) annotation).value();
      } else if (annotation instanceof Default.Enum) {
        return Enum.valueOf((Class<Enum>) method.getReturnType(),
            ((Default.Enum) annotation).value());
      } else if (annotation instanceof Default.InstanceFactory) {
        return InstanceBuilder.ofType(((Default.InstanceFactory) annotation).value())
            .build()
            .create(proxy);
      }
    }

    /*
     * We need to make sure that we return something appropriate for the return type. Thus we return
     * a default value as defined by the JLS.
     */
    return Defaults.defaultValue(method.getReturnType());
  }

  /**
   * Returns a map from the getters method name to the name of the property based upon the passed in
   * {@link PropertyDescriptor}s property descriptors.
   *
   * @param propertyDescriptors A list of {@link PropertyDescriptor}s to use when generating the
   *        map.
   * @return A map of getter method name to property name.
   */
  private static Map<String, String> generateGettersToPropertyNames(
      List<PropertyDescriptor> propertyDescriptors) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (PropertyDescriptor descriptor : propertyDescriptors) {
      if (descriptor.getReadMethod() != null) {
        builder.put(descriptor.getReadMethod().getName(), descriptor.getName());
      }
    }
    return builder.build();
  }

  /**
   * Returns a map from the setters method name to its matching getters method name based upon the
   * passed in {@link PropertyDescriptor}s property descriptors.
   *
   * @param propertyDescriptors A list of {@link PropertyDescriptor}s to use when generating the
   *        map.
   * @return A map of setter method name to getter method name.
   */
  private static Map<String, String> generateSettersToPropertyNames(
      List<PropertyDescriptor> propertyDescriptors) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (PropertyDescriptor descriptor : propertyDescriptors) {
      if (descriptor.getWriteMethod() != null) {
        builder.put(descriptor.getWriteMethod().getName(), descriptor.getName());
      }
    }
    return builder.build();
  }

  static class Serializer extends JsonSerializer<PipelineOptions> {
    @Override
    public void serialize(PipelineOptions value, JsonGenerator jgen, SerializerProvider provider)
        throws IOException, JsonProcessingException {
      ProxyInvocationHandler handler = (ProxyInvocationHandler) Proxy.getInvocationHandler(value);
      synchronized (handler) {
        // We first filter out any properties that have been modified since
        // the last serialization of this PipelineOptions and then verify that
        // they are all serializable.
        Map<String, Object> filteredOptions = Maps.newHashMap(handler.options);
        removeIgnoredOptions(handler.knownInterfaces, filteredOptions);
        ensureSerializable(handler.knownInterfaces, filteredOptions);

        // Now we create the map of serializable options by taking the original
        // set of serialized options (if any) and updating them with any properties
        // instances that have been modified since the previous serialization.
        Map<String, Object> serializableOptions =
            Maps.<String, Object>newHashMap(handler.jsonOptions);
        serializableOptions.putAll(filteredOptions);
        jgen.writeStartObject();
        jgen.writeFieldName("options");
        jgen.writeObject(serializableOptions);
        jgen.writeEndObject();
      }
    }

    /**
     * We remove all properties within the passed in options where there getter is annotated with
     * {@link JsonIgnore @JsonIgnore} from the passed in options using the passed in interfaces.
     */
    private void removeIgnoredOptions(
        Set<Class<? extends PipelineOptions>> interfaces, Map<String, Object> options) {
      // Find all the method names that are annotated with JSON ignore.
      Set<String> jsonIgnoreMethodNames = FluentIterable.from(
          ReflectHelpers.getClosureOfMethodsOnInterfaces(interfaces))
          .filter(JsonIgnorePredicate.INSTANCE).transform(new Function<Method, String>() {
            @Override
            public String apply(Method input) {
              return input.getName();
            }
          }).toSet();

      // Remove all options that have the same method name as the descriptor.
      for (PropertyDescriptor descriptor
          : PipelineOptionsFactory.getPropertyDescriptors(interfaces)) {
        if (jsonIgnoreMethodNames.contains(descriptor.getReadMethod().getName())) {
          options.remove(descriptor.getName());
        }
      }
    }

    /**
     * We use an {@link ObjectMapper} to verify that the passed in options are serializable
     * and deserializable.
     */
    private void ensureSerializable(Set<Class<? extends PipelineOptions>> interfaces,
        Map<String, Object> options) throws IOException {
      // Construct a map from property name to the return type of the getter.
      Map<String, Type> propertyToReturnType = Maps.newHashMap();
      for (PropertyDescriptor descriptor
          : PipelineOptionsFactory.getPropertyDescriptors(interfaces)) {
        if (descriptor.getReadMethod() != null) {
          propertyToReturnType.put(descriptor.getName(),
              descriptor.getReadMethod().getGenericReturnType());
        }
      }

      // Attempt to serialize and deserialize each property.
      for (Map.Entry<String, Object> entry : options.entrySet()) {
        try {
          String serializedValue = MAPPER.writeValueAsString(entry.getValue());
          JavaType type = MAPPER.getTypeFactory()
              .constructType(propertyToReturnType.get(entry.getKey()));
          MAPPER.readValue(serializedValue, type);
        } catch (Exception e) {
          throw new IOException(String.format(
              "Failed to serialize and deserialize property '%s' with value '%s'",
              entry.getKey(), entry.getValue()), e);
        }
      }
    }
  }

  static class Deserializer extends JsonDeserializer<PipelineOptions> {
    @Override
    public PipelineOptions deserialize(JsonParser jp, DeserializationContext ctxt)
        throws IOException, JsonProcessingException {
      ObjectNode objectNode = (ObjectNode) jp.readValueAsTree();
      ObjectNode optionsNode = (ObjectNode) objectNode.get("options");

      Map<String, JsonNode> fields = Maps.newHashMap();
      for (Iterator<Map.Entry<String, JsonNode>> iterator = optionsNode.fields();
          iterator.hasNext(); ) {
        Map.Entry<String, JsonNode> field = iterator.next();
        fields.put(field.getKey(), field.getValue());
      }
      PipelineOptions options =
          new ProxyInvocationHandler(Maps.<String, Object>newHashMap(), fields)
              .as(PipelineOptions.class);
      return options;
    }
  }
}
