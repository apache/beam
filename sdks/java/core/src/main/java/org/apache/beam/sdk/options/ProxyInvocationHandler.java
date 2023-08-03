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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import com.google.auto.value.AutoValue;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.sdk.options.PipelineOptionsFactory.AnnotationPredicates;
import org.apache.beam.sdk.options.PipelineOptionsFactory.Registration;
import org.apache.beam.sdk.options.ValueProvider.RuntimeValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Defaults;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.HashMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableClassToInstanceMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Represents an {@link InvocationHandler} for a {@link Proxy}. The invocation handler uses bean
 * introspection of the proxy class to store and retrieve values based off of the property name.
 *
 * <p>Unset properties use the {@code @Default} metadata on the getter to return values. If there is
 * no {@code @Default} annotation on the getter, then a <a
 * href="https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html">default</a> as
 * per the Java Language Specification for the expected return type is returned.
 *
 * <p>In addition to the getter/setter pairs, this proxy invocation handler supports {@link
 * Object#equals(Object)}, {@link Object#hashCode()}, {@link Object#toString()} and {@link
 * PipelineOptions#as(Class)}.
 */
@ThreadSafe
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class ProxyInvocationHandler implements InvocationHandler, Serializable {
  /**
   * No two instances of this class are considered equivalent hence we generate a random hash code.
   */
  private final int hashCode = ThreadLocalRandom.current().nextInt();

  private final AtomicInteger revision;

  private static final class ComputedProperties {
    final ImmutableClassToInstanceMap<PipelineOptions> interfaceToProxyCache;
    final ImmutableMap<String, String> gettersToPropertyNames;
    final ImmutableMap<String, String> settersToPropertyNames;
    final ImmutableSet<Class<? extends PipelineOptions>> knownInterfaces;

    private ComputedProperties(
        ImmutableClassToInstanceMap<PipelineOptions> interfaceToProxyCache,
        ImmutableMap<String, String> gettersToPropertyNames,
        ImmutableMap<String, String> settersToPropertyNames,
        ImmutableSet<Class<? extends PipelineOptions>> knownInterfaces) {
      this.interfaceToProxyCache = interfaceToProxyCache;
      this.gettersToPropertyNames = gettersToPropertyNames;
      this.settersToPropertyNames = settersToPropertyNames;
      this.knownInterfaces = knownInterfaces;
    }

    <T extends PipelineOptions> ComputedProperties updated(
        Class<T> iface, T instance, List<PropertyDescriptor> propertyDescriptors) {

      // these all use mutable maps and then copyOf, rather than a builder because builders enforce
      // all keys are unique, and its possible they are not here.
      Map<String, String> allNewGetters = Maps.newHashMap(gettersToPropertyNames);
      Map<String, String> allNewSetters = Maps.newHashMap(settersToPropertyNames);
      Set<Class<? extends PipelineOptions>> newKnownInterfaces = Sets.newHashSet(knownInterfaces);
      Map<Class<? extends PipelineOptions>, PipelineOptions> newInterfaceCache =
          Maps.newHashMap(interfaceToProxyCache);

      allNewGetters.putAll(generateGettersToPropertyNames(propertyDescriptors));
      allNewSetters.putAll(generateSettersToPropertyNames(propertyDescriptors));
      newKnownInterfaces.add(iface);
      newInterfaceCache.put(iface, instance);

      return new ComputedProperties(
          ImmutableClassToInstanceMap.copyOf(newInterfaceCache),
          ImmutableMap.copyOf(allNewGetters),
          ImmutableMap.copyOf(allNewSetters),
          ImmutableSet.copyOf(newKnownInterfaces));
    }
  }

  /** Only modified while holding a lock on {@code this}. */
  @SuppressFBWarnings("SE_BAD_FIELD")
  private volatile ComputedProperties computedProperties;

  // ProxyInvocationHandler implements Serializable only for the sake of throwing an informative
  // exception in writeObject()
  /**
   * Enumerating {@code options} must always be done on a copy made before accessing or deriving
   * properties from {@code computedProperties} since concurrent hash maps are <a
   * href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/package-summary.html#Weakly>weakly
   * consistent</a>. This will allow us to ensure that the keys in {code options} will always be a
   * subset of properties stored in {code computedProperties}.
   */
  @SuppressFBWarnings("SE_BAD_FIELD")
  private final ConcurrentHashMap<String, BoundValue> options;

  private final ImmutableMap<String, JsonNode> jsonOptions;

  ProxyInvocationHandler(Map<String, Object> options) {
    this(bindOptions(options), Maps.newHashMap(), 0);
  }

  private static Map<String, BoundValue> bindOptions(Map<String, Object> inputOptions) {
    HashMap<String, BoundValue> options = Maps.newHashMap();
    for (Map.Entry<String, Object> entry : inputOptions.entrySet()) {
      options.put(entry.getKey(), BoundValue.fromExplicitOption(entry.getValue()));
    }

    return options;
  }

  private ProxyInvocationHandler(
      Map<String, BoundValue> options, Map<String, JsonNode> jsonOptions, int revision) {
    this.options = new ConcurrentHashMap<>(options);
    this.jsonOptions = ImmutableMap.copyOf(jsonOptions);
    this.revision = new AtomicInteger(revision);
    this.computedProperties =
        new ComputedProperties(
            ImmutableClassToInstanceMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableSet.copyOf(PipelineOptionsFactory.getRegisteredOptions()));
  }

  @Override
  @SuppressFBWarnings("AT_OPERATION_SEQUENCE_ON_CONCURRENT_ABSTRACTION")
  public Object invoke(Object proxy, Method method, Object[] args) {
    if (args == null && "toString".equals(method.getName())) {
      return toString();
    } else if (args != null && args.length == 1 && "equals".equals(method.getName())) {
      return equals(args[0]);
    } else if (args == null && "hashCode".equals(method.getName())) {
      return hashCode();
    } else if (args == null && "outputRuntimeOptions".equals(method.getName())) {
      return outputRuntimeOptions((PipelineOptions) proxy);
    } else if (args == null && "revision".equals(method.getName())) {
      return revision.get();
    } else if (args != null && "as".equals(method.getName()) && args[0] instanceof Class) {
      @SuppressWarnings("unchecked")
      Class<? extends PipelineOptions> clazz = (Class<? extends PipelineOptions>) args[0];
      return as(clazz);
    } else if (args != null
        && "populateDisplayData".equals(method.getName())
        && args[0] instanceof DisplayData.Builder) {
      @SuppressWarnings("unchecked")
      DisplayData.Builder builder = (DisplayData.Builder) args[0];
      builder.delegate(new PipelineOptionsDisplayData());
      return Void.TYPE;
    }
    String methodName = method.getName();

    ComputedProperties properties = computedProperties;
    if (properties.gettersToPropertyNames.containsKey(methodName)) {
      String propertyName = properties.gettersToPropertyNames.get(methodName);
      // we can't use computeIfAbsent here because evaluating the default may cause more properties
      // to be evaluated, and computeIfAbsent is not re-entrant.
      if (!options.containsKey(propertyName)) {
        // Lazy bind the default to the method.
        Object value =
            jsonOptions.containsKey(propertyName)
                ? getValueFromJson(propertyName, method)
                : getDefault((PipelineOptions) proxy, method);
        options.put(propertyName, BoundValue.fromDefault(value));
      }
      return options.get(propertyName).getValue();
    } else if (properties.settersToPropertyNames.containsKey(methodName)) {
      BoundValue prev =
          options.put(
              properties.settersToPropertyNames.get(methodName),
              BoundValue.fromExplicitOption(args[0]));
      if (prev == null ? args[0] != null : !Objects.equals(args[0], prev.getValue())) {
        revision.incrementAndGet();
      }
      return Void.TYPE;
    }
    throw new RuntimeException(
        "Unknown method [" + method + "] invoked with args [" + Arrays.toString(args) + "].");
  }

  public String getOptionName(Method method) {
    return computedProperties.gettersToPropertyNames.get(method.getName());
  }

  private void writeObject(java.io.ObjectOutputStream stream) throws IOException {
    throw new NotSerializableException(
        "PipelineOptions objects are not serializable and should not be embedded into transforms "
            + "(did you capture a PipelineOptions object in a field or in an anonymous class?). "
            + "Instead, if you're using a DoFn, access PipelineOptions at runtime "
            + "via ProcessContext/StartBundleContext/FinishBundleContext.getPipelineOptions(), "
            + "or pre-extract necessary fields from PipelineOptions "
            + "at pipeline construction time.");
  }

  /** Track whether options values are explicitly set, or retrieved from defaults. */
  @AutoValue
  abstract static class BoundValue {

    abstract @Nullable Object getValue();

    abstract boolean isDefault();

    private static BoundValue of(@Nullable Object value, boolean isDefault) {
      return new AutoValue_ProxyInvocationHandler_BoundValue(value, isDefault);
    }

    /** Create a {@link BoundValue} representing an explicitly set option. */
    static BoundValue fromExplicitOption(@Nullable Object value) {
      return BoundValue.of(value, false);
    }

    /** Create a {@link BoundValue} representing a default option value. */
    static BoundValue fromDefault(@Nullable Object value) {
      return BoundValue.of(value, true);
    }
  }

  /**
   * Backing implementation for {@link PipelineOptions#as(Class)}.
   *
   * @param iface The interface that the returned object needs to implement.
   * @return An object that implements the interface {@code <T>}.
   */
  <T extends PipelineOptions> T as(Class<T> iface) {
    checkNotNull(iface);
    checkArgument(iface.isInterface(), "Not an interface: %s", iface);

    T existingOption = computedProperties.interfaceToProxyCache.getInstance(iface);
    if (existingOption == null) {
      synchronized (this) {
        // double check
        existingOption = computedProperties.interfaceToProxyCache.getInstance(iface);
        if (existingOption == null) {
          Registration<T> registration =
              PipelineOptionsFactory.CACHE
                  .get()
                  .validateWellFormed(iface, computedProperties.knownInterfaces);
          List<PropertyDescriptor> propertyDescriptors = registration.getPropertyDescriptors();

          Class<T> proxyClass = registration.getProxyClass();

          existingOption =
              InstanceBuilder.ofType(proxyClass)
                  .fromClass(proxyClass)
                  .withArg(InvocationHandler.class, this)
                  .build();

          computedProperties =
              computedProperties.updated(iface, existingOption, propertyDescriptors);
        }
      }
    }
    return existingOption;
  }

  /**
   * Returns true if the other object is a ProxyInvocationHandler or is a Proxy object and has the
   * same ProxyInvocationHandler as this.
   *
   * @param obj The object to compare against this.
   * @return true iff the other object is a ProxyInvocationHandler or is a Proxy object and has the
   *     same ProxyInvocationHandler as this.
   */
  @Override
  public boolean equals(@Nullable Object obj) {
    return obj != null
        && ((obj instanceof ProxyInvocationHandler && this == obj)
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

  /** Returns a map of properties which correspond to {@link RuntimeValueProvider}. */
  public Map<String, Map<String, Object>> outputRuntimeOptions(PipelineOptions options) {
    Set<PipelineOptionSpec> optionSpecs =
        PipelineOptionsReflector.getOptionSpecs(computedProperties.knownInterfaces);
    Map<String, Map<String, Object>> properties = Maps.newHashMap();

    for (PipelineOptionSpec spec : optionSpecs) {
      if (spec.getGetterMethod().getReturnType().equals(ValueProvider.class)) {
        Object vp = invoke(options, spec.getGetterMethod(), null);
        if (((ValueProvider) vp).isAccessible()) {
          continue;
        }
        Map<String, Object> property = Maps.newHashMap();
        property.put(
            "type",
            ((ParameterizedType) spec.getGetterMethod().getGenericReturnType())
                .getActualTypeArguments()[0]);
        properties.put(spec.getName(), property);
      }
    }
    return properties;
  }

  /**
   * Nested class to handle display data in order to set the display data namespace to something
   * sensible.
   */
  class PipelineOptionsDisplayData implements HasDisplayData {
    /**
     * Populate display data. See {@link HasDisplayData#populateDisplayData}. All explicitly set
     * pipeline options will be added as display data.
     */
    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      // We must first make a copy of the current options because a concurrent modification
      // may add a new option after we have derived optionSpecs but before we have enumerated
      // all the pipeline options.
      Map<String, BoundValue> copiedOptions = new HashMap<>(options);
      Set<PipelineOptionSpec> optionSpecs =
          PipelineOptionsReflector.getOptionSpecs(computedProperties.knownInterfaces);

      Multimap<String, PipelineOptionSpec> optionsMap = buildOptionNameToSpecMap(optionSpecs);

      for (Map.Entry<String, BoundValue> option : copiedOptions.entrySet()) {
        BoundValue boundValue = option.getValue();
        if (boundValue.isDefault()) {
          continue;
        }

        DisplayDataValue resolved = DisplayDataValue.resolve(boundValue.getValue());
        HashSet<PipelineOptionSpec> specs = new HashSet<>(optionsMap.get(option.getKey()));

        for (PipelineOptionSpec optionSpec : specs) {
          if (!optionSpec.shouldSerialize()) {
            // Options that are excluded for serialization (i.e. those with @JsonIgnore) are also
            // excluded from display data. These options are generally not useful for display.
            continue;
          }

          if (optionSpec.getGetterMethod().isAnnotationPresent(Hidden.class)) {
            continue;
          }

          builder.add(
              DisplayData.item(option.getKey(), resolved.getType(), resolved.getValue())
                  .withNamespace(optionSpec.getDefiningInterface()));
        }
      }

      for (Map.Entry<String, JsonNode> jsonOption : jsonOptions.entrySet()) {
        if (copiedOptions.containsKey(jsonOption.getKey())) {
          // Option overwritten since deserialization; don't re-write
          continue;
        }

        HashSet<PipelineOptionSpec> specs = new HashSet<>(optionsMap.get(jsonOption.getKey()));
        if (specs.isEmpty()) {
          // No PipelineOptions interface for this key not currently loaded
          builder.add(
              DisplayData.item(jsonOption.getKey(), jsonOption.getValue().toString())
                  .withNamespace(UnknownPipelineOptions.class));
          continue;
        }

        for (PipelineOptionSpec spec : specs) {
          if (!spec.shouldSerialize()) {
            continue;
          }

          if (spec.getGetterMethod().isAnnotationPresent(Hidden.class)) {
            continue;
          }

          Object value = getValueFromJson(jsonOption.getKey(), spec.getGetterMethod());
          DisplayDataValue resolved = DisplayDataValue.resolve(value);
          builder.add(
              DisplayData.item(jsonOption.getKey(), resolved.getType(), resolved.getValue())
                  .withNamespace(spec.getDefiningInterface()));
        }
      }
    }
  }

  /** Helper class to resolve a {@link DisplayData} type and value from {@link PipelineOptions}. */
  @AutoValue
  abstract static class DisplayDataValue {
    /** The resolved display data value. May differ from the input to {@link #resolve(Object)} */
    abstract Object getValue();

    /** The resolved display data type. */
    abstract DisplayData.Type getType();

    /**
     * Infer the value and {@link DisplayData.Type type} for the given {@link PipelineOptions}
     * value.
     */
    static DisplayDataValue resolve(@Nullable Object value) {
      DisplayData.Type type = DisplayData.inferType(value);

      if (type == null) {
        value = displayDataString(value);
        type = DisplayData.Type.STRING;
      }

      return new AutoValue_ProxyInvocationHandler_DisplayDataValue(value, type);
    }

    /** Safe {@link Object#toString()} wrapper to extract display data values for various types. */
    private static String displayDataString(@Nullable Object value) {
      if (value == null) {
        return "";
      }
      if (!value.getClass().isArray()) {
        return value.toString();
      }
      if (!value.getClass().getComponentType().isPrimitive()) {
        return Arrays.deepToString((Object[]) value);
      }

      // At this point, we have some type of primitive array. Arrays.deepToString(..) requires an
      // Object array, but will unwrap nested primitive arrays.
      String wrapped = Arrays.deepToString(new Object[] {value});
      return wrapped.substring(1, wrapped.length() - 1);
    }
  }

  /**
   * Marker interface used when the original {@link PipelineOptions} interface is not known at
   * runtime. This can occur if {@link PipelineOptions} are deserialized from JSON.
   *
   * <p>Pipeline authors can ensure {@link PipelineOptions} type information is available at runtime
   * by registering their {@link PipelineOptions options} interfaces. See the "Registration" section
   * of {@link PipelineOptions} documentation.
   */
  interface UnknownPipelineOptions extends PipelineOptions {}

  /**
   * Construct a mapping from an option name to its {@link PipelineOptions} interface(s)
   * declarations. An option may be declared in multiple interfaces. If it is overridden in a type
   * hierarchy, only the overriding interface will be included.
   */
  private Multimap<String, PipelineOptionSpec> buildOptionNameToSpecMap(
      Set<PipelineOptionSpec> props) {

    Multimap<String, PipelineOptionSpec> optionsMap = HashMultimap.create();
    for (PipelineOptionSpec prop : props) {
      optionsMap.put(prop.getName(), prop);
    }

    // Filter out overridden options
    for (Map.Entry<String, Collection<PipelineOptionSpec>> entry : optionsMap.asMap().entrySet()) {

      /* Compare all interfaces for an option pairwise (iface1, iface2) to look for type
      hierarchies. If one is the base-class of the other, remove it from the output and continue
      iterating.

      This is an N^2 operation per-option, but the number of interfaces defining an option
      should always be small (usually 1). */
      List<PipelineOptionSpec> specs = Lists.newArrayList(entry.getValue());
      if (specs.size() < 2) {
        // Only one known implementing interface, no need to check for inheritance
        continue;
      }

      for (int i = 0; i < specs.size() - 1; i++) {
        Class<?> iface1 = specs.get(i).getDefiningInterface();
        for (int j = i + 1; j < specs.size(); j++) {
          Class<?> iface2 = specs.get(j).getDefiningInterface();

          if (iface1.isAssignableFrom(iface2)) {
            optionsMap.remove(entry.getKey(), specs.get(i));
            specs.remove(i);

            // Removed element at current "i" index. Set iterators to re-evaluate
            // new "i" element in outer loop.
            i--;
            j = specs.size();
          } else if (iface2.isAssignableFrom(iface1)) {
            optionsMap.remove(entry.getKey(), specs.get(j));
            specs.remove(j);

            // Removed element at current "j" index. Set iterator to re-evaluate
            // new "j" element in inner-loop.
            j--;
          }
        }
      }
    }

    return optionsMap;
  }

  /**
   * This will output all the currently set values. This is a relatively costly function as it will
   * call {@code toString()} on each object that has been set and format the results in a readable
   * format.
   *
   * @return A pretty printed string representation of this.
   */
  @Override
  public String toString() {
    // Add the options that we received from deserialization
    SortedMap<String, Object> sortedOptions = new TreeMap<>(jsonOptions);
    // Override with any programmatically set options.
    for (Map.Entry<String, BoundValue> entry : options.entrySet()) {
      sortedOptions.put(entry.getKey(), entry.getValue().getValue());
    }

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
    JsonNode jsonNode = jsonOptions.get(propertyName);
    return getValueFromJson(jsonNode, method);
  }

  private static Object getValueFromJson(JsonNode node, Method method) {
    try {
      return PipelineOptionsFactory.deserializeNode(node, method);
    } catch (IOException e) {
      throw new RuntimeException("Unable to parse representation", e);
    }
  }

  /**
   * Returns a default value for the method based upon {@code @Default} metadata on the getter to
   * return values. If there is no {@code @Default} annotation on the getter, then a <a
   * href="https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html">default</a> as
   * per the Java Language Specification for the expected return type is returned.
   *
   * @param proxy The proxy object for which we are attempting to get the default.
   * @param method The getter method that was invoked.
   * @return The default value from an {@link Default} annotation if present, otherwise a default
   *     value as per the Java Language Specification.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private Object getDefault(PipelineOptions proxy, Method method) {
    if (method.getReturnType().equals(RuntimeValueProvider.class)) {
      throw new RuntimeException(
          String.format(
              "Method %s should not have return type "
                  + "RuntimeValueProvider, use ValueProvider instead.",
              method.getName()));
    }
    if (method.getReturnType().equals(StaticValueProvider.class)) {
      throw new RuntimeException(
          String.format(
              "Method %s should not have return type "
                  + "StaticValueProvider, use ValueProvider instead.",
              method.getName()));
    }
    @Nullable Object defaultObject = null;
    for (Annotation annotation : method.getAnnotations()) {
      defaultObject = returnDefaultHelper(annotation, proxy, method);
      if (defaultObject != null) {
        break;
      }
    }
    if (method.getReturnType().equals(ValueProvider.class)) {
      String propertyName = computedProperties.gettersToPropertyNames.get(method.getName());
      return defaultObject == null
          ? new RuntimeValueProvider(
              method.getName(),
              propertyName,
              (Class<? extends PipelineOptions>) method.getDeclaringClass(),
              proxy.getOptionsId())
          : new RuntimeValueProvider(
              method.getName(),
              propertyName,
              (Class<? extends PipelineOptions>) method.getDeclaringClass(),
              defaultObject,
              proxy.getOptionsId());
    } else if (defaultObject != null) {
      return defaultObject;
    }

    /*
     * We need to make sure that we return something appropriate for the return type. Thus we return
     * a default value as defined by the JLS.
     */
    return Defaults.defaultValue(method.getReturnType());
  }

  /** Helper method to return standard Default cases. */
  private @Nullable Object returnDefaultHelper(
      Annotation annotation, PipelineOptions proxy, Method method) {
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
      return Enum.valueOf(
          (Class<Enum>) method.getReturnType(), ((Default.Enum) annotation).value());
    } else if (annotation instanceof Default.InstanceFactory) {
      return InstanceBuilder.ofType(((Default.InstanceFactory) annotation).value())
          .build()
          .create(proxy);
    }
    return null;
  }

  /**
   * Returns a map from the getters method name to the name of the property based upon the passed in
   * {@link PropertyDescriptor}s property descriptors.
   *
   * @param propertyDescriptors A list of {@link PropertyDescriptor}s to use when generating the
   *     map.
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
   *     map.
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
    private static void serializeEntry(
        String name,
        Object value,
        JsonGenerator jgen,
        Map<String, JsonSerializer<Object>> customSerializers)
        throws IOException {

      JsonSerializer<Object> customSerializer = customSerializers.get(name);
      if (value == null || customSerializer == null || value instanceof JsonNode) {
        jgen.writeObject(value);
      } else {
        customSerializer.serialize(value, jgen, PipelineOptionsFactory.SERIALIZER_PROVIDER);
      }
    }

    @Override
    public void serialize(PipelineOptions value, JsonGenerator jgen, SerializerProvider provider)
        throws IOException {
      ProxyInvocationHandler handler = (ProxyInvocationHandler) Proxy.getInvocationHandler(value);
      PipelineOptionsFactory.Cache cache = PipelineOptionsFactory.CACHE.get();
      // We first copy and then filter out any properties that have been modified since
      // the last serialization of this PipelineOptions and then verify that
      // they are all serializable.
      Map<String, BoundValue> filteredOptions = Maps.newHashMap(handler.options);
      Set<Class<? extends PipelineOptions>> knownInterfaces =
          handler.computedProperties.knownInterfaces;
      Map<String, JsonSerializer<Object>> propertyToSerializer =
          getSerializerMap(cache, knownInterfaces);
      removeIgnoredOptions(cache, knownInterfaces, filteredOptions);
      ensureSerializable(cache, knownInterfaces, filteredOptions, propertyToSerializer);

      // Now we create the map of serializable options by taking the original
      // set of serialized options (if any) and updating them with any properties
      // instances that have been modified since the previous serialization.
      Map<String, Object> serializableOptions = Maps.newHashMap(handler.jsonOptions);
      for (Map.Entry<String, BoundValue> entry : filteredOptions.entrySet()) {
        serializableOptions.put(entry.getKey(), entry.getValue().getValue());
      }

      jgen.writeStartObject();
      jgen.writeFieldName("options");

      jgen.writeStartObject();

      for (Map.Entry<String, Object> entry : serializableOptions.entrySet()) {
        jgen.writeFieldName(entry.getKey());
        serializeEntry(entry.getKey(), entry.getValue(), jgen, propertyToSerializer);
      }

      jgen.writeEndObject();

      List<Map<String, Object>> serializedDisplayData = Lists.newArrayList();
      DisplayData displayData = DisplayData.from(value);
      for (DisplayData.Item item : displayData.items()) {
        @SuppressWarnings("unchecked")
        Map<String, Object> serializedItem =
            PipelineOptionsFactory.MAPPER.convertValue(item, Map.class);
        serializedDisplayData.add(serializedItem);
      }

      jgen.writeFieldName("display_data");
      jgen.writeObject(serializedDisplayData);

      jgen.writeNumberField("revision", handler.revision.get());
      jgen.writeEndObject();
    }

    private static Map<String, JsonSerializer<Object>> getSerializerMap(
        PipelineOptionsFactory.Cache cache, Set<Class<? extends PipelineOptions>> interfaces) {

      Map<String, JsonSerializer<Object>> propertyToSerializer = Maps.newHashMap();
      for (PropertyDescriptor descriptor : cache.getPropertyDescriptors(interfaces)) {
        if (descriptor.getReadMethod() != null) {
          JsonSerializer<Object> maybeSerializer =
              PipelineOptionsFactory.getCustomSerializerForMethod(descriptor.getReadMethod());
          if (maybeSerializer != null) {
            propertyToSerializer.put(descriptor.getName(), maybeSerializer);
          }
        }
      }

      return propertyToSerializer;
    }

    /**
     * We remove all properties within the passed in options where there getter is annotated with
     * {@link JsonIgnore @JsonIgnore} from the passed in options using the passed in interfaces.
     */
    private static void removeIgnoredOptions(
        PipelineOptionsFactory.Cache cache,
        Set<Class<? extends PipelineOptions>> interfaces,
        Map<String, ?> options) {
      // Find all the method names that are annotated with JSON ignore.
      Set<String> jsonIgnoreMethodNames =
          FluentIterable.from(ReflectHelpers.getClosureOfMethodsOnInterfaces(interfaces))
              .filter(AnnotationPredicates.JSON_IGNORE.forMethod)
              .transform(Method::getName)
              .toSet();

      // Remove all options that have the same method name as the descriptor.
      for (PropertyDescriptor descriptor : cache.getPropertyDescriptors(interfaces)) {
        if (jsonIgnoreMethodNames.contains(descriptor.getReadMethod().getName())) {
          options.remove(descriptor.getName());
        }
      }
    }

    /**
     * We use an {@link ObjectMapper} to verify that the passed in options are serializable and
     * deserializable.
     */
    private static void ensureSerializable(
        PipelineOptionsFactory.Cache cache,
        Set<Class<? extends PipelineOptions>> interfaces,
        Map<String, BoundValue> options,
        Map<String, JsonSerializer<Object>> propertyToSerializer)
        throws IOException {
      // Construct a map from property name to the return type of the getter.
      Map<String, Method> propertyToReadMethod = Maps.newHashMap();
      for (PropertyDescriptor descriptor : cache.getPropertyDescriptors(interfaces)) {
        if (descriptor.getReadMethod() != null) {
          propertyToReadMethod.put(descriptor.getName(), descriptor.getReadMethod());
        }
      }

      // Attempt to serialize and deserialize each property.
      for (Map.Entry<String, BoundValue> entry : options.entrySet()) {
        try {
          Object boundValue = entry.getValue().getValue();
          if (boundValue != null) {
            TokenBuffer buffer = new TokenBuffer(PipelineOptionsFactory.MAPPER, false);
            serializeEntry(entry.getKey(), boundValue, buffer, propertyToSerializer);
            Method method = propertyToReadMethod.get(entry.getKey());
            getValueFromJson(buffer.asParser().<JsonNode>readValueAsTree(), method);
          }
        } catch (Exception e) {
          throw new IOException(
              String.format(
                  "Failed to serialize and deserialize property '%s' with value '%s'",
                  entry.getKey(), entry.getValue().getValue()),
              e);
        }
      }
    }
  }

  static class Deserializer extends JsonDeserializer<PipelineOptions> {
    @Override
    public PipelineOptions deserialize(JsonParser jp, DeserializationContext ctxt)
        throws IOException {
      ObjectNode objectNode = jp.readValueAsTree();
      JsonNode rawOptionsNode = objectNode.get("options");

      Map<String, JsonNode> fields = Maps.newHashMap();
      if (rawOptionsNode != null && !rawOptionsNode.isNull()) {
        ObjectNode optionsNode = (ObjectNode) rawOptionsNode;
        for (Iterator<Map.Entry<String, JsonNode>> iterator = optionsNode.fields();
            iterator != null && iterator.hasNext(); ) {
          Map.Entry<String, JsonNode> field = iterator.next();
          fields.put(field.getKey(), field.getValue());
        }
      }
      int revision = objectNode.hasNonNull("revision") ? objectNode.get("revision").asInt() : 0;
      PipelineOptions options =
          new ProxyInvocationHandler(Maps.newHashMap(), fields, revision).as(PipelineOptions.class);
      ValueProvider.RuntimeValueProvider.setRuntimeOptions(options);
      return options;
    }
  }
}
