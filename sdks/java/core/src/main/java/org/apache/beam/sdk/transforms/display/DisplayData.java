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
package org.apache.beam.sdk.transforms.display;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.transforms.PTransform;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonValue;

import org.apache.avro.reflect.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Static display data associated with a pipeline component. Display data is useful for
 * pipeline runner UIs and diagnostic dashboards to display details about
 * {@link PTransform PTransforms} that make up a pipeline.
 *
 * <p>Components specify their display data by implementing the {@link HasDisplayData}
 * interface.
 */
public class DisplayData implements Serializable {
  private static final DisplayData EMPTY = new DisplayData(Maps.<Identifier, Item<?>>newHashMap());
  private static final DateTimeFormatter TIMESTAMP_FORMATTER = ISODateTimeFormat.dateTime();

  private final ImmutableMap<Identifier, Item<?>> entries;

  private DisplayData(Map<Identifier, Item<?>> entries) {
    this.entries = ImmutableMap.copyOf(entries);
  }

  /**
   * Default empty {@link DisplayData} instance.
   */
  public static DisplayData none() {
    return EMPTY;
  }

  /**
   * Collect the {@link DisplayData} from a component. This will traverse all subcomponents
   * specified via {@link Builder#include} in the given component. Data in this component will be in
   * a namespace derived from the component.
   */
  public static DisplayData from(HasDisplayData component) {
    checkNotNull(component, "component argument cannot be null");
    return InternalBuilder.forRoot(component).build();
  }

  /**
   * Infer the {@link Type} for the given object.
   *
   * <p>Use this method if the type of metadata is not known at compile time. For example:
   *
   * <pre>
   * {@code
   * @Override
   * public void populateDisplayData(DisplayData.Builder builder) {
   *   Optional<DisplayData.Type> type = DisplayData.inferType(foo);
   *   if (type.isPresent()) {
   *     builder.add(DisplayData.item("foo", type.get(), foo));
   *   }
   * }
   * }
   * </pre>
   *
   * @return The inferred {@link Type}, or null if the type cannot be inferred,
   */
  @Nullable
  public static Type inferType(@Nullable Object value) {
    return Type.tryInferFrom(value);
  }

  @JsonValue
  public Collection<Item<?>> items() {
    return entries.values();
  }

  public Map<Identifier, Item<?>> asMap() {
    return entries;
  }

  @Override
  public int hashCode() {
    return entries.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof DisplayData) {
      DisplayData that = (DisplayData) obj;
      return Objects.equals(this.entries, that.entries);
    }

    return false;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    boolean isFirstLine = true;
    for (Item<?> entry : entries.values()) {
      if (isFirstLine) {
        isFirstLine = false;
      } else {
        builder.append("\n");
      }

      builder.append(entry);
    }

    return builder.toString();
  }

  private static String namespaceOf(Class<?> clazz) {
    return clazz.getName();
  }

  /**
   * Utility to build up display data from a component and its included
   * subcomponents.
   */
  public interface Builder {
    /**
     * Register display data from the specified subcomponent. For example, a {@link PTransform}
     * which delegates to a user-provided function can implement {@link HasDisplayData} on the
     * function and include it from the {@link PTransform}:
     *
     * <pre><code>{@literal @Override}
     * public void populateDisplayData(DisplayData.Builder builder) {
     *   super.populateDisplayData(builder);
     *
     *   builder
     *     .add(DisplayData.item("userFn", userFn)) // To register the class name of the userFn
     *     .include(userFn); // To allow the userFn to register additional display data
     * }
     * </code></pre>
     *
     * Using {@code include(subcomponent)} will associate each of the registered items with the
     * namespace of the {@code subcomponent} being registered. To register display data in the
     * current namespace, such as from a base class implementation, use
     * {@code subcomponent.populateDisplayData(builder)} instead.
     *
     * @see HasDisplayData#populateDisplayData(DisplayData.Builder)
     */
    Builder include(HasDisplayData subComponent);

    /**
     * Register display data from the specified subcomponent, overriding the namespace of
     * subcomponent display items with the specified namespace.
     *
     * @see #include(HasDisplayData)
     */
    Builder include(HasDisplayData subComponent, Class<?> namespace);

    /**
     * Register display data from the specified subcomponent, overriding the namespace of
     * subcomponent display items with the specified namespace.
     *
     * @see #include(HasDisplayData)
     */
    Builder include(HasDisplayData subComponent, String namespace);

    /**
     * Register the given display item.
     */
    Builder add(Item<?> item);

    /**
     * Register the given display item if the value is not null.
     */
    Builder addIfNotNull(Item<?> item);

    /**
     * Register the given display item if the value is different than the specified default.
     */
    <T> Builder addIfNotDefault(Item<T> item, @Nullable T defaultValue);
  }

  /**
   * {@link Item Items} are the unit of display data. Each item is identified by a given key
   * and namespace from the component the display item belongs to.
   *
   * <p>{@link Item Items} are registered via {@link DisplayData.Builder#add}
   * within {@link HasDisplayData#populateDisplayData} implementations.
   */
  @AutoValue
  public abstract static class Item<T> implements Serializable {

    /**
     * The namespace for the display item. The namespace defaults to the component which
     * the display item belongs to.
     */
    @Nullable
    @JsonGetter("namespace")
    public abstract String getNamespace();

    /**
     * The key for the display item. Each display item is created with a key and value
     * via {@link DisplayData#item).
     */
    @JsonGetter("key")
    public abstract String getKey();

    /**
     * Retrieve the {@link DisplayData.Type} of display data. All metadata conforms to a
     * predefined set of allowed types.
     */
    @JsonGetter("type")
    public abstract Type getType();

    /**
     * Retrieve the value of the display item. The value is translated from the input to
     * {@link DisplayData#item} into a format suitable for display. Translation is based on the
     * item's {@link #getType() type}.
     *
     * <p>The value will only be {@literal null} if the input value during creation was null.
     */
    @JsonGetter("value")
    @Nullable
    public abstract Object getValue();

    /**
     * Return the optional short value for an item, or null if none is provided.
     *
     * <p>The short value is an alternative display representation for items having a long display
     * value. For example, the {@link #getValue() value} for {@link Type#JAVA_CLASS} items contains
     * the full class name with package, while the short value contains just the class name.
     *
     * A {@link #getValue() value} will be provided for each display item, and some types may also
     * provide a short-value. If a short value is provided, display data consumers may
     * choose to display it instead of or in addition to the {@link #getValue() value}.
     */
    @JsonGetter("shortValue")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    public abstract Object getShortValue();

    /**
     * Retrieve the optional label for an item. The label is a human-readable description of what
     * the metadata represents. UIs may choose to display the label instead of the item key.
     *
     * <p>If no label was specified, this will return {@code null}.
     */
    @JsonGetter("label")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    public abstract String getLabel();

    /**
     * Retrieve the optional link URL for an item. The URL points to an address where the reader
     * can find additional context for the display data.
     *
     * <p>If no URL was specified, this will return {@code null}.
     */
    @JsonGetter("linkUrl")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    public abstract String getLinkUrl();

    private static <T> Item<T> create(String key, Type type, @Nullable T value) {
      FormattedItemValue formatted = type.safeFormat(value);
      return of(null, key, type, formatted.getLongValue(), formatted.getShortValue(), null, null);
    }

    /**
     * Set the item {@link Item#getNamespace() namespace} from the given {@link Class}.
     *
     * <p>This method does not alter the current instance, but instead returns a new {@link Item}
     * with the namespace set.
     */
    public Item<T> withNamespace(Class<?> namespace) {
      checkNotNull(namespace, "namespace argument cannot be null");
      return withNamespace(namespaceOf(namespace));
    }

    /** @see #withNamespace(Class) */
    public Item<T> withNamespace(String namespace) {
      checkNotNull(namespace, "namespace argument cannot be null");
      return of(
          namespace, getKey(), getType(), getValue(), getShortValue(), getLabel(), getLinkUrl());
    }

    /**
     * Set the item {@link Item#getLabel() label}.
     *
     * <p>Specifying a null value will clear the label if it was previously defined.
     *
     * <p>This method does not alter the current instance, but instead returns a new {@link Item}
     * with the label set.
     */
    public Item<T> withLabel(String label) {
      return of(
          getNamespace(), getKey(), getType(), getValue(), getShortValue(), label, getLinkUrl());
    }

    /**
     * Set the item {@link Item#getLinkUrl() link url}.
     *
     * <p>Specifying a null value will clear the link url if it was previously defined.
     *
     * <p>This method does not alter the current instance, but instead returns a new {@link Item}
     * with the link url set.
     */
    public Item<T> withLinkUrl(String url) {
      return of(getNamespace(), getKey(), getType(), getValue(), getShortValue(), getLabel(), url);
    }

    /**
     * Creates a similar item to the current instance but with the specified value.
     *
     * <p>This should only be used internally. It is useful to compare the value of a
     * {@link DisplayData.Item} to the value derived from a specified input.
     */
    private Item<T> withValue(Object value) {
      FormattedItemValue formatted = getType().safeFormat(value);
      return of(getNamespace(), getKey(), getType(), formatted.getLongValue(),
          formatted.getShortValue(), getLabel(), getLinkUrl());
    }

    private static <T> Item<T> of(
        @Nullable String namespace,
        String key,
        Type type,
        @Nullable Object value,
        @Nullable Object shortValue,
        @Nullable String label,
        @Nullable String linkUrl) {
      return new AutoValue_DisplayData_Item<>(
          namespace, key, type, value, shortValue, label, linkUrl);
    }

    @Override
    public String toString() {
      return String.format("%s:%s=%s", getNamespace(), getKey(), getValue());
    }
  }

  /**
   * Unique identifier for a display data item within a component.
   * Identifiers are composed of the key they are registered with and a namespace generated from
   * the class of the component which registered the item.
   *
   * <p>Display data registered with the same key from different components will have different
   * namespaces and thus will both be represented in the composed {@link DisplayData}. If a
   * single component registers multiple metadata items with the same key, only the most recent
   * item will be retained; previous versions are discarded.
   */
  public static class Identifier {
    private final String ns;
    private final String key;

    public static Identifier of(Class<?> namespace, String key) {
      return of(namespaceOf(namespace), key);
    }

    public static Identifier of(String namespace, String key) {
      return new Identifier(namespace, key);
    }

    private Identifier(String ns, String key) {
      this.ns = ns;
      this.key = key;
    }

    public String getNamespace() {
      return ns;
    }

    public String getKey() {
      return key;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof Identifier) {
        Identifier that = (Identifier) obj;
        return Objects.equals(this.ns, that.ns)
          && Objects.equals(this.key, that.key);
      }

      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(ns, key);
    }

    @Override
    public String toString() {
      return String.format("%s:%s", ns, key);
    }
  }

  /**
   * Display data type.
   */
  public enum Type {
    STRING {
      @Override
      FormattedItemValue format(Object value) {
        return new FormattedItemValue(checkType(value, String.class, STRING));
      }
    },
    INTEGER {
      @Override
      FormattedItemValue format(Object value) {
        if (value instanceof Integer) {
          long l = ((Integer) value).longValue();
          return format(l);
        }

        return new FormattedItemValue(checkType(value, Long.class, INTEGER));
      }
    },
    FLOAT {
      @Override
      FormattedItemValue format(Object value) {
        return new FormattedItemValue(checkType(value, Number.class, FLOAT));
      }
    },
    BOOLEAN() {
      @Override
      FormattedItemValue format(Object value) {
        return new FormattedItemValue(checkType(value, Boolean.class, BOOLEAN));
      }
    },
    TIMESTAMP() {
      @Override
      FormattedItemValue format(Object value) {
        Instant instant = checkType(value, Instant.class, TIMESTAMP);
        return new FormattedItemValue((TIMESTAMP_FORMATTER.print(instant)));
      }
    },
    DURATION {
      @Override
      FormattedItemValue format(Object value) {
        Duration duration = checkType(value, Duration.class, DURATION);
        return new FormattedItemValue(duration.getMillis());
      }
    },
    JAVA_CLASS {
      @Override
      FormattedItemValue format(Object value) {
        Class<?> clazz = checkType(value, Class.class, JAVA_CLASS);
        return new FormattedItemValue(clazz.getName(), clazz.getSimpleName());
      }
    };

    private static <T> T checkType(Object value, Class<T> clazz, DisplayData.Type expectedType) {
      if (!clazz.isAssignableFrom(value.getClass())) {
        throw new ClassCastException(String.format(
            "Value is not valid for DisplayData type %s: %s", expectedType, value));
      }

      @SuppressWarnings("unchecked") // type checked above.
      T typedValue = (T) value;
      return typedValue;
    }

    /**
     * Format the display data value into a long string representation, and optionally
     * a shorter representation for display.
     *
     * <p>Internal-only. Value objects can be safely cast to the expected Java type.
     */
    abstract FormattedItemValue format(Object value);

    /**
     * Safe version of {@link Type#format(Object)}, which checks for null input value and if so
     * returns a {@link FormattedItemValue} with null value properties.
     *
     * @see #format(Object)
     */
    FormattedItemValue safeFormat(@Nullable Object value) {
      if (value == null) {
        return FormattedItemValue.NULL_VALUES;
      }

      return format(value);
    }

    @Nullable
    private static Type tryInferFrom(@Nullable Object value) {
      if (value instanceof Integer || value instanceof Long) {
        return INTEGER;
      } else if (value instanceof Double || value instanceof Float) {
        return  FLOAT;
      } else if (value instanceof Boolean) {
        return  BOOLEAN;
      } else if (value instanceof Instant) {
        return  TIMESTAMP;
      } else if (value instanceof Duration) {
        return  DURATION;
      } else if (value instanceof Class<?>) {
        return  JAVA_CLASS;
      } else if (value instanceof String) {
        return  STRING;
      } else {
        return null;
      }
    }
  }

  static class FormattedItemValue {
    /**
     * Default instance which contains null values.
     */
    private static final FormattedItemValue NULL_VALUES = new FormattedItemValue(null);

    private final Object shortValue;
    private final Object longValue;

    private FormattedItemValue(Object longValue) {
      this(longValue, null);
    }

    private FormattedItemValue(Object longValue, Object shortValue) {
      this.longValue = longValue;
      this.shortValue = shortValue;
    }

    Object getLongValue() {
      return this.longValue;
    }

    Object getShortValue() {
      return this.shortValue;
    }
  }

  private static class InternalBuilder implements Builder {
    private final Map<Identifier, Item<?>> entries;
    private final Set<Object> visited;

    private String latestNs;

    private InternalBuilder() {
      this.entries = Maps.newHashMap();
      this.visited = Sets.newIdentityHashSet();
    }

    private static InternalBuilder forRoot(HasDisplayData instance) {
      InternalBuilder builder = new InternalBuilder();
      builder.include(instance);
      return builder;
    }

    @Override
    public Builder include(HasDisplayData subComponent) {
      checkNotNull(subComponent, "subComponent argument cannot be null");
      return include(subComponent, subComponent.getClass());
    }

    @Override
    public Builder include(HasDisplayData subComponent, Class<?> namespace) {
      checkNotNull(namespace, "Input namespace override cannot be null");
      return include(subComponent, namespaceOf(namespace));
    }

    @Override
    public Builder include(HasDisplayData subComponent, String namespace) {
      checkNotNull(subComponent, "subComponent argument cannot be null");
      checkNotNull(namespace, "Input namespace override cannot be null");

      boolean newComponent = visited.add(subComponent);
      if (newComponent) {
        String prevNs = this.latestNs;
        this.latestNs = namespace;

        try {
          subComponent.populateDisplayData(this);
        } catch (PopulateDisplayDataException e) {
          // Don't re-wrap exceptions recursively.
          throw e;
        } catch (Throwable e) {
          String msg = String.format("Error while populating display data for component: %s",
              namespace);
          throw new PopulateDisplayDataException(msg, e);
        }

        this.latestNs = prevNs;
      }

      return this;
    }

    /**
     * Marker exception class for exceptions encountered while populating display data.
     */
    private static class PopulateDisplayDataException extends RuntimeException {
      PopulateDisplayDataException(String message, Throwable cause) {
        super(message, cause);
      }
    }

    @Override
    public Builder add(Item<?> item) {
      checkNotNull(item, "Input display item cannot be null");
      return addItemIf(true, item);
    }

    @Override
    public Builder addIfNotNull(Item<?> item) {
      checkNotNull(item, "Input display item cannot be null");
      return addItemIf(item.getValue() != null, item);
    }

    @Override
    public <T> Builder addIfNotDefault(Item<T> item, @Nullable T defaultValue) {
      checkNotNull(item, "Input display item cannot be null");
      Item<T> defaultItem = item.withValue(defaultValue);
      return addItemIf(!Objects.equals(item, defaultItem), item);
    }

    private Builder addItemIf(boolean condition, Item<?> item) {
      if (!condition) {
        return this;
      }

      checkNotNull(item, "Input display item cannot be null");
      checkNotNull(item.getValue(), "Input display value cannot be null");
      if (item.getNamespace() == null) {
        item = item.withNamespace(latestNs);
      }

      Identifier id = Identifier.of(item.getNamespace(), item.getKey());
      checkArgument(!entries.containsKey(id),
          "Display data key (%s) is not unique within the specified namespace (%s).",
          item.getKey(), item.getNamespace());

      entries.put(id, item);
      return this;
    }

    private DisplayData build() {
      return new DisplayData(this.entries);
    }
  }

  /**
   * Create a display item for the specified key and string value.
   */
  public static Item<String> item(String key, @Nullable String value) {
    return item(key, Type.STRING, value);
  }

  /**
   * Create a display item for the specified key and integer value.
   */
  public static Item<Integer> item(String key, @Nullable Integer value) {
    return item(key, Type.INTEGER, value);
  }

  /**
   * Create a display item for the specified key and integer value.
   */
  public static Item<Long> item(String key, @Nullable Long value) {
    return item(key, Type.INTEGER, value);
  }

  /**
   * Create a display item for the specified key and floating point value.
   */
  public static Item<Float> item(String key, @Nullable Float value) {
    return item(key, Type.FLOAT, value);
  }

  /**
   * Create a display item for the specified key and floating point value.
   */
  public static Item<Double> item(String key, @Nullable Double value) {
    return item(key, Type.FLOAT, value);
  }

  /**
   * Create a display item for the specified key and boolean value.
   */
  public static Item<Boolean> item(String key, @Nullable Boolean value) {
    return item(key, Type.BOOLEAN, value);
  }

  /**
   * Create a display item for the specified key and timestamp value.
   */
  public static Item<Instant> item(String key, @Nullable Instant value) {
    return item(key, Type.TIMESTAMP, value);
  }

  /**
   * Create a display item for the specified key and duration value.
   */
  public static Item<Duration> item(String key, @Nullable Duration value) {
    return item(key, Type.DURATION, value);
  }

  /**
   * Create a display item for the specified key and class value.
   */
  public static <T> Item<Class<T>> item(String key, @Nullable Class<T> value) {
    return item(key, Type.JAVA_CLASS, value);
  }

  /**
   * Create a display item for the specified key, type, and value. This method should be used
   * if the type of the input value can only be determined at runtime. Otherwise,
   * {@link HasDisplayData} implementors should call one of the typed factory methods, such as
   * {@link #item(String, String)} or {@link #item(String, Integer)}.
   *
   * @throws ClassCastException if the value cannot be formatted as the given type.
   *
   *  @see Type#inferType(Object)
   */
  public static <T> Item<T> item(String key, Type type, @Nullable T value) {
    checkNotNull(key, "key argument cannot be null");
    checkNotNull(type, "type argument cannot be null");

    return Item.create(key, type, value);
  }
}
