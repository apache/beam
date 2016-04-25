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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;

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
public class DisplayData {
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
    checkNotNull(component);
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
   *     builder.add("foo", type.get(), foo);
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

  private static String namespaceOf(ClassForDisplay clazz) {
    return clazz.getName();
  }

  /**
   * Utility to build up display data from a component and its included
   * subcomponents.
   */
  public interface Builder {
    /**
     * Register display data from the specified subcomponent.
     *
     * @see #include(HasDisplayData, String)
     */
    Builder include(HasDisplayData subComponent);

    /**
     * Register display data from the specified subcomponent, using the specified namespace.
     *
     * @see #include(HasDisplayData, String)
     */
    Builder include(HasDisplayData subComponent, Class<?> namespace);

    /**
     * Register display data from the specified subcomponent, using the specified namespace.
     *
     * @see #include(HasDisplayData, String)
     */
    Builder include(HasDisplayData subComponent, ClassForDisplay namespace);

    /**
     * Register display data from the specified subcomponent, using the specified namespace.
     *
     * <p>For example, a {@link ParDo} transform includes display data from the encapsulated
     * {@link DoFn}.
     */
    Builder include(HasDisplayData subComponent, String namespace);

    /**
     * Register the given display data item.
     */
    Builder add(Item<?> item);

    /**
     * Register the given string display data if the value is not null.
     */
    Builder addIfNotNull(Item<?> item);

    /**
     * Register the given string display data if the value is different than the specified default.
     */
    <T> Builder addIfNotDefault(Item<T> item, @Nullable T defaultValue);
  }

  /**
   * A display data item. DisplayData items are registered via {@link DisplayData.Builder#add}
   * within {@link HasDisplayData#populateDisplayData} implementations. Each metadata item is
   * uniquely identified by the specified key and namespace generated from the registering
   * component's class name.
   */
  @AutoValue
  public abstract static class Item<T> {

    @Nullable
    public abstract String getNamespace();

    @JsonGetter("key")
    public abstract String getKey();

    /**
     * Retrieve the {@link DisplayData.Type} of display data. All metadata conforms to a
     * predefined set of allowed types.
     */
    @JsonGetter("type")
    public abstract Type getType();

    /**
     * Retrieve the value of the metadata item.
     */
    @JsonGetter("value")
    @Nullable
    public abstract Object getValue();

    /**
     * Return the optional short value for an item. Types may provide a short-value to displayed
     * instead of or in addition to the full {@link Item#getValue() value}.
     *
     * <p>Some display data types will not provide a short value, in which case the return value
     * will be null.
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

    public static <T> Item<T> create(String key, Type type, @Nullable T value) {
      FormattedItemValue formatted = safeFormat(type, value);
      return of(null, key, type, formatted.getLongValue(), formatted.getShortValue(), null, null);
    }

    private static FormattedItemValue safeFormat(Type type, @Nullable Object value) {
      if (value == null) {
        return FormattedItemValue.DEFAULT;
      }

      return type.format(value);
    }

    public Item<T> withNamespace(Class<?> namespace) {
      checkNotNull(namespace);
      return withNamespace(ClassForDisplay.of(namespace));
    }

    private Item<T> withNamespace(ClassForDisplay namespace) {
      checkNotNull(namespace);
      return withNamespace(namespaceOf(namespace));
    }

    public Item<T> withNamespace(String namespace) {
      checkNotNull(namespace);
      return of(
          namespace, getKey(), getType(), getValue(), getShortValue(), getLabel(), getLinkUrl());
    }

    public Item<T> withLabel(String label) {
      return of(
          getNamespace(), getKey(), getType(), getValue(), getShortValue(), label, getLinkUrl());
    }

    public Item<T> withLinkUrl(String url) {
      return of(getNamespace(), getKey(), getType(), getValue(), getShortValue(), getLabel(), url);
    }

    public Item<T> withValue(Object value) {
      FormattedItemValue formatted = safeFormat(getType(), value);
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

    public static Identifier of(ClassForDisplay namespace, String key) {
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
        if (value instanceof Class<?>) {
          ClassForDisplay classForDisplay = ClassForDisplay.of((Class<?>) value);
          return format(classForDisplay);
        }

        ClassForDisplay clazz = checkType(value, ClassForDisplay.class, JAVA_CLASS);
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
      } else if (value instanceof Class<?> || value instanceof ClassForDisplay) {
        return  JAVA_CLASS;
      } else if (value instanceof String) {
        return  STRING;
      } else {
        return null;
      }
    }
  }

  static class FormattedItemValue {
    static final FormattedItemValue DEFAULT = new FormattedItemValue(null);

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
      checkNotNull(subComponent);
      return include(subComponent, subComponent.getClass());
    }

    @Override
    public Builder include(HasDisplayData subComponent, Class<?> namespace) {
      checkNotNull(namespace);
      return include(subComponent, ClassForDisplay.of(namespace));
    }

    @Override
    public Builder include(HasDisplayData subComponent, ClassForDisplay namespace) {
      checkNotNull(namespace);
      return include(subComponent, namespaceOf(namespace));
    }

    @Override
    public Builder include(HasDisplayData subComponent, String namespace) {
      checkNotNull(subComponent);
      checkNotNull(namespace);

      boolean newComponent = visited.add(subComponent);
      if (newComponent) {
        String prevNs = this.latestNs;
        this.latestNs = namespace;
        subComponent.populateDisplayData(this);
        this.latestNs = prevNs;
      }

      return this;
    }

    @Override
    public Builder add(Item<?> item) {
      checkNotNull(item);
      return addItemIf(true, item);
    }

    @Override
    public Builder addIfNotNull(Item<?> item) {
      checkNotNull(item);
      return addItemIf(item.getValue() != null, item);
    }

    @Override
    public <T> Builder addIfNotDefault(Item<T> item, @Nullable T defaultValue) {
      checkNotNull(item);
      Item<T> defaultItem = item.withValue(defaultValue);
      return addItemIf(!Objects.equals(item, defaultItem), item);
    }

    private Builder addItemIf(boolean condition, Item<?> item) {
      if (!condition) {
        return this;
      }

      checkNotNull(item);
      checkNotNull(item.getValue());
      if (item.getNamespace() == null) {
        item = item.withNamespace(latestNs);
      }

      Identifier id = Identifier.of(item.getNamespace(), item.getKey());
      if (entries.containsKey(id)) {
        throw new IllegalArgumentException("DisplayData key already exists. All display data "
            + "for a component must be registered with a unique key.\nKey: " + id);
      }

      entries.put(id, item);
      return this;
    }

    private DisplayData build() {
      return new DisplayData(this.entries);
    }
  }

  public static Item<String> item(String key, @Nullable String value) {
    return item(key, Type.STRING, value);
  }

  public static Item<Integer> item(String key, @Nullable Integer value) {
    return item(key, Type.INTEGER, value);
  }

  public static Item<Long> item(String key, @Nullable Long value) {
    return item(key, Type.INTEGER, value);
  }

  public static Item<Float> item(String key, @Nullable Float value) {
    return item(key, Type.FLOAT, value);
  }

  public static Item<Double> item(String key, @Nullable Double value) {
    return item(key, Type.FLOAT, value);
  }

  public static Item<Boolean> item(String key, @Nullable Boolean value) {
    return item(key, Type.BOOLEAN, value);
  }

  public static Item<Instant> item(String key, @Nullable Instant value) {
    return item(key, Type.TIMESTAMP, value);
  }

  public static Item<Duration> item(String key, @Nullable Duration value) {
    return item(key, Type.DURATION, value);
  }

  public static <T> Item<Class<T>> item(String key, @Nullable Class<T> value) {
    return item(key, Type.JAVA_CLASS, value);
  }

  public static Item<ClassForDisplay> item(String key, @Nullable ClassForDisplay value) {
    return item(key, Type.JAVA_CLASS, value);
  }

  public static <T> Item<T> item(String key, Type type, @Nullable T value) {
    checkNotNull(key);
    checkNotNull(type);

    return Item.create(key, type, value);
  }
}
