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
package com.google.cloud.dataflow.sdk.transforms.display;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;

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
 * Static display metadata associated with a pipeline component. Display data is useful for
 * pipeline runner UIs and diagnostic dashboards to display details about
 * {@link PTransform PTransforms} that make up a pipeline.
 *
 * <p>Components specify their display data by implementing the {@link HasDisplayData}
 * interface.
 */
public class DisplayData {
  private static final DisplayData EMPTY = new DisplayData(Maps.<Identifier, Item>newHashMap());
  private static final DateTimeFormatter TIMESTAMP_FORMATTER = ISODateTimeFormat.dateTime();

  private final ImmutableMap<Identifier, Item> entries;

  private DisplayData(Map<Identifier, Item> entries) {
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

  public Collection<Item> items() {
    return entries.values();
  }

  public Map<Identifier, Item> asMap() {
    return entries;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    boolean isFirstLine = true;
    for (Item entry : entries.values()) {
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
   * Utility to build up display metadata from a component and its included
   * subcomponents.
   */
  public interface Builder {
    /**
     * Register display metadata from the specified subcomponent. For example, a {@link ParDo}
     * transform includes display metadata from the encapsulated {@link DoFn}.
     */
    Builder include(HasDisplayData subComponent);

    /**
     * Register display metadata from the specified subcomponent, using the specified namespace.
     * For example, a {@link ParDo} transform includes display metadata from the encapsulated
     * {@link DoFn}.
     */
    Builder include(HasDisplayData subComponent, Class<?> namespace);

    /**
     * Register the given string display metadata. The metadata item will be registered with type
     * {@link DisplayData.Type#STRING}, and is identified by the specified key and namespace from
     * the current transform or component.
     */
    ItemBuilder add(String key, String value);

    /**
     * Register the given string display data if the value is not null.
     *
     * @see DisplayData.Builder#add(String, String)
     */
    ItemBuilder addIfNotNull(String key, @Nullable String value);

    /**
     * Register the given string display data if the value is different than the specified default.
     *
     * @see DisplayData.Builder#add(String, String)
     */
    ItemBuilder addIfNotDefault(String key, @Nullable String value, @Nullable String defaultValue);

    /**
     * Register the given numeric display metadata. The metadata item will be registered with type
     * {@link DisplayData.Type#INTEGER}, and is identified by the specified key and namespace from
     * the current transform or component.
     */
    ItemBuilder add(String key, long value);

    /**
     * Register the given numeric display data if the value is different than the specified default.
     *
     * @see DisplayData.Builder#add(String, long)
     */
    ItemBuilder addIfNotDefault(String key, long value, long defaultValue);

    /**
     * Register the given floating point display metadata. The metadata item will be registered with
     * type {@link DisplayData.Type#FLOAT}, and is identified by the specified key and namespace
     * from the current transform or component.
     */
    ItemBuilder add(String key, double value);

    /**
     * Register the given floating point display data if the value is different than the specified
     * default.
     *
     * @see DisplayData.Builder#add(String, double)
     */
    ItemBuilder addIfNotDefault(String key, double value, double defaultValue);

    /**
     * Register the given boolean display metadata. The metadata item will be registered with
     * type {@link DisplayData.Type#BOOLEAN}, and is identified by the specified key and namespace
     * from the current transform or component.
     */
    ItemBuilder add(String key, boolean value);

    /**
     * Register the given boolean display data if the value is different than the specified default.
     *
     * @see DisplayData.Builder#add(String, boolean)
     */
    ItemBuilder addIfNotDefault(String key, boolean value, boolean defaultValue);

    /**
     * Register the given timestamp display metadata. The metadata item will be registered with type
     * {@link DisplayData.Type#TIMESTAMP}, and is identified by the specified key and namespace from
     * the current transform or component.
     */
    ItemBuilder add(String key, Instant value);

    /**
     * Register the given timestamp display data if the value is not null.
     *
     * @see DisplayData.Builder#add(String, Instant)
     */
    ItemBuilder addIfNotNull(String key, @Nullable Instant value);

    /**
     * Register the given timestamp display data if the value is different than the specified
     * default.
     *
     * @see DisplayData.Builder#add(String, Instant)
     */
    ItemBuilder addIfNotDefault(
        String key, @Nullable Instant value, @Nullable Instant defaultValue);

    /**
     * Register the given duration display metadata. The metadata item will be registered with type
     * {@link DisplayData.Type#DURATION}, and is identified by the specified key and namespace from
     * the current transform or component.
     */
    ItemBuilder add(String key, Duration value);

    /**
     * Register the given duration display data if the value is not null.
     *
     * @see DisplayData.Builder#add(String, Duration)
     */
    ItemBuilder addIfNotNull(String key, @Nullable Duration value);

    /**
     * Register the given duration display data if the value is different than the specified
     * default.
     *
     * @see DisplayData.Builder#add(String, Duration)
     */
    ItemBuilder addIfNotDefault(
        String key, @Nullable Duration value, @Nullable Duration defaultValue);

    /**
     * Register the given class display metadata. The metadata item will be registered with type
     * {@link DisplayData.Type#JAVA_CLASS}, and is identified by the specified key and namespace
     * from the current transform or component.
     */
    ItemBuilder add(String key, Class<?> value);

    /**
     * Register the given class display data if the value is not null.
     *
     * @see DisplayData.Builder#add(String, Class)
     */
    ItemBuilder addIfNotNull(String key, @Nullable Class<?> value);

    /**
     * Register the given class display data if the value is different than the specified default.
     *
     * @see DisplayData.Builder#add(String, Class)
     */
    ItemBuilder addIfNotDefault(
        String key, @Nullable Class<?> value, @Nullable Class<?> defaultValue);

  /**
   * Register the given display metadata with the specified type.
   *
   * <p> The added display data is identified by the specified key and namespace from the current
   * transform or component.
   *
   * @throws ClassCastException if the value cannot be safely cast to the specified type.
   * @see DisplayData#inferType(Object)
   */
    ItemBuilder add(String key, Type type, Object value);
  }

  /**
   * Utility to append optional fields to display metadata, or register additional display metadata
   * items.
   */
  public interface ItemBuilder extends Builder {
    /**
     * Add a human-readable label to describe the most-recently added metadata field.
     * A label is optional; if unspecified, UIs should display the metadata key to identify the
     * display item.
     *
     * <p>Specifying a null value will clear the label if it was previously defined.
     */
    ItemBuilder withLabel(@Nullable String label);

    /**
     * Add a link URL to the most-recently added display metadata. A link URL is optional and
     * can be provided to point the reader to additional details about the metadata.
     *
     * <p>Specifying a null value will clear the URL if it was previously defined.
     */
    ItemBuilder withLinkUrl(@Nullable String url);

    /**
     * Adds an explicit namespace to the most-recently added display metadata. The namespace
     * and key uniquely identify the display metadata.
     *
     * <p>Leaving the namespace unspecified will default to the registering instance's class.
     */
    ItemBuilder withNamespace(Class<?> namespace);
  }

  /**
   * A display metadata item. DisplayData items are registered via {@link Builder#add} within
   * {@link HasDisplayData#populateDisplayData} implementations. Each metadata item is uniquely
   * identified by the specified key and namespace generated from the registering component's
   * class name.
   */
  public static class Item {
    private final String key;
    private final String ns;
    private final Type type;
    private final String value;
    private final String shortValue;
    private final String label;
    private final String url;

    private static Item create(Class<?> nsClass, String key, Type type, Object value) {
      FormattedItemValue formatted = type.format(value);
      String namespace = namespaceOf(nsClass);
      return new Item(
          namespace, key, type, formatted.getLongValue(), formatted.getShortValue(), null, null);
    }

    private Item(
        String namespace,
        String key,
        Type type,
        String value,
        String shortValue,
        String url,
        String label) {
      this.ns = namespace;
      this.key = key;
      this.type = type;
      this.value = value;
      this.shortValue = shortValue;
      this.url = url;
      this.label = label;
    }

    @JsonGetter("namespace")
    public String getNamespace() {
      return ns;
    }

    @JsonGetter("key")
    public String getKey() {
      return key;
    }

    /**
     * Retrieve the {@link DisplayData.Type} of display metadata. All metadata conforms to a
     * predefined set of allowed types.
     */
    @JsonGetter("type")
    public Type getType() {
      return type;
    }

    /**
     * Retrieve the value of the metadata item.
     */
    @JsonGetter("value")
    public String getValue() {
      return value;
    }

    /**
     * Return the optional short value for an item. Types may provide a short-value to displayed
     * instead of or in addition to the full {@link Item#value}.
     *
     * <p>Some display data types will not provide a short value, in which case the return value
     * will be null.
     */
    @JsonGetter("shortValue")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    public String getShortValue() {
      return shortValue;
    }

    /**
     * Retrieve the optional label for an item. The label is a human-readable description of what
     * the metadata represents. UIs may choose to display the label instead of the item key.
     *
     * <p>If no label was specified, this will return {@code null}.
     */
    @JsonGetter("label")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    public String getLabel() {
      return label;
    }

    /**
     * Retrieve the optional link URL for an item. The URL points to an address where the reader
     * can find additional context for the display metadata.
     *
     * <p>If no URL was specified, this will return {@code null}.
     */
    @JsonGetter("linkUrl")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    public String getLinkUrl() {
      return url;
    }

    @Override
    public String toString() {
      return String.format("%s:%s=%s", ns, key, value);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof Item) {
        Item that = (Item) obj;
        return Objects.equals(this.ns, that.ns)
            && Objects.equals(this.key, that.key)
            && Objects.equals(this.type, that.type)
            && Objects.equals(this.value, that.value)
            && Objects.equals(this.shortValue, that.shortValue)
            && Objects.equals(this.label, that.label)
            && Objects.equals(this.url, that.url);
      }

      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          this.ns,
          this.key,
          this.type,
          this.value,
          this.shortValue,
          this.label,
          this.url);
    }

    private Item withLabel(String label) {
      return new Item(this.ns, this.key, this.type, this.value, this.shortValue, this.url, label);
    }

    private Item withUrl(String url) {
      return new Item(this.ns, this.key, this.type, this.value, this.shortValue, url, this.label);
    }

    private Item withNamespace(Class<?> nsClass) {
      String namespace = namespaceOf(nsClass);
      return new Item(
          namespace, this.key, this.type, this.value, this.shortValue, this.url, this.label);
    }
  }

  /**
   * Unique identifier for a display metadata item within a component.
   * Identifiers are composed of the key they are registered with and a namespace generated from
   * the class of the component which registered the item.
   *
   * <p>Display metadata registered with the same key from different components will have different
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
   * Display metadata type.
   */
  public enum Type {
    STRING {
      @Override
      FormattedItemValue format(Object value) {
        return new FormattedItemValue(value.toString());
      }
    },
    INTEGER {
      @Override
      FormattedItemValue format(Object value) {
        Number number = (Number) value;
        return new FormattedItemValue(Long.toString(number.longValue()));
      }
    },
    FLOAT {
      @Override
      FormattedItemValue format(Object value) {
        return new FormattedItemValue(Double.toString((Double) value));
      }
    },
    BOOLEAN() {
      @Override
      FormattedItemValue format(Object value) {
        return new FormattedItemValue(Boolean.toString((boolean) value));
      }
    },
    TIMESTAMP() {
      @Override
      FormattedItemValue format(Object value) {
        return new FormattedItemValue((TIMESTAMP_FORMATTER.print((Instant) value)));
      }
    },
    DURATION {
      @Override
      FormattedItemValue format(Object value) {
        return new FormattedItemValue(Long.toString(((Duration) value).getMillis()));
      }
    },
    JAVA_CLASS {
      @Override
      FormattedItemValue format(Object value) {
        Class<?> clazz = (Class<?>) value;
        return new FormattedItemValue(clazz.getName(), clazz.getSimpleName());
      }
    };

    /**
     * Format the display metadata value into a long string representation, and optionally
     * a shorter representation for display.
     *
     * <p>Internal-only. Value objects can be safely cast to the expected Java type.
     */
    abstract FormattedItemValue format(Object value);

    @Nullable
    private static Type tryInferFrom(@Nullable Object value) {
      Type type;
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
    private final String shortValue;
    private final String longValue;

    private FormattedItemValue(String longValue) {
      this(longValue, null);
    }

    private FormattedItemValue(String longValue, String shortValue) {
      this.longValue = longValue;
      this.shortValue = shortValue;
    }

    String getLongValue() {
      return this.longValue;
    }

    String getShortValue() {
      return this.shortValue;
    }
  }

  private static class InternalBuilder implements ItemBuilder {
    private final Map<Identifier, Item> entries;
    private final Set<Object> visited;

    private Class<?> latestNs;

    @Nullable
    private Item latestItem;

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
      checkNotNull(subComponent);
      checkNotNull(namespace);

      commitLatest();
      boolean newComponent = visited.add(subComponent);
      if (newComponent) {
        Class prevNs = this.latestNs;
        this.latestNs = namespace;
        subComponent.populateDisplayData(this);
        this.latestNs = prevNs;
      }

      return this;
    }

    @Override
    public ItemBuilder add(String key, String value) {
      checkNotNull(value);
      return addItemIf(true, key, Type.STRING, value);
    }

    @Override
    public ItemBuilder addIfNotNull(String key, @Nullable String value) {
      return addItemIf(value != null, key, Type.STRING, value);
    }

    @Override
    public ItemBuilder addIfNotDefault(
        String key, @Nullable String value, @Nullable String defaultValue) {
      return addItemIf(!Objects.equals(value, defaultValue), key, Type.STRING, value);
    }

    @Override
    public ItemBuilder add(String key, long value) {
      return addItemIf(true, key, Type.INTEGER, value);
    }

    @Override
    public ItemBuilder addIfNotDefault(String key, long value, long defaultValue) {
      return addItemIf(value != defaultValue, key, Type.INTEGER, value);
    }

    @Override
    public ItemBuilder add(String key, double value) {
      return addItemIf(true, key, Type.FLOAT, value);
    }

    @Override
    public ItemBuilder addIfNotDefault(String key, double value, double defaultValue) {
      return addItemIf(value != defaultValue, key, Type.FLOAT, value);
    }

    @Override
    public ItemBuilder add(String key, boolean value) {
      return addItemIf(true, key, Type.BOOLEAN, value);
    }

    @Override
    public ItemBuilder addIfNotDefault(String key, boolean value, boolean defaultValue) {
      return addItemIf(value != defaultValue, key, Type.BOOLEAN, value);
    }

    @Override
    public ItemBuilder add(String key, Instant value) {
      checkNotNull(value);
      return addItemIf(true, key, Type.TIMESTAMP, value);
    }

    @Override
    public ItemBuilder addIfNotNull(String key, @Nullable Instant value) {
      return addItemIf(value != null, key, Type.TIMESTAMP, value);
    }

    @Override
    public ItemBuilder addIfNotDefault(
        String key, @Nullable Instant value, @Nullable Instant defaultValue) {
      return addItemIf(!Objects.equals(value, defaultValue), key, Type.TIMESTAMP, value);
    }

    @Override
    public ItemBuilder add(String key, Duration value) {
      checkNotNull(value);
      return addItemIf(true, key, Type.DURATION, value);
    }

    @Override
    public ItemBuilder addIfNotNull(String key, @Nullable Duration value) {
      return addItemIf(value != null, key, Type.DURATION, value);
    }

    @Override
    public ItemBuilder addIfNotDefault(
        String key, @Nullable Duration value, @Nullable Duration defaultValue) {
      return addItemIf(!Objects.equals(value, defaultValue), key, Type.DURATION, value);
    }

    @Override
    public ItemBuilder add(String key, Class<?> value) {
      checkNotNull(value);
      return addItemIf(true, key, Type.JAVA_CLASS, value);
    }

    @Override
    public ItemBuilder addIfNotNull(String key, @Nullable Class<?> value) {
      return addItemIf(value != null, key, Type.JAVA_CLASS, value);
    }

    @Override
    public ItemBuilder addIfNotDefault(
        String key, @Nullable Class<?> value, @Nullable Class<?> defaultValue) {
      return addItemIf(!Objects.equals(value, defaultValue), key, Type.JAVA_CLASS, value);
    }

    @Override
    public ItemBuilder add(String key, Type type, Object value) {
      checkNotNull(value);
      checkNotNull(type);
      return addItemIf(true, key, type, value);
    }

    private ItemBuilder addItemIf(boolean condition, String key, Type type, Object value) {
      checkNotNull(key);
      checkArgument(!key.isEmpty());

      commitLatest();
      if (condition) {
        latestItem = Item.create(latestNs, key, type, value);
      }

      return this;
    }

    private void commitLatest() {
      if (latestItem == null) {
        return;
      }

      Identifier id = Identifier.of(latestItem.getNamespace(), latestItem.getKey());
      if (entries.containsKey(id)) {
        throw new IllegalArgumentException("DisplayData key already exists. All display data "
          + "for a component must be registered with a unique key.\nKey: " + id);
      }

      entries.put(id, latestItem);
      latestItem = null;
    }

    @Override
    public ItemBuilder withLabel(@Nullable String label) {
      if (latestItem != null) {
        latestItem = latestItem.withLabel(label);
      }

      return this;
    }

    @Override
    public ItemBuilder withLinkUrl(@Nullable String url) {
      if (latestItem != null) {
        latestItem = latestItem.withUrl(url);
      }

      return this;
    }

    @Override
    public ItemBuilder withNamespace(Class<?> namespace) {
      checkNotNull(namespace);
      if (latestItem != null) {
        latestItem = latestItem.withNamespace(namespace);
      }

      return this;
    }

    private DisplayData build() {
      commitLatest();
      return new DisplayData(this.entries);
    }
  }
}
