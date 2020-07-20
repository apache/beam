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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Static display data associated with a pipeline component. Display data is useful for pipeline
 * runner UIs and diagnostic dashboards to display details about {@link PTransform PTransforms} that
 * make up a pipeline.
 *
 * <p>Components specify their display data by implementing the {@link HasDisplayData} interface.
 */
public class DisplayData implements Serializable {
  private static final DisplayData EMPTY = new DisplayData(Maps.newHashMap());
  private static final DateTimeFormatter TIMESTAMP_FORMATTER = ISODateTimeFormat.dateTime();

  private final ImmutableMap<Identifier, Item> entries;

  private DisplayData(Map<Identifier, Item> entries) {
    this.entries = ImmutableMap.copyOf(entries);
  }

  /** Default empty {@link DisplayData} instance. */
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

    InternalBuilder builder = new InternalBuilder();
    builder.include(Path.root(), component);

    return builder.build();
  }

  /**
   * Infer the {@link Type} for the given object.
   *
   * <p>Use this method if the type of metadata is not known at compile time. For example:
   *
   * <pre>{@code @Override
   * public void populateDisplayData(DisplayData.Builder builder) {
   *   Optional<DisplayData.Type> type = DisplayData.inferType(foo);
   *   if (type.isPresent()) {
   *     builder.add(DisplayData.item("foo", type.get(), foo));
   *   }
   * }
   * }</pre>
   *
   * @return The inferred {@link Type}, or null if the type cannot be inferred,
   */
  public static @Nullable Type inferType(@Nullable Object value) {
    return Type.tryInferFrom(value);
  }

  @JsonValue
  public Collection<Item> items() {
    return entries.values();
  }

  public Map<Identifier, Item> asMap() {
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

  /** Utility to build up display data from a component and its included subcomponents. */
  public interface Builder {
    /**
     * Register display data from the specified subcomponent at the given path. For example, a
     * {@link PTransform} which delegates to a user-provided function can implement {@link
     * HasDisplayData} on the function and include it from the {@link PTransform}:
     *
     * <pre>{@code @Override
     * public void populateDisplayData(DisplayData.Builder builder) {
     *   super.populateDisplayData(builder);
     *
     *   builder
     *     // To register the class name of the userFn
     *     .add(DisplayData.item("userFn", userFn.getClass()))
     *     // To allow the userFn to register additional display data
     *     .include("userFn", userFn);
     * }
     * }</pre>
     *
     * <p>Using {@code include(path, subcomponent)} will associate each of the registered items with
     * the namespace of the {@code subcomponent} being registered, with the specified path element
     * relative to the current path. To register display data in the current path and namespace,
     * such as from a base class implementation, use {@code
     * subcomponent.populateDisplayData(builder)} instead.
     *
     * @see HasDisplayData#populateDisplayData(DisplayData.Builder)
     */
    Builder include(String path, HasDisplayData subComponent);

    /**
     * Register display data from the specified component on behalf of the current component.
     * Display data items will be added with the subcomponent namespace but the current component
     * path.
     *
     * <p>This is useful for components which simply wrap other components and wish to retain the
     * display data from the wrapped component. Such components should implement {@code
     * populateDisplayData} as:
     *
     * <pre>{@code @Override
     * public void populateDisplayData(DisplayData.Builder builder) {
     *   builder.delegate(wrapped);
     * }
     * }</pre>
     */
    Builder delegate(HasDisplayData component);

    /** Register the given display item. */
    Builder add(ItemSpec<?> item);

    /** Register the given display item if the value is not null. */
    Builder addIfNotNull(ItemSpec<?> item);

    /** Register the given display item if the value is different than the specified default. */
    <T> Builder addIfNotDefault(ItemSpec<T> item, @Nullable T defaultValue);
  }

  /**
   * {@link Item Items} are the unit of display data. Each item is identified by a given path, key,
   * and namespace from the component the display item belongs to.
   *
   * <p>{@link Item Items} are registered via {@link DisplayData.Builder#add} within {@link
   * HasDisplayData#populateDisplayData} implementations.
   */
  @AutoValue
  public abstract static class Item implements Serializable {

    /** The path for the display item within a component hierarchy. */
    @Nullable
    @JsonIgnore
    public abstract Path getPath();

    /**
     * The namespace for the display item. The namespace defaults to the component which the display
     * item belongs to.
     */
    @Nullable
    @JsonGetter("namespace")
    public abstract Class<?> getNamespace();

    /**
     * The key for the display item. Each display item is created with a key and value via {@link
     * DisplayData#item}.
     */
    @JsonGetter("key")
    public abstract String getKey();

    /**
     * Retrieve the {@link DisplayData.Type} of display data. All metadata conforms to a predefined
     * set of allowed types.
     */
    @JsonGetter("type")
    public abstract Type getType();

    /**
     * Retrieve the value of the display item. The value is translated from the input to {@link
     * DisplayData#item} into a format suitable for display. Translation is based on the item's
     * {@link #getType() type}.
     */
    @JsonGetter("value")
    public abstract @Nullable Object getValue();

    /**
     * Return the optional short value for an item, or null if none is provided.
     *
     * <p>The short value is an alternative display representation for items having a long display
     * value. For example, the {@link #getValue() value} for {@link Type#JAVA_CLASS} items contains
     * the full class name with package, while the short value contains just the class name.
     *
     * <p>A {@link #getValue() value} will be provided for each display item, and some types may
     * also provide a short-value. If a short value is provided, display data consumers may choose
     * to display it instead of or in addition to the {@link #getValue() value}.
     */
    @JsonGetter("shortValue")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public abstract @Nullable Object getShortValue();

    /**
     * Retrieve the optional label for an item. The label is a human-readable description of what
     * the metadata represents. UIs may choose to display the label instead of the item key.
     *
     * <p>If no label was specified, this will return {@code null}.
     */
    @JsonGetter("label")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public abstract @Nullable String getLabel();

    /**
     * Retrieve the optional link URL for an item. The URL points to an address where the reader can
     * find additional context for the display data.
     *
     * <p>If no URL was specified, this will return {@code null}.
     */
    @JsonGetter("linkUrl")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public abstract @Nullable String getLinkUrl();

    private static Item create(ItemSpec<?> spec, Path path) {
      checkNotNull(spec, "spec cannot be null");
      checkNotNull(path, "path cannot be null");
      Class<?> ns = checkNotNull(spec.getNamespace(), "namespace must be set");

      return new AutoValue_DisplayData_Item(
          path,
          ns,
          spec.getKey(),
          spec.getType(),
          spec.getValue(),
          spec.getShortValue(),
          spec.getLabel(),
          spec.getLinkUrl());
    }

    @Override
    public String toString() {
      return String.format("%s%s:%s=%s", getPath(), getNamespace().getName(), getKey(), getValue());
    }
  }

  /**
   * Specifies an {@link Item} to register as display data. Each item is identified by a given path,
   * key, and namespace from the component the display item belongs to.
   *
   * <p>{@link Item Items} are registered via {@link DisplayData.Builder#add} within {@link
   * HasDisplayData#populateDisplayData} implementations.
   */
  @AutoValue
  public abstract static class ItemSpec<T> implements Serializable {
    /**
     * The namespace for the display item. If unset, defaults to the component which the display
     * item is registered to.
     */
    public abstract @Nullable Class<?> getNamespace();

    /**
     * The key for the display item. Each display item is created with a key and value via {@link
     * DisplayData#item}.
     */
    public abstract String getKey();

    /**
     * The {@link DisplayData.Type} of display data. All display data conforms to a predefined set
     * of allowed types.
     */
    public abstract Type getType();

    /**
     * The value of the display item. The value is translated from the input to {@link
     * DisplayData#item} into a format suitable for display. Translation is based on the item's
     * {@link #getType() type}.
     */
    public abstract @Nullable Object getValue();

    /**
     * The optional short value for an item, or {@code null} if none is provided.
     *
     * <p>The short value is an alternative display representation for items having a long display
     * value. For example, the {@link #getValue() value} for {@link Type#JAVA_CLASS} items contains
     * the full class name with package, while the short value contains just the class name.
     *
     * <p>A {@link #getValue() value} will be provided for each display item, and some types may
     * also provide a short-value. If a short value is provided, display data consumers may choose
     * to display it instead of or in addition to the {@link #getValue() value}.
     */
    public abstract @Nullable Object getShortValue();

    /**
     * The optional label for an item. The label is a human-readable description of what the
     * metadata represents. UIs may choose to display the label instead of the item key.
     */
    public abstract @Nullable String getLabel();

    /**
     * The optional link URL for an item. The URL points to an address where the reader can find
     * additional context for the display data.
     */
    public abstract @Nullable String getLinkUrl();

    private static <T> ItemSpec<T> create(String key, Type type, @Nullable T value) {
      return ItemSpec.<T>builder().setKey(key).setType(type).setRawValue(value).build();
    }

    /**
     * Set the item {@link ItemSpec#getNamespace() namespace} from the given {@link Class}.
     *
     * <p>This method does not alter the current instance, but instead returns a new {@link
     * ItemSpec} with the namespace set.
     */
    public ItemSpec<T> withNamespace(Class<?> namespace) {
      checkNotNull(namespace, "namespace argument cannot be null");
      return toBuilder().setNamespace(namespace).build();
    }

    /**
     * Set the item {@link Item#getLabel() label}.
     *
     * <p>Specifying a null value will clear the label if it was previously defined.
     *
     * <p>This method does not alter the current instance, but instead returns a new {@link
     * ItemSpec} with the label set.
     */
    public ItemSpec<T> withLabel(@Nullable String label) {
      return toBuilder().setLabel(label).build();
    }

    /**
     * Set the item {@link Item#getLinkUrl() link url}.
     *
     * <p>Specifying a null value will clear the link url if it was previously defined.
     *
     * <p>This method does not alter the current instance, but instead returns a new {@link
     * ItemSpec} with the link url set.
     */
    public ItemSpec<T> withLinkUrl(@Nullable String url) {
      return toBuilder().setLinkUrl(url).build();
    }

    /**
     * Creates a similar item to the current instance but with the specified value.
     *
     * <p>This should only be used internally. It is useful to compare the value of a {@link
     * DisplayData.Item} to the value derived from a specified input.
     */
    private ItemSpec<T> withValue(T value) {
      return toBuilder().setRawValue(value).build();
    }

    @Override
    public String toString() {
      return String.format("%s:%s=%s", getNamespace(), getKey(), getValue());
    }

    static <T> ItemSpec.Builder<T> builder() {
      return new AutoValue_DisplayData_ItemSpec.Builder<>();
    }

    abstract ItemSpec.Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      public abstract ItemSpec.Builder<T> setKey(String key);

      public abstract ItemSpec.Builder<T> setNamespace(@Nullable Class<?> namespace);

      public abstract ItemSpec.Builder<T> setType(Type type);

      public abstract ItemSpec.Builder<T> setValue(@Nullable Object longValue);

      public abstract ItemSpec.Builder<T> setShortValue(@Nullable Object shortValue);

      public abstract ItemSpec.Builder<T> setLabel(@Nullable String label);

      public abstract ItemSpec.Builder<T> setLinkUrl(@Nullable String url);

      public abstract ItemSpec<T> build();

      abstract Type getType();

      ItemSpec.Builder<T> setRawValue(@Nullable T value) {
        FormattedItemValue formatted = getType().safeFormat(value);
        return this.setValue(formatted.getLongValue()).setShortValue(formatted.getShortValue());
      }
    }
  }

  /**
   * Unique identifier for a display data item within a component.
   *
   * <p>Identifiers are composed of:
   *
   * <ul>
   *   <li>A {@link #getPath() path} based on the component hierarchy
   *   <li>The {@link #getKey() key} it is registered with
   *   <li>A {@link #getNamespace() namespace} generated from the class of the component which
   *       registered the item.
   * </ul>
   *
   * <p>Display data registered with the same key from different components will have different
   * namespaces and thus will both be represented in the composed {@link DisplayData}. If a single
   * component registers multiple metadata items with the same key, only the most recent item will
   * be retained; previous versions are discarded.
   */
  @AutoValue
  public abstract static class Identifier implements Serializable {
    public abstract Path getPath();

    public abstract Class<?> getNamespace();

    public abstract String getKey();

    public static Identifier of(Path path, Class<?> namespace, String key) {
      return new AutoValue_DisplayData_Identifier(path, namespace, key);
    }

    @Override
    public String toString() {
      return String.format("%s%s:%s", getPath(), getNamespace(), getKey());
    }
  }

  /**
   * Structured path of registered display data within a component hierarchy.
   *
   * <p>Display data items registered directly by a component will have the {@link Path#root() root}
   * path. If the component {@link Builder#include includes} a sub-component, its display data will
   * be registered at the path specified. Each sub-component path is created by appending a child
   * element to the path of its parent component, forming a hierarchy.
   */
  public static class Path implements Serializable {
    private final ImmutableList<String> components;

    private Path(ImmutableList<String> components) {
      this.components = components;
    }

    /** Path for display data registered by a top-level component. */
    public static Path root() {
      return new Path(ImmutableList.of());
    }

    /**
     * Construct a path from an absolute component path hierarchy.
     *
     * <p>For the root path, use {@link Path#root()}.
     *
     * @param firstPath Path of the first sub-component.
     * @param paths Additional path components.
     */
    public static Path absolute(String firstPath, String... paths) {
      ImmutableList.Builder<String> builder = ImmutableList.builder();

      validatePathElement(firstPath);
      builder.add(firstPath);
      for (String path : paths) {
        validatePathElement(path);
        builder.add(path);
      }

      return new Path(builder.build());
    }

    /**
     * Hierarchy list of component paths making up the full path, starting with the top-level child
     * component path. For the {@link #root root} path, returns the empty list.
     */
    public List<String> getComponents() {
      return components;
    }

    /**
     * Extend the path by appending a sub-component path. The new path element is added to the end
     * of the path hierarchy.
     *
     * <p>Returns a new {@link Path} instance; the originating {@link Path} is not modified.
     */
    public Path extend(String path) {
      validatePathElement(path);
      return new Path(
          ImmutableList.<String>builder().addAll(components.iterator()).add(path).build());
    }

    private static void validatePathElement(String path) {
      checkNotNull(path);
      checkArgument(!"".equals(path), "path cannot be empty");
    }

    @Override
    public String toString() {
      StringBuilder b = new StringBuilder().append("[");
      Joiner.on("/").appendTo(b, components);
      b.append("]");
      return b.toString();
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof Path && Objects.equals(components, ((Path) obj).components);
    }

    @Override
    public int hashCode() {
      return components.hashCode();
    }
  }

  /** Display data type. */
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
        return new FormattedItemValue(TIMESTAMP_FORMATTER.print(instant));
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
        throw new ClassCastException(
            String.format("Value is not valid for DisplayData type %s: %s", expectedType, value));
      }

      @SuppressWarnings("unchecked") // type checked above.
      T typedValue = (T) value;
      return typedValue;
    }

    /**
     * Format the display data value into a long string representation, and optionally a shorter
     * representation for display.
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

    private static @Nullable Type tryInferFrom(@Nullable Object value) {
      if (value instanceof Integer || value instanceof Long) {
        return INTEGER;
      } else if (value instanceof Double || value instanceof Float) {
        return FLOAT;
      } else if (value instanceof Boolean) {
        return BOOLEAN;
      } else if (value instanceof Instant) {
        return TIMESTAMP;
      } else if (value instanceof Duration) {
        return DURATION;
      } else if (value instanceof Class<?>) {
        return JAVA_CLASS;
      } else if (value instanceof String) {
        return STRING;
      } else {
        return null;
      }
    }
  }

  static class FormattedItemValue {
    /** Default instance which contains null values. */
    private static final FormattedItemValue NULL_VALUES = new FormattedItemValue(null);

    private final @Nullable Object shortValue;
    private final @Nullable Object longValue;

    private FormattedItemValue(@Nullable Object longValue) {
      this(longValue, null);
    }

    private FormattedItemValue(@Nullable Object longValue, @Nullable Object shortValue) {
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
    private final Map<Identifier, Item> entries;
    private final Set<HasDisplayData> visitedComponents;
    private final Map<Path, HasDisplayData> visitedPathMap;

    private @Nullable Path latestPath;
    private @Nullable Class<?> latestNs;

    private InternalBuilder() {
      this.entries = Maps.newHashMap();
      this.visitedComponents = Sets.newIdentityHashSet();
      this.visitedPathMap = Maps.newHashMap();
    }

    @Override
    public Builder include(String path, HasDisplayData subComponent) {
      checkNotNull(subComponent, "subComponent argument cannot be null");
      checkNotNull(path, "path argument cannot be null");

      Path absolutePath = latestPath.extend(path);

      HasDisplayData existingComponent = visitedPathMap.get(absolutePath);
      if (existingComponent != null) {
        throw new IllegalArgumentException(
            String.format(
                "Specified path '%s' already used for "
                    + "subcomponent %s. Subcomponents must be included using unique paths.",
                path, existingComponent));
      }

      return include(absolutePath, subComponent);
    }

    @Override
    public Builder delegate(HasDisplayData component) {
      checkNotNull(component);

      return include(latestPath, component);
    }

    private Builder include(Path path, HasDisplayData subComponent) {
      if (visitedComponents.contains(subComponent)) {
        // Component previously registered; ignore in order to break cyclic dependencies
        return this;
      }

      // New component; add it.
      visitedComponents.add(subComponent);
      visitedPathMap.put(path, subComponent);
      Class<?> namespace = subComponent.getClass();
      // Common case: AutoValue classes such as AutoValue_FooIO_Read. It's more useful
      // to show the user the FooIO.Read class, which is the direct superclass of the AutoValue
      // generated class.
      if (namespace.getSimpleName().startsWith("AutoValue_")) {
        namespace = namespace.getSuperclass();
      }
      if (namespace.isSynthetic() && namespace.getSimpleName().contains("$$Lambda")) {
        try {
          namespace =
              Class.forName(namespace.getCanonicalName().replaceFirst("\\$\\$Lambda.*", ""));
        } catch (Exception e) {
          throw new PopulateDisplayDataException(
              "Failed to get the enclosing class of lambda " + subComponent, e);
        }
      }

      Path prevPath = latestPath;
      Class<?> prevNs = latestNs;
      latestPath = path;
      latestNs = namespace;

      try {
        subComponent.populateDisplayData(this);
      } catch (PopulateDisplayDataException e) {
        // Don't re-wrap exceptions recursively.
        throw e;
      } catch (Throwable e) {
        String msg =
            String.format(
                "Error while populating display data for component '%s': %s",
                namespace.getName(), e.getMessage());
        throw new PopulateDisplayDataException(msg, e);
      }

      latestPath = prevPath;
      latestNs = prevNs;

      return this;
    }

    /** Marker exception class for exceptions encountered while populating display data. */
    private static class PopulateDisplayDataException extends RuntimeException {
      PopulateDisplayDataException(String message, Throwable cause) {
        super(message, cause);
      }
    }

    @Override
    public Builder add(ItemSpec<?> item) {
      checkNotNull(item, "Input display item cannot be null");
      return addItemIf(true, item);
    }

    @Override
    public Builder addIfNotNull(ItemSpec<?> item) {
      checkNotNull(item, "Input display item cannot be null");
      return addItemIf(item.getValue() != null, item);
    }

    @Override
    public <T> Builder addIfNotDefault(ItemSpec<T> item, @Nullable T defaultValue) {
      checkNotNull(item, "Input display item cannot be null");
      ItemSpec<T> defaultItem = item.withValue(defaultValue);
      return addItemIf(!Objects.equals(item, defaultItem), item);
    }

    private Builder addItemIf(boolean condition, ItemSpec<?> spec) {
      if (!condition) {
        return this;
      }

      checkNotNull(spec, "Input display item cannot be null");
      checkNotNull(spec.getValue(), "Input display value cannot be null");

      if (spec.getNamespace() == null) {
        spec = spec.withNamespace(latestNs);
      }
      Item item = Item.create(spec, latestPath);

      Identifier id = Identifier.of(item.getPath(), item.getNamespace(), item.getKey());
      checkArgument(
          !entries.containsKey(id),
          "Display data key (%s) is not unique within the specified path and namespace: %s%s.",
          item.getKey(),
          item.getPath(),
          item.getNamespace());

      entries.put(id, item);
      return this;
    }

    private DisplayData build() {
      return new DisplayData(this.entries);
    }
  }

  /** Create a display item for the specified key and string value. */
  public static ItemSpec<String> item(String key, @Nullable String value) {
    return item(key, Type.STRING, value);
  }

  /** Create a display item for the specified key and {@link ValueProvider}. */
  public static ItemSpec<?> item(String key, @Nullable ValueProvider<?> value) {
    if (value == null) {
      return item(key, Type.STRING, null);
    }
    if (value.isAccessible()) {
      Object got = value.get();
      if (got == null) {
        return item(key, Type.STRING, null);
      }
      Type type = inferType(got);
      if (type != null) {
        return item(key, type, got);
      }
    }
    // General case: not null and type not inferable. Fall back to toString of the VP itself.
    return item(key, Type.STRING, String.valueOf(value));
  }

  /** Create a display item for the specified key and integer value. */
  public static ItemSpec<Integer> item(String key, @Nullable Integer value) {
    return item(key, Type.INTEGER, value);
  }

  /** Create a display item for the specified key and integer value. */
  public static ItemSpec<Long> item(String key, @Nullable Long value) {
    return item(key, Type.INTEGER, value);
  }

  /** Create a display item for the specified key and floating point value. */
  public static ItemSpec<Float> item(String key, @Nullable Float value) {
    return item(key, Type.FLOAT, value);
  }

  /** Create a display item for the specified key and floating point value. */
  public static ItemSpec<Double> item(String key, @Nullable Double value) {
    return item(key, Type.FLOAT, value);
  }

  /** Create a display item for the specified key and boolean value. */
  public static ItemSpec<Boolean> item(String key, @Nullable Boolean value) {
    return item(key, Type.BOOLEAN, value);
  }

  /** Create a display item for the specified key and timestamp value. */
  public static ItemSpec<Instant> item(String key, @Nullable Instant value) {
    return item(key, Type.TIMESTAMP, value);
  }

  /** Create a display item for the specified key and duration value. */
  public static ItemSpec<Duration> item(String key, @Nullable Duration value) {
    return item(key, Type.DURATION, value);
  }

  /** Create a display item for the specified key and class value. */
  public static <T> ItemSpec<Class<T>> item(String key, @Nullable Class<T> value) {
    return item(key, Type.JAVA_CLASS, value);
  }

  /**
   * Create a display item for the specified key, type, and value. This method should be used if the
   * type of the input value can only be determined at runtime. Otherwise, {@link HasDisplayData}
   * implementors should call one of the typed factory methods, such as {@link #item(String,
   * String)} or {@link #item(String, Integer)}.
   *
   * @throws ClassCastException if the value cannot be formatted as the given type.
   * @see Type#inferType(Object)
   */
  public static <T> ItemSpec<T> item(String key, Type type, @Nullable T value) {
    checkNotNull(key, "key argument cannot be null");
    checkNotNull(type, "type argument cannot be null");

    return ItemSpec.create(key, type, value);
  }
}
