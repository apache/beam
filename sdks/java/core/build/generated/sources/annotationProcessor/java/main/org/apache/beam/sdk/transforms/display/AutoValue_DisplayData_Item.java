package org.apache.beam.sdk.transforms.display;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DisplayData_Item extends DisplayData.Item {

  private final DisplayData.@Nullable Path path;

  private final @Nullable Class<?> namespace;

  private final String key;

  private final DisplayData.Type type;

  private final @Nullable Object value;

  private final @Nullable Object shortValue;

  private final @Nullable String label;

  private final @Nullable String linkUrl;

  AutoValue_DisplayData_Item(
      DisplayData.@Nullable Path path,
      @Nullable Class<?> namespace,
      String key,
      DisplayData.Type type,
      @Nullable Object value,
      @Nullable Object shortValue,
      @Nullable String label,
      @Nullable String linkUrl) {
    this.path = path;
    this.namespace = namespace;
    if (key == null) {
      throw new NullPointerException("Null key");
    }
    this.key = key;
    if (type == null) {
      throw new NullPointerException("Null type");
    }
    this.type = type;
    this.value = value;
    this.shortValue = shortValue;
    this.label = label;
    this.linkUrl = linkUrl;
  }

  @JsonIgnore
  @Override
  public DisplayData.@Nullable Path getPath() {
    return path;
  }

  @JsonGetter("namespace")
  @Override
  public @Nullable Class<?> getNamespace() {
    return namespace;
  }

  @JsonGetter("key")
  @Override
  public String getKey() {
    return key;
  }

  @JsonGetter("type")
  @Override
  public DisplayData.Type getType() {
    return type;
  }

  @JsonGetter("value")
  @Override
  public @Nullable Object getValue() {
    return value;
  }

  @JsonGetter("shortValue")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Override
  public @Nullable Object getShortValue() {
    return shortValue;
  }

  @JsonGetter("label")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Override
  public @Nullable String getLabel() {
    return label;
  }

  @JsonGetter("linkUrl")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Override
  public @Nullable String getLinkUrl() {
    return linkUrl;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DisplayData.Item) {
      DisplayData.Item that = (DisplayData.Item) o;
      return (this.path == null ? that.getPath() == null : this.path.equals(that.getPath()))
          && (this.namespace == null ? that.getNamespace() == null : this.namespace.equals(that.getNamespace()))
          && this.key.equals(that.getKey())
          && this.type.equals(that.getType())
          && (this.value == null ? that.getValue() == null : this.value.equals(that.getValue()))
          && (this.shortValue == null ? that.getShortValue() == null : this.shortValue.equals(that.getShortValue()))
          && (this.label == null ? that.getLabel() == null : this.label.equals(that.getLabel()))
          && (this.linkUrl == null ? that.getLinkUrl() == null : this.linkUrl.equals(that.getLinkUrl()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (path == null) ? 0 : path.hashCode();
    h$ *= 1000003;
    h$ ^= (namespace == null) ? 0 : namespace.hashCode();
    h$ *= 1000003;
    h$ ^= key.hashCode();
    h$ *= 1000003;
    h$ ^= type.hashCode();
    h$ *= 1000003;
    h$ ^= (value == null) ? 0 : value.hashCode();
    h$ *= 1000003;
    h$ ^= (shortValue == null) ? 0 : shortValue.hashCode();
    h$ *= 1000003;
    h$ ^= (label == null) ? 0 : label.hashCode();
    h$ *= 1000003;
    h$ ^= (linkUrl == null) ? 0 : linkUrl.hashCode();
    return h$;
  }

}
