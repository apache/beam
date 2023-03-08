package org.apache.beam.sdk.transforms.display;

import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DisplayData_ItemSpec<T> extends DisplayData.ItemSpec<T> {

  private final @Nullable Class<?> namespace;

  private final String key;

  private final DisplayData.Type type;

  private final @Nullable Object value;

  private final @Nullable Object shortValue;

  private final @Nullable String label;

  private final @Nullable String linkUrl;

  private AutoValue_DisplayData_ItemSpec(
      @Nullable Class<?> namespace,
      String key,
      DisplayData.Type type,
      @Nullable Object value,
      @Nullable Object shortValue,
      @Nullable String label,
      @Nullable String linkUrl) {
    this.namespace = namespace;
    this.key = key;
    this.type = type;
    this.value = value;
    this.shortValue = shortValue;
    this.label = label;
    this.linkUrl = linkUrl;
  }

  @Override
  public @Nullable Class<?> getNamespace() {
    return namespace;
  }

  @Override
  public String getKey() {
    return key;
  }

  @Override
  public DisplayData.Type getType() {
    return type;
  }

  @Override
  public @Nullable Object getValue() {
    return value;
  }

  @Override
  public @Nullable Object getShortValue() {
    return shortValue;
  }

  @Override
  public @Nullable String getLabel() {
    return label;
  }

  @Override
  public @Nullable String getLinkUrl() {
    return linkUrl;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DisplayData.ItemSpec) {
      DisplayData.ItemSpec<?> that = (DisplayData.ItemSpec<?>) o;
      return (this.namespace == null ? that.getNamespace() == null : this.namespace.equals(that.getNamespace()))
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

  @Override
  DisplayData.ItemSpec.Builder<T> toBuilder() {
    return new Builder<T>(this);
  }

  static final class Builder<T> extends DisplayData.ItemSpec.Builder<T> {
    private @Nullable Class<?> namespace;
    private String key;
    private DisplayData.Type type;
    private @Nullable Object value;
    private @Nullable Object shortValue;
    private @Nullable String label;
    private @Nullable String linkUrl;
    Builder() {
    }
    private Builder(DisplayData.ItemSpec<T> source) {
      this.namespace = source.getNamespace();
      this.key = source.getKey();
      this.type = source.getType();
      this.value = source.getValue();
      this.shortValue = source.getShortValue();
      this.label = source.getLabel();
      this.linkUrl = source.getLinkUrl();
    }
    @Override
    public DisplayData.ItemSpec.Builder<T> setNamespace(@Nullable Class<?> namespace) {
      this.namespace = namespace;
      return this;
    }
    @Override
    public DisplayData.ItemSpec.Builder<T> setKey(String key) {
      if (key == null) {
        throw new NullPointerException("Null key");
      }
      this.key = key;
      return this;
    }
    @Override
    public DisplayData.ItemSpec.Builder<T> setType(DisplayData.Type type) {
      if (type == null) {
        throw new NullPointerException("Null type");
      }
      this.type = type;
      return this;
    }
    @Override
    DisplayData.Type getType() {
      if (type == null) {
        throw new IllegalStateException("Property \"type\" has not been set");
      }
      return type;
    }
    @Override
    public DisplayData.ItemSpec.Builder<T> setValue(@Nullable Object value) {
      this.value = value;
      return this;
    }
    @Override
    public DisplayData.ItemSpec.Builder<T> setShortValue(@Nullable Object shortValue) {
      this.shortValue = shortValue;
      return this;
    }
    @Override
    public DisplayData.ItemSpec.Builder<T> setLabel(@Nullable String label) {
      this.label = label;
      return this;
    }
    @Override
    public DisplayData.ItemSpec.Builder<T> setLinkUrl(@Nullable String linkUrl) {
      this.linkUrl = linkUrl;
      return this;
    }
    @Override
    public DisplayData.ItemSpec<T> build() {
      if (this.key == null
          || this.type == null) {
        StringBuilder missing = new StringBuilder();
        if (this.key == null) {
          missing.append(" key");
        }
        if (this.type == null) {
          missing.append(" type");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_DisplayData_ItemSpec<T>(
          this.namespace,
          this.key,
          this.type,
          this.value,
          this.shortValue,
          this.label,
          this.linkUrl);
    }
  }

}
