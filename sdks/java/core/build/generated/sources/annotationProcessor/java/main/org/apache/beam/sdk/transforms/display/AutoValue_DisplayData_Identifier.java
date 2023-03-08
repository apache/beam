package org.apache.beam.sdk.transforms.display;

import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DisplayData_Identifier extends DisplayData.Identifier {

  private final DisplayData.Path path;

  private final Class<?> namespace;

  private final String key;

  AutoValue_DisplayData_Identifier(
      DisplayData.Path path,
      Class<?> namespace,
      String key) {
    if (path == null) {
      throw new NullPointerException("Null path");
    }
    this.path = path;
    if (namespace == null) {
      throw new NullPointerException("Null namespace");
    }
    this.namespace = namespace;
    if (key == null) {
      throw new NullPointerException("Null key");
    }
    this.key = key;
  }

  @Override
  public DisplayData.Path getPath() {
    return path;
  }

  @Override
  public Class<?> getNamespace() {
    return namespace;
  }

  @Override
  public String getKey() {
    return key;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DisplayData.Identifier) {
      DisplayData.Identifier that = (DisplayData.Identifier) o;
      return this.path.equals(that.getPath())
          && this.namespace.equals(that.getNamespace())
          && this.key.equals(that.getKey());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= path.hashCode();
    h$ *= 1000003;
    h$ ^= namespace.hashCode();
    h$ *= 1000003;
    h$ ^= key.hashCode();
    return h$;
  }

}
