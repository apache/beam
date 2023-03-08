package org.apache.beam.sdk.metrics;

import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_MetricNameFilter extends MetricNameFilter {

  private final String namespace;

  private final @Nullable String name;

  AutoValue_MetricNameFilter(
      String namespace,
      @Nullable String name) {
    if (namespace == null) {
      throw new NullPointerException("Null namespace");
    }
    this.namespace = namespace;
    this.name = name;
  }

  @Override
  public String getNamespace() {
    return namespace;
  }

  @Override
  public @Nullable String getName() {
    return name;
  }

  @Override
  public String toString() {
    return "MetricNameFilter{"
        + "namespace=" + namespace + ", "
        + "name=" + name
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof MetricNameFilter) {
      MetricNameFilter that = (MetricNameFilter) o;
      return this.namespace.equals(that.getNamespace())
          && (this.name == null ? that.getName() == null : this.name.equals(that.getName()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= namespace.hashCode();
    h$ *= 1000003;
    h$ ^= (name == null) ? 0 : name.hashCode();
    return h$;
  }

}
