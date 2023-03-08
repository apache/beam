package org.apache.beam.sdk.metrics;

import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_MetricName extends MetricName {

  private final String namespace;

  private final String name;

  AutoValue_MetricName(
      String namespace,
      String name) {
    if (namespace == null) {
      throw new NullPointerException("Null namespace");
    }
    this.namespace = namespace;
    if (name == null) {
      throw new NullPointerException("Null name");
    }
    this.name = name;
  }

  @Override
  public String getNamespace() {
    return namespace;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof MetricName) {
      MetricName that = (MetricName) o;
      return this.namespace.equals(that.getNamespace())
          && this.name.equals(that.getName());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= namespace.hashCode();
    h$ *= 1000003;
    h$ ^= name.hashCode();
    return h$;
  }

}
