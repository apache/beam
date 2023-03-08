package org.apache.beam.sdk.metrics;

import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_MetricKey extends MetricKey {

  private final @Nullable String stepName;

  private final MetricName metricName;

  AutoValue_MetricKey(
      @Nullable String stepName,
      MetricName metricName) {
    this.stepName = stepName;
    if (metricName == null) {
      throw new NullPointerException("Null metricName");
    }
    this.metricName = metricName;
  }

  @Override
  public @Nullable String stepName() {
    return stepName;
  }

  @Override
  public MetricName metricName() {
    return metricName;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof MetricKey) {
      MetricKey that = (MetricKey) o;
      return (this.stepName == null ? that.stepName() == null : this.stepName.equals(that.stepName()))
          && this.metricName.equals(that.metricName());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (stepName == null) ? 0 : stepName.hashCode();
    h$ *= 1000003;
    h$ ^= metricName.hashCode();
    return h$;
  }

}
