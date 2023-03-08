package org.apache.beam.runners.core.metrics;

import javax.annotation.Generated;
import org.apache.beam.sdk.metrics.MetricKey;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_MetricUpdates_MetricUpdate<T> extends MetricUpdates.MetricUpdate<T> {

  private final MetricKey key;

  private final T update;

  AutoValue_MetricUpdates_MetricUpdate(
      MetricKey key,
      T update) {
    if (key == null) {
      throw new NullPointerException("Null key");
    }
    this.key = key;
    if (update == null) {
      throw new NullPointerException("Null update");
    }
    this.update = update;
  }

  @Override
  public MetricKey getKey() {
    return key;
  }

  @Override
  public T getUpdate() {
    return update;
  }

  @Override
  public String toString() {
    return "MetricUpdate{"
        + "key=" + key + ", "
        + "update=" + update
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof MetricUpdates.MetricUpdate) {
      MetricUpdates.MetricUpdate<?> that = (MetricUpdates.MetricUpdate<?>) o;
      return this.key.equals(that.getKey())
          && this.update.equals(that.getUpdate());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= key.hashCode();
    h$ *= 1000003;
    h$ ^= update.hashCode();
    return h$;
  }

}
