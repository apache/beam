package org.apache.beam.sdk.metrics;

import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_MetricResult<T> extends MetricResult<T> {

  private final MetricKey key;

  private final T committedOrNull;

  private final T attempted;

  AutoValue_MetricResult(
      MetricKey key,
      T committedOrNull,
      T attempted) {
    if (key == null) {
      throw new NullPointerException("Null key");
    }
    this.key = key;
    this.committedOrNull = committedOrNull;
    if (attempted == null) {
      throw new NullPointerException("Null attempted");
    }
    this.attempted = attempted;
  }

  @Override
  public MetricKey getKey() {
    return key;
  }

  @Override
  public T getCommittedOrNull() {
    return committedOrNull;
  }

  @Override
  public T getAttempted() {
    return attempted;
  }

  @Override
  public String toString() {
    return "MetricResult{"
        + "key=" + key + ", "
        + "committedOrNull=" + committedOrNull + ", "
        + "attempted=" + attempted
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof MetricResult) {
      MetricResult<?> that = (MetricResult<?>) o;
      return this.key.equals(that.getKey())
          && (this.committedOrNull == null ? that.getCommittedOrNull() == null : this.committedOrNull.equals(that.getCommittedOrNull()))
          && this.attempted.equals(that.getAttempted());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= key.hashCode();
    h$ *= 1000003;
    h$ ^= (committedOrNull == null) ? 0 : committedOrNull.hashCode();
    h$ *= 1000003;
    h$ ^= attempted.hashCode();
    return h$;
  }

}
