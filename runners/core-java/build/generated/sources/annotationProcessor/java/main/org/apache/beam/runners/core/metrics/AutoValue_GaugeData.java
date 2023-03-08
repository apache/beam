package org.apache.beam.runners.core.metrics;

import javax.annotation.Generated;
import org.joda.time.Instant;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_GaugeData extends GaugeData {

  private final long value;

  private final Instant timestamp;

  AutoValue_GaugeData(
      long value,
      Instant timestamp) {
    this.value = value;
    if (timestamp == null) {
      throw new NullPointerException("Null timestamp");
    }
    this.timestamp = timestamp;
  }

  @Override
  public long value() {
    return value;
  }

  @Override
  public Instant timestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return "GaugeData{"
        + "value=" + value + ", "
        + "timestamp=" + timestamp
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof GaugeData) {
      GaugeData that = (GaugeData) o;
      return this.value == that.value()
          && this.timestamp.equals(that.timestamp());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (int) ((value >>> 32) ^ value);
    h$ *= 1000003;
    h$ ^= timestamp.hashCode();
    return h$;
  }

}
