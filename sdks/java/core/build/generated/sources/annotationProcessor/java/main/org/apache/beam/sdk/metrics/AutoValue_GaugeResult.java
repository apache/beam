package org.apache.beam.sdk.metrics;

import javax.annotation.Generated;
import org.joda.time.Instant;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_GaugeResult extends GaugeResult {

  private final long value;

  private final Instant timestamp;

  AutoValue_GaugeResult(
      long value,
      Instant timestamp) {
    this.value = value;
    if (timestamp == null) {
      throw new NullPointerException("Null timestamp");
    }
    this.timestamp = timestamp;
  }

  @Override
  public long getValue() {
    return value;
  }

  @Override
  public Instant getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return "GaugeResult{"
        + "value=" + value + ", "
        + "timestamp=" + timestamp
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof GaugeResult) {
      GaugeResult that = (GaugeResult) o;
      return this.value == that.getValue()
          && this.timestamp.equals(that.getTimestamp());
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
