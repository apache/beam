package org.apache.beam.sdk.transforms.windowing;

import javax.annotation.Generated;
import org.joda.time.Duration;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_TimestampTransform_Delay extends TimestampTransform.Delay {

  private final Duration delay;

  AutoValue_TimestampTransform_Delay(
      Duration delay) {
    if (delay == null) {
      throw new NullPointerException("Null delay");
    }
    this.delay = delay;
  }

  @Override
  public Duration getDelay() {
    return delay;
  }

  @Override
  public String toString() {
    return "Delay{"
        + "delay=" + delay
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof TimestampTransform.Delay) {
      TimestampTransform.Delay that = (TimestampTransform.Delay) o;
      return this.delay.equals(that.getDelay());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= delay.hashCode();
    return h$;
  }

}
