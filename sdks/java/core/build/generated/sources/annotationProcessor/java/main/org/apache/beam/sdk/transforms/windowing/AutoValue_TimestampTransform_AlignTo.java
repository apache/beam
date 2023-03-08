package org.apache.beam.sdk.transforms.windowing;

import javax.annotation.Generated;
import org.joda.time.Duration;
import org.joda.time.Instant;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_TimestampTransform_AlignTo extends TimestampTransform.AlignTo {

  private final Duration period;

  private final Instant offset;

  AutoValue_TimestampTransform_AlignTo(
      Duration period,
      Instant offset) {
    if (period == null) {
      throw new NullPointerException("Null period");
    }
    this.period = period;
    if (offset == null) {
      throw new NullPointerException("Null offset");
    }
    this.offset = offset;
  }

  @Override
  public Duration getPeriod() {
    return period;
  }

  @Override
  public Instant getOffset() {
    return offset;
  }

  @Override
  public String toString() {
    return "AlignTo{"
        + "period=" + period + ", "
        + "offset=" + offset
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof TimestampTransform.AlignTo) {
      TimestampTransform.AlignTo that = (TimestampTransform.AlignTo) o;
      return this.period.equals(that.getPeriod())
          && this.offset.equals(that.getOffset());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= period.hashCode();
    h$ *= 1000003;
    h$ ^= offset.hashCode();
    return h$;
  }

}
