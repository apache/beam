package org.apache.beam.sdk.io;

import javax.annotation.Generated;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;
import org.joda.time.Duration;
import org.joda.time.Instant;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_GenerateSequence extends GenerateSequence {

  private final long from;

  private final long to;

  private final @Nullable SerializableFunction<Long, Instant> timestampFn;

  private final long elementsPerPeriod;

  private final @Nullable Duration period;

  private final @Nullable Duration maxReadTime;

  private AutoValue_GenerateSequence(
      long from,
      long to,
      @Nullable SerializableFunction<Long, Instant> timestampFn,
      long elementsPerPeriod,
      @Nullable Duration period,
      @Nullable Duration maxReadTime) {
    this.from = from;
    this.to = to;
    this.timestampFn = timestampFn;
    this.elementsPerPeriod = elementsPerPeriod;
    this.period = period;
    this.maxReadTime = maxReadTime;
  }

  @Pure
  @Override
  long getFrom() {
    return from;
  }

  @Pure
  @Override
  long getTo() {
    return to;
  }

  @Pure
  @Override
  @Nullable SerializableFunction<Long, Instant> getTimestampFn() {
    return timestampFn;
  }

  @Pure
  @Override
  long getElementsPerPeriod() {
    return elementsPerPeriod;
  }

  @Pure
  @Override
  @Nullable Duration getPeriod() {
    return period;
  }

  @Pure
  @Override
  @Nullable Duration getMaxReadTime() {
    return maxReadTime;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof GenerateSequence) {
      GenerateSequence that = (GenerateSequence) o;
      return this.from == that.getFrom()
          && this.to == that.getTo()
          && (this.timestampFn == null ? that.getTimestampFn() == null : this.timestampFn.equals(that.getTimestampFn()))
          && this.elementsPerPeriod == that.getElementsPerPeriod()
          && (this.period == null ? that.getPeriod() == null : this.period.equals(that.getPeriod()))
          && (this.maxReadTime == null ? that.getMaxReadTime() == null : this.maxReadTime.equals(that.getMaxReadTime()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (int) ((from >>> 32) ^ from);
    h$ *= 1000003;
    h$ ^= (int) ((to >>> 32) ^ to);
    h$ *= 1000003;
    h$ ^= (timestampFn == null) ? 0 : timestampFn.hashCode();
    h$ *= 1000003;
    h$ ^= (int) ((elementsPerPeriod >>> 32) ^ elementsPerPeriod);
    h$ *= 1000003;
    h$ ^= (period == null) ? 0 : period.hashCode();
    h$ *= 1000003;
    h$ ^= (maxReadTime == null) ? 0 : maxReadTime.hashCode();
    return h$;
  }

  @Override
  GenerateSequence.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends GenerateSequence.Builder {
    private Long from;
    private Long to;
    private @Nullable SerializableFunction<Long, Instant> timestampFn;
    private Long elementsPerPeriod;
    private @Nullable Duration period;
    private @Nullable Duration maxReadTime;
    Builder() {
    }
    private Builder(GenerateSequence source) {
      this.from = source.getFrom();
      this.to = source.getTo();
      this.timestampFn = source.getTimestampFn();
      this.elementsPerPeriod = source.getElementsPerPeriod();
      this.period = source.getPeriod();
      this.maxReadTime = source.getMaxReadTime();
    }
    @Override
    GenerateSequence.Builder setFrom(long from) {
      this.from = from;
      return this;
    }
    @Override
    GenerateSequence.Builder setTo(long to) {
      this.to = to;
      return this;
    }
    @Override
    GenerateSequence.Builder setTimestampFn(@Nullable SerializableFunction<Long, Instant> timestampFn) {
      this.timestampFn = timestampFn;
      return this;
    }
    @Override
    GenerateSequence.Builder setElementsPerPeriod(long elementsPerPeriod) {
      this.elementsPerPeriod = elementsPerPeriod;
      return this;
    }
    @Override
    GenerateSequence.Builder setPeriod(@Nullable Duration period) {
      this.period = period;
      return this;
    }
    @Override
    GenerateSequence.Builder setMaxReadTime(@Nullable Duration maxReadTime) {
      this.maxReadTime = maxReadTime;
      return this;
    }
    @Override
    GenerateSequence build() {
      if (this.from == null
          || this.to == null
          || this.elementsPerPeriod == null) {
        StringBuilder missing = new StringBuilder();
        if (this.from == null) {
          missing.append(" from");
        }
        if (this.to == null) {
          missing.append(" to");
        }
        if (this.elementsPerPeriod == null) {
          missing.append(" elementsPerPeriod");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_GenerateSequence(
          this.from,
          this.to,
          this.timestampFn,
          this.elementsPerPeriod,
          this.period,
          this.maxReadTime);
    }
  }

}
