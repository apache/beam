package org.apache.beam.sdk.testing;

import javax.annotation.Generated;
import org.joda.time.Duration;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_TestStream_ProcessingTimeEvent<T> extends TestStream.ProcessingTimeEvent<T> {

  private final TestStream.EventType type;

  private final Duration processingTimeAdvance;

  AutoValue_TestStream_ProcessingTimeEvent(
      TestStream.EventType type,
      Duration processingTimeAdvance) {
    if (type == null) {
      throw new NullPointerException("Null type");
    }
    this.type = type;
    if (processingTimeAdvance == null) {
      throw new NullPointerException("Null processingTimeAdvance");
    }
    this.processingTimeAdvance = processingTimeAdvance;
  }

  @Override
  public TestStream.EventType getType() {
    return type;
  }

  @Override
  public Duration getProcessingTimeAdvance() {
    return processingTimeAdvance;
  }

  @Override
  public String toString() {
    return "ProcessingTimeEvent{"
        + "type=" + type + ", "
        + "processingTimeAdvance=" + processingTimeAdvance
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof TestStream.ProcessingTimeEvent) {
      TestStream.ProcessingTimeEvent<?> that = (TestStream.ProcessingTimeEvent<?>) o;
      return this.type.equals(that.getType())
          && this.processingTimeAdvance.equals(that.getProcessingTimeAdvance());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= type.hashCode();
    h$ *= 1000003;
    h$ ^= processingTimeAdvance.hashCode();
    return h$;
  }

}
