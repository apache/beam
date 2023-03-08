package org.apache.beam.sdk.testing;

import javax.annotation.Generated;
import org.joda.time.Instant;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_TestStream_WatermarkEvent<T> extends TestStream.WatermarkEvent<T> {

  private final TestStream.EventType type;

  private final Instant watermark;

  AutoValue_TestStream_WatermarkEvent(
      TestStream.EventType type,
      Instant watermark) {
    if (type == null) {
      throw new NullPointerException("Null type");
    }
    this.type = type;
    if (watermark == null) {
      throw new NullPointerException("Null watermark");
    }
    this.watermark = watermark;
  }

  @Override
  public TestStream.EventType getType() {
    return type;
  }

  @Override
  public Instant getWatermark() {
    return watermark;
  }

  @Override
  public String toString() {
    return "WatermarkEvent{"
        + "type=" + type + ", "
        + "watermark=" + watermark
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof TestStream.WatermarkEvent) {
      TestStream.WatermarkEvent<?> that = (TestStream.WatermarkEvent<?>) o;
      return this.type.equals(that.getType())
          && this.watermark.equals(that.getWatermark());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= type.hashCode();
    h$ *= 1000003;
    h$ ^= watermark.hashCode();
    return h$;
  }

}
