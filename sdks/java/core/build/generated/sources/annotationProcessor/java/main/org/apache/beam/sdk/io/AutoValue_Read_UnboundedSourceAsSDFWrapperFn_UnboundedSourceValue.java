package org.apache.beam.sdk.io;

import java.util.Arrays;
import javax.annotation.Generated;
import org.joda.time.Instant;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_Read_UnboundedSourceAsSDFWrapperFn_UnboundedSourceValue<T> extends Read.UnboundedSourceAsSDFWrapperFn.UnboundedSourceValue<T> {

  private final byte[] id;

  private final T value;

  private final Instant timestamp;

  private final Instant watermark;

  AutoValue_Read_UnboundedSourceAsSDFWrapperFn_UnboundedSourceValue(
      byte[] id,
      T value,
      Instant timestamp,
      Instant watermark) {
    if (id == null) {
      throw new NullPointerException("Null id");
    }
    this.id = id;
    if (value == null) {
      throw new NullPointerException("Null value");
    }
    this.value = value;
    if (timestamp == null) {
      throw new NullPointerException("Null timestamp");
    }
    this.timestamp = timestamp;
    if (watermark == null) {
      throw new NullPointerException("Null watermark");
    }
    this.watermark = watermark;
  }

  @SuppressWarnings("mutable")
  @Override
  public byte[] getId() {
    return id;
  }

  @Override
  public T getValue() {
    return value;
  }

  @Override
  public Instant getTimestamp() {
    return timestamp;
  }

  @Override
  public Instant getWatermark() {
    return watermark;
  }

  @Override
  public String toString() {
    return "UnboundedSourceValue{"
        + "id=" + Arrays.toString(id) + ", "
        + "value=" + value + ", "
        + "timestamp=" + timestamp + ", "
        + "watermark=" + watermark
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof Read.UnboundedSourceAsSDFWrapperFn.UnboundedSourceValue) {
      Read.UnboundedSourceAsSDFWrapperFn.UnboundedSourceValue<?> that = (Read.UnboundedSourceAsSDFWrapperFn.UnboundedSourceValue<?>) o;
      return Arrays.equals(this.id, (that instanceof AutoValue_Read_UnboundedSourceAsSDFWrapperFn_UnboundedSourceValue) ? ((AutoValue_Read_UnboundedSourceAsSDFWrapperFn_UnboundedSourceValue<?>) that).id : that.getId())
          && this.value.equals(that.getValue())
          && this.timestamp.equals(that.getTimestamp())
          && this.watermark.equals(that.getWatermark());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= Arrays.hashCode(id);
    h$ *= 1000003;
    h$ ^= value.hashCode();
    h$ *= 1000003;
    h$ ^= timestamp.hashCode();
    h$ *= 1000003;
    h$ ^= watermark.hashCode();
    return h$;
  }

}
