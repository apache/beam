package org.apache.beam.sdk.transforms;

import javax.annotation.Generated;
import javax.annotation.Nullable;
import org.joda.time.Duration;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_GroupIntoBatches_BatchingParams<InputT> extends GroupIntoBatches.BatchingParams<InputT> {

  private final long batchSize;

  private final long batchSizeBytes;

  private final SerializableFunction<InputT, Long> elementByteSize;

  private final Duration maxBufferingDuration;

  AutoValue_GroupIntoBatches_BatchingParams(
      long batchSize,
      long batchSizeBytes,
      @Nullable SerializableFunction<InputT, Long> elementByteSize,
      Duration maxBufferingDuration) {
    this.batchSize = batchSize;
    this.batchSizeBytes = batchSizeBytes;
    this.elementByteSize = elementByteSize;
    if (maxBufferingDuration == null) {
      throw new NullPointerException("Null maxBufferingDuration");
    }
    this.maxBufferingDuration = maxBufferingDuration;
  }

  @Override
  public long getBatchSize() {
    return batchSize;
  }

  @Override
  public long getBatchSizeBytes() {
    return batchSizeBytes;
  }

  @Nullable
  @Override
  public SerializableFunction<InputT, Long> getElementByteSize() {
    return elementByteSize;
  }

  @Override
  public Duration getMaxBufferingDuration() {
    return maxBufferingDuration;
  }

  @Override
  public String toString() {
    return "BatchingParams{"
        + "batchSize=" + batchSize + ", "
        + "batchSizeBytes=" + batchSizeBytes + ", "
        + "elementByteSize=" + elementByteSize + ", "
        + "maxBufferingDuration=" + maxBufferingDuration
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof GroupIntoBatches.BatchingParams) {
      GroupIntoBatches.BatchingParams<?> that = (GroupIntoBatches.BatchingParams<?>) o;
      return this.batchSize == that.getBatchSize()
          && this.batchSizeBytes == that.getBatchSizeBytes()
          && (this.elementByteSize == null ? that.getElementByteSize() == null : this.elementByteSize.equals(that.getElementByteSize()))
          && this.maxBufferingDuration.equals(that.getMaxBufferingDuration());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (int) ((batchSize >>> 32) ^ batchSize);
    h$ *= 1000003;
    h$ ^= (int) ((batchSizeBytes >>> 32) ^ batchSizeBytes);
    h$ *= 1000003;
    h$ ^= (elementByteSize == null) ? 0 : elementByteSize.hashCode();
    h$ *= 1000003;
    h$ ^= maxBufferingDuration.hashCode();
    return h$;
  }

}
