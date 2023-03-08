package org.apache.beam.sdk.io;

import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_BoundedReadFromUnboundedSource_Shard<T> extends BoundedReadFromUnboundedSource.Shard<T> {

  private final @Nullable UnboundedSource<T, ?> source;

  private final long maxNumRecords;

  private final @Nullable Duration maxReadTime;

  private AutoValue_BoundedReadFromUnboundedSource_Shard(
      @Nullable UnboundedSource<T, ?> source,
      long maxNumRecords,
      @Nullable Duration maxReadTime) {
    this.source = source;
    this.maxNumRecords = maxNumRecords;
    this.maxReadTime = maxReadTime;
  }

  @Override
  @Nullable UnboundedSource<T, ?> getSource() {
    return source;
  }

  @Override
  long getMaxNumRecords() {
    return maxNumRecords;
  }

  @Override
  @Nullable Duration getMaxReadTime() {
    return maxReadTime;
  }

  @Override
  public String toString() {
    return "Shard{"
        + "source=" + source + ", "
        + "maxNumRecords=" + maxNumRecords + ", "
        + "maxReadTime=" + maxReadTime
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof BoundedReadFromUnboundedSource.Shard) {
      BoundedReadFromUnboundedSource.Shard<?> that = (BoundedReadFromUnboundedSource.Shard<?>) o;
      return (this.source == null ? that.getSource() == null : this.source.equals(that.getSource()))
          && this.maxNumRecords == that.getMaxNumRecords()
          && (this.maxReadTime == null ? that.getMaxReadTime() == null : this.maxReadTime.equals(that.getMaxReadTime()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (source == null) ? 0 : source.hashCode();
    h$ *= 1000003;
    h$ ^= (int) ((maxNumRecords >>> 32) ^ maxNumRecords);
    h$ *= 1000003;
    h$ ^= (maxReadTime == null) ? 0 : maxReadTime.hashCode();
    return h$;
  }

  @Override
  BoundedReadFromUnboundedSource.Shard.Builder<T> toBuilder() {
    return new Builder<T>(this);
  }

  static final class Builder<T> extends BoundedReadFromUnboundedSource.Shard.Builder<T> {
    private @Nullable UnboundedSource<T, ?> source;
    private Long maxNumRecords;
    private @Nullable Duration maxReadTime;
    Builder() {
    }
    private Builder(BoundedReadFromUnboundedSource.Shard<T> source) {
      this.source = source.getSource();
      this.maxNumRecords = source.getMaxNumRecords();
      this.maxReadTime = source.getMaxReadTime();
    }
    @Override
    BoundedReadFromUnboundedSource.Shard.Builder<T> setSource(UnboundedSource<T, ?> source) {
      this.source = source;
      return this;
    }
    @Override
    BoundedReadFromUnboundedSource.Shard.Builder<T> setMaxNumRecords(long maxNumRecords) {
      this.maxNumRecords = maxNumRecords;
      return this;
    }
    @Override
    BoundedReadFromUnboundedSource.Shard.Builder<T> setMaxReadTime(@Nullable Duration maxReadTime) {
      this.maxReadTime = maxReadTime;
      return this;
    }
    @Override
    BoundedReadFromUnboundedSource.Shard<T> build() {
      if (this.maxNumRecords == null) {
        String missing = " maxNumRecords";
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_BoundedReadFromUnboundedSource_Shard<T>(
          this.source,
          this.maxNumRecords,
          this.maxReadTime);
    }
  }

}
