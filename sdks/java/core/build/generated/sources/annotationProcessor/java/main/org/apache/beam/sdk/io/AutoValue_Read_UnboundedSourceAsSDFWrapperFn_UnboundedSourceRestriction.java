package org.apache.beam.sdk.io;

import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_Read_UnboundedSourceAsSDFWrapperFn_UnboundedSourceRestriction<OutputT, CheckpointT extends UnboundedSource.CheckpointMark> extends Read.UnboundedSourceAsSDFWrapperFn.UnboundedSourceRestriction<OutputT, CheckpointT> {

  private final UnboundedSource<OutputT, CheckpointT> source;

  private final CheckpointT checkpoint;

  private final Instant watermark;

  AutoValue_Read_UnboundedSourceAsSDFWrapperFn_UnboundedSourceRestriction(
      UnboundedSource<OutputT, CheckpointT> source,
      CheckpointT checkpoint,
      Instant watermark) {
    if (source == null) {
      throw new NullPointerException("Null source");
    }
    this.source = source;
    this.checkpoint = checkpoint;
    if (watermark == null) {
      throw new NullPointerException("Null watermark");
    }
    this.watermark = watermark;
  }

  @Override
  public UnboundedSource<OutputT, CheckpointT> getSource() {
    return source;
  }

  @Override
  public CheckpointT getCheckpoint() {
    return checkpoint;
  }

  @Override
  public Instant getWatermark() {
    return watermark;
  }

  @Override
  public String toString() {
    return "UnboundedSourceRestriction{"
        + "source=" + source + ", "
        + "checkpoint=" + checkpoint + ", "
        + "watermark=" + watermark
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof Read.UnboundedSourceAsSDFWrapperFn.UnboundedSourceRestriction) {
      Read.UnboundedSourceAsSDFWrapperFn.UnboundedSourceRestriction<?, ?> that = (Read.UnboundedSourceAsSDFWrapperFn.UnboundedSourceRestriction<?, ?>) o;
      return this.source.equals(that.getSource())
          && (this.checkpoint == null ? that.getCheckpoint() == null : this.checkpoint.equals(that.getCheckpoint()))
          && this.watermark.equals(that.getWatermark());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= source.hashCode();
    h$ *= 1000003;
    h$ ^= (checkpoint == null) ? 0 : checkpoint.hashCode();
    h$ *= 1000003;
    h$ ^= watermark.hashCode();
    return h$;
  }

}
