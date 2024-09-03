package org.apache.beam.sdk.extensions.ordered;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.joda.time.Instant;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class CompletedSequenceRange {
  public static final CompletedSequenceRange EMPTY =
      CompletedSequenceRange.of(Long.MIN_VALUE, Long.MIN_VALUE, Instant.ofEpochMilli(
          Long.MIN_VALUE));

  public abstract long getStart();
  public abstract long getEnd();
  public abstract Instant getTimestamp();

  public boolean isEmpty() {
    return this.equals(EMPTY);
  }

  public static CompletedSequenceRange of(long start, long end, Instant timestamp) {
    return new AutoValue_CompletedSequenceRange(start, end, timestamp);
  }
}
