package org.apache.beam.sdk.extensions.ordered;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Instant;

/**
 * A range of contiguous event sequences and the latest timestamp of the events in the range.
 */
@AutoValue
public abstract class ContiguousSequenceRange {
  public static final ContiguousSequenceRange EMPTY =
      ContiguousSequenceRange.of(Long.MIN_VALUE, Long.MIN_VALUE, Instant.ofEpochMilli(
          Long.MIN_VALUE));

  /**
   * @return inclusive starting sequence
   */
  public abstract long getStart();

  /**
   * @return inclusive end sequence
   */
  public abstract long getEnd();

  /**
   * @return latest timestamp of all events in the range
   */
  public abstract Instant getTimestamp();

  public static ContiguousSequenceRange of(long start, long end, Instant timestamp) {
    return new AutoValue_ContiguousSequenceRange(start, end, timestamp);
  }

  static class CompletedSequenceRangeCoder extends CustomCoder<ContiguousSequenceRange> {

    private static final CompletedSequenceRangeCoder INSTANCE = new CompletedSequenceRangeCoder();

    static CompletedSequenceRangeCoder of() {
      return INSTANCE;
    }

    private CompletedSequenceRangeCoder() {
    }

    @Override
    public void encode(ContiguousSequenceRange value,
        @UnknownKeyFor @NonNull @Initialized OutputStream outStream)
        throws @UnknownKeyFor @NonNull @Initialized CoderException, @UnknownKeyFor @NonNull @Initialized IOException {
      VarLongCoder.of().encode(value.getStart(), outStream);
      VarLongCoder.of().encode(value.getEnd(), outStream);
      InstantCoder.of().encode(value.getTimestamp(), outStream);
    }

    @Override
    public ContiguousSequenceRange decode(@UnknownKeyFor @NonNull @Initialized InputStream inStream)
        throws @UnknownKeyFor @NonNull @Initialized CoderException, @UnknownKeyFor @NonNull @Initialized IOException {
      long start = VarLongCoder.of().decode(inStream);
      long end = VarLongCoder.of().decode(inStream);
      Instant timestamp = InstantCoder.of().decode(inStream);
      return ContiguousSequenceRange.of(start, end, timestamp);
    }
  }
}
