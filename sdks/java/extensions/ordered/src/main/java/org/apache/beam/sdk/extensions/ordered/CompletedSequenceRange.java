package org.apache.beam.sdk.extensions.ordered;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.ordered.CompletedSequenceRange.CompletedSequenceRangeCoder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Instant;

@AutoValue
public abstract class CompletedSequenceRange {
//  private static final long serialVersionUID = 1L;
  public static final CompletedSequenceRange EMPTY =
      CompletedSequenceRange.of(Long.MIN_VALUE, Long.MIN_VALUE, Instant.ofEpochMilli(
          Long.MIN_VALUE));

  public abstract long getStart();
  public abstract long getEnd();
  public abstract Instant getTimestamp();

  public static CompletedSequenceRange of(long start, long end, Instant timestamp) {
    return new AutoValue_CompletedSequenceRange(start, end, timestamp);
  }

  static class CompletedSequenceRangeCoder extends CustomCoder<CompletedSequenceRange> {

    private static final CompletedSequenceRangeCoder INSTANCE = new CompletedSequenceRangeCoder();

    static CompletedSequenceRangeCoder of() {
      return INSTANCE;
    }

    private CompletedSequenceRangeCoder() {
    }

    @Override
    public void encode(CompletedSequenceRange value,
        @UnknownKeyFor @NonNull @Initialized OutputStream outStream)
        throws @UnknownKeyFor @NonNull @Initialized CoderException, @UnknownKeyFor @NonNull @Initialized IOException {
      VarLongCoder.of().encode(value.getStart(), outStream);
      VarLongCoder.of().encode(value.getEnd(), outStream);
      InstantCoder.of().encode(value.getTimestamp(), outStream);
    }

    @Override
    public CompletedSequenceRange decode(@UnknownKeyFor @NonNull @Initialized InputStream inStream)
        throws @UnknownKeyFor @NonNull @Initialized CoderException, @UnknownKeyFor @NonNull @Initialized IOException {
      long start = VarLongCoder.of().decode(inStream);
      long end = VarLongCoder.of().decode(inStream);
      Instant timestamp = InstantCoder.of().decode(inStream);
      return CompletedSequenceRange.of(start, end, timestamp);
    }
  }
}
