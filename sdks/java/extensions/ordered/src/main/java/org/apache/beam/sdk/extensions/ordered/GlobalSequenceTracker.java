package org.apache.beam.sdk.extensions.ordered;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.extensions.ordered.GlobalSequenceTracker.SequenceAndTimestamp;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Range;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.RangeMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.TreeRangeMap;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;
import org.joda.time.Instant;

class GlobalSequenceTracker extends
    PTransform<PCollection<SequenceAndTimestamp>, PCollectionView<SequenceAndTimestamp>> {

  @Override
  public PCollectionView<SequenceAndTimestamp> expand(PCollection<SequenceAndTimestamp> input) {
    return
        input
            .apply("Setup Triggering", Window.<SequenceAndTimestamp>into(new GlobalWindows())
                .accumulatingFiredPanes()
                .triggering(
                    Repeatedly.forever(AfterFirst.of(
                        AfterPane.elementCountAtLeast(1),
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(Duration.standardSeconds(5))))))
            .apply("Create Side Input",
                Combine.globally(new GlobalSequenceCombiner()).asSingletonView());
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  static abstract class SequenceAndTimestamp {

    public abstract long getSequence();

    public abstract Instant getTimestamp();

    public static SequenceAndTimestamp create(long sequence, Instant timestamp) {
      return new AutoValue_GlobalSequenceTracker_SequenceAndTimestamp(sequence, timestamp);
    }
  }

  static class GlobalSequenceCombiner extends
      CombineFn<SequenceAndTimestamp, RangeMap<Long, Instant>, SequenceAndTimestamp> {

    public static final BiFunction<@NonNull Instant, @Nullable Instant, @Nullable Instant> OLDEST_TIMESTAMP_SELECTOR = (instant1, instant2) -> {
      if (instant2 == null) {
        return instant1;
      }
      @NonNull Instant nonNullableSecondValue = instant2;
      return instant1.isAfter(nonNullableSecondValue) ? instant1 : nonNullableSecondValue;
    };


    @Override
    public RangeMap<Long, Instant> createAccumulator() {
      return TreeRangeMap.create();
    }

    @Override
    public RangeMap<Long, Instant> addInput(RangeMap<Long, Instant> accum,
        SequenceAndTimestamp event) {
      accum.merge(Range.singleton(event.getSequence()), event.getTimestamp(),
          OLDEST_TIMESTAMP_SELECTOR);
      return accum;
    }

    @Override
    public RangeMap<Long, Instant> mergeAccumulators(
        Iterable<RangeMap<Long, Instant>> accumulators) {
      RangeMap<Long, Instant> newAccum = createAccumulator();
      for (RangeMap<Long, Instant> accum : accumulators) {
        for (Map.Entry<Range<Long>, Instant> entry : accum.asMapOfRanges().entrySet()) {
          newAccum.merge(entry.getKey(), entry.getValue(), OLDEST_TIMESTAMP_SELECTOR);
        }
      }
      return newAccum;
    }

    @Override
    public SequenceAndTimestamp extractOutput(RangeMap<Long, Instant> accum) {
      SequenceAndTimestamp output = SequenceAndTimestamp.create(Long.MIN_VALUE, Instant.EPOCH);
      Iterator<Entry<Range<Long>, Instant>> iter = accum.asMapOfRanges().entrySet().iterator();
      Map.Entry<Range<Long>, Instant> prevEntry = null;
      while (iter.hasNext()) {
        Map.Entry<Range<Long>, Instant> entry = iter.next();
        if (prevEntry != null
            && entry.getKey().lowerEndpoint() > prevEntry.getKey().upperEndpoint() + 1) {
          // We have a hole! Break out.
          break;
        }
        output = SequenceAndTimestamp.create(entry.getKey().upperEndpoint(), entry.getValue());
        prevEntry = entry;
      }
      return output;
    }


    static class AccumulatorCoder extends Coder<RangeMap<Long, Instant>> {

      @Override
      public void encode(RangeMap<Long, Instant> value,
          @UnknownKeyFor @NonNull @Initialized OutputStream outStream)
          throws @UnknownKeyFor@NonNull@Initialized CoderException, @UnknownKeyFor@NonNull@Initialized IOException {

      }

      @Override
      public RangeMap<Long, Instant> decode(
          @UnknownKeyFor @NonNull @Initialized InputStream inStream)
          throws @UnknownKeyFor@NonNull@Initialized CoderException, @UnknownKeyFor@NonNull@Initialized IOException {
        return TreeRangeMap.create();
      }

      @Override
      public @UnknownKeyFor @NonNull @Initialized List<? extends @UnknownKeyFor @NonNull @Initialized Coder<@UnknownKeyFor @NonNull @Initialized ?>> getCoderArguments() {
        return new ArrayList<Coder<RangeMap<Long, Instant>>>();
      }

      @Override
      public void verifyDeterministic()
          throws @UnknownKeyFor@NonNull@Initialized NonDeterministicException {

      }
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized Coder<RangeMap<Long, Instant>> getAccumulatorCoder(
        @UnknownKeyFor @NonNull @Initialized CoderRegistry registry,
        @UnknownKeyFor @NonNull @Initialized Coder<SequenceAndTimestamp> inputCoder)
        throws @UnknownKeyFor@NonNull@Initialized CannotProvideCoderException {
      return new AccumulatorCoder();
    }
  }
}
