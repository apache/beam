package org.apache.beam.sdk.extensions.ordered;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.ordered.combiner.DefaultSequenceCombiner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;
import org.joda.time.Instant;

class GlobalSequenceTracker<EventKeyT, EventT, ResultT, StateT extends MutableState<EventT, ResultT>> extends
    PTransform<PCollection<TimestampedValue<KV<EventKeyT, KV<Long, EventT>>>>, PCollectionView<CompletedSequenceRange>> {

  private final DefaultSequenceCombiner<EventKeyT, EventT, StateT> sequenceCombiner;

  public GlobalSequenceTracker(EventExaminer<EventT, StateT> eventExaminer) {
    this.sequenceCombiner = new DefaultSequenceCombiner<>(eventExaminer);
  }

  @Override
  public PCollectionView<CompletedSequenceRange> expand(
      PCollection<TimestampedValue<KV<EventKeyT, KV<Long, EventT>>>> input) {
    return
        input
            // TODO: get the windowing strategy from the input rather than assume global windows.
            .apply("Setup Triggering",
                Window.<TimestampedValue<KV<EventKeyT, KV<Long, EventT>>>>into(new GlobalWindows())
                    .accumulatingFiredPanes()
                    .triggering(
                        Repeatedly.forever(AfterFirst.of(
                            AfterPane.elementCountAtLeast(1),
                            AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(Duration.standardSeconds(5))))))
            .apply("Create Side Input",
                Combine.globally(sequenceCombiner).asSingletonView());
  }

  static class SequenceAndTimestampCoder extends CustomCoder<CompletedSequenceRange> {

    private static final SequenceAndTimestampCoder INSTANCE = new SequenceAndTimestampCoder();

    static SequenceAndTimestampCoder of() {
      return INSTANCE;
    }

    private SequenceAndTimestampCoder() {
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
