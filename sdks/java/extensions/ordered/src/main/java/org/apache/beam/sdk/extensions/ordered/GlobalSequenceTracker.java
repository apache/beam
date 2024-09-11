package org.apache.beam.sdk.extensions.ordered;

import org.apache.beam.sdk.extensions.ordered.CompletedSequenceRange.CompletedSequenceRangeCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;

class GlobalSequenceTracker<EventKeyT, EventT, ResultT, StateT extends MutableState<EventT, ResultT>> extends
    PTransform<PCollection<TimestampedValue<KV<EventKeyT, KV<Long, EventT>>>>, PCollectionView<CompletedSequenceRange>> {

  private final Combine.GloballyAsSingletonView<TimestampedValue<KV<EventKeyT, KV<Long, EventT>>>, CompletedSequenceRange> sideInputProducer;

  public GlobalSequenceTracker(
      Combine.GloballyAsSingletonView<TimestampedValue<KV<EventKeyT, KV<Long, EventT>>>, CompletedSequenceRange> sideInputProducer) {
    this.sideInputProducer = sideInputProducer;
  }

  @Override
  public PCollectionView<CompletedSequenceRange> expand(
      PCollection<TimestampedValue<KV<EventKeyT, KV<Long, EventT>>>> input) {
    input.getPipeline().getCoderRegistry().registerCoderForClass(
        CompletedSequenceRange.class,
        CompletedSequenceRangeCoder.of());

    return
        input
            // TODO: get the windowing strategy from the input rather than assume global windows.
            .apply("Setup Triggering",
                Window.<TimestampedValue<KV<EventKeyT, KV<Long, EventT>>>>into(
                    new GlobalWindows())
                    .accumulatingFiredPanes()
                    .triggering(
                        Repeatedly.forever(AfterFirst.of(
                            AfterPane.elementCountAtLeast(1),
                            AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(Duration.standardSeconds(5))))))
            .apply("Create Side Input", sideInputProducer);
  }
}
