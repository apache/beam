package org.apache.beam.sdk.extensions.ordered;

import org.apache.beam.sdk.extensions.ordered.ContiguousSequenceRange.CompletedSequenceRangeCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * PTransform to produce the side input of the maximum contiguous range of sequence numbers
 *
 * @param <EventKeyT> type of event key
 * @param <EventT>    type of event
 * @param <ResultT>   type of processing result
 * @param <StateT>    type of state
 */
class GlobalSequenceTracker<EventKeyT, EventT, ResultT, StateT extends MutableState<EventT, ResultT>> extends
    PTransform<PCollection<TimestampedValue<KV<EventKeyT, KV<Long, EventT>>>>, PCollectionView<ContiguousSequenceRange>> {

  private final Combine.GloballyAsSingletonView<TimestampedValue<KV<EventKeyT, KV<Long, EventT>>>, ContiguousSequenceRange> sideInputProducer;
  private final @Nullable Duration frequencyOfGeneration;

  public GlobalSequenceTracker(
      Combine.GloballyAsSingletonView<TimestampedValue<KV<EventKeyT, KV<Long, EventT>>>, ContiguousSequenceRange> sideInputProducer) {
    this.sideInputProducer = sideInputProducer;
    this.frequencyOfGeneration = null;
  }

  public GlobalSequenceTracker(
      Combine.GloballyAsSingletonView<TimestampedValue<KV<EventKeyT, KV<Long, EventT>>>, ContiguousSequenceRange> sideInputProducer,
      Duration globalSequenceGenerationFrequency) {
    this.sideInputProducer = sideInputProducer;
    this.frequencyOfGeneration = globalSequenceGenerationFrequency;
  }

  @Override
  public PCollectionView<ContiguousSequenceRange> expand(
      PCollection<TimestampedValue<KV<EventKeyT, KV<Long, EventT>>>> input) {
    input.getPipeline().getCoderRegistry().registerCoderForClass(
        ContiguousSequenceRange.class, CompletedSequenceRangeCoder.of());

    if (frequencyOfGeneration != null) {
      // This branch will only be executed in case of streaming pipelines.
      // For batch pipelines the side input should only be computed once.
      input = input
          .apply("Triggering Setup",
              // Reproduce the windowing of the input PCollection, but change the triggering
              // in order to create a slowing changing side input
              Window.<TimestampedValue<KV<EventKeyT, KV<Long, EventT>>>>into(
                      (WindowFn<? super TimestampedValue<KV<EventKeyT, KV<Long, EventT>>>, ?>)
                          input.getWindowingStrategy().getWindowFn()
                  )
                  .accumulatingFiredPanes()
                  // TODO: verify that we don't need to have the lateness parameterized
                  .withAllowedLateness(Duration.ZERO)
                  .triggering(
                      Repeatedly.forever(AfterFirst.of(
                          AfterPane.elementCountAtLeast(1),
                          AfterProcessingTime.pastFirstElementInPane()
                              .plusDelayOf(frequencyOfGeneration)))));
    }
    return input.apply("Create Side Input", sideInputProducer);
  }
}
