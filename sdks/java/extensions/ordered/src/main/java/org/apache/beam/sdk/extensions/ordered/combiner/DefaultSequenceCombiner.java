package org.apache.beam.sdk.extensions.ordered.combiner;

import java.util.Iterator;
import java.util.function.BiFunction;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.extensions.ordered.CompletedSequenceRange;
import org.apache.beam.sdk.extensions.ordered.EventExaminer;
import org.apache.beam.sdk.extensions.ordered.MutableState;
import org.apache.beam.sdk.extensions.ordered.combiner.SequenceRangeAccumulator.SequenceRangeAccumulatorCoder;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSequenceCombiner<EventKeyT, EventT, StateT extends MutableState<EventT, ?>> extends
    CombineFn<TimestampedValue<KV<EventKeyT, KV<Long, EventT>>>, SequenceRangeAccumulator, CompletedSequenceRange> {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultSequenceCombiner.class);

  public static final BiFunction<@NonNull Instant, @Nullable Instant, @Nullable Instant> OLDEST_TIMESTAMP_SELECTOR = (instant1, instant2) -> {
    if (instant2 == null) {
      return instant1;
    }
    @NonNull Instant nonNullableSecondValue = instant2;
    return instant1.isAfter(nonNullableSecondValue) ? instant1 : nonNullableSecondValue;
  };
  private final EventExaminer<EventT, StateT> eventExaminer;

  public DefaultSequenceCombiner(EventExaminer<EventT, StateT> eventExaminer) {
    this.eventExaminer = eventExaminer;
  }

  @Override
  public SequenceRangeAccumulator createAccumulator() {
    return new SequenceRangeAccumulator();
  }

  @Override
  public SequenceRangeAccumulator addInput(SequenceRangeAccumulator accum,
      TimestampedValue<KV<EventKeyT, KV<Long, EventT>>> event) {
    long sequence = event.getValue().getValue().getKey();

    accum.add(sequence, event.getTimestamp(),
        eventExaminer.isInitialEvent(sequence, event.getValue().getValue().getValue()));

    return accum;
  }

  @Override
  public SequenceRangeAccumulator mergeAccumulators(
      Iterable<SequenceRangeAccumulator> accumulators) {
    // There should be at least one accumulator.
    Iterator<SequenceRangeAccumulator> iterator = accumulators.iterator();
    SequenceRangeAccumulator result = iterator.next();
    while (iterator.hasNext()) {
      result.merge(iterator.next());
    }
    return result;
  }

  @Override
  public CompletedSequenceRange extractOutput(SequenceRangeAccumulator accum) {
    CompletedSequenceRange result = accum.largestContinuousRange();
    if(LOG.isTraceEnabled()) {
      LOG.trace("Returning completed sequence range: " + result);
    }
    return result;
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized Coder<SequenceRangeAccumulator> getAccumulatorCoder(
      @UnknownKeyFor @NonNull @Initialized CoderRegistry registry,
      @UnknownKeyFor @NonNull @Initialized Coder<TimestampedValue<KV<EventKeyT, KV<Long, EventT>>>> inputCoder)
      throws @UnknownKeyFor @NonNull @Initialized CannotProvideCoderException {
    return new SequenceRangeAccumulatorCoder();
  }
}
