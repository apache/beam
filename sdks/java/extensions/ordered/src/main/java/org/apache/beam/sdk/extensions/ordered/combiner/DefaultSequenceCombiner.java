/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.ordered.combiner;

import java.util.Iterator;
import java.util.function.BiFunction;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.extensions.ordered.ContiguousSequenceRange;
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

/**
 * Default global sequence combiner.
 *
 * <p>Produces the largest {@link ContiguousSequenceRange} of contiguous longs which starts from the
 * initial event identified by {@link EventExaminer#isInitialEvent(long, EventT)}.
 *
 * <p>This combiner currently doesn't use {@link EventExaminer#isLastEvent(long, EventT)}.
 *
 * @param <EventKeyT> type of key
 * @param <EventT> type of event
 * @param <StateT> type of state
 */
public class DefaultSequenceCombiner<EventKeyT, EventT, StateT extends MutableState<EventT, ?>>
    extends CombineFn<
        TimestampedValue<KV<EventKeyT, KV<Long, EventT>>>,
        SequenceRangeAccumulator,
        ContiguousSequenceRange> {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultSequenceCombiner.class);

  public static final BiFunction<@NonNull Instant, @Nullable Instant, @Nullable Instant>
      OLDEST_TIMESTAMP_SELECTOR =
          (instant1, instant2) -> {
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
  public SequenceRangeAccumulator addInput(
      SequenceRangeAccumulator accum, TimestampedValue<KV<EventKeyT, KV<Long, EventT>>> event) {
    long sequence = event.getValue().getValue().getKey();

    accum.add(
        sequence,
        event.getTimestamp(),
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
  public ContiguousSequenceRange extractOutput(SequenceRangeAccumulator accum) {
    ContiguousSequenceRange result = accum.largestContinuousRange();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Returning completed sequence range: " + result);
    }
    return result;
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized Coder<SequenceRangeAccumulator> getAccumulatorCoder(
      @UnknownKeyFor @NonNull @Initialized CoderRegistry registry,
      @UnknownKeyFor @NonNull @Initialized
          Coder<TimestampedValue<KV<EventKeyT, KV<Long, EventT>>>> inputCoder)
      throws @UnknownKeyFor @NonNull @Initialized CannotProvideCoderException {
    return SequenceRangeAccumulatorCoder.of();
  }
}
