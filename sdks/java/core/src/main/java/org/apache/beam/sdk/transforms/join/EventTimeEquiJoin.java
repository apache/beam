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
package org.apache.beam.sdk.transforms.join;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.TimerMap;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Returns a {@link PTransform} that performs a {@link EventTimeEquiJoin} on two PCollections. A
 * {@link EventTimeEquiJoin} joins elements with equal keys bounded by the difference in event time.
 *
 * <p>Example of performing a {@link EventTimeEquiJoin}:
 *
 * <pre>{@code
 * PCollection<KV<K, V1>> pt1 = ...;
 * PCollection<KV<K, V2>> pt2 = ...;
 *
 * PCollection<KV<K, Pair<V1, V2>> eventTimeEquiJoinCollection =
 *   pt1.apply(EventTimeEquiJoin.<K, V1, V2>of(pt2));
 *
 * @param secondCollection the second collection to use in the join.
 * @param <K> the type of the keys in the input {@code PCollection}s
 * @param <V1> the type of the value in the first {@code PCollection}
 * @param <V2> the type of the value in the second {@code PCollection}
 * </pre>
 */
@AutoValue
public abstract class EventTimeEquiJoin<K, V1, V2>
    extends PTransform<PCollection<KV<K, V1>>, PCollection<KV<K, Pair<V1, V2>>>> {
  /** Returns a {@link PTransform} that performs a {@link EventTimeEquiJoin} on two PCollections. */
  public static <K, V1, V2> EventTimeEquiJoin<K, V1, V2> of(
      PCollection<KV<K, V2>> secondCollection) {
    return new AutoValue_EventTimeEquiJoin.Builder<K, V1, V2>()
        .setSecondCollection(secondCollection)
        .setFirstCollectionValidFor(Duration.ZERO)
        .setSecondCollectionValidFor(Duration.ZERO)
        .setAllowedLateness(Duration.ZERO)
        .build();
  }

  abstract PCollection<KV<K, V2>> getSecondCollection();

  abstract Duration getFirstCollectionValidFor();

  abstract Duration getSecondCollectionValidFor();

  abstract Duration getAllowedLateness();

  abstract Builder<K, V1, V2> toBuilder();

  @AutoValue.Builder
  public abstract static class Builder<K, V1, V2> {
    public abstract Builder<K, V1, V2> setSecondCollection(PCollection<KV<K, V2>> value);

    public abstract Builder<K, V1, V2> setFirstCollectionValidFor(Duration value);

    public abstract Builder<K, V1, V2> setSecondCollectionValidFor(Duration value);

    public abstract Builder<K, V1, V2> setAllowedLateness(Duration value);

    abstract EventTimeEquiJoin<K, V1, V2> build();
  }

  /**
   * Returns a {@code Impl<K, V1, V2>} {@code PTransform} that matches elements from the first and
   * second collection with the same keys if their timestamps are within the given interval.
   *
   * @param interval the allowed difference between the timestamps to allow a match
   */
  public EventTimeEquiJoin<K, V1, V2> within(Duration interval) {
    return toBuilder()
        .setFirstCollectionValidFor(interval)
        .setSecondCollectionValidFor(interval)
        .build();
  }

  /**
   * Returns a {@code Impl<K, V1, V2>} {@code PTransform} that matches elements from the first and
   * second collection with the same keys if the collection's element comes within the valid time
   * range for the other collection.
   *
   * @param firstCollectionValidFor the valid time range for the first collection
   * @param secondCollectionValidFor the valid time range for the second collection
   */
  public EventTimeEquiJoin<K, V1, V2> within(
      Duration firstCollectionValidFor, Duration secondCollectionValidFor) {
    return toBuilder()
        .setFirstCollectionValidFor(firstCollectionValidFor)
        .setSecondCollectionValidFor(secondCollectionValidFor)
        .build();
  }

  /**
   * Returns a {@code Impl<K, V1, V2>} {@code PTransform} that matches elements from the first and
   * second collection with the same keys and allows for late elements
   *
   * @param allowedLateness the amount of time late elements are allowed.
   */
  public EventTimeEquiJoin<K, V1, V2> withAllowedLateness(Duration allowedLateness) {
    return toBuilder().setAllowedLateness(allowedLateness).build();
  }

  @Override
  public PCollection<KV<K, Pair<V1, V2>>> expand(PCollection<KV<K, V1>> input) {
    Coder<K> keyCoder = JoinUtils.getKeyCoder(input);
    Coder<V1> firstValueCoder = JoinUtils.getValueCoder(input);
    Coder<V2> secondValueCoder = JoinUtils.getValueCoder(getSecondCollection());
    UnionCoder unionCoder = UnionCoder.of(ImmutableList.of(firstValueCoder, secondValueCoder));
    KvCoder<K, RawUnionValue> kvCoder = KvCoder.of(JoinUtils.getKeyCoder(input), unionCoder);
    PCollectionList<KV<K, RawUnionValue>> union =
        PCollectionList.of(JoinUtils.makeUnionTable(0, input, kvCoder))
            .and(JoinUtils.makeUnionTable(1, getSecondCollection(), kvCoder));
    return union
        .apply("Flatten", Flatten.pCollections())
        .apply(
            "Join",
            ParDo.of(
                new EventTimeEquiJoinDoFn<>(
                    firstValueCoder,
                    secondValueCoder,
                    getFirstCollectionValidFor(),
                    getSecondCollectionValidFor(),
                    getAllowedLateness())))
        .setCoder(KvCoder.of(keyCoder, PairCoder.<V1, V2>of(firstValueCoder, secondValueCoder)));
  }

  private static class EventTimeEquiJoinDoFn<K, V1, V2>
      extends DoFn<KV<K, RawUnionValue>, KV<K, Pair<V1, V2>>> {
    private static final int FIRST_TAG = 0;
    private static final int SECOND_TAG = 1;

    // Bucket cleanup timers into TIMER_BUCKET length buckets.
    private static final long TIMER_BUCKET = Duration.standardMinutes(1).getMillis();

    // How long elements in the first and second collection are valid (can be matched) for.
    private final Duration firstCollectionValidFor;
    private final Duration secondCollectionValidFor;

    // How long past the watermark that late elements can show up.
    private final Duration allowedLateness;

    @StateId("v1Items")
    private final StateSpec<OrderedListState<V1>> firstCollectionItems;

    @StateId("v2Items")
    private final StateSpec<OrderedListState<V2>> secondCollectionItems;

    @TimerFamily("cleanupTimers")
    private final TimerSpec cleanupTimers = TimerSpecs.timerMap(TimeDomain.EVENT_TIME);

    public EventTimeEquiJoinDoFn(
        Coder<V1> firstValueCoder,
        Coder<V2> secondValueCoder,
        Duration firstValidFor,
        Duration secondValidFor,
        Duration allowedLateness) {
      this.firstCollectionValidFor = firstValidFor;
      this.secondCollectionValidFor = secondValidFor;
      this.allowedLateness = allowedLateness;
      firstCollectionItems = StateSpecs.orderedList(firstValueCoder);
      secondCollectionItems = StateSpecs.orderedList(secondValueCoder);
    }

    @FunctionalInterface
    private interface Output<T1, T2> {
      void apply(T1 one, T2 two);
    }

    private <T, O> void processHelper(
        Output<T, O> output,
        KV<K, RawUnionValue> element,
        Instant ts,
        OrderedListState<T> thisCollection,
        OrderedListState<O> otherCollection,
        TimerMap cleanupTimers,
        Duration thisCollectionValidFor,
        Duration otherCollectionValidFor) {
      thisCollection.add(TimestampedValue.of((T) element.getValue().getValue(), ts));
      Instant beginning = ts.minus(otherCollectionValidFor);
      Instant end = ts.plus(thisCollectionValidFor).plus(1L);
      for (TimestampedValue<O> value : otherCollection.readRange(beginning, end)) {
        output.apply((T) element.getValue().getValue(), value.getValue());
      }
      Instant cleanupTime = ts.plus(allowedLateness).plus(thisCollectionValidFor);
      Instant nextBucketStart =
          Instant.ofEpochMilli(
              cleanupTime.getMillis() / TIMER_BUCKET * TIMER_BUCKET + TIMER_BUCKET);
      cleanupTimers.get(Long.toString(nextBucketStart.getMillis())).set(nextBucketStart);
    }

    @ProcessElement
    public void process(
        ProcessContext context,
        @Element KV<K, RawUnionValue> element,
        @Timestamp Instant ts,
        @StateId("v1Items") OrderedListState<V1> firstItems,
        @StateId("v2Items") OrderedListState<V2> secondItems,
        @TimerFamily("cleanupTimers") TimerMap cleanupTimers) {
      switch (element.getValue().getUnionTag()) {
        case FIRST_TAG:
          processHelper(
              (V1 v1, V2 v2) -> {
                context.output(KV.of(element.getKey(), Pair.of(v1, v2)));
              },
              element,
              ts,
              firstItems,
              secondItems,
              cleanupTimers,
              firstCollectionValidFor,
              secondCollectionValidFor);
          break;
        case SECOND_TAG:
          processHelper(
              (V2 v2, V1 v1) -> {
                context.output(KV.of(element.getKey(), Pair.of(v1, v2)));
              },
              element,
              ts,
              secondItems,
              firstItems,
              cleanupTimers,
              secondCollectionValidFor,
              firstCollectionValidFor);
      }
    }

    @OnTimerFamily("cleanupTimers")
    public void onCleanupTimer(
        @TimerId String timerId,
        @StateId("v1Items") OrderedListState<V1> firstItems,
        @StateId("v2Items") OrderedListState<V2> secondItems) {
      Instant currentTime = Instant.ofEpochMilli(Long.valueOf(timerId)).minus(allowedLateness);
      firstItems.clearRange(Instant.ofEpochMilli(0), currentTime.minus(firstCollectionValidFor));
      secondItems.clearRange(Instant.ofEpochMilli(0), currentTime.minus(secondCollectionValidFor));
    }
  }
}
