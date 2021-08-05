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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.facebook.presto.hadoop.$internal.org.apache.http.annotation.Experimental;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerMap;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
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
 * Currently only inner join is supported.
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
 * @param <K> the type of the keys in the input {@code PCollection}s
 * @param <V1> the type of the value in the first {@code PCollection}
 * @param <V2> the type of the value in the second {@code PCollection}
 * </pre>
 */
@AutoValue
@Experimental
public abstract class EventTimeEquiJoin<K, V1, V2>
    extends PTransform<PCollection<KV<K, V1>>, PCollection<KV<K, Pair<V1, V2>>>> {

  /* Where the output timestamp for each element is taken from. */
  public enum OutputTimestampFrom {
    FIRST_COLLECTION,
    SECOND_COLLECTION,
    MINIMUM_TIMESTAMP,
    MAXIMUM_TIMESTAMP
  }

  /**
   * Returns a {@link PTransform} that performs a {@link EventTimeEquiJoin} inner join on two
   * PCollections.
   */
  public static <K, V1, V2> EventTimeEquiJoin<K, V1, V2> innerJoin(
      PCollection<KV<K, V2>> secondCollection) {
    return new AutoValue_EventTimeEquiJoin.Builder<K, V1, V2>()
        .setSecondCollection(secondCollection)
        .setFirstCollectionValidFor(Duration.ZERO)
        .setSecondCollectionValidFor(Duration.ZERO)
        .setAllowedLateness(Duration.ZERO)
        .setOutputTimestampFrom(OutputTimestampFrom.MINIMUM_TIMESTAMP)
        .build();
  }

  abstract PCollection<KV<K, V2>> getSecondCollection();

  abstract Duration getFirstCollectionValidFor();

  abstract Duration getSecondCollectionValidFor();

  abstract Duration getAllowedLateness();

  abstract OutputTimestampFrom getOutputTimestampFrom();

  abstract Builder<K, V1, V2> toBuilder();

  @AutoValue.Builder
  public abstract static class Builder<K, V1, V2> {
    public abstract Builder<K, V1, V2> setSecondCollection(PCollection<KV<K, V2>> value);

    public abstract Builder<K, V1, V2> setFirstCollectionValidFor(Duration value);

    public abstract Builder<K, V1, V2> setSecondCollectionValidFor(Duration value);

    public abstract Builder<K, V1, V2> setAllowedLateness(Duration value);

    public abstract Builder<K, V1, V2> setOutputTimestampFrom(
        OutputTimestampFrom outputTimestampFrom);

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
   * second collection with the same keys and allows for late elements.
   *
   * @param allowedLateness the amount of time late elements are allowed.
   */
  public EventTimeEquiJoin<K, V1, V2> withAllowedLateness(Duration allowedLateness) {
    checkArgument(
        allowedLateness.isLongerThan(Duration.ZERO),
        "Allowed lateness for EventTimeEquiJoin must be positive.");
    return toBuilder().setAllowedLateness(allowedLateness).build();
  }

  /**
   * Returns a {@code Impl<K, V1, V2>} {@code PTransform} that matches elements from the first and
   * second collection with the same keys and allows for late elements.
   *
   * @param outputTimestampFrom where to pull the output timestamp from
   */
  public EventTimeEquiJoin<K, V1, V2> withOutputTimestampFrom(
      OutputTimestampFrom outputTimestampFrom) {
    return toBuilder().setOutputTimestampFrom(outputTimestampFrom).build();
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
                    getAllowedLateness(),
                    getOutputTimestampFrom())))
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

    // How to generate the output timestamp.
    private final OutputTimestampFrom outputTimestampFrom;

    @StateId("firstItems")
    private final StateSpec<OrderedListState<V1>> firstCollectionItems;

    @StateId("secondItems")
    private final StateSpec<OrderedListState<V2>> secondCollectionItems;

    // Timestamp of the oldest element that has not already been cleaned up used to ensure we don't
    // accept elements for timestamps we already cleaned up.
    @StateId("oldestFirstTimestamp")
    private final StateSpec<ValueState<Instant>> oldestFirstTimestamp;

    @StateId("oldestSecondTimestamp")
    private final StateSpec<ValueState<Instant>> oldestSecondTimestamp;

    @TimerFamily("cleanupTimers")
    private final TimerSpec cleanupTimers = TimerSpecs.timerMap(TimeDomain.EVENT_TIME);

    // Watermark holds for elements in the first collection.
    @TimerFamily("firstCollectionHolds")
    private final TimerSpec firstCollectionHolds = TimerSpecs.timerMap(TimeDomain.EVENT_TIME);

    // Watermark holds for elements in the second collection.
    @TimerFamily("secondCollectionHolds")
    private final TimerSpec secondCollectionHolds = TimerSpecs.timerMap(TimeDomain.EVENT_TIME);

    public EventTimeEquiJoinDoFn(
        Coder<V1> firstValueCoder,
        Coder<V2> secondValueCoder,
        Duration firstValidFor,
        Duration secondValidFor,
        Duration allowedLateness,
        OutputTimestampFrom outputTimestampFrom) {
      this.firstCollectionValidFor = firstValidFor;
      this.secondCollectionValidFor = secondValidFor;
      this.allowedLateness = allowedLateness;
      this.outputTimestampFrom = outputTimestampFrom;
      firstCollectionItems = StateSpecs.orderedList(firstValueCoder);
      secondCollectionItems = StateSpecs.orderedList(secondValueCoder);
      oldestFirstTimestamp = StateSpecs.value();
      oldestSecondTimestamp = StateSpecs.value();
    }

    @FunctionalInterface
    private interface Output<T1, T2> {
      void output(T1 one, T2 two, Instant tsOne, Instant tsTwo);
    }

    /** Adds a timer at the next bucket past time to fire at the bucket boundary. */
    private Timer addTimer(TimerMap timers, Instant time) {
      Instant nextBucketStart =
          Instant.ofEpochMilli(time.getMillis() / TIMER_BUCKET * TIMER_BUCKET + TIMER_BUCKET);
      Timer timer = timers.get(Long.toString(nextBucketStart.getMillis()));
      timer.set(nextBucketStart);
      return timer;
    }

    private <ThisT, OtherT> void processHelper(
        Output<ThisT, OtherT> output,
        KV<K, RawUnionValue> element,
        Instant ts,
        OrderedListState<ThisT> thisCollection,
        OrderedListState<OtherT> otherCollection,
        ValueState<Instant> oldestTimestampState,
        TimerMap cleanupTimers,
        TimerMap watermarkHolds,
        boolean elementAffectsWatermarkHolds,
        Duration thisCollectionValidFor,
        Duration otherCollectionValidFor) {
      Instant oldestTimestamp = oldestTimestampState.read();
      if (oldestTimestamp == null) {
        oldestTimestamp = new Instant(BoundedWindow.TIMESTAMP_MIN_VALUE);
        oldestTimestampState.write(new Instant(BoundedWindow.TIMESTAMP_MIN_VALUE));
      }
      if (ts.isBefore(oldestTimestamp)) {
        return; // Skip elements that are already cleaned up.
      }
      thisCollection.add(TimestampedValue.of((ThisT) element.getValue().getValue(), ts));
      Instant beginning = ts.minus(otherCollectionValidFor);
      Instant end = ts.plus(thisCollectionValidFor).plus(1L);
      for (TimestampedValue<OtherT> value : otherCollection.readRange(beginning, end)) {
        output.output(
            (ThisT) element.getValue().getValue(), value.getValue(), ts, value.getTimestamp());
      }

      addTimer(cleanupTimers, ts.plus(allowedLateness).plus(thisCollectionValidFor));

      if (elementAffectsWatermarkHolds) {
        addTimer(watermarkHolds, ts.plus(thisCollectionValidFor)).withOutputTimestamp(ts);
      }
    }

    @ProcessElement
    public void process(
        OutputReceiver<KV<K, Pair<V1, V2>>> output,
        @Element KV<K, RawUnionValue> element,
        @Timestamp Instant ts,
        @StateId("firstItems") OrderedListState<V1> firstItems,
        @StateId("secondItems") OrderedListState<V2> secondItems,
        @StateId("oldestFirstTimestamp") ValueState<Instant> oldestFirstTimestamp,
        @StateId("oldestSecondTimestamp") ValueState<Instant> oldestSecondTimestamp,
        @TimerFamily("cleanupTimers") TimerMap cleanupTimers,
        @TimerFamily("firstCollectionHolds") TimerMap firstCollectionHolds,
        @TimerFamily("secondCollectionHolds") TimerMap secondCollectionHolds) {
      boolean isMin = outputTimestampFrom == OutputTimestampFrom.MINIMUM_TIMESTAMP;
      switch (element.getValue().getUnionTag()) {
        case FIRST_TAG:
          processHelper(
              (V1 v1, V2 v2, Instant t1, Instant t2) ->
                  output.outputWithTimestamp(
                      KV.of(element.getKey(), Pair.of(v1, v2)), getOutputTimestamp(t1, t2)),
              element,
              ts,
              firstItems,
              secondItems,
              oldestFirstTimestamp,
              cleanupTimers,
              firstCollectionHolds,
              isMin || outputTimestampFrom == OutputTimestampFrom.FIRST_COLLECTION,
              firstCollectionValidFor,
              secondCollectionValidFor);
          break;
        case SECOND_TAG:
          processHelper(
              (V2 v2, V1 v1, Instant t2, Instant t1) ->
                  output.outputWithTimestamp(
                      KV.of(element.getKey(), Pair.of(v1, v2)), getOutputTimestamp(t1, t2)),
              element,
              ts,
              secondItems,
              firstItems,
              oldestSecondTimestamp,
              cleanupTimers,
              secondCollectionHolds,
              isMin || outputTimestampFrom == OutputTimestampFrom.SECOND_COLLECTION,
              secondCollectionValidFor,
              firstCollectionValidFor);
          break;
        default:
          throw new RuntimeException("Unexpected union tag for EventTimeEquiJoin.");
      }
    }

    @Override
    public Duration getAllowedTimestampSkew() {
      // We don't need the check because we hold the watermark back with timers.
      return Duration.millis(Long.MAX_VALUE);
    }

    /** Calculates the output timestamp from the timestamps of each element. * */
    private Instant getOutputTimestamp(Instant tsOne, Instant tsTwo) {
      switch (outputTimestampFrom) {
        case FIRST_COLLECTION:
          return tsOne;
        case SECOND_COLLECTION:
          return tsTwo;
        case MAXIMUM_TIMESTAMP:
          return tsOne.isAfter(tsTwo) ? tsOne : tsTwo;
        case MINIMUM_TIMESTAMP:
          return tsOne.isBefore(tsTwo) ? tsOne : tsTwo;
        default:
          throw new RuntimeException("Unexpected enum option.");
      }
    }

    @OnTimerFamily("firstCollectionHolds")
    public void onFirstCollectionHolds() {}

    @OnTimerFamily("secondCollectionHolds")
    public void onSecondCollectionHolds() {}

    @OnTimerFamily("cleanupTimers")
    public void onCleanupTimer(
        OnTimerContext context,
        @StateId("firstItems") OrderedListState<V1> firstItems,
        @StateId("secondItems") OrderedListState<V2> secondItems,
        @StateId("oldestFirstTimestamp") ValueState<Instant> oldestFirstTimestampState,
        @StateId("oldestSecondTimestamp") ValueState<Instant> oldestSecondTimestampState) {
      Instant ts = context.fireTimestamp();
      Instant currentTime = ts.minus(allowedLateness);
      Instant oldestFirstTimestamp = currentTime.minus(firstCollectionValidFor);
      Instant oldestSecondTimestamp = currentTime.minus(secondCollectionValidFor);
      oldestFirstTimestampState.write(oldestFirstTimestamp);
      oldestSecondTimestampState.write(oldestSecondTimestamp);
      firstItems.clearRange(BoundedWindow.TIMESTAMP_MIN_VALUE, oldestFirstTimestamp);
      secondItems.clearRange(BoundedWindow.TIMESTAMP_MIN_VALUE, oldestSecondTimestamp);
    }
  }
}
