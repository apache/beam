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
 * A {@link PTransform} that performs a {@link EventTimeEquiJoin} on two PCollections. A
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
 * </pre>
 */
public class EventTimeEquiJoin {

  public static <K, V1, V2> Impl<K, V1, V2> of(PCollection<KV<K, V2>> secondCollection) {
    return new AutoValue_BiTemporalJoin_Impl.Builder<K, V1, V2>()
        .setSecondCollection(secondCollection)
        .setFirstValidFor(Duration.ZERO)
        .setSecondValidFor(Duration.ZERO)
        .setAllowedLateness(Duration.ZERO)
        .build();
  }

  
  @AutoValue
  public abstract static class Impl<K, V1, V2>
      extends PTransform<PCollection<KV<K, V1>>, PCollection<KV<K, Pair<V1, V2>>>> {
    abstract PCollection<KV<K, V2>> getSecondCollection();

    abstract Duration getFirstValidFor();

    abstract Duration getSecondValidFor();

    abstract Duration getAllowedLateness();

    abstract Builder<K, V1, V2> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K, V1, V2> {
      abstract Builder<K, V1, V2> setSecondCollection(PCollection<KV<K, V2>> rhsCollection);

      public abstract Builder<K, V1, V2> setFirstValidFor(Duration value);

      public abstract Builder<K, V1, V2> setSecondValidFor(Duration value);

      public abstract Builder<K, V1, V2> setAllowedLateness(Duration value);

      abstract Impl<K, V1, V2> build();
    }

    public Impl<K, V1, V2> within(Duration interval) {
      return toBuilder().setFirstValidFor(interval).setSecondValidFor(interval).build();
    }

    public Impl<K, V1, V2> within(Duration firstElementWithin, Duration secondElementWithin) {
      return toBuilder()
          .setFirstValidFor(secondElementWithin)
          .setSecondValidFor(firstElementWithin)
          .build();
    }

    public Impl<K, V1, V2> withAllowedLateness(Duration allowedLateness) {
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
                  new BiTemporalJoinDoFn<>(
                      firstValueCoder,
                      secondValueCoder,
                      getFirstValidFor(),
                      getSecondValidFor(),
                      getAllowedLateness())))
          .setCoder(
              KvCoder.of(keyCoder, PairCoder.<V1, V2>of(firstValueCoder, secondValueCoder)));
    }
  }

  private static class BiTemporalJoinDoFn<K, V1, V2>
      extends DoFn<KV<K, RawUnionValue>, KV<K, Pair<V1, V2>>> {
    private static final int FIRST_TAG = 0;
    private static final int SECOND_TAG = 1;

    // Bucket cleanup timers into TIMER_BUCKET length buckets.
    private static final long TIMER_BUCKET = Duration.standardMinutes(1).getMillis();

    // How long elements in the first and second collection are valid (can be matched) for.
    private final Duration firstValidFor;
    private final Duration secondValidFor;

    // How long past the watermark that late elements can show up.
    private final Duration allowedLateness;

    @StateId("v1Items")
    private final StateSpec<OrderedListState<V1>> firstItems;

    @StateId("v2Items")
    private final StateSpec<OrderedListState<V2>> secondItems;

    @TimerFamily("cleanupTimers")
    private final TimerSpec cleanupTimers = TimerSpecs.timerMap(TimeDomain.EVENT_TIME);

    public BiTemporalJoinDoFn(
        Coder<V1> firstValueCoder,
        Coder<V2> secondValueCoder,
        Duration firstValidFor,
        Duration secondValidFor,
        Duration allowedLateness) {
      this.firstValidFor = firstValidFor;
      this.secondValidFor = secondValidFor;
      this.allowedLateness = allowedLateness;
      firstItems = StateSpecs.orderedList(firstValueCoder);
      secondItems = StateSpecs.orderedList(secondValueCoder);

    }

    @ProcessElement
    public void process(
        ProcessContext context,
        @Element KV<K, RawUnionValue> element,
        @Timestamp Instant ts,
        @StateId("v1Items") OrderedListState<V1> firstItems,
        @StateId("v2Items") OrderedListState<V2> secondItems,
        @TimerFamily("cleanupTimers") TimerMap cleanupTimers) {
      String result =
          "Processing element: "
              + element + " at " + ts.toInstant().getMillis();
      Duration validFor = null;
      switch (element.getValue().getUnionTag()) {
        case FIRST_TAG:
          validFor = firstValidFor;
          firstItems.add(TimestampedValue.of((V1) element.getValue().getValue(), ts));

          result += " and looking for match in " + printRanges(secondItems) + ".";
          for (TimestampedValue<V2> value :
              getRange(secondItems, ts, firstValidFor, secondValidFor)) {
            result += " match (" + value.getValue();
            context.output(
                KV.of(
                    element.getKey(),
                    Pair.of((V1) element.getValue().getValue(), value.getValue())));
          }
          break;
        case SECOND_TAG:
          validFor = secondValidFor;
          secondItems.add(TimestampedValue.of((V2) element.getValue().getValue(), ts));
          result += " and looking for match in " + printRanges(firstItems);
          for (TimestampedValue<V1> value :
              getRange(firstItems, ts, secondValidFor, firstValidFor)) {
            result += " match (" + value.getValue() + ") ";
            context.output(
                KV.of(
                    element.getKey(),
                    Pair.of(value.getValue(), (V2) element.getValue().getValue())));
          }
      }
      Instant cleanupTime = ts.plus(allowedLateness).plus(validFor);
      Instant nextBucketStart =
          Instant.ofEpochMilli(
              cleanupTime.getMillis() / TIMER_BUCKET * TIMER_BUCKET + TIMER_BUCKET);
      cleanupTimers.get(Long.toString(nextBucketStart.getMillis())).set(nextBucketStart);

      System.out.println(result);
    }

    @OnTimerFamily("cleanupTimers")
    public void onCleanupTimer(
        @TimerId String timerId,
        @StateId("v1Items") OrderedListState<V1> firstItems,
        @StateId("v2Items") OrderedListState<V2> secondItems) {
      Instant currentTime = Instant.ofEpochMilli(Long.valueOf(timerId)).minus(allowedLateness);
      firstItems.clearRange(Instant.ofEpochMilli(0), currentTime.minus(firstValidFor));
      secondItems.clearRange(Instant.ofEpochMilli(0), currentTime.minus(secondValidFor));
      System.out.println("Cleaning up! " + timerId + " first: 0-"
                             + currentTime.minus(firstValidFor).getMillis()
                             +  " Second: 0-" + currentTime.minus(secondValidFor).getMillis()
                             + " at time " + Instant.ofEpochMilli(Long.valueOf(timerId)).getMillis());
    }

    private <V> Iterable<TimestampedValue<V>> getRange(
        OrderedListState<V> otherItems,
        Instant ts,
        Duration newElementDelay,
        Duration otherElementDelay) {
      Instant beginning = ts.minus(otherElementDelay);
      Instant end = ts.plus(newElementDelay).plus(1L);
      System.out.println(
          "Looking in range " + beginning.getMillis() + " - " + end.getMillis() + " " + printRanges(otherItems));
      return otherItems.readRange(beginning, end);
    }

    private <V> String printRanges(OrderedListState<V> otherItems) {
      String a = "[";
      for (TimestampedValue<V> t : otherItems.read()) {
        a += t.getValue() + " " + t.getTimestamp().toInstant().getMillis() + ",";
      }
      a += "]";
      return a;
    }
  }
}
