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
package org.apache.beam.sdk.extensions.timeseries;

import com.google.auto.value.AutoValue;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.SortedMapCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.transforms.WithKeys;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.TimerMap;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TimestampedValue.TimestampedValueCoder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Fill gaps in timeseries. Values are expected to have Beam schemas registered.
 *
 * <p>This transform views the original PCollection as a collection of timeseries, each with a different key. They
 * key to be used and the timeseries bucket size are both specified in the {@link #of} creation method. Multiple
 * fields can be specified for the key - the key extracted will be a composite of all of them. Any elements in the
 * original {@link PCollection} will appear unchanged in the output PCollection, with timestamp and window unchanged.
 * Any gaps in timeseries (i.e. buckets with no elements) will be filled in the output PCollection with a single element
 * (by default the latest element seen or propagated into the previous bucket). The timestamp of the filled element is
 * the end of the bucket, and the original PCollection's window function is used to assign it to a window.
 *
 *
 * <p>Example usage: the following code views each user,country pair in the input {@link PCollection} as a timeseries
 * with bucket size one second. If any of these timeseries has a bucket with no elements, then the latest element from
 * the previous bucket (i.e. the one with the largest timestamp) wil be propagated into the missing bucket. If there
 * are multiple missing buckets, then they all will be filled up to 1 hour - the maximum gap size specified in
 * {@link #withMaxGapFillBuckets}.
 *
 * <pre>{@code PCollection<MyType> input = readInput();
 * PCollection<MyType> gapFilled =
 *   input.apply("fillGaps",
 *      FillGaps.of(Duration.standardSeconds(1), "userId", "country")
 *        .withMaxGapFillBuckets(3600L)));
 *  gapFilled.apply(MySink.create());
 *     }</pre>
 *
 * <p>By default, the latest element from the previous bucket is propagated into missing buckets. The user can override
 * this using the {@link #withMergeFunction} method. Several built-in merge functions are provided for -
 * {@link #keepLatest()} (the default), {@link #keepEarliest()}, an {@link #keepNull()}.
 *
 * <p>Sometimes elements need to be modified before being propagated into a missing bucket. For example, consider the
 * following element type containing a timestamp:
 *
 * <pre>{@code @DefaultSchema(JavaFieldSchema.class)
 * class MyType {
 *   MyData data;
 *   Instant timestamp;
 *   @SchemaCreate
 *   MyType(MyData data, Instant timestamp) {
 *       this.data = data;
 *       this.timestamp - timestamp;
 *   }
 * })</pre>
 *
 * The element timestamps should always be contained in its current timeseries bucket, so the element needs to be
 * modified when propagated to a new bucket. This can be done using the {@link #withInterpolateFunction}} method, as
 * follows:
 *
 * <pre>{@code PCollection<MyType> input = readInput();
 * PCollection<MyType> gapFilled =
 *   input.apply("fillGaps",
 *      FillGaps.of(Duration.standardSeconds(1), "userId", "country")
 *        .withInterpolateFunction(p -> new MyType(p.getValue().getValue().data, p.getNextWindow().maxTimestamp()))
 *        .withMaxGapFillBuckets(360L)));
 *  gapFilled.apply(MySink.create());
 *  }</pre>
 */
@AutoValue
public abstract class FillGaps<ValueT>
    extends PTransform<PCollection<ValueT>, PCollection<ValueT>> {
  // We garbage collect every 60 windows by default.
  private static final int GC_EVERY_N_BUCKETS = 60;

  /**
   * Argument to {@link #withMergeFunction}. Always propagates the element with the latest
   * timestamp.
   */
  public static <ValueT>
      SerializableBiFunction<
              TimestampedValue<ValueT>, TimestampedValue<ValueT>, TimestampedValue<ValueT>>
          keepLatest() {
    return (v1, v2) -> v1.getTimestamp().isAfter(v2.getTimestamp()) ? v1 : v2;
  }

  /**
   * Argument to {@link #withMergeFunction}. Always propagates the element with the earliest
   * timestamp.
   */
  public static <ValueT>
      SerializableBiFunction<
              TimestampedValue<ValueT>, TimestampedValue<ValueT>, TimestampedValue<ValueT>>
          keepEarliest() {
    return (v1, v2) -> v1.getTimestamp().isAfter(v2.getTimestamp()) ? v2 : v1;
  }

  /** Argument to withInterpolateFunction function. */
  @AutoValue
  public abstract static class InterpolateData<ValueT> {
    public abstract TimestampedValue<ValueT> getValue();

    public abstract BoundedWindow getPreviousWindow();

    public abstract BoundedWindow getNextWindow();
  }

  abstract Duration getTimeseriesBucketDuration();

  abstract Long getMaxGapFillBuckets();

  abstract Instant getStopTime();

  abstract FieldAccessDescriptor getKeyDescriptor();

  abstract SerializableBiFunction<
          TimestampedValue<ValueT>, TimestampedValue<ValueT>, TimestampedValue<ValueT>>
      getMergeValues();

  abstract int getGcEveryNBuckets();

  @Nullable
  abstract SerializableFunction<InterpolateData<ValueT>, ValueT> getInterpolateFunction();

  abstract Builder<ValueT> toBuilder();

  @AutoValue.Builder
  abstract static class Builder<ValueT> {
    abstract Builder<ValueT> setTimeseriesBucketDuration(Duration value);

    abstract Builder<ValueT> setMaxGapFillBuckets(Long value);

    abstract Builder<ValueT> setStopTime(Instant value);

    abstract Builder<ValueT> setKeyDescriptor(FieldAccessDescriptor keyDescriptor);

    abstract Builder<ValueT> setMergeValues(
        SerializableBiFunction<
                TimestampedValue<ValueT>, TimestampedValue<ValueT>, TimestampedValue<ValueT>>
            mergeValues);

    abstract Builder<ValueT> setInterpolateFunction(
        @Nullable SerializableFunction<InterpolateData<ValueT>, ValueT> interpolateFunction);

    abstract Builder<ValueT> setGcEveryNBuckets(int gcEveryNBuckets);

    abstract FillGaps<ValueT> build();
  }

  /** Construct the transform for the given duration and key fields. */
  public static <ValueT> FillGaps<ValueT> of(Duration windowDuration, String... keys) {
    return of(windowDuration, FieldAccessDescriptor.withFieldNames(keys));
  }

  /** Construct the transform for the given duration and key fields. */
  public static <ValueT> FillGaps<ValueT> of(
      Duration windowDuration, FieldAccessDescriptor keyDescriptor) {
    return new AutoValue_FillGaps.Builder<ValueT>()
        .setTimeseriesBucketDuration(windowDuration)
        .setMaxGapFillBuckets(Long.MAX_VALUE)
        .setStopTime(BoundedWindow.TIMESTAMP_MAX_VALUE)
        .setKeyDescriptor(keyDescriptor)
        .setMergeValues(keepLatest())
        .setGcEveryNBuckets(GC_EVERY_N_BUCKETS)
        .build();
  }

  /* The max gap duration that will be filled. The transform will stop filling timeseries buckets after this duration. */
  public FillGaps<ValueT> withMaxGapFillBuckets(Long value) {
    return toBuilder().setMaxGapFillBuckets(value).build();
  }

  /* A hard (event-time) stop time for the transform. */
  public FillGaps<ValueT> withStopTime(Instant stopTime) {
    return toBuilder().setStopTime(stopTime).build();
  }

  /**
   * If there are multiple values in a single timeseries bucket, this function is used to specify
   * what to propagate to the next bucket. If not specified, then the value with the latest
   * timestamp will be propagated.
   */
  public FillGaps<ValueT> withMergeFunction(
      SerializableBiFunction<
              TimestampedValue<ValueT>, TimestampedValue<ValueT>, TimestampedValue<ValueT>>
          mergeFunction) {
    return toBuilder().setMergeValues(mergeFunction).build();
  }

  /**
   * This function can be used to modify elements before propagating to the next bucket. A common
   * use case is to modify a contained timestamp to match that of the new bucket.
   */
  public FillGaps<ValueT> withInterpolateFunction(
      SerializableFunction<InterpolateData<ValueT>, ValueT> interpolateFunction) {
    return toBuilder().setInterpolateFunction(interpolateFunction).build();
  }

  @Override
  public PCollection<ValueT> expand(PCollection<ValueT> input) {
    if (!input.hasSchema()) {
      throw new RuntimeException("The input to FillGaps must have a schema.");
    }

    FixedWindows bucketWindows = FixedWindows.of(getTimeseriesBucketDuration());
    // TODO(reuvenlax, BEAM-12795): We need to create KVs to use state/timers. Once BEAM-12795 is
    // fixed we can dispense with the KVs here.
    PCollection<KV<Row, ValueT>> keyedValues =
        input
            .apply("FixedWindow", Window.into(bucketWindows))
            .apply("withKeys", WithKeys.of(getKeyDescriptor()));

    WindowFn<ValueT, BoundedWindow> originalWindowFn =
        (WindowFn<ValueT, BoundedWindow>) input.getWindowingStrategy().getWindowFn();
    return keyedValues
        .apply("globalWindow", Window.into(new GlobalWindows()))
        .apply(
            "fillGaps",
            ParDo.of(
                new FillGapsDoFn<>(
                    bucketWindows,
                    input.getCoder(),
                    getStopTime(),
                    getMaxGapFillBuckets(),
                    getMergeValues(),
                    getInterpolateFunction(),
                    getGcEveryNBuckets())))
        .apply("applyOriginalWindow", Window.into(originalWindowFn))
        .setCoder(input.getCoder());
  }

  public static class FillGapsDoFn<ValueT> extends DoFn<KV<Row, ValueT>, ValueT> {
    // The window size used.
    private final FixedWindows bucketWindows;
    // The garbage-collection window (GC_EVERY_N_BUCKETS * fixedWindows.getSize()).
    private final FixedWindows gcWindows;
    // The stop time.
    private final Instant stopTime;
    // The max gap-duration to fill. Once the gap fill exceeds this, we will stop filling the gap.
    private final long maxGapFillBuckets;

    private final SerializableBiFunction<
            TimestampedValue<ValueT>, TimestampedValue<ValueT>, TimestampedValue<ValueT>>
        mergeValues;

    @Nullable
    private final SerializableFunction<InterpolateData<ValueT>, ValueT> interpolateFunction;

    // A timer map used to fill potential gaps. Each logical "window" will have a separate timer
    // which will be cleared if an element arrives in that window. This way the timer will only fire
    // if there is a gap, at which point it will fill the gap.
    @TimerFamily("gapTimers")
    @SuppressWarnings({"UnusedVariable"})
    private final TimerSpec gapFillingTimersSpec = TimerSpecs.timerMap(TimeDomain.EVENT_TIME);

    // Timers used to garbage collect state.
    @TimerFamily("gcTimers")
    @SuppressWarnings({"UnusedVariable"})
    private final TimerSpec gcTimersSpec = TimerSpecs.timerMap(TimeDomain.EVENT_TIME);

    // Keep track of windows already seen. In the future we can replace this with OrderedListState.
    // Keyed by window end timestamp (which is 1ms greater than the window max timestamp).
    @StateId("seenBuckets")
    @SuppressWarnings({"UnusedVariable"})
    private final StateSpec<ValueState<SortedMap<Instant, TimestampedValue<ValueT>>>>
        seenBucketsSpec;

    // For every window, keep track of how long the filled gap is in buckets. If a window was
    // populated by a received element - i.e.
    // it's not
    // a gap fill - then there is no value in this map for that window.
    // Keyed by window end timestamp (which is 1ms greater than the window max timestamp).
    @StateId("gapDurationMap")
    @SuppressWarnings({"UnusedVariable"})
    private final StateSpec<ValueState<SortedMap<Instant, Long>>> gapDurationSpec;

    FillGapsDoFn(
        FixedWindows bucketWindows,
        Coder<ValueT> valueCoder,
        Instant stopTime,
        long maxGapFillBuckets,
        SerializableBiFunction<
                TimestampedValue<ValueT>, TimestampedValue<ValueT>, TimestampedValue<ValueT>>
            mergeValues,
        @Nullable SerializableFunction<InterpolateData<ValueT>, ValueT> interpolateFunction,
        int gcEveryNBuckets) {
      this.bucketWindows = bucketWindows;
      this.gcWindows = FixedWindows.of(bucketWindows.getSize().multipliedBy(gcEveryNBuckets));
      this.stopTime = stopTime;
      this.maxGapFillBuckets = maxGapFillBuckets;
      this.seenBucketsSpec =
          StateSpecs.value(
              SortedMapCoder.of(InstantCoder.of(), TimestampedValueCoder.of(valueCoder)));
      this.gapDurationSpec =
          StateSpecs.value(SortedMapCoder.of(InstantCoder.of(), VarLongCoder.of()));
      this.mergeValues = mergeValues;
      this.interpolateFunction = interpolateFunction;
    }

    @ProcessElement
    public void process(
        @Element KV<Row, ValueT> element,
        @Timestamp Instant ts,
        @TimerFamily("gapTimers") TimerMap gapTimers,
        @TimerFamily("gcTimers") TimerMap gcTimers,
        @AlwaysFetched @StateId("seenBuckets")
            ValueState<SortedMap<Instant, TimestampedValue<ValueT>>> seenBuckets,
        OutputReceiver<ValueT> output) {
      if (ts.isAfter(stopTime)) {
        return;
      }

      Instant windowEndTs = bucketWindows.assignWindow(ts).end();
      if (processEvent(
          () -> TimestampedValue.of(element.getValue(), ts),
          windowEndTs,
          gapTimers,
          gcTimers,
          seenBuckets,
          -1,
          output)) {
        // We've seen data for this window, so clear any gap-filling timer.
        gapTimers.get(windowToTimerTag(windowEndTs)).clear();
      }
    }

    private String windowToTimerTag(Instant endTs) {
      return Long.toString(endTs.getMillis());
    }

    private Instant windowFromTimerTag(String key) {
      return Instant.ofEpochMilli(Long.parseLong(key));
    }

    @OnTimerFamily("gapTimers")
    public void onTimer(
        @TimerId String timerId,
        @Timestamp Instant timestamp,
        @TimerFamily("gapTimers") TimerMap gapTimers,
        @TimerFamily("gcTimers") TimerMap gcTimers,
        @AlwaysFetched @StateId("seenBuckets")
            ValueState<SortedMap<Instant, TimestampedValue<ValueT>>> seenBuckets,
        @AlwaysFetched @StateId("gapDurationMap") ValueState<SortedMap<Instant, Long>> gapDurations,
        OutputReceiver<ValueT> output) {
      Instant bucketEndTs = windowFromTimerTag(timerId);
      Instant bucketMaxTs = bucketEndTs.minus(Duration.millis(1));
      Instant previousBucketEndTs = bucketEndTs.minus(bucketWindows.getSize());
      Instant previousBucketMaxTs = previousBucketEndTs.minus(Duration.millis(1));

      Map<Instant, TimestampedValue<ValueT>> seenBucketMap = seenBuckets.read();
      if (seenBucketMap == null) {
        throw new RuntimeException("Unexpected timer fired with no seenBucketMap.");
      }

      @Nullable SortedMap<Instant, Long> gapDurationsMap = gapDurations.read();
      long gapSize = 0;
      if (gapDurationsMap != null) {
        gapSize = gapDurationsMap.getOrDefault(previousBucketEndTs, 0L);
      }
      // If the timer fires and we've never seen an element for this window then we reach into the
      // previous
      // window and copy its value into this window. This relies on the fact that timers fire in
      // order
      // for a given key, so if there are multiple gap windows  then the previous window will be
      // filled
      // by the time we get here.
      // processEvent will also set a timer for the next window if we haven't already seen an
      // element
      // for that window.
      processEvent(
          () -> {
            TimestampedValue<ValueT> previous = seenBucketMap.get(previousBucketEndTs);
            if (previous == null) {
              throw new RuntimeException(
                  "Processing bucket for "
                      + bucketEndTs
                      + " before processing bucket "
                      + "for "
                      + previousBucketEndTs);
            }
            ValueT value = previous.getValue();
            if (interpolateFunction != null) {
              BoundedWindow previousBucket = bucketWindows.assignWindow(previousBucketMaxTs);
              BoundedWindow currentBucket = bucketWindows.assignWindow(bucketMaxTs);
              Preconditions.checkState(!currentBucket.equals(previousBucket));
              value =
                  interpolateFunction.apply(
                      new AutoValue_FillGaps_InterpolateData<>(
                          previous, previousBucket, currentBucket));
            }
            return TimestampedValue.of(value, bucketMaxTs);
          },
          bucketEndTs,
          gapTimers,
          gcTimers,
          seenBuckets,
          gapSize,
          output);
      if (!seenBucketMap.containsKey(bucketEndTs.plus(bucketWindows.getSize()))) {
        // The next bucket is still empty, so update gapDurations
        if (gapDurationsMap == null) {
          gapDurationsMap = Maps.newTreeMap();
        }
        gapDurationsMap.put(bucketEndTs, gapSize + 1);
        gapDurations.write(gapDurationsMap);
      }
    }

    @OnTimerFamily("gcTimers")
    public void onGcTimer(
        @Timestamp Instant timerTs,
        @AlwaysFetched @StateId("seenBuckets")
            ValueState<SortedMap<Instant, TimestampedValue<ValueT>>> seenBuckets,
        @AlwaysFetched @StateId("gapDurationMap")
            ValueState<SortedMap<Instant, Long>> gapDurations) {
      gcMap(seenBuckets, timerTs.minus(gcWindows.getSize()));
      gcMap(gapDurations, timerTs.minus(gcWindows.getSize()));
    }

    // returns true if this is the first event for the bucket.
    private boolean processEvent(
        Supplier<TimestampedValue<ValueT>> getValue,
        Instant bucketEndTs,
        TimerMap gapTimers,
        TimerMap gcTimers,
        ValueState<SortedMap<Instant, TimestampedValue<ValueT>>> seenBuckets,
        long gapSize,
        OutputReceiver<ValueT> output) {
      TimestampedValue<ValueT> value = getValue.get();
      output.outputWithTimestamp(value.getValue(), value.getTimestamp());

      boolean firstElementInBucket = true;
      TimestampedValue<ValueT> valueToWrite = value;
      SortedMap<Instant, TimestampedValue<ValueT>> seenBucketsMap = seenBuckets.read();
      if (seenBucketsMap == null) {
        seenBucketsMap = Maps.newTreeMap();
      } else {
        @Nullable TimestampedValue<ValueT> existing = seenBucketsMap.get(bucketEndTs);
        if (existing != null) {
          valueToWrite = mergeValues.apply(existing, value);
          // No need to set a timer as we've already seen an element for this window before.
          firstElementInBucket = false;
        }
      }

      // Update the seenWindows state variable.
      seenBucketsMap.put(bucketEndTs, valueToWrite);
      seenBuckets.write(seenBucketsMap);

      if (firstElementInBucket) {
        // Potentially set a timer for the next window.

        // Here we calculate how long the gap-extension duration (the total size of all gaps filled
        // since
        // the last element seen) would be for the next window. If this would exceed the max-gap
        // duration
        // set by the user, then stop.
        Instant nextBucketEndTs = bucketEndTs.plus(bucketWindows.getSize());
        Instant nextBucketMaxTs = nextBucketEndTs.minus(Duration.millis(1));
        // Set a gap-filling timer for the next window if we haven't yet seen that window.
        if (nextBucketMaxTs.isBefore(stopTime)
            && gapSize + 1 < maxGapFillBuckets
            && !seenBucketsMap.containsKey(nextBucketEndTs)) {
          gapTimers
              .get(windowToTimerTag(nextBucketEndTs))
              .withOutputTimestamp(bucketEndTs)
              .set(nextBucketEndTs);
        }

        // Set a gcTimer
        Instant gcTs = gcWindows.assignWindow(nextBucketEndTs).end();
        gcTimers.get(windowToTimerTag(gcTs)).set(gcTs);
      }
      return firstElementInBucket;
    }

    private static <V> void gcMap(ValueState<SortedMap<Instant, V>> mapState, Instant ts) {
      SortedMap<Instant, V> map = mapState.read();
      if (map != null) {
        // Clear all map elements that are for windows strictly less than timerTs.
        map.headMap(ts).clear();
        if (map.isEmpty()) {
          mapState.clear();
        } else {
          mapState.write(map);
        }
      }
    }
  }
}
