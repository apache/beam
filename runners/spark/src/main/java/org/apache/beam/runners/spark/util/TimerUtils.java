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
package org.apache.beam.runners.spark.util;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.spark.stateful.SparkTimerInternals;
import org.apache.beam.runners.spark.translation.AbstractInOutIterator;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext$;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction3;

/**
 * Utility class for handling timers in the Spark runner. Provides functionality for managing timer
 * operations in stateful processing.
 */
public class TimerUtils {

  public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  /**
   * A serializable version of the AbstractFunction3 class from Scala. Used for stateful operations
   * in Spark Streaming.
   *
   * @param <T1> Type of the first parameter
   * @param <T2> Type of the second parameter
   * @param <T3> Type of the third parameter
   * @param <ReturnT> Return type of the function
   */
  abstract static class SerializableFunction3<T1, T2, T3, ReturnT>
      extends AbstractFunction3<T1, T2, T3, ReturnT> implements Serializable {}

  /**
   * A marker class used to identify timer keys and values in Spark transformations. Helps
   * distinguish between regular data flow elements and timer events.
   */
  public static class TimerMarker implements Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public String toString() {
      return "TIMER_MARKER";
    }

    @Override
    public boolean equals(@Nullable Object o) {
      return o instanceof TimerMarker;
    }

    @Override
    public int hashCode() {
      return 1; // All instances are equal, so they should have the same hash code
    }
  }

  /** Constant marker used to identify timer values in transformations. */
  public static final TimerMarker TIMER_MARKER = new TimerMarker();

  private static class WindowedValueForTimerMarker<T> implements Serializable, WindowedValue<T> {
    private final T value;

    public WindowedValueForTimerMarker(T value) {
      this.value = value;
    }

    @Override
    public PaneInfo getPaneInfo() {
      return PaneInfo.NO_FIRING;
    }

    @Override
    public @Nullable String getCurrentRecordId() {
      return null;
    }

    @Override
    public @Nullable Long getCurrentRecordOffset() {
      return null;
    }

    @Override
    public Iterable<WindowedValue<T>> explodeWindows() {
      return Collections.emptyList();
    }

    @Override
    public T getValue() {
      return value;
    }

    @Override
    public <NewT> WindowedValue<NewT> withValue(NewT newValue) {
      return new WindowedValueForTimerMarker<>(newValue);
    }

    @Override
    public Instant getTimestamp() {
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    @Override
    public Collection<? extends BoundedWindow> getWindows() {
      return Collections.emptyList();
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (o instanceof WindowedValueForTimerMarker) {
        WindowedValueForTimerMarker<?> that = (WindowedValueForTimerMarker<?>) o;
        return Objects.equals(this.getValue(), that.getValue());
      } else {
        return super.equals(o);
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(getValue());
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("value", getValue())
          .add("pane", getPaneInfo())
          .toString();
    }
  }

  /**
   * Fires all expired timers using the provided iterator.
   *
   * <p>Gets expired timers from {@link SparkTimerInternals} and processes each one. The {@link
   * AbstractInOutIterator#fireTimer} method automatically deletes each timer after processing.
   *
   * @param sparkTimerInternals Source of timer data
   * @param windowingStrategy Used to determine which timers are expired
   * @param abstractInOutIterator Iterator that processes and then removes the timers
   * @param <W> Window type
   */
  public static <W extends BoundedWindow> void triggerExpiredTimers(
      SparkTimerInternals sparkTimerInternals,
      WindowingStrategy<?, W> windowingStrategy,
      AbstractInOutIterator<?, ?, ?> abstractInOutIterator) {
    final Collection<TimerInternals.TimerData> expiredTimers =
        getExpiredTimers(sparkTimerInternals, windowingStrategy);

    if (!expiredTimers.isEmpty()) {
      expiredTimers.forEach(abstractInOutIterator::fireTimer);
    }
  }

  public static <W extends BoundedWindow> void dropExpiredTimers(
      SparkTimerInternals sparkTimerInternals, WindowingStrategy<?, W> windowingStrategy) {
    final Collection<TimerInternals.TimerData> expiredTimers =
        getExpiredTimers(sparkTimerInternals, windowingStrategy);

    // Remove the expired timer from the timerInternals structure
    if (!expiredTimers.isEmpty()) {
      expiredTimers.forEach(sparkTimerInternals::deleteTimer);
    }
  }

  private static <W extends BoundedWindow> Collection<TimerInternals.TimerData> getExpiredTimers(
      SparkTimerInternals sparkTimerInternals, WindowingStrategy<?, W> windowingStrategy) {
    return sparkTimerInternals.getTimers().stream()
        .filter(
            timer ->
                timer
                    .getTimestamp()
                    .plus(windowingStrategy.getAllowedLateness())
                    .isBefore(
                        timer.getDomain().equals(TimeDomain.PROCESSING_TIME)
                            ? sparkTimerInternals.currentProcessingTime()
                            : sparkTimerInternals.currentInputWatermarkTime()))
        .collect(Collectors.toList());
  }

  /**
   * Converts a standard DStream into a periodic DStream that ensures all keys are processed in
   * every micro-batch, even if they don't receive new data.
   *
   * <p>This method addresses a fundamental challenge in stateful processing with Spark Streaming:
   * ensuring that timer events for all keys are processed in every batch, regardless of whether
   * those keys receive new data. Without this mechanism, timers associated with inactive keys would
   * not fire at the appropriate times.
   *
   * <p>Implementation details:
   *
   * <ol>
   *   <li>First, it extracts all unique keys from the input DStream and marks them with a special
   *       timer key marker.
   *   <li>Then, it uses Spark's mapWithState operation to maintain a persistent set of all keys
   *       that have ever been seen.
   *   <li>For each batch, it takes a snapshot of this state (all known keys).
   *   <li>It then transforms the original DStream by combining it with synthetic events for all
   *       known keys.
   *   <li>These synthetic events use the special {@link WindowedValueForTimerMarker} class with the
   *       {@link TimerUtils#TIMER_MARKER} to allow downstream operations to distinguish them from
   *       regular data events.
   * </ol>
   *
   * <p>When processed downstream, the synthetic key-value pairs will trigger evaluation of any
   * pending timers for all keys, even those that haven't received new data in the current batch.
   * This ensures consistent and reliable timer execution across the entire stateful pipeline.
   *
   * <p>This approach is critical for implementing features like windowing, sessions, and other
   * time-based operations that rely on timers firing at the correct moments, regardless of data
   * activity.
   *
   * @param <KeyT> The type of keys
   * @param <ValueT> The type of values
   * @param originalLRDD The original DStream of windowed key-value pairs to be enhanced with
   *     periodic timer events
   * @return A new DStream that includes periodic events for all known keys, ensuring timer
   *     processing for every key in each batch
   */
  @SuppressWarnings({"unchecked", "nullness"})
  public static <KeyT, ValueT> JavaDStream<WindowedValue<KV<KeyT, ValueT>>> toPeriodicDStream(
      JavaDStream<WindowedValue<KV<KeyT, ValueT>>> originalLRDD) {
    // extract only unique keys from original RDD
    final JavaPairDStream<KeyT, KeyT> uniqueKeysDStream =
        originalLRDD.mapPartitionsToPair(
            (Iterator<WindowedValue<KV<KeyT, ValueT>>> iterator) -> {
              final Set<KeyT> uniqueKeys = new HashSet<>();
              while (iterator.hasNext()) {
                final KV<KeyT, ValueT> kv = iterator.next().getValue();
                uniqueKeys.add(kv.getKey());
              }

              return Iterators.transform(uniqueKeys.iterator(), key -> Tuple2.apply(null, key));
            });

    final JavaPairDStream</*(null)*/ KeyT, /*Actual Keys*/ Set<KeyT>> keyStateSnapshotDStream =
        uniqueKeysDStream
            .mapWithState(
                StateSpec.function(
                    new SerializableFunction3<
                        /*(null)*/ KeyT,
                        /*Actual Key*/ Option<KeyT>,
                        /*State*/ State<Set<KeyT>>,
                        /*Return type - not needed as we use stateSnapshots*/ Void>() {
                      @Override
                      public Void apply(
                          KeyT timerKeyMarker, Option<KeyT> actualKey, State<Set<KeyT>> state) {
                        Set<KeyT> keyState;
                        if (state.exists()) {
                          keyState = state.get();
                        } else {
                          keyState = new HashSet<>();
                        }

                        keyState.add(actualKey.get());
                        state.update(keyState);
                        // We don't need to return anything as we use stateSnapshots
                        return null;
                      }
                    }))
            .stateSnapshots();

    return originalLRDD.transformWith(
        keyStateSnapshotDStream,
        (JavaRDD<WindowedValue<KV<KeyT, ValueT>>> wvRDD,
            JavaPairRDD<KeyT, Set<KeyT>> stateRDD,
            Time time) -> {
          final int numPartitions = wvRDD.getNumPartitions();

          final Set<KeyT> keySet = new HashSet<>();

          // add all keys to keySet
          stateRDD.distinct().values().collect().forEach(keySet::addAll);

          final List<WindowedValue<KV<KeyT, ValueT>>> collect =
              keySet.stream()
                  .map(key -> new WindowedValueForTimerMarker<>(KV.of(key, (ValueT) TIMER_MARKER)))
                  .collect(Collectors.toList());

          final JavaRDD<WindowedValue<KV<KeyT, ValueT>>> keyConvertedRDD =
              JavaRDD.fromRDD(
                  stateRDD
                      .context()
                      .parallelize(
                          JavaConverters.asScalaIterator(collect.iterator()).toSeq(),
                          numPartitions > 0 ? numPartitions : 1,
                          JavaSparkContext$.MODULE$.fakeClassTag()),
                  JavaSparkContext$.MODULE$.fakeClassTag());

          return wvRDD.union(keyConvertedRDD);
        });
  }
}
