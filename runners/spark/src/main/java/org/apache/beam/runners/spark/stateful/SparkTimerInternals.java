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
package org.apache.beam.runners.spark.stateful;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder.SparkWatermarks;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** An implementation of {@link TimerInternals} for the SparkRunner. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SparkTimerInternals implements TimerInternals {
  private final Instant highWatermark;
  private final Instant synchronizedProcessingTime;
  private final Set<TimerData> timers = Sets.newConcurrentHashSet();

  private Instant inputWatermark;

  private SparkTimerInternals(
      Instant lowWatermark, Instant highWatermark, Instant synchronizedProcessingTime) {
    this.inputWatermark = lowWatermark;
    this.highWatermark = highWatermark;
    this.synchronizedProcessingTime = synchronizedProcessingTime;
  }

  /** Build the {@link TimerInternals} according to the feeding streams. */
  public static SparkTimerInternals forStreamFromSources(
      List<Integer> sourceIds, Map<Integer, SparkWatermarks> watermarks) {
    // if watermarks are invalid for the specific ids, use defaults.
    if (watermarks == null
        || watermarks.isEmpty()
        || Collections.disjoint(sourceIds, watermarks.keySet())) {
      return new SparkTimerInternals(
          BoundedWindow.TIMESTAMP_MIN_VALUE, BoundedWindow.TIMESTAMP_MIN_VALUE, new Instant(0));
    }
    // there might be more than one stream feeding this stream, slowest WM is the right one.
    Instant slowestLowWatermark = BoundedWindow.TIMESTAMP_MAX_VALUE;
    Instant slowestHighWatermark = BoundedWindow.TIMESTAMP_MAX_VALUE;
    // synchronized processing time should clearly be synchronized.
    Instant synchronizedProcessingTime = null;
    for (Integer sourceId : sourceIds) {
      SparkWatermarks sparkWatermarks = watermarks.get(sourceId);
      if (sparkWatermarks != null) {
        // keep slowest WMs.
        slowestLowWatermark =
            slowestLowWatermark.isBefore(sparkWatermarks.getLowWatermark())
                ? slowestLowWatermark
                : sparkWatermarks.getLowWatermark();
        slowestHighWatermark =
            slowestHighWatermark.isBefore(sparkWatermarks.getHighWatermark())
                ? slowestHighWatermark
                : sparkWatermarks.getHighWatermark();
        if (synchronizedProcessingTime == null) {
          // firstime set.
          synchronizedProcessingTime = sparkWatermarks.getSynchronizedProcessingTime();
        } else {
          // assert on following.
          checkArgument(
              sparkWatermarks.getSynchronizedProcessingTime().equals(synchronizedProcessingTime),
              "Synchronized time is expected to keep synchronized across sources.");
        }
      }
    }
    return new SparkTimerInternals(
        slowestLowWatermark, slowestHighWatermark, synchronizedProcessingTime);
  }

  /** Build a global {@link TimerInternals} for all feeding streams. */
  public static SparkTimerInternals global(Map<Integer, SparkWatermarks> watermarks) {
    return watermarks == null
        ? forStreamFromSources(Collections.emptyList(), null)
        : forStreamFromSources(Lists.newArrayList(watermarks.keySet()), watermarks);
  }

  public Collection<TimerData> getTimers() {
    return timers;
  }

  public void addTimers(Iterator<TimerData> timers) {
    while (timers.hasNext()) {
      TimerData timer = timers.next();
      this.timers.add(timer);
    }
  }

  @Override
  public void setTimer(TimerData timer) {
    this.timers.add(timer);
  }

  @Override
  public void deleteTimer(
      StateNamespace namespace, String timerId, String timerFamilyId, TimeDomain timeDomain) {
    this.timers.stream()
        .filter(
            timer ->
                namespace.equals(timer.getNamespace())
                    && timerId.equals(timer.getTimerId())
                    && timerFamilyId.equals(timer.getTimerFamilyId())
                    && timeDomain.equals(timer.getDomain()))
        .forEach(this::deleteTimer);
  }

  @Override
  public void deleteTimer(TimerData timer) {
    this.timers.remove(timer);
  }

  @Override
  public Instant currentProcessingTime() {
    return Instant.now();
  }

  @Override
  public @Nullable Instant currentSynchronizedProcessingTime() {
    return synchronizedProcessingTime;
  }

  @Override
  public Instant currentInputWatermarkTime() {
    return inputWatermark;
  }

  /** Advances the watermark. */
  public void advanceWatermark() {
    inputWatermark = highWatermark;
  }

  @Override
  public @Nullable Instant currentOutputWatermarkTime() {
    return null;
  }

  @Override
  public void setTimer(
      StateNamespace namespace,
      String timerId,
      String timerFamilyId,
      Instant target,
      Instant outputTimestamp,
      TimeDomain timeDomain) {
    this.setTimer(
        TimerData.of(timerId, timerFamilyId, namespace, target, outputTimestamp, timeDomain));
  }

  @Override
  public void deleteTimer(StateNamespace namespace, String timerId, String timerFamilyId) {
    throw new UnsupportedOperationException("Deleting a timer by ID is not yet supported.");
  }

  public static Collection<byte[]> serializeTimers(
      Collection<TimerData> timers, TimerDataCoderV2 timerDataCoder) {
    return CoderHelpers.toByteArrays(timers, timerDataCoder);
  }

  public static Iterator<TimerData> deserializeTimers(
      Collection<byte[]> serTimers, TimerDataCoderV2 timerDataCoder) {
    return CoderHelpers.fromByteArrays(serTimers, timerDataCoder).iterator();
  }

  /**
   * Checks if there are any expired timers in the {@link TimeDomain#PROCESSING_TIME} domain.
   *
   * <p>A timer is considered expired when its timestamp is less than the current processing time.
   *
   * @return {@code true} if at least one expired processing timer exists, {@code false} otherwise.
   */
  public boolean hasNextProcessingTimer() {
    final Instant currentProcessingTime = this.currentProcessingTime();
    return this.timers.stream()
        .anyMatch(
            (TimerData timerData) ->
                timerData.getDomain().equals(TimeDomain.PROCESSING_TIME)
                    && currentProcessingTime.isAfter(timerData.getTimestamp()));
  }

  /**
   * Finds the latest timer in {@link TimeDomain#PROCESSING_TIME} domain that has expired based on
   * the current processing time.
   *
   * <p>A timer is considered expired when its timestamp is less than the current processing time.
   * If multiple expired timers exist, the one with the latest timestamp will be returned.
   *
   * @return The expired processing timer with the latest timestamp if one exists, or {@code null}
   *     if no processing timers are ready to fire.
   */
  public @Nullable TimerData getNextProcessingTimer() {
    final Instant currentProcessingTime = this.currentProcessingTime();
    return this.timers.stream()
        .filter(
            (TimerData timerData) ->
                timerData.getDomain().equals(TimeDomain.PROCESSING_TIME)
                    && currentProcessingTime.isAfter(timerData.getTimestamp()))
        .max(Comparator.comparing(TimerData::getTimestamp))
        .orElse(null);
  }

  @Override
  public String toString() {
    return "SparkTimerInternals{"
        + "highWatermark="
        + highWatermark
        + ", synchronizedProcessingTime="
        + synchronizedProcessingTime
        + ", timers="
        + timers
        + ", inputWatermark="
        + inputWatermark
        + '}';
  }
}
