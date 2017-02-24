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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder.SparkWatermarks;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.spark.broadcast.Broadcast;
import org.joda.time.Instant;


/**
 * An implementation of {@link TimerInternals} for the SparkRunner.
 */
class SparkTimerInternals implements TimerInternals {
  private final Instant highWatermark;
  private final Instant synchronizedProcessingTime;
  private final Set<TimerData> timers = Sets.newHashSet();

  private Instant inputWatermark;

  private SparkTimerInternals(
      Instant lowWatermark, Instant highWatermark, Instant synchronizedProcessingTime) {
    this.inputWatermark = lowWatermark;
    this.highWatermark = highWatermark;
    this.synchronizedProcessingTime = synchronizedProcessingTime;
  }

  /** Build the {@link TimerInternals} according to the feeding streams. */
  public static SparkTimerInternals forStreamFromSources(
      List<Integer> sourceIds,
      @Nullable Broadcast<Map<Integer, SparkWatermarks>> broadcast) {
    // if broadcast is invalid for the specific ids, use defaults.
    if (broadcast == null || broadcast.getValue().isEmpty()
        || Collections.disjoint(sourceIds, broadcast.getValue().keySet())) {
      return new SparkTimerInternals(
          BoundedWindow.TIMESTAMP_MIN_VALUE, BoundedWindow.TIMESTAMP_MIN_VALUE, new Instant(0));
    }
    // there might be more than one stream feeding this stream, slowest WM is the right one.
    Instant slowestLowWatermark = BoundedWindow.TIMESTAMP_MAX_VALUE;
    Instant slowestHighWatermark = BoundedWindow.TIMESTAMP_MAX_VALUE;
    // synchronized processing time should clearly be synchronized.
    Instant synchronizedProcessingTime = null;
    for (Integer sourceId: sourceIds) {
      SparkWatermarks sparkWatermarks = broadcast.getValue().get(sourceId);
      if (sparkWatermarks != null) {
        // keep slowest WMs.
        slowestLowWatermark = slowestLowWatermark.isBefore(sparkWatermarks.getLowWatermark())
            ? slowestLowWatermark : sparkWatermarks.getLowWatermark();
        slowestHighWatermark = slowestHighWatermark.isBefore(sparkWatermarks.getHighWatermark())
            ? slowestHighWatermark : sparkWatermarks.getHighWatermark();
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

  Collection<TimerData> getTimers() {
    return timers;
  }

  /** This should only be called after processing the element. */
  Collection<TimerData> getTimersReadyToProcess() {
    Set<TimerData> toFire = Sets.newHashSet();
    Iterator<TimerData> iterator = timers.iterator();
    while (iterator.hasNext()) {
      TimerData timer = iterator.next();
      if (timer.getTimestamp().isBefore(inputWatermark)) {
        toFire.add(timer);
        iterator.remove();
      }
    }
    return toFire;
  }

  void addTimers(Iterable<TimerData> timers) {
    for (TimerData timer: timers) {
      this.timers.add(timer);
    }
  }

  @Override
  public void setTimer(TimerData timer) {
    this.timers.add(timer);
  }

  @Override
  public void deleteTimer(StateNamespace namespace, String timerId, TimeDomain timeDomain) {
    throw new UnsupportedOperationException("Deleting a timer by ID is not yet supported.");
  }

  @Override
  public void deleteTimer(TimerData timer) {
    this.timers.remove(timer);
  }

  @Override
  public Instant currentProcessingTime() {
    return Instant.now();
  }

  @Nullable
  @Override
  public Instant currentSynchronizedProcessingTime() {
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

  @Nullable
  @Override
  public Instant currentOutputWatermarkTime() {
    return null;
  }

  @Override
  public void setTimer(
      StateNamespace namespace,
      String timerId,
      Instant target,
      TimeDomain timeDomain) {
    throw new UnsupportedOperationException("Setting a timer by ID not yet supported.");
  }

  @Override
  public void deleteTimer(StateNamespace namespace, String timerId) {
    throw new UnsupportedOperationException("Deleting a timer by ID is not yet supported.");
  }

  public static Collection<byte[]> serializeTimers(
      Collection<TimerData> timers, TimerDataCoder timerDataCoder) {
    return CoderHelpers.toByteArrays(timers, timerDataCoder);
  }

  public static Iterable<TimerData> deserializeTimers(
      Collection<byte[]> serTimers, TimerDataCoder timerDataCoder) {
    return CoderHelpers.fromByteArrays(serTimers, timerDataCoder);
  }

}
