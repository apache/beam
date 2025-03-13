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
package org.apache.beam.runners.core;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.TreeSet;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowTracing;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.HashBasedTable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Table;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** {@link TimerInternals} with all watermarks and processing clock simulated in-memory. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class InMemoryTimerInternals implements TimerInternals {

  /** The current set timers by namespace and ID. */
  Table<StateNamespace, String, TimerData> existingTimers = HashBasedTable.create();

  /** Pending input watermark timers, in timestamp order. */
  private final NavigableSet<TimerData> watermarkTimers = new TreeSet<>();

  /** Pending processing time timers, in timestamp order. */
  private final NavigableSet<TimerData> processingTimers = new TreeSet<>();

  /** Pending synchronized processing time timers, in timestamp order. */
  private final NavigableSet<TimerData> synchronizedProcessingTimers = new TreeSet<>();

  /** Current input watermark. */
  private Instant inputWatermarkTime = BoundedWindow.TIMESTAMP_MIN_VALUE;

  /** Current output watermark. */
  private @Nullable Instant outputWatermarkTime = null;

  /** Current processing time. */
  private Instant processingTime = BoundedWindow.TIMESTAMP_MIN_VALUE;

  /** Current synchronized processing time. */
  private Instant synchronizedProcessingTime = BoundedWindow.TIMESTAMP_MIN_VALUE;

  /** Class.getSimpleName() cached to avoid allocations for tracing. */
  private static final String SIMPLE_NAME = InMemoryTimerInternals.class.getSimpleName();

  @Override
  public @Nullable Instant currentOutputWatermarkTime() {
    return outputWatermarkTime;
  }

  /** Returns true when there are still timers to be fired. */
  public boolean hasPendingTimers() {
    return !existingTimers.isEmpty();
  }

  /**
   * Returns when the next timer in the given time domain will fire, or {@code null} if there are no
   * timers scheduled in that time domain.
   */
  public @Nullable Instant getNextTimer(TimeDomain domain) {
    try {
      switch (domain) {
        case EVENT_TIME:
          return watermarkTimers.first().getTimestamp();
        case PROCESSING_TIME:
          return processingTimers.first().getTimestamp();
        case SYNCHRONIZED_PROCESSING_TIME:
          return synchronizedProcessingTimers.first().getTimestamp();
        default:
          throw new IllegalArgumentException("Unexpected time domain: " + domain);
      }
    } catch (NoSuchElementException exc) {
      return null;
    }
  }

  private NavigableSet<TimerData> timersForDomain(TimeDomain domain) {
    switch (domain) {
      case EVENT_TIME:
        return watermarkTimers;
      case PROCESSING_TIME:
        return processingTimers;
      case SYNCHRONIZED_PROCESSING_TIME:
        return synchronizedProcessingTimers;
      default:
        throw new IllegalArgumentException("Unexpected time domain: " + domain);
    }
  }

  @Override
  public void setTimer(
      StateNamespace namespace,
      String timerId,
      String timerFamilyId,
      Instant target,
      Instant outputTimestamp,
      TimeDomain timeDomain) {
    setTimer(TimerData.of(timerId, timerFamilyId, namespace, target, outputTimestamp, timeDomain));
  }

  /**
   * @deprecated use {@link #setTimer(StateNamespace, String, String, Instant, Instant,
   *     TimeDomain)}.
   */
  @Deprecated
  @Override
  public void setTimer(TimerData timerData) {
    WindowTracing.trace("{}.setTimer: {}", SIMPLE_NAME, timerData);

    @Nullable String colKey = timerData.getTimerId() + '+' + timerData.getTimerFamilyId();
    TimerData existing = existingTimers.get(timerData.getNamespace(), colKey);
    if (existing == null) {
      existingTimers.put(timerData.getNamespace(), colKey, timerData);
      timersForDomain(timerData.getDomain()).add(timerData);
    } else {
      checkArgument(
          timerData.getDomain().equals(existing.getDomain()),
          "Attempt to set %s for time domain %s, but it is already set for time domain %s",
          timerData.getTimerId(),
          timerData.getDomain(),
          existing.getDomain());

      if (!timerData.getTimestamp().equals(existing.getTimestamp())) {
        NavigableSet<TimerData> timers = timersForDomain(timerData.getDomain());
        timers.remove(existing);
        timers.add(timerData);
        existingTimers.put(timerData.getNamespace(), colKey, timerData);
      }
    }
  }

  @Override
  public void deleteTimer(
      StateNamespace namespace, String timerId, String timerFamilyId, TimeDomain timeDomain) {
    TimerData removedTimer = existingTimers.remove(namespace, timerId + '+' + timerFamilyId);
    if (removedTimer != null) {
      Preconditions.checkState(
          removedTimer.getDomain().equals(timeDomain),
          "%s doesn't match time domain %s of timer",
          timeDomain,
          removedTimer.getDomain());
      timersForDomain(timeDomain).remove(removedTimer);
    }
  }

  /** @deprecated use {@link #deleteTimer(StateNamespace, String, String, TimeDomain)}. */
  @Deprecated
  @Override
  public void deleteTimer(StateNamespace namespace, String timerId, String timerFamilyId) {
    TimerData removedTimer = existingTimers.remove(namespace, timerId + '+' + timerFamilyId);
    if (removedTimer != null) {
      timersForDomain(removedTimer.getDomain()).remove(removedTimer);
    }
  }

  /** @deprecated use {@link #deleteTimer(StateNamespace, String, String, TimeDomain)}. */
  @Deprecated
  @Override
  public void deleteTimer(TimerData timer) {
    deleteTimer(
        timer.getNamespace(), timer.getTimerId(), timer.getTimerFamilyId(), timer.getDomain());
  }

  @Override
  public Instant currentProcessingTime() {
    return processingTime;
  }

  @Override
  public @Nullable Instant currentSynchronizedProcessingTime() {
    return synchronizedProcessingTime;
  }

  @Override
  public Instant currentInputWatermarkTime() {
    return inputWatermarkTime;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("watermarkTimers", watermarkTimers)
        .add("processingTimers", processingTimers)
        .add("synchronizedProcessingTimers", synchronizedProcessingTimers)
        .add("inputWatermarkTime", inputWatermarkTime)
        .add("outputWatermarkTime", outputWatermarkTime)
        .add("processingTime", processingTime)
        .toString();
  }

  /** Advances input watermark to the given value. */
  public void advanceInputWatermark(Instant newInputWatermark) throws Exception {
    checkNotNull(newInputWatermark);
    checkState(
        !newInputWatermark.isBefore(inputWatermarkTime),
        "Cannot move input watermark time backwards from %s to %s",
        inputWatermarkTime,
        newInputWatermark);
    WindowTracing.trace(
        "{}.advanceInputWatermark: from {} to {}",
        SIMPLE_NAME,
        inputWatermarkTime,
        newInputWatermark);
    inputWatermarkTime = newInputWatermark;
  }

  /** Advances output watermark to the given value. */
  public void advanceOutputWatermark(Instant newOutputWatermark) {
    checkNotNull(newOutputWatermark);
    final Instant adjustedOutputWatermark;
    if (newOutputWatermark.isAfter(inputWatermarkTime)) {
      WindowTracing.trace(
          "{}.advanceOutputWatermark: clipping output watermark from {} to {}",
          SIMPLE_NAME,
          newOutputWatermark,
          inputWatermarkTime);
      adjustedOutputWatermark = inputWatermarkTime;
    } else {
      adjustedOutputWatermark = newOutputWatermark;
    }

    checkState(
        outputWatermarkTime == null || !adjustedOutputWatermark.isBefore(outputWatermarkTime),
        "Cannot move output watermark time backwards from %s to %s",
        outputWatermarkTime,
        adjustedOutputWatermark);
    WindowTracing.trace(
        "{}.advanceOutputWatermark: from {} to {}",
        SIMPLE_NAME,
        outputWatermarkTime,
        adjustedOutputWatermark);
    outputWatermarkTime = adjustedOutputWatermark;
  }

  /** Advances processing time to the given value. */
  public void advanceProcessingTime(Instant newProcessingTime) throws Exception {
    checkNotNull(newProcessingTime);
    checkState(
        !newProcessingTime.isBefore(processingTime),
        "Cannot move processing time backwards from %s to %s",
        processingTime,
        newProcessingTime);
    WindowTracing.trace(
        "{}.advanceProcessingTime: from {} to {}", SIMPLE_NAME, processingTime, newProcessingTime);
    processingTime = newProcessingTime;
  }

  /** Advances synchronized processing time to the given value. */
  public void advanceSynchronizedProcessingTime(Instant newSynchronizedProcessingTime)
      throws Exception {
    checkNotNull(newSynchronizedProcessingTime);
    checkState(
        !newSynchronizedProcessingTime.isBefore(synchronizedProcessingTime),
        "Cannot move processing time backwards from %s to %s",
        synchronizedProcessingTime,
        newSynchronizedProcessingTime);
    WindowTracing.trace(
        "{}.advanceProcessingTime: from {} to {}",
        SIMPLE_NAME,
        synchronizedProcessingTime,
        newSynchronizedProcessingTime);
    synchronizedProcessingTime = newSynchronizedProcessingTime;
  }

  /** Returns the next eligible event time timer, if none returns null. */
  public @Nullable TimerData removeNextEventTimer() {
    TimerData timer = removeNextTimer(inputWatermarkTime, TimeDomain.EVENT_TIME);
    if (timer != null) {
      WindowTracing.trace(
          "{}.removeNextEventTimer: firing {} at {}", SIMPLE_NAME, timer, inputWatermarkTime);
    }
    return timer;
  }

  /** Returns the next eligible processing time timer, if none returns null. */
  public @Nullable TimerData removeNextProcessingTimer() {
    TimerData timer = removeNextTimer(processingTime, TimeDomain.PROCESSING_TIME);
    if (timer != null) {
      WindowTracing.trace(
          "{}.removeNextProcessingTimer: firing {} at {}", SIMPLE_NAME, timer, processingTime);
    }
    return timer;
  }

  /** Returns the next eligible synchronized processing time timer, if none returns null. */
  public @Nullable TimerData removeNextSynchronizedProcessingTimer() {
    TimerData timer =
        removeNextTimer(synchronizedProcessingTime, TimeDomain.SYNCHRONIZED_PROCESSING_TIME);
    if (timer != null) {
      WindowTracing.trace(
          "{}.removeNextSynchronizedProcessingTimer: firing {} at {}",
          SIMPLE_NAME,
          timer,
          synchronizedProcessingTime);
    }
    return timer;
  }

  private @Nullable TimerData removeNextTimer(Instant currentTime, TimeDomain domain) {
    NavigableSet<TimerData> timers = timersForDomain(domain);

    if (!timers.isEmpty() && currentTime.isAfter(timers.first().getTimestamp())) {
      TimerData timer = timers.pollFirst();
      existingTimers.remove(
          timer.getNamespace(), timer.getTimerId() + '+' + timer.getTimerFamilyId());
      return timer;
    } else {
      return null;
    }
  }
}
