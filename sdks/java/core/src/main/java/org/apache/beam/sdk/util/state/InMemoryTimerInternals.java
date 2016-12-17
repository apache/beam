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
package org.apache.beam.sdk.util.state;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.MoreObjects;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.TimerInternals;
import org.apache.beam.sdk.util.WindowTracing;
import org.joda.time.Instant;

/**
 * @deprecated use {@code org.apache.beam.runners.core.InMemoryTimerInternals}.
 */
@Deprecated
public class InMemoryTimerInternals implements TimerInternals {

  /** At most one timer per timestamp is kept. */
  private Set<TimerData> existingTimers = new HashSet<>();

  /** Pending input watermark timers, in timestamp order. */
  private PriorityQueue<TimerData> watermarkTimers = new PriorityQueue<>(11);

  /** Pending processing time timers, in timestamp order. */
  private PriorityQueue<TimerData> processingTimers = new PriorityQueue<>(11);

  /** Pending synchronized processing time timers, in timestamp order. */
  private PriorityQueue<TimerData> synchronizedProcessingTimers = new PriorityQueue<>(11);

  /** Current input watermark. */
  private Instant inputWatermarkTime = BoundedWindow.TIMESTAMP_MIN_VALUE;

  /** Current output watermark. */
  @Nullable private Instant outputWatermarkTime = null;

  /** Current processing time. */
  private Instant processingTime = BoundedWindow.TIMESTAMP_MIN_VALUE;

  /** Current synchronized processing time. */
  private Instant synchronizedProcessingTime = BoundedWindow.TIMESTAMP_MIN_VALUE;

  @Override
  @Nullable
  public Instant currentOutputWatermarkTime() {
    return outputWatermarkTime;
  }

  /**
   * Returns when the next timer in the given time domain will fire, or {@code null}
   * if there are no timers scheduled in that time domain.
   */
  @Nullable
  public Instant getNextTimer(TimeDomain domain) {
    final TimerData data;
    switch (domain) {
      case EVENT_TIME:
        data = watermarkTimers.peek();
        break;
      case PROCESSING_TIME:
        data = processingTimers.peek();
        break;
      case SYNCHRONIZED_PROCESSING_TIME:
        data = synchronizedProcessingTimers.peek();
        break;
      default:
        throw new IllegalArgumentException("Unexpected time domain: " + domain);
    }
    return (data == null) ? null : data.getTimestamp();
  }

  private PriorityQueue<TimerData> queue(TimeDomain domain) {
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
  public void setTimer(StateNamespace namespace, String timerId, Instant target,
      TimeDomain timeDomain) {
    throw new UnsupportedOperationException("Setting a timer by ID is not yet supported.");
  }

  @Override
  public void setTimer(TimerData timerData) {
    WindowTracing.trace("{}.setTimer: {}", getClass().getSimpleName(), timerData);
    if (existingTimers.add(timerData)) {
      queue(timerData.getDomain()).add(timerData);
    }
  }

  @Override
  public void deleteTimer(StateNamespace namespace, String timerId) {
    throw new UnsupportedOperationException("Canceling a timer by ID is not yet supported.");
  }

  @Override
  public void deleteTimer(TimerData timer) {
    WindowTracing.trace("{}.deleteTimer: {}", getClass().getSimpleName(), timer);
    existingTimers.remove(timer);
    queue(timer.getDomain()).remove(timer);
  }

  @Override
  public Instant currentProcessingTime() {
    return processingTime;
  }

  @Override
  @Nullable
  public Instant currentSynchronizedProcessingTime() {
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
        getClass().getSimpleName(), inputWatermarkTime, newInputWatermark);
    inputWatermarkTime = newInputWatermark;
  }

  /** Advances output watermark to the given value. */
  public void advanceOutputWatermark(Instant newOutputWatermark) {
    checkNotNull(newOutputWatermark);
    final Instant adjustedOutputWatermark;
    if (newOutputWatermark.isAfter(inputWatermarkTime)) {
      WindowTracing.trace(
          "{}.advanceOutputWatermark: clipping output watermark from {} to {}",
          getClass().getSimpleName(), newOutputWatermark, inputWatermarkTime);
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
        getClass().getSimpleName(), outputWatermarkTime, adjustedOutputWatermark);
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
        "{}.advanceProcessingTime: from {} to {}",
        getClass().getSimpleName(), processingTime, newProcessingTime);
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
        getClass().getSimpleName(), synchronizedProcessingTime, newSynchronizedProcessingTime);
    synchronizedProcessingTime = newSynchronizedProcessingTime;
  }

  /** Returns the next eligible event time timer, if none returns null. */
  @Nullable
  public TimerData removeNextEventTimer() {
    TimerData timer = removeNextTimer(inputWatermarkTime, TimeDomain.EVENT_TIME);
    if (timer != null) {
      WindowTracing.trace(
          "{}.removeNextEventTimer: firing {} at {}",
          getClass().getSimpleName(), timer, inputWatermarkTime);
    }
    return timer;
  }

  /** Returns the next eligible processing time timer, if none returns null. */
  @Nullable
  public TimerData removeNextProcessingTimer() {
    TimerData timer = removeNextTimer(processingTime, TimeDomain.PROCESSING_TIME);
    if (timer != null) {
      WindowTracing.trace(
          "{}.removeNextProcessingTimer: firing {} at {}",
          getClass().getSimpleName(), timer, processingTime);
    }
    return timer;
  }

  /** Returns the next eligible synchronized processing time timer, if none returns null. */
  @Nullable
  public TimerData removeNextSynchronizedProcessingTimer() {
    TimerData timer = removeNextTimer(
        synchronizedProcessingTime, TimeDomain.SYNCHRONIZED_PROCESSING_TIME);
    if (timer != null) {
      WindowTracing.trace(
          "{}.removeNextSynchronizedProcessingTimer: firing {} at {}",
          getClass().getSimpleName(), timer, synchronizedProcessingTime);
    }
    return timer;
  }

  @Nullable
  private TimerData removeNextTimer(Instant currentTime, TimeDomain domain) {
    PriorityQueue<TimerData> queue = queue(domain);
    if (!queue.isEmpty() && currentTime.isAfter(queue.peek().getTimestamp())) {
      TimerData timer = queue.remove();
      existingTimers.remove(timer);
      return timer;
    } else {
      return null;
    }
  }
}
