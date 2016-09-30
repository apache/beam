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
 * Simulates the firing of timers and progression of input and output watermarks for a single
 * computation and key in a Windmill-like streaming environment.
 */
public class InMemoryTimerInternals implements TimerInternals {
  /** At most one timer per timestamp is kept. */
  private Set<TimerData> existingTimers = new HashSet<>();

  /** Pending input watermark timers, in timestamp order. */
  private PriorityQueue<TimerData> watermarkTimers = new PriorityQueue<>(11);

  /** Pending processing time timers, in timestamp order. */
  private PriorityQueue<TimerData> processingTimers = new PriorityQueue<>(11);

  /** Current input watermark. */
  @Nullable private Instant inputWatermarkTime = BoundedWindow.TIMESTAMP_MIN_VALUE;

  /** Current output watermark. */
  @Nullable private Instant outputWatermarkTime = null;

  /** Current processing time. */
  private Instant processingTime = BoundedWindow.TIMESTAMP_MIN_VALUE;

  /** Current synchronized processing time. */
  @Nullable private Instant synchronizedProcessingTime = null;

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
      case SYNCHRONIZED_PROCESSING_TIME:
        data = processingTimers.peek();
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
      case SYNCHRONIZED_PROCESSING_TIME:
        return processingTimers;
      default:
        throw new IllegalArgumentException("Unexpected time domain: " + domain);
    }
  }

  @Override
  public void setTimer(TimerData timer) {
    WindowTracing.trace("TestTimerInternals.setTimer: {}", timer);
    if (existingTimers.add(timer)) {
      queue(timer.getDomain()).add(timer);
    }
  }

  @Override
  public void deleteTimer(TimerData timer) {
    WindowTracing.trace("TestTimerInternals.deleteTimer: {}", timer);
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
    return checkNotNull(inputWatermarkTime);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("watermarkTimers", watermarkTimers)
        .add("processingTimers", processingTimers)
        .add("inputWatermarkTime", inputWatermarkTime)
        .add("outputWatermarkTime", outputWatermarkTime)
        .add("processingTime", processingTime)
        .toString();
  }

  /** Advances input watermark to the given value and fires event-time timers accordingly. */
  public void advanceInputWatermark(
      TimerCallback timerCallback, Instant newInputWatermark) throws Exception {
    checkNotNull(newInputWatermark);
    checkState(
        !newInputWatermark.isBefore(inputWatermarkTime),
        "Cannot move input watermark time backwards from %s to %s",
        inputWatermarkTime,
        newInputWatermark);
    WindowTracing.trace(
        "TestTimerInternals.advanceInputWatermark: from {} to {}",
        inputWatermarkTime,
        newInputWatermark);
    inputWatermarkTime = newInputWatermark;
    advanceAndFire(timerCallback, newInputWatermark, TimeDomain.EVENT_TIME);
  }

  /** Advances output watermark to the given value. */
  public void advanceOutputWatermark(Instant newOutputWatermark) {
    checkNotNull(newOutputWatermark);
    final Instant adjustedOutputWatermark;
    if (newOutputWatermark.isAfter(inputWatermarkTime)) {
      WindowTracing.trace(
          "TestTimerInternals.advanceOutputWatermark: clipping output watermark from {} to {}",
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
        "TestTimerInternals.advanceOutputWatermark: from {} to {}",
        outputWatermarkTime,
        adjustedOutputWatermark);
    outputWatermarkTime = adjustedOutputWatermark;
  }

  /** Advances processing time to the given value and fires processing-time timers accordingly. */
  public void advanceProcessingTime(
      TimerCallback timerCallback, Instant newProcessingTime) throws Exception {
    checkState(
        !newProcessingTime.isBefore(processingTime),
        "Cannot move processing time backwards from %s to %s",
        processingTime,
        newProcessingTime);
    WindowTracing.trace(
        "TestTimerInternals.advanceProcessingTime: from {} to {}",
        processingTime,
        newProcessingTime);
    processingTime = newProcessingTime;
    advanceAndFire(timerCallback, newProcessingTime, TimeDomain.PROCESSING_TIME);
  }

  /**
   * Advances synchronized processing time to the given value and fires processing-time timers
   * accordingly.
   */
  public void advanceSynchronizedProcessingTime(
      TimerCallback timerCallback, Instant newSynchronizedProcessingTime)
      throws Exception {
    checkState(
        !newSynchronizedProcessingTime.isBefore(synchronizedProcessingTime),
        "Cannot move processing time backwards from %s to %s",
        processingTime,
        newSynchronizedProcessingTime);
    WindowTracing.trace(
        "TestTimerInternals.advanceProcessingTime: from {} to {}",
        synchronizedProcessingTime,
        newSynchronizedProcessingTime);
    synchronizedProcessingTime = newSynchronizedProcessingTime;
    advanceAndFire(
        timerCallback, newSynchronizedProcessingTime, TimeDomain.SYNCHRONIZED_PROCESSING_TIME);
  }

  private void advanceAndFire(
      TimerCallback timerCallback, Instant currentTime, TimeDomain domain)
      throws Exception {
    checkNotNull(timerCallback);
    PriorityQueue<TimerData> queue = queue(domain);
    while (!queue.isEmpty() && currentTime.isAfter(queue.peek().getTimestamp())) {
      // Remove before firing, so that if the callback adds another identical
      // timer we don't remove it.
      TimerData timer = queue.remove();
      WindowTracing.trace(
          "InMemoryTimerInternals.advanceAndFire: firing {} at {}", timer, currentTime);
      timerCallback.onTimer(timer);
    }
  }
}
