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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.MoreObjects;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.TimerInternals;

import org.joda.time.Instant;

/**
 * TimerInternals that uses priority queues to manage the timers that are ready to fire.
 */
public class BatchTimerInternals implements TimerInternals {
  /** Set of timers that are scheduled used for deduplicating timers. */
  private Set<TimerData> existingTimers = new HashSet<>();

  // Keep these queues separate so we can advance over them separately.
  private PriorityQueue<TimerData> watermarkTimers = new PriorityQueue<>(11);
  private PriorityQueue<TimerData> processingTimers = new PriorityQueue<>(11);

  private Instant inputWatermarkTime;
  private Instant processingTime;

  private PriorityQueue<TimerData> queue(TimeDomain domain) {
    return TimeDomain.EVENT_TIME.equals(domain) ? watermarkTimers : processingTimers;
  }

  public BatchTimerInternals(Instant processingTime) {
    this.processingTime = processingTime;
    this.inputWatermarkTime = BoundedWindow.TIMESTAMP_MIN_VALUE;
  }

  @Override
  public void setTimer(TimerData timer) {
    if (existingTimers.add(timer)) {
      queue(timer.getDomain()).add(timer);
    }
  }

  @Override
  public void deleteTimer(TimerData timer) {
    existingTimers.remove(timer);
    queue(timer.getDomain()).remove(timer);
  }

  @Override
  public Instant currentProcessingTime() {
    return processingTime;
  }

  /**
   * {@inheritDoc}
   *
   * @return {@link BoundedWindow#TIMESTAMP_MAX_VALUE}: in batch mode, upstream processing
   * is already complete.
   */
  @Override
  @Nullable
  public Instant currentSynchronizedProcessingTime() {
    return BoundedWindow.TIMESTAMP_MAX_VALUE;
  }

  @Override
  public Instant currentInputWatermarkTime() {
    return inputWatermarkTime;
  }

  @Override
  @Nullable
  public Instant currentOutputWatermarkTime() {
    // The output watermark is always undefined in batch mode.
    return null;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("watermarkTimers", watermarkTimers)
        .add("processingTimers", processingTimers)
        .toString();
  }

  public void advanceInputWatermark(ReduceFnRunner<?, ?, ?, ?> runner, Instant newInputWatermark)
      throws Exception {
    checkState(!newInputWatermark.isBefore(inputWatermarkTime),
        "Cannot move input watermark time backwards from %s to %s", inputWatermarkTime,
        newInputWatermark);
    inputWatermarkTime = newInputWatermark;
    advance(runner, newInputWatermark, TimeDomain.EVENT_TIME);
  }

  public void advanceProcessingTime(ReduceFnRunner<?, ?, ?, ?> runner, Instant newProcessingTime)
      throws Exception {
    checkState(!newProcessingTime.isBefore(processingTime),
        "Cannot move processing time backwards from %s to %s", processingTime, newProcessingTime);
    processingTime = newProcessingTime;
    advance(runner, newProcessingTime, TimeDomain.PROCESSING_TIME);
  }

  private void advance(ReduceFnRunner<?, ?, ?, ?> runner, Instant newTime, TimeDomain domain)
      throws Exception {
    PriorityQueue<TimerData> timers = queue(domain);
    boolean shouldFire = false;

    do {
      TimerData timer = timers.peek();
      // Timers fire if the new time is ahead of the timer
      shouldFire = timer != null && newTime.isAfter(timer.getTimestamp());
      if (shouldFire) {
        // Remove before firing, so that if the trigger adds another identical
        // timer we don't remove it.
        timers.remove();
        runner.onTimer(timer);
      }
    } while (shouldFire);
  }
}
