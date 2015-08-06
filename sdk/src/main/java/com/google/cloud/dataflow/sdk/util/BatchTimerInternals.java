/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.common.base.MoreObjects;

import org.joda.time.Instant;

import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * TimerInternals that uses priority queues to manage the timers that are ready to fire.
 */
public class BatchTimerInternals implements TimerInternals {

  /** Set of timers that are scheduled used for deduplicating timers. */
  private Set<TimerData> existingTimers = new HashSet<>();

  // Keep these queues separate so we can advance over them separately.
  private PriorityQueue<TimerData> watermarkTimers = new PriorityQueue<>(11);
  private PriorityQueue<TimerData> processingTimers = new PriorityQueue<>(11);

  private Instant watermarkTime;
  private Instant processingTime;

  private PriorityQueue<TimerData> queue(TimeDomain domain) {
    return TimeDomain.EVENT_TIME.equals(domain) ? watermarkTimers : processingTimers;
  }

  public BatchTimerInternals(Instant processingTime) {
    this.processingTime = processingTime;
    this.watermarkTime = BoundedWindow.TIMESTAMP_MIN_VALUE;
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

  @Override
  public Instant currentWatermarkTime() {
    return watermarkTime;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("watermarkTimers", watermarkTimers)
        .add("processingTimers", processingTimers)
        .toString();
  }

  public void advanceWatermark(ReduceFnRunner<?, ?, ?, ?> runner, Instant newWatermark) {
    this.watermarkTime = newWatermark;
    advance(runner, newWatermark, TimeDomain.EVENT_TIME);
  }

  public void advanceProcessingTime(ReduceFnRunner<?, ?, ?, ?> runner, Instant newProcessingTime) {
    this.processingTime = newProcessingTime;
    advance(runner, newProcessingTime, TimeDomain.PROCESSING_TIME);
  }

  private void advance(ReduceFnRunner<?, ?, ?, ?> runner, Instant newTime, TimeDomain domain) {
    PriorityQueue<TimerData> timers = queue(domain);
    boolean shouldFire = false;

    do {
      TimerData timer = timers.peek();
      // Timers fire if the new time is >= the timer
      shouldFire = timer != null && !newTime.isBefore(timer.getTimestamp());
      if (shouldFire) {
        // Remove before firing, so that if the trigger adds another identical
        // timer we don't remove it.
        timers.remove();

        runner.onTimer(timer);
      }
    } while (shouldFire);
  }
}
