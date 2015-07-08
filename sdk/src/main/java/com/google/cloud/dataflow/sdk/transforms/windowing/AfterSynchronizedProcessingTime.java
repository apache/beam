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
package com.google.cloud.dataflow.sdk.transforms.windowing;

import com.google.cloud.dataflow.sdk.coders.InstantCoder;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnceTrigger;
import com.google.cloud.dataflow.sdk.util.TimerManager.TimeDomain;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.common.base.Objects;

import org.joda.time.Instant;

import java.util.List;

class AfterSynchronizedProcessingTime<W extends BoundedWindow> extends OnceTrigger<W> {

  private static final long serialVersionUID = 0L;

  private static final CodedTupleTag<Instant> DELAYED_UNTIL_TAG =
      CodedTupleTag.of("delayed-until", InstantCoder.of());

  public AfterSynchronizedProcessingTime() {
    super(null);
  }

  @Override
  public TriggerResult onElement(OnElementContext c)
      throws Exception {
    Instant delayUntil = c.lookup(DELAYED_UNTIL_TAG, c.window());
    if (delayUntil == null) {
      delayUntil = c.currentProcessingTime();
      c.setTimer(c.window(), delayUntil, TimeDomain.SYNCHRONIZED_PROCESSING_TIME);
      c.store(DELAYED_UNTIL_TAG, c.window(), delayUntil);
    }

    return TriggerResult.CONTINUE;
  }

  @Override
  public MergeResult onMerge(OnMergeContext c) throws Exception {
    // If the processing time timer has fired in any of the windows being merged, it would have
    // fired at the same point if it had been added to the merged window. So, we just report it as
    // finished.
    if (c.finishedInAnyMergingWindow(c.current())) {
      return MergeResult.ALREADY_FINISHED;
    }

    // Otherwise, determine the earliest delay for all of the windows, and delay to that point.
    Instant earliestTimer = BoundedWindow.TIMESTAMP_MAX_VALUE;
    for (Instant delayedUntil : c.lookup(DELAYED_UNTIL_TAG, c.oldWindows()).values()) {
      if (delayedUntil != null && delayedUntil.isBefore(earliestTimer)) {
        earliestTimer = delayedUntil;
      }
    }

    if (earliestTimer != null) {
      c.store(DELAYED_UNTIL_TAG, c.window(), earliestTimer);
      c.setTimer(c.window(), earliestTimer, TimeDomain.SYNCHRONIZED_PROCESSING_TIME);
    }

    return MergeResult.CONTINUE;
  }

  @Override
  public TriggerResult onTimer(OnTimerContext c) throws Exception {
    return TriggerResult.FIRE_AND_FINISH;
  }

  @Override
  public void clear(TriggerContext c) throws Exception {
    c.remove(DELAYED_UNTIL_TAG, c.window());
    c.deleteTimer(c.window(), TimeDomain.SYNCHRONIZED_PROCESSING_TIME);
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(W window) {
    return BoundedWindow.TIMESTAMP_MAX_VALUE;
  }

  @Override
  protected Trigger<W> getContinuationTrigger(List<Trigger<W>> continuationTriggers) {
    return this;
  }

  @Override
  public String toString() {
    return "AfterSynchronizedProcessingTime.pastFirstElementInPane()";
  }

  @Override
  public boolean equals(Object obj) {
    return this == obj || obj instanceof AfterSynchronizedProcessingTime;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(AfterSynchronizedProcessingTime.class);
  }
}
