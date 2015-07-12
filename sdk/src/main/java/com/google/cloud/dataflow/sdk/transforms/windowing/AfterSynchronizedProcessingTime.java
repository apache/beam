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

import static com.google.cloud.dataflow.sdk.transforms.windowing.TimeTrigger.DELAYED_UNTIL_TAG;

import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnceTrigger;
import com.google.cloud.dataflow.sdk.util.ReduceFn.MergingStateContext;
import com.google.cloud.dataflow.sdk.util.ReduceFn.StateContext;
import com.google.cloud.dataflow.sdk.util.TimeDomain;
import com.google.cloud.dataflow.sdk.util.state.CombiningValueState;
import com.google.common.base.Objects;

import org.joda.time.Instant;

import java.util.List;

class AfterSynchronizedProcessingTime<W extends BoundedWindow> extends OnceTrigger<W> {

  private static final long serialVersionUID = 0L;

  public AfterSynchronizedProcessingTime() {
    super(null);
  }

  @Override
  public TriggerResult onElement(OnElementContext c)
      throws Exception {
    CombiningValueState<Instant, Instant> delayUntilState = c.state().access(DELAYED_UNTIL_TAG);
    Instant delayUntil = delayUntilState.get().read();
    if (delayUntil == null) {
      delayUntil = c.timers().currentProcessingTime();
      c.timers().setTimer(delayUntil, TimeDomain.SYNCHRONIZED_PROCESSING_TIME);
      delayUntilState.add(delayUntil);
    }

    return TriggerResult.CONTINUE;
  }

  @Override
  public MergeResult onMerge(OnMergeContext c) throws Exception {
    // If the processing time timer has fired in any of the windows being merged, it would have
    // fired at the same point if it had been added to the merged window. So, we just report it as
    // finished.
    if (c.trigger().finishedInAnyMergingWindow()) {
      return MergeResult.ALREADY_FINISHED;
    }

    // Otherwise, determine the earliest delay for all of the windows, and delay to that point.
    CombiningValueState<Instant, Instant> mergingDelays =
        c.state().accessAcrossMergingWindows(DELAYED_UNTIL_TAG);
    Instant earliestTimer = mergingDelays.get().read();
    if (earliestTimer != null) {
      mergingDelays.clear();
      mergingDelays.add(earliestTimer);
      c.timers().setTimer(earliestTimer, TimeDomain.SYNCHRONIZED_PROCESSING_TIME);
    }

    return MergeResult.CONTINUE;
  }

  @Override
  public TriggerResult onTimer(OnTimerContext c) throws Exception {
    if (c.timeDomain() != TimeDomain.SYNCHRONIZED_PROCESSING_TIME) {
      return TriggerResult.CONTINUE;
    }

    Instant delayedUntil = c.state().access(DELAYED_UNTIL_TAG).get().read();
    if (delayedUntil == null || delayedUntil.isAfter(c.timestamp())) {
      return TriggerResult.CONTINUE;
    }

    return TriggerResult.FIRE_AND_FINISH;
  }

  @Override
  public void prefetchOnElement(StateContext state) {
    state.access(DELAYED_UNTIL_TAG).get();
  }

  @Override
  public void prefetchOnMerge(MergingStateContext state) {
    state.accessAcrossMergingWindows(DELAYED_UNTIL_TAG).get();
  }

  @Override
  public void prefetchOnTimer(StateContext state) {
    state.access(DELAYED_UNTIL_TAG).get();
  }

  @Override
  public void clear(TriggerContext c) throws Exception {
    CombiningValueState<Instant, Instant> delayed = c.state().access(DELAYED_UNTIL_TAG);
    Instant timestamp = delayed.get().read();
    delayed.clear();
    if (timestamp != null) {
      c.timers().deleteTimer(timestamp, TimeDomain.SYNCHRONIZED_PROCESSING_TIME);
    }
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
