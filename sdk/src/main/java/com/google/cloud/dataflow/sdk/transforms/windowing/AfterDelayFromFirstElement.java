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

import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.util.ReduceFn.MergingStateContext;
import com.google.cloud.dataflow.sdk.util.ReduceFn.StateContext;
import com.google.cloud.dataflow.sdk.util.TimeDomain;
import com.google.cloud.dataflow.sdk.util.state.CombiningValueState;

import org.joda.time.Instant;

import java.util.List;

import javax.annotation.Nullable;

/**
 * A base class for triggers that happen after a processing time delay from the arrival
 * of the first element in a pane.
 */
abstract class AfterDelayFromFirstElement<W extends BoundedWindow> extends TimeTrigger<W> {

  /**
   * To complete an implementation, return the desired time from the TriggerContext.
   */
  @Nullable
  public abstract Instant getCurrentTime(Trigger<W>.TriggerContext context);

  private final TimeDomain timeDomain;

  public AfterDelayFromFirstElement(
      TimeDomain timeDomain, List<SerializableFunction<Instant, Instant>> timestampMappers) {
    super(timestampMappers);
    this.timeDomain = timeDomain;
  }

  private Instant getTargetTimestamp(OnElementContext c) {
    return computeTargetTimestamp(c.currentProcessingTime());
  }

  @Override
  public void prefetchOnElement(StateContext state) {
    state.access(DELAYED_UNTIL_TAG).get();
  }

  @Override
  public void onElement(OnElementContext c) throws Exception {
    CombiningValueState<Instant, Instant> delayUntilState = c.state().access(DELAYED_UNTIL_TAG);
    Instant oldDelayUntil = delayUntilState.get().read();

    // Since processing time can only advance, resulting in target wake-up times we would
    // ignore anyhow, we don't bother with it if it is already set.
    if (oldDelayUntil != null) {
      return;
    }

    Instant targetTimestamp = getTargetTimestamp(c);
    delayUntilState.add(targetTimestamp);
    c.setTimer(targetTimestamp, timeDomain);
  }

  @Override
  public void prefetchOnMerge(MergingStateContext state) {
    state.mergingAccess(DELAYED_UNTIL_TAG).get();
  }

  @Override
  public void onMerge(OnMergeContext c) throws Exception {
    // If the trigger is already finished, there is no way it will become re-activated
    if (c.trigger().isFinished()) {
      return;
    }

    // Determine the earliest point across all the windows, and delay to that.
    CombiningValueState<Instant, Instant> mergingDelays =
        c.state().mergingAccess(DELAYED_UNTIL_TAG);

    Instant earliestTargetTime = mergingDelays.get().read();
    if (earliestTargetTime != null) {
      mergingDelays.clear();
      mergingDelays.add(earliestTargetTime);
      c.setTimer(earliestTargetTime, timeDomain);
    }
  }

  @Override
  public void prefetchShouldFire(StateContext state) {
    state.access(DELAYED_UNTIL_TAG).get();
  }

  @Override
  public void clear(TriggerContext c) throws Exception {
    c.state().access(DELAYED_UNTIL_TAG).clear();
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(W window) {
    return BoundedWindow.TIMESTAMP_MAX_VALUE;
  }

  @Override
  public boolean shouldFire(Trigger<W>.TriggerContext context) throws Exception {
    Instant delayedUntil = context.state().access(DELAYED_UNTIL_TAG).get().read();
    return delayedUntil != null
        && getCurrentTime(context) != null
        && getCurrentTime(context).isAfter(delayedUntil);
  }

  @Override
  protected void onOnlyFiring(Trigger<W>.TriggerContext context) throws Exception {
    clear(context);
  }
}
