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

import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.util.ReduceFn.MergingStateContext;
import com.google.cloud.dataflow.sdk.util.ReduceFn.StateContext;
import com.google.cloud.dataflow.sdk.util.TimeDomain;
import com.google.cloud.dataflow.sdk.util.state.CombiningValueState;

import org.joda.time.Instant;

import java.util.List;
import java.util.Objects;

/**
 * {@code AfterProcessingTime} triggers fire based on the current processing time. They operate in
 * the real-time domain.
 *
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used
 */
@Experimental(Experimental.Kind.TRIGGER)
public class AfterProcessingTime<W extends BoundedWindow> extends TimeTrigger<W> {

  private static final long serialVersionUID = 0L;

  private AfterProcessingTime(List<SerializableFunction<Instant, Instant>> transforms) {
    super(transforms);
  }

  /**
   * Creates a trigger that fires when the current processing time passes the processing time
   * at which this trigger saw the first element in a pane.
   */
  public static <W extends BoundedWindow> AfterProcessingTime<W> pastFirstElementInPane() {
    return new AfterProcessingTime<W>(IDENTITY);
  }

  @Override
  protected AfterProcessingTime<W> newWith(
      List<SerializableFunction<Instant, Instant>> transforms) {
    return new AfterProcessingTime<W>(transforms);
  }

  @Override
  public TriggerResult onElement(OnElementContext c)
      throws Exception {
    CombiningValueState<Instant, Instant> delayUntilState = c.state().access(DELAYED_UNTIL_TAG);
    Instant delayUntil = delayUntilState.get().read();
    if (delayUntil == null) {
      delayUntil = computeTargetTimestamp(c.timers().currentProcessingTime());
      c.timers().setTimer(delayUntil, TimeDomain.PROCESSING_TIME);
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

    // Determine the earliest point across all the windows, and delay to that.
    CombiningValueState<Instant, Instant> mergingDelays =
        c.state().accessAcrossMergingWindows(DELAYED_UNTIL_TAG);
    Instant earliestTimer = mergingDelays.get().read();
    if (earliestTimer != null) {
      mergingDelays.clear();
      mergingDelays.add(earliestTimer);
      c.timers().setTimer(earliestTimer, TimeDomain.PROCESSING_TIME);
    }

    return MergeResult.CONTINUE;
  }

  @Override
  public TriggerResult onTimer(OnTimerContext c) throws Exception {
    if (c.timeDomain() != TimeDomain.PROCESSING_TIME) {
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
      c.timers().deleteTimer(timestamp, TimeDomain.PROCESSING_TIME);
    }
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(W window) {
    return BoundedWindow.TIMESTAMP_MAX_VALUE;
  }

  @Override
  protected Trigger<W> getContinuationTrigger(List<Trigger<W>> continuationTriggers) {
    return new AfterSynchronizedProcessingTime<W>();
  }

  @Override
  public String toString() {
    return "AfterProcessingTime.pastFirstElementInPane(" + timestampMappers + ")";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof AfterProcessingTime)) {
      return false;
    }
    AfterProcessingTime<?> that = (AfterProcessingTime<?>) obj;
    return Objects.equals(this.timestampMappers, that.timestampMappers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), this.timestampMappers);
  }
}
