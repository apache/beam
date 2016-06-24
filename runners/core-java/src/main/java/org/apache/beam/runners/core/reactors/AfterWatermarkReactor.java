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
package org.apache.beam.runners.core.reactors;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.reactors.TriggerReactor.OnceTriggerReactor;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.util.TimeDomain;

/**
 * {@code AfterWatermark} triggers fire based on progress of the system watermark. This time is a
 * lower-bound, sometimes heuristically established, on event times that have been fully processed
 * by the pipeline.
 *
 * <p>For sources that provide non-heuristic watermarks (e.g.
 * {@link org.apache.beam.sdk.io.PubsubIO} when using arrival times as event times), the
 * watermark is a strict guarantee that no data with an event time earlier than
 * that watermark will ever be observed in the pipeline. In this case, it's safe to assume that any
 * pane triggered by an {@code AfterWatermark} trigger with a reference point at or beyond the end
 * of the window will be the last pane ever for that window.
 *
 * <p>For sources that provide heuristic watermarks (e.g.
 * {@link org.apache.beam.sdk.io.PubsubIO} when using user-supplied event times), the
 * watermark itself becomes an <i>estimate</i> that no data with an event time earlier than that
 * watermark (i.e. "late data") will ever be observed in the pipeline. These heuristics can
 * often be quite accurate, but the chance of seeing late data for any given window is non-zero.
 * Thus, if absolute correctness over time is important to your use case, you may want to consider
 * using a trigger that accounts for late data. The default trigger,
 * {@code Repeatedly.forever(AfterWatermark.pastEndOfWindow())}, which fires
 * once when the watermark passes the end of the window and then immediately therafter when any
 * late data arrives, is one such example.
 *
 * <p>The watermark is the clock that defines {@link TimeDomain#EVENT_TIME}.
 *
 * <p>Additionaly firings before or after the watermark can be requested by calling
 * {@code AfterWatermark.pastEndOfWindow.withEarlyFirings(OnceTrigger)} or
 * {@code AfterWatermark.pastEndOfWindow.withEarlyFirings(OnceTrigger)}.
 */
@Experimental(Experimental.Kind.TRIGGER)
public class AfterWatermarkReactor {

  private static final String TO_STRING = "AfterWatermark.pastEndOfWindow()";

  // Static factory class.
  private AfterWatermarkReactor() {}

  /**
   * Creates a trigger that fires when the watermark passes the end of the window.
   */
  public static FromEndOfWindow pastEndOfWindow() {
    return new FromEndOfWindow();
  }

  /**
   * @see AfterWatermark
   */
  public static class AfterWatermarkEarlyAndLate extends TriggerReactor {

    private static final int EARLY_INDEX = 0;
    private static final int LATE_INDEX = 1;

    private final OnceTriggerReactor earlyTrigger;
    @Nullable
    private final OnceTriggerReactor lateTrigger;

    @SuppressWarnings("unchecked")
    private AfterWatermarkEarlyAndLate(
        OnceTriggerReactor earlyTrigger, OnceTriggerReactor lateTrigger) {
      super(
          lateTrigger == null
              ? ImmutableList.<TriggerReactor>of(earlyTrigger)
              : ImmutableList.<TriggerReactor>of(earlyTrigger, lateTrigger));
      this.earlyTrigger = checkNotNull(earlyTrigger, "earlyTrigger should not be null");
      this.lateTrigger = lateTrigger;
    }

    public TriggerReactor withEarlyFirings(OnceTriggerReactor earlyTrigger) {
      return new AfterWatermarkEarlyAndLate(earlyTrigger, lateTrigger);
    }

    public TriggerReactor withLateFirings(OnceTriggerReactor lateTrigger) {
      return new AfterWatermarkEarlyAndLate(earlyTrigger, lateTrigger);
    }

    @Override
    public void onElement(OnElementContext c) throws Exception {
      if (!c.trigger().isMerging()) {
        // If merges can never happen, we just run the unfinished subtrigger
        c.trigger().firstUnfinishedSubTrigger().invokeOnElement(c);
      } else {
        // If merges can happen, we run for all subtriggers because they might be
        // de-activated or re-activated
        for (ExecutableTriggerReactor subTrigger : c.trigger().subTriggers()) {
          subTrigger.invokeOnElement(c);
        }
      }
    }

    @Override
    public void onMerge(OnMergeContext c) throws Exception {
      // NOTE that the ReduceFnRunner will delete all end-of-window timers for the
      // merged-away windows.

      ExecutableTriggerReactor earlySubtrigger = c.trigger().subTrigger(EARLY_INDEX);
      // We check the early trigger to determine if we are still processing it or
      // if the end of window has transitioned us to the late trigger
      OnMergeContext earlyContext = c.forTrigger(earlySubtrigger);

      // If the early trigger is still active in any merging window then it is still active in
      // the new merged window, because even if the merged window is "done" some pending elements
      // haven't had a chance to fire.
      if (!earlyContext.trigger().finishedInAllMergingWindows() || !endOfWindowReached(c)) {
        earlyContext.trigger().setFinished(false);
        if (lateTrigger != null) {
          ExecutableTriggerReactor lateSubtrigger = c.trigger().subTrigger(LATE_INDEX);
          OnMergeContext lateContext = c.forTrigger(lateSubtrigger);
          lateContext.trigger().setFinished(false);
          lateSubtrigger.invokeClear(lateContext);
        }
      } else {
        // Otherwise the early trigger and end-of-window bit is done for good.
        earlyContext.trigger().setFinished(true);
        if (lateTrigger != null) {
          c.trigger().subTrigger(LATE_INDEX).invokeOnMerge(c);
        }
      }
    }

    private boolean endOfWindowReached(TriggerReactor.TriggerContext context) {
      return context.currentEventTime() != null
          && context.currentEventTime().isAfter(context.window().maxTimestamp());
    }

    @Override
    public boolean shouldFire(TriggerReactor.TriggerContext context) throws Exception {
      if (!context.trigger().isFinished(EARLY_INDEX)) {
        // We have not yet transitioned to late firings.
        // We should fire if either the trigger is ready or we reach the end of the window.
        return context.trigger().subTrigger(EARLY_INDEX).invokeShouldFire(context)
            || endOfWindowReached(context);
      } else if (lateTrigger == null) {
        return false;
      } else {
        // We are running the late trigger
        return context.trigger().subTrigger(LATE_INDEX).invokeShouldFire(context);
      }
    }

    @Override
    public void onFire(TriggerReactor.TriggerContext context) throws Exception {
      if (!context.forTrigger(context.trigger().subTrigger(EARLY_INDEX)).trigger().isFinished()) {
        onNonLateFiring(context);
      } else if (lateTrigger != null) {
        onLateFiring(context);
      } else {
        // all done
        context.trigger().setFinished(true);
      }
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder(TO_STRING);

      if (!(earlyTrigger instanceof NeverReactor)) {
        builder
            .append(".withEarlyFirings(")
            .append(earlyTrigger)
            .append(")");
      }

      if (lateTrigger != null && !(lateTrigger instanceof NeverReactor)) {
        builder
            .append(".withLateFirings(")
            .append(lateTrigger)
            .append(")");
      }

      return builder.toString();
    }

    private void onNonLateFiring(TriggerReactor.TriggerContext context) throws Exception {
      // We have not yet transitioned to late firings.
      ExecutableTriggerReactor earlySubtrigger = context.trigger().subTrigger(EARLY_INDEX);
      TriggerReactor.TriggerContext earlyContext = context.forTrigger(earlySubtrigger);

      if (!endOfWindowReached(context)) {
        // This is an early firing, since we have not arrived at the end of the window
        // Implicitly repeats
        earlySubtrigger.invokeOnFire(context);
        earlySubtrigger.invokeClear(context);
        earlyContext.trigger().setFinished(false);
      } else {
        // We have arrived at the end of the window; terminate the early trigger
        // and clear out the late trigger's state
        if (earlySubtrigger.invokeShouldFire(context)) {
          earlySubtrigger.invokeOnFire(context);
        }
        earlyContext.trigger().setFinished(true);
        earlySubtrigger.invokeClear(context);

        if (lateTrigger == null) {
          // Done if there is no late trigger.
          context.trigger().setFinished(true);
        } else {
          // If there is a late trigger, we transition to it, and need to clear its state
          // because it was run in parallel.
          context.trigger().subTrigger(LATE_INDEX).invokeClear(context);
        }
      }

    }

    private void onLateFiring(TriggerReactor.TriggerContext context) throws Exception {
      // We are firing the late trigger, with implicit repeat
      ExecutableTriggerReactor lateSubtrigger = context.trigger().subTrigger(LATE_INDEX);
      lateSubtrigger.invokeOnFire(context);
      // It is a OnceTrigger, so it must have finished; unfinished it and clear it
      lateSubtrigger.invokeClear(context);
      context.forTrigger(lateSubtrigger).trigger().setFinished(false);
    }
  }

  /**
   * A watermark trigger targeted relative to the end of the window.
   */
  public static class FromEndOfWindow extends OnceTriggerReactor {

    private FromEndOfWindow() {
      super(null);
    }

    /**
     * Creates a new {@code Trigger} like the this, except that it fires repeatedly whenever
     * the given {@code Trigger} fires before the watermark has passed the end of the window.
     */
    public AfterWatermarkEarlyAndLate withEarlyFirings(OnceTriggerReactor earlyFirings) {
      checkNotNull(earlyFirings, "Must specify the trigger to use for early firings");
      return new AfterWatermarkEarlyAndLate(earlyFirings, null);
    }

    /**
     * Creates a new {@code Trigger} like the this, except that it fires repeatedly whenever
     * the given {@code Trigger} fires after the watermark has passed the end of the window.
     */
    public AfterWatermarkEarlyAndLate withLateFirings(OnceTriggerReactor lateFirings) {
      checkNotNull(lateFirings, "Must specify the trigger to use for late firings");
      return new AfterWatermarkEarlyAndLate(NeverReactor.ever(), lateFirings);
    }

    @Override
    public void onElement(OnElementContext c) throws Exception {
      // We're interested in knowing when the input watermark passes the end of the window.
      // (It is possible this has already happened, in which case the timer will be fired
      // almost immediately).
      c.setTimer(c.window().maxTimestamp(), TimeDomain.EVENT_TIME);
    }

    @Override
    public void onMerge(OnMergeContext c) throws Exception {
      // NOTE that the ReduceFnRunner will delete all end-of-window timers for the
      // merged-away windows.

      if (!c.trigger().finishedInAllMergingWindows()) {
        // If the trigger is still active in any merging window then it is still active in the new
        // merged window, because even if the merged window is "done" some pending elements haven't
        // had a chance to fire
        c.trigger().setFinished(false);
      } else if (!endOfWindowReached(c)) {
        // If the end of the new window has not been reached, then the trigger is active again.
        c.trigger().setFinished(false);
      } else {
        // Otherwise it is done for good
        c.trigger().setFinished(true);
      }
    }

    @Override
    public String toString() {
      return TO_STRING;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof FromEndOfWindow;
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass());
    }

    @Override
    public boolean shouldFire(TriggerReactor.TriggerContext context) throws Exception {
      return endOfWindowReached(context);
    }

    private boolean endOfWindowReached(TriggerReactor.TriggerContext context) {
      return context.currentEventTime() != null
          && context.currentEventTime().isAfter(context.window().maxTimestamp());
    }

    @Override
    protected void onOnlyFiring(TriggerReactor.TriggerContext context) throws Exception { }
  }
}
