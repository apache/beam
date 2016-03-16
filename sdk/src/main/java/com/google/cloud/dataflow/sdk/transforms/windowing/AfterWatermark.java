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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnceTrigger;
import com.google.cloud.dataflow.sdk.util.ExecutableTrigger;
import com.google.cloud.dataflow.sdk.util.TimeDomain;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.joda.time.Instant;

import java.util.List;
import java.util.Objects;

/**
 * <p>{@code AfterWatermark} triggers fire based on progress of the system watermark. This time is a
 * lower-bound, sometimes heuristically established, on event times that have been fully processed
 * by the pipeline.
 *
 * <p>For sources that provide non-heuristic watermarks (e.g.
 * {@link com.google.cloud.dataflow.sdk.io.PubsubIO} when using arrival times as event times), the
 * watermark is a strict guarantee that no data with an event time earlier than
 * that watermark will ever be observed in the pipeline. In this case, it's safe to assume that any
 * pane triggered by an {@code AfterWatermark} trigger with a reference point at or beyond the end
 * of the window will be the last pane ever for that window.
 *
 * <p>For sources that provide heuristic watermarks (e.g.
 * {@link com.google.cloud.dataflow.sdk.io.PubsubIO} when using user-supplied event times), the
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
 * Additionaly firings before or after the watermark can be requested by calling
 * {@code AfterWatermark.pastEndOfWindow.withEarlyFirings(OnceTrigger)} or
 * {@code AfterWatermark.pastEndOfWindow.withEarlyFirings(OnceTrigger)}.
 *
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used.
 */
@Experimental(Experimental.Kind.TRIGGER)
public class AfterWatermark<W extends BoundedWindow> {

  // Static factory class.
  private AfterWatermark() {}

  /**
   * Creates a trigger that fires when the watermark passes the end of the window.
   */
  public static <W extends BoundedWindow> FromEndOfWindow<W> pastEndOfWindow() {
    return new FromEndOfWindow<W>();
  }

  /**
   * Interface for building an AfterWatermarkTrigger with early firings already filled in.
   */
  public interface AfterWatermarkEarly<W extends BoundedWindow> extends TriggerBuilder<W> {
    /**
     * Creates a new {@code Trigger} like the this, except that it fires repeatedly whenever
     * the given {@code Trigger} fires after the watermark has passed the end of the window.
     */
    TriggerBuilder<W> withLateFirings(OnceTrigger<W> lateTrigger);
  }

  /**
   * Interface for building an AfterWatermarkTrigger with late firings already filled in.
   */
  public interface AfterWatermarkLate<W extends BoundedWindow> extends TriggerBuilder<W> {
    /**
     * Creates a new {@code Trigger} like the this, except that it fires repeatedly whenever
     * the given {@code Trigger} fires before the watermark has passed the end of the window.
     */
    TriggerBuilder<W> withEarlyFirings(OnceTrigger<W> earlyTrigger);
  }

  /**
   * A trigger which never fires. Used for the "early" trigger when only a late trigger was
   * specified.
   */
  private static class NeverTrigger<W extends BoundedWindow> extends OnceTrigger<W> {

    protected NeverTrigger() {
      super(null);
    }

    @Override
    public void onElement(OnElementContext c) throws Exception { }

    @Override
    public void onMerge(OnMergeContext c) throws Exception { }

    @Override
    protected Trigger<W> getContinuationTrigger(List<Trigger<W>> continuationTriggers) {
      return this;
    }

    @Override
    public Instant getWatermarkThatGuaranteesFiring(W window) {
      return BoundedWindow.TIMESTAMP_MAX_VALUE;
    }

    @Override
    public boolean shouldFire(Trigger<W>.TriggerContext context) throws Exception {
      return false;
    }

    @Override
    protected void onOnlyFiring(Trigger<W>.TriggerContext context) throws Exception {
      throw new UnsupportedOperationException(
          String.format("%s should never fire", getClass().getSimpleName()));
    }
  }

  private static class AfterWatermarkEarlyAndLate<W extends BoundedWindow>
      extends Trigger<W>
      implements TriggerBuilder<W>, AfterWatermarkEarly<W>, AfterWatermarkLate<W> {

    private static final int EARLY_INDEX = 0;
    private static final int LATE_INDEX = 1;

    private final OnceTrigger<W> earlyTrigger;
    private final OnceTrigger<W> lateTrigger;

    @SuppressWarnings("unchecked")
    private AfterWatermarkEarlyAndLate(OnceTrigger<W> earlyTrigger, OnceTrigger<W> lateTrigger) {
      super(lateTrigger == null
          ? ImmutableList.<Trigger<W>>of(earlyTrigger)
          : ImmutableList.<Trigger<W>>of(earlyTrigger, lateTrigger));
      this.earlyTrigger = checkNotNull(earlyTrigger, "earlyTrigger should not be null");
      this.lateTrigger = lateTrigger;
    }

    @Override
    public TriggerBuilder<W> withEarlyFirings(OnceTrigger<W> earlyTrigger) {
      return new AfterWatermarkEarlyAndLate<W>(earlyTrigger, lateTrigger);
    }

    @Override
    public TriggerBuilder<W> withLateFirings(OnceTrigger<W> lateTrigger) {
      return new AfterWatermarkEarlyAndLate<W>(earlyTrigger, lateTrigger);
    }

    @Override
    public void onElement(OnElementContext c) throws Exception {
      if (!c.trigger().isMerging()) {
        // If merges can never happen, we just run the unfinished subtrigger
        c.trigger().firstUnfinishedSubTrigger().invokeOnElement(c);
      } else {
        // If merges can happen, we run for all subtriggers because they might be
        // de-activated or re-activated
        for (ExecutableTrigger<W> subTrigger : c.trigger().subTriggers()) {
          subTrigger.invokeOnElement(c);
        }
      }
    }

    @Override
    public void onMerge(OnMergeContext c) throws Exception {
      // NOTE that the ReduceFnRunner will delete all end-of-window timers for the
      // merged-away windows.

      ExecutableTrigger<W> earlySubtrigger = c.trigger().subTrigger(EARLY_INDEX);
      // We check the early trigger to determine if we are still processing it or
      // if the end of window has transitioned us to the late trigger
      OnMergeContext earlyContext = c.forTrigger(earlySubtrigger);

      // If the early trigger is still active in any merging window then it is still active in
      // the new merged window, because even if the merged window is "done" some pending elements
      // haven't had a chance to fire.
      if (!earlyContext.trigger().finishedInAllMergingWindows() || !endOfWindowReached(c)) {
        earlyContext.trigger().setFinished(false);
        if (lateTrigger != null) {
          ExecutableTrigger<W> lateSubtrigger = c.trigger().subTrigger(LATE_INDEX);
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

    @Override
    public Trigger<W> getContinuationTrigger() {
      return new AfterWatermarkEarlyAndLate<W>(
          earlyTrigger.getContinuationTrigger(),
          lateTrigger == null ? null : lateTrigger.getContinuationTrigger());
    }

    @Override
    protected Trigger<W> getContinuationTrigger(List<Trigger<W>> continuationTriggers) {
      throw new UnsupportedOperationException(
          "Should not call getContinuationTrigger(List<Trigger<W>>)");
    }

    @Override
    public Instant getWatermarkThatGuaranteesFiring(W window) {
      // Even without an early or late trigger, we'll still produce a firing at the watermark.
      return window.maxTimestamp();
    }

    private boolean endOfWindowReached(Trigger<W>.TriggerContext context) {
      return context.currentEventTime() != null
          && context.currentEventTime().isAfter(context.window().maxTimestamp());
    }

    @Override
    public boolean shouldFire(Trigger<W>.TriggerContext context) throws Exception {
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
    public void onFire(Trigger<W>.TriggerContext context) throws Exception {
      if (!context.forTrigger(context.trigger().subTrigger(EARLY_INDEX)).trigger().isFinished()) {
        onNonLateFiring(context);
      } else if (lateTrigger != null) {
        onLateFiring(context);
      } else {
        // all done
        context.trigger().setFinished(true);
      }
    }

    private void onNonLateFiring(Trigger<W>.TriggerContext context) throws Exception {
      // We have not yet transitioned to late firings.
      ExecutableTrigger<W> earlySubtrigger = context.trigger().subTrigger(EARLY_INDEX);
      Trigger<W>.TriggerContext earlyContext = context.forTrigger(earlySubtrigger);

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

    private void onLateFiring(Trigger<W>.TriggerContext context) throws Exception {
      // We are firing the late trigger, with implicit repeat
      ExecutableTrigger<W> lateSubtrigger = context.trigger().subTrigger(LATE_INDEX);
      lateSubtrigger.invokeOnFire(context);
      // It is a OnceTrigger, so it must have finished; unfinished it and clear it
      lateSubtrigger.invokeClear(context);
      context.forTrigger(lateSubtrigger).trigger().setFinished(false);
    }
  }

  /**
   * A watermark trigger targeted relative to the end of the window.
   */
  public static class FromEndOfWindow<W extends BoundedWindow> extends OnceTrigger<W> {

    private FromEndOfWindow() {
      super(null);
    }

    /**
     * Creates a new {@code Trigger} like the this, except that it fires repeatedly whenever
     * the given {@code Trigger} fires before the watermark has passed the end of the window.
     */
    public AfterWatermarkEarly<W> withEarlyFirings(OnceTrigger<W> earlyFirings) {
      Preconditions.checkNotNull(earlyFirings,
          "Must specify the trigger to use for early firings");
      return new AfterWatermarkEarlyAndLate<W>(earlyFirings, null);
    }

    /**
     * Creates a new {@code Trigger} like the this, except that it fires repeatedly whenever
     * the given {@code Trigger} fires after the watermark has passed the end of the window.
     */
    public AfterWatermarkLate<W> withLateFirings(OnceTrigger<W> lateFirings) {
      Preconditions.checkNotNull(lateFirings,
          "Must specify the trigger to use for late firings");
      return new AfterWatermarkEarlyAndLate<W>(new NeverTrigger<W>(), lateFirings);
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
    public Instant getWatermarkThatGuaranteesFiring(W window) {
      return window.maxTimestamp();
    }

    @Override
    public FromEndOfWindow<W> getContinuationTrigger(List<Trigger<W>> continuationTriggers) {
      return this;
    }

    @Override
    public String toString() {
      return "AfterWatermark.pastEndOfWindow()";
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
    public boolean shouldFire(Trigger<W>.TriggerContext context) throws Exception {
      return endOfWindowReached(context);
    }

    private boolean endOfWindowReached(Trigger<W>.TriggerContext context) {
      return context.currentEventTime() != null
          && context.currentEventTime().isAfter(context.window().maxTimestamp());
    }

    @Override
    protected void onOnlyFiring(Trigger<W>.TriggerContext context) throws Exception { }
  }
}
