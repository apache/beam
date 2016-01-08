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
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnceTrigger;
import com.google.cloud.dataflow.sdk.util.ExecutableTrigger;
import com.google.cloud.dataflow.sdk.util.ReduceFn.MergingStateContext;
import com.google.cloud.dataflow.sdk.util.ReduceFn.StateContext;
import com.google.cloud.dataflow.sdk.util.TimeDomain;
import com.google.cloud.dataflow.sdk.util.state.CombiningValueState;
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
   * Creates a trigger that fires when the watermark passes timestamp of the first element added to
   * the pane.
   */
  static <W extends BoundedWindow> TimeTrigger<W> pastFirstElementInPane() {
    return new FromFirstElementInPane<W>(TimeTrigger.IDENTITY);
  }

  /**
   * Creates a trigger that fires when the watermark passes the end of the window.
   */
  public static <W extends BoundedWindow> FromEndOfWindow<W> pastEndOfWindow() {
    return new FromEndOfWindow<W>();
  }

  /**
   * A watermark trigger targeted relative to the event time of the first element in the pane.
   */
  private static class FromFirstElementInPane<W extends BoundedWindow> extends TimeTrigger<W> {

    private FromFirstElementInPane(
        List<SerializableFunction<Instant, Instant>> delayFunction) {
      super(delayFunction);
    }

    @Override
    public void prefetchOnElement(StateContext state) {
      state.access(DELAYED_UNTIL_TAG).get();
    }

    @Override
    public TriggerResult onElement(OnElementContext c) throws Exception {
      CombiningValueState<Instant, Instant> delayUntilState = c.state().access(DELAYED_UNTIL_TAG);
      Instant delayUntil = delayUntilState.get().read();
      if (delayUntil == null) {
        delayUntil = computeTargetTimestamp(c.eventTimestamp());
        c.setTimer(delayUntil, TimeDomain.EVENT_TIME);
        delayUntilState.add(delayUntil);
      }

      return TriggerResult.CONTINUE;
    }

    @Override
    public void prefetchOnMerge(MergingStateContext state) {
      state.mergingAccess(DELAYED_UNTIL_TAG).get();
    }

    @Override
    public MergeResult onMerge(OnMergeContext c) throws Exception {
      // If the watermark time timer has fired in any of the windows being merged, it would have
      // fired at the same point if it had been added to the merged window. So, we just record it as
      // finished.
      if (c.trigger().finishedInAnyMergingWindow()) {
        return MergeResult.ALREADY_FINISHED;
      }

      // To have gotten here, we must not have fired in any of the oldWindows. Determine the event
      // timestamp from the minimum (we could also just pick one, or try to record the arrival times
      // of this first element in each pane).
      // Determine the earliest point across all the windows, and delay to that.
      CombiningValueState<Instant, Instant> mergingDelays =
          c.state().mergingAccess(DELAYED_UNTIL_TAG);
      Instant earliestTimer = mergingDelays.get().read();
      if (earliestTimer != null) {
        mergingDelays.clear();
        mergingDelays.add(earliestTimer);
        c.setTimer(earliestTimer, TimeDomain.EVENT_TIME);
      }

      return MergeResult.CONTINUE;
    }

    @Override
    public void prefetchOnTimer(StateContext state) {
      state.access(DELAYED_UNTIL_TAG).get();
    }

    @Override
    public TriggerResult onTimer(OnTimerContext c) throws Exception {
      if (c.timeDomain() != TimeDomain.EVENT_TIME) {
        return TriggerResult.CONTINUE;
      }

      Instant delayedUntil = c.state().access(DELAYED_UNTIL_TAG).get().read();
      if (delayedUntil == null || delayedUntil.isAfter(c.timestamp())) {
        return TriggerResult.CONTINUE;
      }

      return TriggerResult.FIRE_AND_FINISH;
    }

    @Override
    public void clear(TriggerContext c) throws Exception {
      CombiningValueState<Instant, Instant> delayed = c.state().access(DELAYED_UNTIL_TAG);
      Instant timestamp = delayed.get().read();
      delayed.clear();
      if (timestamp != null) {
        c.deleteTimer(timestamp, TimeDomain.EVENT_TIME);
      }
    }

    @Override
    public Instant getWatermarkThatGuaranteesFiring(W window) {
      return computeTargetTimestamp(window.maxTimestamp());
    }

    @Override
    protected FromFirstElementInPane<W> newWith(
        List<SerializableFunction<Instant, Instant>> transforms) {
      return new FromFirstElementInPane<W>(transforms);
    }

    @Override
    public OnceTrigger<W> getContinuationTrigger(List<Trigger<W>> continuationTriggers) {
      return this;
    }

    @Override
    public String toString() {
      return "AfterWatermark.pastFirstElementInPane(" + timestampMappers + ")";
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof FromFirstElementInPane)) {
        return false;
      }
      FromFirstElementInPane<?> that = (FromFirstElementInPane<?>) obj;
      return Objects.equals(this.timestampMappers, that.timestampMappers);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(timestampMappers);
    }
  }

  /**
   * Interface for building an AfterWatermarkTrigger with early firings already filled in.
   */
  public interface AfterWatermarkEarly<W extends BoundedWindow> extends TriggerBuilder<W> {
    /**
     * Creates a new {@code Trigger} like the this, except that it fires repeatedly whenever
     * the given {@code Trigger} fires before the watermark has passed the end of the window.
     */
    TriggerBuilder<W> withLateFirings(OnceTrigger<W> lateTrigger);
  }

  /**
   * Interface for building an AfterWatermarkTrigger with late firings already filled in.
   */
  public interface AfterWatermarkLate<W extends BoundedWindow> extends TriggerBuilder<W> {
    /**
     * Creates a new {@code Trigger} like the this, except that it fires repeatedly whenever
     * the given {@code Trigger} fires after the watermark has passed the end of the window.
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
    public TriggerResult onElement(OnElementContext c) throws Exception {
      return TriggerResult.CONTINUE;
    }

    @Override
    public MergeResult onMerge(OnMergeContext c) throws Exception {
      return c.trigger().finishedInAnyMergingWindow()
          ? MergeResult.ALREADY_FINISHED
          : MergeResult.CONTINUE;
    }

    @Override
    public TriggerResult onTimer(OnTimerContext c) throws Exception {
      return TriggerResult.CONTINUE;
    }

    @Override
    protected Trigger<W> getContinuationTrigger(List<Trigger<W>> continuationTriggers) {
      return this;
    }

    @Override
    public Instant getWatermarkThatGuaranteesFiring(W window) {
      return BoundedWindow.TIMESTAMP_MAX_VALUE;
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
      this.earlyTrigger =
          Preconditions.checkNotNull(earlyTrigger, "earlyTrigger should not be null");
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
    public TriggerResult onElement(OnElementContext c) throws Exception {
      // We always have an early trigger, even if it is the one that never fires. It will be marked
      // as finished once the watermark has passed the end of the window.

      if (!c.trigger().isFinished(EARLY_INDEX)) {
        // We're running the early trigger. If the window function is merging, we need to also
        // pass the events to the late trigger, so that merging data is available.
        ExecutableTrigger<W> current = c.trigger().subTrigger(EARLY_INDEX);
        TriggerResult result = current.invokeElement(c);
        if (result.isFire()) {
          // the subtriggers are OnceTriggers that are implicitly repeated. Rather than having
          // wrapped them explicitly, we implement that logic here. This allows us to take advantage
          // of the fact that they're being repeated to improve the implementation of this trigger.
          current.invokeClear(c);
          c.trigger().setFinished(false, EARLY_INDEX);

          if (lateTrigger != null && c.trigger().isMerging()) {
            c.trigger().subTrigger(LATE_INDEX).invokeClear(c);
          }

          return TriggerResult.FIRE;
        } else {

          if (lateTrigger != null && c.trigger().isMerging()) {
            if (c.trigger().subTrigger(LATE_INDEX).invokeElement(c).isFinish()) {
              // If late trigegr finishes, clear it out and keep going.
              c.trigger().subTrigger(LATE_INDEX).invokeClear(c);
              c.trigger().setFinished(false, LATE_INDEX);
            }
          }

          return TriggerResult.CONTINUE;
        }
      } else if (lateTrigger != null) {
        // We're running the late trigger -- otherwise the root would have finished when the early
        // finished.
        ExecutableTrigger<W> current = c.trigger().subTrigger(LATE_INDEX);
        TriggerResult result = current.invokeElement(c);
        if (result.isFire()) {
          // the subtriggers are OnceTriggers that need an implicit repeat around them. So, reset
          // the trigger after it fires.
          current.invokeClear(c);
          c.trigger().setFinished(false, LATE_INDEX);
          return TriggerResult.FIRE;
        } else {
          return TriggerResult.CONTINUE;
        }
      } else {
        throw new IllegalStateException(
            "Shouldn't receive elements after the watermark with no late trigger");
      }
    }

    @Override
    public MergeResult onMerge(OnMergeContext c) throws Exception {
      boolean pastEndOfWindow = false;

      // If the watermark was past the end of a window that is past the end of the new window,
      // then the watermark must also be past the end of this window. What's more, we've already
      // fired some elements for that trigger firing, so we report FINISHED (without firing).
      OnMergeContext earlySubContext = c.forTrigger(c.trigger().subTrigger(EARLY_INDEX));
      for (W finishedWindow : earlySubContext.trigger().getFinishedMergingWindows()) {
        if (!finishedWindow.maxTimestamp().isBefore(c.window().maxTimestamp())) {
          pastEndOfWindow = true;
          break;
        }
      }

      c.trigger().setFinished(pastEndOfWindow, EARLY_INDEX);

      if (pastEndOfWindow) {
        // If we've already fired the AtWatermark for a watermark that is >= the end of this window,
        // then we should merge the late trigger (if any)
        if (lateTrigger != null) {
          ExecutableTrigger<W> lateTrigger = c.trigger().subTrigger(LATE_INDEX);
          MergeResult result = lateTrigger.invokeMerge(c);
          // clear merge state if it got marked finished
          if (result.isFire()) {
            c.trigger().setFinished(false, 1);
            lateTrigger.invokeClear(c);
            return MergeResult.FIRE;
          } else {
            return MergeResult.CONTINUE;
          }
        } else {
          throw new IllegalStateException(
              "Shouldn't merge with windows that have already finished");
        }
      } else {
        // We haven't reached the watermark yet, so merge the early trigger.
        ExecutableTrigger<W> earlyTrigger = c.trigger().subTrigger(EARLY_INDEX);
        MergeResult result = earlyTrigger.invokeMerge(c);

        // clear merge state if it got marked finished
        if (result.isFire()) {
          c.trigger().setFinished(false, EARLY_INDEX);
          earlyTrigger.invokeClear(c);
          return MergeResult.FIRE;
        } else {
          return MergeResult.CONTINUE;
        }
      }
    }

    @Override
    public TriggerResult onTimer(OnTimerContext c) throws Exception {
      boolean isOnTime = c.timeDomain() == TimeDomain.EVENT_TIME
          && c.timestamp().isEqual(c.window().maxTimestamp());

      if (!isOnTime) {
        // If this timer isn't the ON_TIME firing, send it to the current subtree.
        ExecutableTrigger<W> current = c.trigger().firstUnfinishedSubTrigger();
        if (current == null) {
          return TriggerResult.CONTINUE;
        }
        TriggerResult result = current.invokeTimer(c);
        if (result.isFire()) {
          // the subtriggers are OnceTriggers that need an implicit repeat around them. So, reset
          // the trigger after it fires.
          c.trigger().setFinished(false, LATE_INDEX);
          current.invokeClear(c);
          return TriggerResult.FIRE;
        } else {
          return TriggerResult.CONTINUE;
        }
      } else {
        // Mark the early trigger finished.
        c.trigger().setFinished(true, EARLY_INDEX);

        if (lateTrigger != null) {
          // In case we ran the late trigger in parallel, clear out its state.

          if (c.trigger().isMerging()) {
            // If we were pre-running the late trigger, clear out any state since we're firing.
            c.trigger().setFinished(false, LATE_INDEX);
            c.trigger().subTrigger(LATE_INDEX).invokeClear(c);
          }

          return TriggerResult.FIRE;
        } else {
          return TriggerResult.FIRE_AND_FINISH;
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
    public TriggerResult onElement(OnElementContext c) throws Exception {
      c.setTimer(c.window().maxTimestamp(), TimeDomain.EVENT_TIME);
      return TriggerResult.CONTINUE;
    }

    @Override
    public MergeResult onMerge(OnMergeContext c) throws Exception {
      // If the watermark was past the end of a window that is past the end of the new window,
      // then the watermark must also be past the end of this window. What's more, we've already
      // fired some elements for that trigger firing, so we report FINISHED (without firing).
      for (W finishedWindow : c.trigger().getFinishedMergingWindows()) {
        if (!finishedWindow.maxTimestamp().isBefore(c.window().maxTimestamp())) {
          return MergeResult.ALREADY_FINISHED;
        }
      }

      // Otherwise, set a timer for this window, and return.
      c.setTimer(c.window().maxTimestamp(), TimeDomain.EVENT_TIME);
      return MergeResult.CONTINUE;
    }

    @Override
    public TriggerResult onTimer(OnTimerContext c) throws Exception {
      if (c.timeDomain() != TimeDomain.EVENT_TIME
          || c.timestamp().isBefore(c.window().maxTimestamp())) {
        return TriggerResult.CONTINUE;
      } else {
        return TriggerResult.FIRE_AND_FINISH;
      }
    }

    @Override
    public void clear(TriggerContext c) throws Exception {
      c.deleteTimer(c.window().maxTimestamp(), TimeDomain.EVENT_TIME);
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
  }
}
