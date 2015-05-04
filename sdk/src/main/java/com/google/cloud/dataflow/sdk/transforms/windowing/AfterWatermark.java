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
import com.google.cloud.dataflow.sdk.coders.InstantCoder;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;

import org.joda.time.Instant;

import java.util.List;

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
 * watermark (i.e. "late data) will ever be observed in the pipeline. These heuristics can
 * often be quite accurate, but the chance of seeing late data for any given window is non-zero.
 * Thus, if absolute correctness over time is important to your use case, you may want to consider
 * using a trigger that accounts for late data. The default trigger,
 * {@code Repeatedly.forever(AfterWatermark.pastEndOfWindow())}, which fires
 * once when the watermark passes the end of the window and then immediately therafter when any
 * late data arrive, is one such example.
 *
 * <p> The watermark is the clock that defines {@link Trigger.TimeDomain#EVENT_TIME}.
 *
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used.
 */
@Experimental(Experimental.Kind.TRIGGER)
public abstract class AfterWatermark<W extends BoundedWindow>
    extends TimeTrigger<W, AfterWatermark<W>> {

  private static final long serialVersionUID = 0L;

  protected AfterWatermark(List<SerializableFunction<Instant, Instant>> transforms) {
    super(transforms);
  }

  /**
   * Creates a trigger that fires when the watermark passes timestamp of the first element in the
   * pane.
   */
  static <W extends BoundedWindow> AfterWatermark<W> pastFirstElementInPane() {
    return new FromFirstElementInPane<W>(IDENTITY);
  }

  /**
   * Creates a trigger that fires when the watermark passes the end of the window.
   */
  public static <W extends BoundedWindow> AfterWatermark<W> pastEndOfWindow() {
    return new FromEndOfWindow<W>(IDENTITY);
  }

  private static class FromFirstElementInPane<W extends BoundedWindow> extends AfterWatermark<W> {

    private static final long serialVersionUID = 0L;

    private static final CodedTupleTag<Instant> DELAYED_UNTIL_TAG =
        CodedTupleTag.of("delayed-until", InstantCoder.of());

    private FromFirstElementInPane(
        List<SerializableFunction<Instant, Instant>> delayFunction) {
      super(delayFunction);
    }

    @Override
    public TriggerResult onElement(TriggerContext<W> c, OnElementEvent<W> e) throws Exception {
      Instant delayUntil = c.lookup(DELAYED_UNTIL_TAG, e.window());
      if (delayUntil == null) {
        delayUntil = computeTargetTimestamp(e.eventTimestamp());
        c.setTimer(e.window(), delayUntil, TimeDomain.EVENT_TIME);
        c.store(DELAYED_UNTIL_TAG, e.window(), delayUntil);
      }

      return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onMerge(TriggerContext<W> c, OnMergeEvent<W> e) throws Exception {
      // To have gotten here, we must not have fired in any of the oldWindows.
      Instant earliestTimer = BoundedWindow.TIMESTAMP_MAX_VALUE;
      for (Instant delayedUntil : c.lookup(DELAYED_UNTIL_TAG, e.oldWindows()).values()) {
        if (delayedUntil != null && delayedUntil.isBefore(earliestTimer)) {
          earliestTimer = delayedUntil;
        }
      }

      if (earliestTimer != null) {
        c.store(DELAYED_UNTIL_TAG, e.newWindow(), earliestTimer);
        c.setTimer(e.newWindow(), earliestTimer, TimeDomain.EVENT_TIME);
      }

      return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onTimer(TriggerContext<W> c, OnTimerEvent<W> e) throws Exception {
      return TriggerResult.FIRE_AND_FINISH;
    }

    @Override
    public void clear(TriggerContext<W> c, W window) throws Exception {
      c.remove(DELAYED_UNTIL_TAG, window);
      c.deleteTimer(window, TimeDomain.EVENT_TIME);
    }

    @Override
    public boolean willNeverFinish() {
      return false;
    }

    @Override
    public Instant getWatermarkCutoff(W window) {
      return computeTargetTimestamp(window.maxTimestamp());
    }

    @Override
    protected AfterWatermark<W> newWith(
        List<SerializableFunction<Instant, Instant>> transforms) {
      return new FromFirstElementInPane<W>(transforms);
    }
  }

  private static class FromEndOfWindow<W extends BoundedWindow> extends AfterWatermark<W> {

    private static final long serialVersionUID = 0L;

    private FromEndOfWindow(
        List<SerializableFunction<Instant, Instant>> composed) {
      super(composed);
    }

    @Override
    public TriggerResult onElement(TriggerContext<W> c, OnElementEvent<W> e) throws Exception {
      c.setTimer(e.window(),
          computeTargetTimestamp(e.window().maxTimestamp()), TimeDomain.EVENT_TIME);
      return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onMerge(Trigger.TriggerContext<W> c, OnMergeEvent<W> e) throws Exception {
      for (W oldWindow : e.oldWindows()) {
        c.deleteTimer(oldWindow, TimeDomain.EVENT_TIME);
      }

      c.setTimer(e.newWindow(), e.newWindow().maxTimestamp(), TimeDomain.EVENT_TIME);
      return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onTimer(TriggerContext<W> c, OnTimerEvent<W> e) throws Exception {
      return TriggerResult.FIRE_AND_FINISH;
    }

    @Override
    public void clear(TriggerContext<W> c, W window) throws Exception {
      c.deleteTimer(window, TimeDomain.EVENT_TIME);
    }

    @Override
    public boolean willNeverFinish() {
      return false;
    }

    @Override
    public Instant getWatermarkCutoff(W window) {
      return computeTargetTimestamp(window.maxTimestamp());
    }

    @Override
    protected AfterWatermark<W> newWith(
        List<SerializableFunction<Instant, Instant>> transforms) {
      return new FromEndOfWindow<>(transforms);
    }
  }
}
