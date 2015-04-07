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

import org.joda.time.Instant;

/**
 * Repeat a trigger, either until some condition is met or forever.
 *
 * <p>For example, to fire after the end of the window, and every time late data arrives:
 * <pre> {@code
 *     Repeatedly.forever(AfterWatermark.isPastEndOfWindow());
 * } </pre>
 *
 * <p>{@code Repeatedly.forever(someTrigger)} behaves like the infinite
 * {@code SequenceOf(someTrigger, someTrigger, someTrigger, ...)}.
 *
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
 * {@code Trigger}
 */
public class Repeatedly<W extends BoundedWindow> implements Trigger<W> {

  private static final long serialVersionUID = 0L;

  private Trigger<W> repeated;

  /**
   * Create a composite trigger that repeatedly executes the trigger {@code toRepeat}, firing each
   * time it fires and ignoring any indications to finish.
   *
   * <p>Unless used with {@link #finishing} the composite trigger will never finish.
   *
   * @param repeated the trigger to execute repeatedly.
   */
  public static <W extends BoundedWindow> Repeatedly<W> forever(Trigger<W> repeated) {
    return new Repeatedly<W>(repeated);
  }

  private Repeatedly(Trigger<W> repeated) {
    this.repeated = repeated;
  }

  /**
   * Specify an ending condition for this {@code Repeated} trigger. When {@code until} fires the
   * composite trigger will fire and finish.
   *
   * @param until the trigger that will fire when we should stop repeating.
   */
  public Trigger<W> finishing(AtMostOnceTrigger<W> until) {
    return new Until<W>(this, until);
  }

  private TriggerResult wrap(TriggerContext<W> c, W window, TriggerResult result) throws Exception {
    if (result.isFire() || result.isFinish()) {
      repeated.clear(c, window);
    }
    return result.isFire() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onElement(TriggerContext<W> c, OnElementEvent<W> e)
      throws Exception {
    return wrap(c, e.window(), repeated.onElement(c, e));
  }

  @Override
  public TriggerResult onMerge(TriggerContext<W> c, OnMergeEvent<W> e) throws Exception {
    return wrap(c, e.newWindow(), repeated.onMerge(c, e));
  }

  @Override
  public TriggerResult onTimer(TriggerContext<W> c, OnTimerEvent<W> e) throws Exception {
    return wrap(c, e.window(), repeated.onTimer(c, e));
  }

  @Override
  public void clear(TriggerContext<W> c, W window) throws Exception {
    repeated.clear(c, window);
  }

  @Override
  public boolean willNeverFinish() {
    // Repeatedly without an until will never finish.
    return true;
  }

  @Override
  public Instant getWatermarkCutoff(W window) {
    // This trigger fires once the repeated trigger fires.
    return repeated.getWatermarkCutoff(window);
  }

  @Override
  public boolean isCompatible(Trigger<?> other) {
    if (!(other instanceof Repeatedly)) {
      return false;
    }

    Repeatedly<?> that = (Repeatedly<?>) other;
    return repeated.isCompatible(that.repeated);
  }

  /**
   * Repeats the given trigger forever, until the "until" trigger fires.
   *
   * <p> TODO: Move this to the top level.
   */
  public static class Until<W extends BoundedWindow> implements Trigger<W> {

    private static final int ACTUAL = 0;
    private static final int UNTIL = 1;
    private static final long serialVersionUID = 0L;

    private Trigger<W> actual;
    private AtMostOnceTrigger<W> until;

    private Until(Trigger<W> actual, AtMostOnceTrigger<W> until) {
      this.actual = actual;
      this.until = until;
    }

    @Override
    public TriggerResult onElement(TriggerContext<W> c, OnElementEvent<W> e) throws Exception {
      TriggerResult untilResult = until.onElement(c.forChild(UNTIL), e);
      if (untilResult != TriggerResult.CONTINUE) {
        return TriggerResult.FIRE_AND_FINISH;
      }

      return actual.onElement(c.forChild(ACTUAL), e);
    }

    @Override
    public TriggerResult onMerge(TriggerContext<W> c, OnMergeEvent<W> e) throws Exception {
      TriggerResult untilResult = until.onMerge(c.forChild(UNTIL), e);
      if (untilResult != TriggerResult.CONTINUE) {
        return TriggerResult.FIRE_AND_FINISH;
      }

      return actual.onMerge(c.forChild(ACTUAL), e);
    }

    @Override
    public TriggerResult onTimer(TriggerContext<W> c, OnTimerEvent<W> e) throws Exception {

      if (e.isForCurrentLayer()) {
        throw new IllegalStateException("Until shouldn't receive any timers.");
      } else if (e.getChildIndex() == ACTUAL) {
        return actual.onTimer(c.forChild(ACTUAL), e.withoutOuterTrigger());
      } else {
        if (until.onTimer(c.forChild(UNTIL), e.withoutOuterTrigger()) != TriggerResult.CONTINUE) {
          return TriggerResult.FIRE_AND_FINISH;
        }
      }

      return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TriggerContext<W> c, W window) throws Exception {
      actual.clear(c.forChild(ACTUAL), window);
      until.clear(c.forChild(UNTIL), window);
    }

    @Override
    public boolean willNeverFinish() {
      return false;
    }

    @Override
    public Instant getWatermarkCutoff(W window) {
      // This trigger fires once either the trigger or the until trigger fires.
      Instant actualDeadline = actual.getWatermarkCutoff(window);
      Instant untilDeadline = until.getWatermarkCutoff(window);
      return actualDeadline.isBefore(untilDeadline) ? actualDeadline : untilDeadline;
    }

    @Override
    public boolean isCompatible(Trigger<?> other) {
      if (!(other instanceof Until)) {
        return false;
      }

      Until<?> that = (Until<?>) other;
      return actual.isCompatible(that.actual)
          && until.isCompatible(that.until);
    }
  }
}
