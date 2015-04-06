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

import java.util.Arrays;

/**
 * Repeat a trigger, either until some condition is met or forever.
 *
 * <p>For example, to fire after the end of the window, and every time late data arrives:
 * <pre> {@code
 * Repeatedly.forever(WhenWatermark.isPastEndOfWindow());
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
   * <p>Unless used with {@link #until} the composite trigger will never finish.
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
   * composite trigger will finish.
   *
   * <p>If {@code until} finishes before firing we stop executing it and the {@code Repeated}
   * trigger will never finish.
   *
   * @param until the trigger that will fire when we should stop repeating.
   */
  public RepeatedlyUntil<W> until(AtMostOnceTrigger<W> until) {
    return new RepeatedlyUntil<W>(repeated, until);
  }


  private TriggerResult wrap(TriggerContext<W> c, W window, TriggerResult result) throws Exception {
    if (result.isFire() || result.isFinish()) {
      repeated.clear(c, window);
    }
    return result.isFire() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onElement(
      TriggerContext<W> c, Object value, Instant timestamp, W window, WindowStatus status)
      throws Exception {
    return wrap(c, window, repeated.onElement(c, value, timestamp, window, status));
  }

  @Override
  public TriggerResult onMerge(TriggerContext<W> c, Iterable<W> oldWindows, W newWindow)
      throws Exception {
    return wrap(c, newWindow, repeated.onMerge(c, oldWindows, newWindow));
  }

  @Override
  public TriggerResult onTimer(TriggerContext<W> c, TriggerId<W> triggerId) throws Exception {
    return wrap(c, triggerId.getWindow(), repeated.onTimer(c, triggerId));
  }

  @Override
  public void clear(TriggerContext<W> c, W window) throws Exception {
    repeated.clear(c, window);
  }

  @Override
  public boolean willNeverFinish() {
    return true;
  }

  /**
   * Repeats the given trigger forever, until the "until" trigger fires.
   */
  public static class RepeatedlyUntil<W extends BoundedWindow> extends CompositeTrigger<W> {

    private static final long serialVersionUID = 0L;

    private RepeatedlyUntil(Trigger<W> repeat, AtMostOnceTrigger<W> until) {
      super(Arrays.asList(repeat, until));
    }

    private TriggerResult handleResult(
        TriggerContext<W> c, SubTriggerExecutor subExecutor, W window,
        TriggerResult repeated, TriggerResult until) throws Exception {
      if (repeated.isFinish() && !until.isFire()) {
        subExecutor.reset(c, 0, window);
      }

      return TriggerResult.valueOf(repeated.isFire(), until.isFire());
    }

    @Override
    public TriggerResult onElement(
        TriggerContext<W> c, Object value, Instant timestamp, W window, WindowStatus status)
        throws Exception {
      SubTriggerExecutor subExecutor = subExecutor(c, window);

      TriggerResult until = subExecutor.isFinished(1)
          ? TriggerResult.CONTINUE // if we already finished the until, treat it like Never Stop
          : subExecutor.onElement(c, 1, value, timestamp, window, status);
      return handleResult(c, subExecutor, window,
          subExecutor.onElement(c, 0, value, timestamp, window, status), until);
    }

    @Override
    public TriggerResult onMerge(
        TriggerContext<W> c, Iterable<W> oldWindows, W newWindow) throws Exception {
      SubTriggerExecutor subExecutor = subExecutor(c, oldWindows, newWindow);

      TriggerResult until = subExecutor.isFinished(1)
          ? TriggerResult.CONTINUE // if we already finished the until, treat it like Never Stop
          : subExecutor.onMerge(c, 1, oldWindows, newWindow);

      // Even if the merged until says fire, we should still evaluate (and maybe fire) from the
      // merging of the repeated trigger.
      return handleResult(c, subExecutor, newWindow,
          subExecutor.onMerge(c, 0, oldWindows, newWindow), until);
    }

    @Override
    public TriggerResult afterChildTimer(
        TriggerContext<W> c, W window, int childIdx, TriggerResult result) throws Exception {
      if (childIdx == 0) {
        // If the first trigger finishes, we need to reset it
        if (result.isFinish()) {
          subExecutor(c, window).reset(c, 0, window);
        }
        return result.isFire() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
      } else {
        return result.isFire() ? TriggerResult.FINISH : TriggerResult.CONTINUE;
      }
    }

    @Override
    public boolean willNeverFinish() {
      return false;
    }
  }
}
