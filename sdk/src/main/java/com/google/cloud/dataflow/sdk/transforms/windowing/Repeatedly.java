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
 *            {@code Trigger}
 */
public class Repeatedly<W extends BoundedWindow> extends Trigger<W> {

  private static final long serialVersionUID = 0L;

  private Trigger<W> repeated;

  /**
   * Create a composite trigger that repeatedly executes the trigger {@code toRepeat}, firing each
   * time it fires and ignoring any indications to finish.
   *
   * <p>Unless used with {@link Trigger#orFinally} the composite trigger will never finish.
   *
   * @param repeated the trigger to execute repeatedly.
   */
  public static <W extends BoundedWindow> Repeatedly<W> forever(Trigger<W> repeated) {
    return new Repeatedly<W>(repeated);
  }

  private Repeatedly(Trigger<W> repeated) {
    this.repeated = repeated;
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
}
