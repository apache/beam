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

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.coders.InstantCoder;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;

import org.joda.time.Instant;

/**
 * A trigger that fires after a given amount of delay from the first element arriving.
 *
 * <p>TODO: Generalize this as appropriate, and add support to hook it up.
 *
 * @param <W> The type of windows being triggered/encoded.
 */
public class DelayAfterFirstInPane<W extends BoundedWindow> implements Trigger<Object, W> {

  private static final Instant ALREADY_FIRED = BoundedWindow.TIMESTAMP_MAX_VALUE;

  private SerializableFunction<Instant, Instant> delayFunction;
  private CodedTupleTag<Instant> delayedUntilTag =
      CodedTupleTag.of("delayed-until", InstantCoder.of());

  /**
   * Delay after the first element in the window arrives.
   *
   * @param delayFunction Transformation to apply the current processing time to compute the delay.
   *     It should only move values forward: delayFunction(now) >= now
   *     It should be monotonically increasing: If a < b, then delayFunction(a) <= delayFunction(b)
   */
  public DelayAfterFirstInPane(SerializableFunction<Instant, Instant> delayFunction) {
    this.delayFunction = delayFunction;
  }

  @Override
  public void onElement(TriggerContext<W> c, Object value, W window, WindowStatus status)
      throws Exception {
    Instant delayUntil = c.lookup(delayedUntilTag, window);
    if (delayUntil == null) {
      delayUntil = delayFunction.apply(c.currentProcessingTime());
      c.setTimer(window, delayUntil, TimeDomain.PROCESSING_TIME);
      c.store(delayedUntilTag, window, delayUntil);
    }
  }

  @Override
  public void onMerge(TriggerContext<W> c, Iterable<W> oldWindows, W newWindow) throws Exception {
    // We want to fire after the minimum delayed-until in the window. If that means we've already
    // fired, we should stop.
    Instant delayedUntil = null;
    for (Instant oldDelayedUntil : c.lookup(delayedUntilTag, oldWindows)) {
      if (oldDelayedUntil != null) {
        delayedUntil = (delayedUntil != null && delayedUntil.isBefore(oldDelayedUntil))
            ? delayedUntil : oldDelayedUntil;
      }
    }

    // Delete the old timers.
    for (W oldWindow : oldWindows) {
      c.deleteTimer(oldWindow, TimeDomain.PROCESSING_TIME);
      c.remove(delayedUntilTag, oldWindow);
    }

    // Now, (re)set the timer if we need to:
    if (delayedUntil != null && delayedUntil.isBefore(ALREADY_FIRED)) {
      c.setTimer(newWindow, delayedUntil, TimeDomain.PROCESSING_TIME);
    }
    c.store(delayedUntilTag, newWindow, delayedUntil);
  }

  @Override
  public void onTimer(TriggerContext<W> c, W window) throws Exception {
    c.store(delayedUntilTag, window, ALREADY_FIRED);
    c.emitWindow(window);
  }
}
