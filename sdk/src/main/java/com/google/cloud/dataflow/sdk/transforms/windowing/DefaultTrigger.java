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
 * A trigger that fires repeatedly when the watermark passes the end of the window.
 *
 * @param <W> The type of windows being triggered/encoded.
 */
public class DefaultTrigger<W extends BoundedWindow> implements Trigger<W>{

  private static final long serialVersionUID = 0L;

  private DefaultTrigger() {}

  /**
   * Returns the default trigger.
   */
  public static <W extends BoundedWindow> DefaultTrigger<W> of() {
    return new DefaultTrigger<W>();
  }

  @Override
  public TriggerResult onElement(
      TriggerContext<W> c, Object value, Instant timestamp, W window,
      WindowStatus status) throws Exception {
    c.setTimer(window, window.maxTimestamp(), TimeDomain.EVENT_TIME);
    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onMerge(
      TriggerContext<W> c, Iterable<W> oldWindows, W newWindow) throws Exception {
    for (W oldWindow : oldWindows) {
      c.deleteTimer(oldWindow, TimeDomain.EVENT_TIME);
    }

    c.setTimer(newWindow, newWindow.maxTimestamp(), TimeDomain.EVENT_TIME);
    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onTimer(TriggerContext<W> c, TriggerId<W> triggerId) throws Exception {
    return TriggerResult.FIRE;
  }

  @Override
  public void clear(TriggerContext<W> c, W window) throws Exception {
    c.deleteTimer(window, TimeDomain.EVENT_TIME);
  }

  @Override
  public boolean willNeverFinish() {
    return true;
  }

  @Override
  public boolean isCompatible(Trigger<?> other) {
    // Semantically, all default triggers are identical
    return other instanceof DefaultTrigger;
  }
}
