/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.client.triggers;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link Trigger} that fires once the time passes the end of the window. */
@Audience(Audience.Type.CLIENT)
public class TimeTrigger implements Trigger<TimeInterval> {

  private static final Logger LOG = LoggerFactory.getLogger(TimeTrigger.class);

  @Override
  public boolean isStateful() {
    return false;
  }

  @Override
  public TriggerResult onElement(long time, TimeInterval window, TriggerContext ctx) {
    return registerTimer(window, ctx);
  }

  @Override
  public TriggerResult onTimer(long time, TimeInterval window, TriggerContext ctx) {
    if (time == window.maxTimestamp()) {
      LOG.debug("Firing TimeTrigger, time {}, window: {}", time, window);
      return TriggerResult.FLUSH_AND_PURGE;
    }
    return TriggerResult.NOOP;
  }

  @Override
  public void onClear(TimeInterval window, TriggerContext ctx) {
    ctx.deleteTimer(window.maxTimestamp(), window);
  }

  @Override
  public void onMerge(TimeInterval window, TriggerContext.TriggerMergeContext ctx) {
    registerTimer(window, ctx);
  }

  private TriggerResult registerTimer(TimeInterval window, TriggerContext ctx) {
    if (ctx.registerTimer(window.maxTimestamp(), window)) {
      return TriggerResult.NOOP;
    }

    // if the time already passed discard the late coming element
    return TriggerResult.PURGE;
  }
}
