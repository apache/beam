/**
 * Copyright 2016 Seznam a.s.
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
package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;

public class AttachedWindowTrigger<WID extends Window, T>
    extends Trigger<T, AttachedWindow<WID>>
{
  @Override
  public TriggerResult onElement(T element,
                                 long timestamp,
                                 AttachedWindow<WID> window,
                                 TriggerContext ctx)
      throws Exception
  {
    ctx.registerEventTimeTimer(window.getEmissionWatermark());
    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onProcessingTime(long time, AttachedWindow<WID> window, TriggerContext ctx)
      throws Exception
  {
    throw new UnsupportedOperationException("processing time not supported!");
  }

  @Override
  public TriggerResult onEventTime(long time, AttachedWindow<WID> window, TriggerContext ctx)
      throws Exception
  {
    if (window.getEmissionWatermark() == time) {
      return TriggerResult.FIRE_AND_PURGE;
    } else {
      // attached windows are registered _only_ for the maxTimestamp()
      throw new IllegalStateException("Invalid timer for attached window");
    }
  }

  @Override
  public void clear(AttachedWindow<WID> window, TriggerContext ctx)
      throws Exception
  {
    // ~ attached-windows are purged only when their trigger fires in which case
    // the trigger itself get's clear; however, attached windows have
    // maxTimestamp == Long.MAX_VALUE and we need to clean-up the registered
    // clean-up trigger to avoid mem-leak in long running streams
    ctx.deleteEventTimeTimer(window.maxTimestamp());
  }
}
