/**
 * Copyright 2016 Seznam.cz, a.s.
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

import cz.seznam.euphoria.core.client.dataset.windowing.Batch;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;

import java.util.Collections;

public class AttachedWindowing<T, WID extends Window> implements Windowing<T, WID> {

  @Override
  @SuppressWarnings("unchecked")
  public Iterable<WID> assignWindowsToElement(WindowedElement<?, T> el) {
    return Collections.singleton((WID) el.getWindow());
  }

  @Override
  public Trigger<WID> getTrigger() {
    return new AttachedWindowTrigger<>();
  }

  private static class AttachedWindowTrigger<WID extends Window> implements Trigger<WID> {

    @Override
    public TriggerResult onElement(long time, WID window, TriggerContext ctx) {
      // FIXME batch window shouldn't be used in stream flow in the future
      // issue #38 on GitHub
      if (window instanceof Batch.BatchWindow) return TriggerResult.NOOP;

      ctx.registerTimer(time, window);
      return TriggerResult.NOOP;
    }

    @Override
    public TriggerResult onTimer(long time, WID window, TriggerContext ctx) {
      return TriggerResult.FLUSH_AND_PURGE;
    }

    @Override
    public void onClear(WID window, TriggerContext ctx) {

    }

    @Override
    public void onMerge(WID window, TriggerContext.TriggerMergeContext ctx) {
      throw new UnsupportedOperationException("Merging of attached windows not allowed");
    }
  }
}
