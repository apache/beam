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
import cz.seznam.euphoria.core.client.dataset.windowing.Window;

import java.util.List;

/**
 * Composite {@link Trigger} that fires once after at least one of sub-triggers
 * have fired. In other words sub-triggers are composed using logical OR.
 */
@Audience(Audience.Type.CLIENT)
public class AfterFirstCompositeTrigger<W extends Window> implements Trigger<W> {

  private final List<Trigger<W>> subtriggers;

  public AfterFirstCompositeTrigger(List<Trigger<W>> triggers) {
    this.subtriggers = triggers;
  }

  @Override
  public boolean isStateful() {
    for (Trigger<W> t : subtriggers) {
      if (t.isStateful()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public TriggerResult onElement(long time, W window, TriggerContext ctx) {
    TriggerResult r = TriggerResult.NOOP;

    // propagate event to all triggers
    for (Trigger<W> t : subtriggers) {
      r = TriggerResult.merge(r,
              t.onElement(time, window, ctx));
    }

    return r;
  }

  @Override
  public TriggerResult onTimer(long time, W window, TriggerContext ctx) {
    TriggerResult r = TriggerResult.NOOP;

    // propagate event to all triggers
    for (Trigger<W> t : subtriggers) {
      r = TriggerResult.merge(r,
              t.onTimer(time, window, ctx));
    }

    return r;
  }

  @Override
  public void onClear(W window, TriggerContext ctx) {
    for (Trigger<W> t : subtriggers) {
      t.onClear(window, ctx);
    }
  }

  @Override
  public void onMerge(W window, TriggerContext.TriggerMergeContext ctx) {
    for (Trigger<W> t : subtriggers) {
      t.onMerge(window, ctx);
    }
  }
}
