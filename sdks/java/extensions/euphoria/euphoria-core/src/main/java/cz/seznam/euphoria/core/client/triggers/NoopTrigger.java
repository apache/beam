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

/**
 * A trigger implementation which actually never fires any of the observed windows.
 */
@Audience(Audience.Type.CLIENT)
public final class NoopTrigger<W extends Window> implements Trigger<W> {

  private static final NoopTrigger INSTANCE = new NoopTrigger();

  @SuppressWarnings("unchecked")
  public static <W extends Window> NoopTrigger<W> get() {
    return (NoopTrigger) INSTANCE;
  }

  private NoopTrigger() {}

  @Override
  public boolean isStateful() {
    return false;
  }

  @Override
  public TriggerResult onElement(long time, W window, TriggerContext ctx) {
    return TriggerResult.NOOP;
  }

  @Override
  public TriggerResult onTimer(long time, W window, TriggerContext ctx) {
    return TriggerResult.NOOP;
  }

  @Override
  public void onClear(W window, TriggerContext ctx) {

  }

  @Override
  public void onMerge(W window, TriggerContext.TriggerMergeContext ctx) {

  }
}
