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
package cz.seznam.euphoria.core.client.triggers;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;

/**
 * A {@link Trigger} that fires once the count of elements reaches given count.
 */
public class CountTrigger<W extends Window> implements Trigger<W> {

  private static final ValueStorageDescriptor<Long> COUNT_DESCR =
          ValueStorageDescriptor.of("count", Long.class, 0L, (x, y) -> x + y );

  private final long maxCount;

  public CountTrigger(long maxCount) {
    this.maxCount = maxCount;
  }

  @Override
  public TriggerResult onElement(long time, W window, TriggerContext ctx) {
    ValueStorage<Long> count = ctx.getValueStorage(COUNT_DESCR);

    count.set(count.get() + 1L);

    if (count.get() >= maxCount) {
      count.clear();
      return TriggerResult.FLUSH_AND_PURGE;
    }
    return TriggerResult.NOOP;
  }

  @Override
  public TriggerResult onTimer(long time, W window, TriggerContext ctx) {
    return TriggerResult.NOOP;
  }

  @Override
  public void onClear(W window, TriggerContext ctx) {
    ctx.getValueStorage(COUNT_DESCR).clear();
  }

  @Override
  public TriggerResult onMerge(W window, TriggerContext.TriggerMergeContext ctx) {
    ctx.mergeStoredState(COUNT_DESCR);
    ValueStorage<Long> count = ctx.getValueStorage(COUNT_DESCR);

    if (count.get() >= maxCount) {
      count.clear();
      return TriggerResult.FLUSH_AND_PURGE;
    }
    return TriggerResult.NOOP;
  }
}
