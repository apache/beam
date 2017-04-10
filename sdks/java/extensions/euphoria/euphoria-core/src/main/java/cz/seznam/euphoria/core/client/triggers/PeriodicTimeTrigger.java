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

import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;

/**
 * A {@link Trigger} that is periodically fired based on given time interval.
 * Used to implement "early triggering" functionality.
 */
public class PeriodicTimeTrigger implements Trigger<TimeInterval> {

  /** Next fire stamp (when merging the lowest timestamp is taken) */
  private static final ValueStorageDescriptor<Long> FIRE_TIME_DESCR =
          ValueStorageDescriptor.of("fire-time", Long.class, Long.MAX_VALUE, Math::min);

  private final long interval;

  public PeriodicTimeTrigger(long interval) {
    this.interval = interval;
  }

  @Override
  public TriggerResult onElement(long time, TimeInterval window, TriggerContext ctx) {
    ValueStorage<Long> fireStamp = ctx.getValueStorage(FIRE_TIME_DESCR);

    if (fireStamp.get() == Long.MAX_VALUE) {
      // register first timer aligned with window start
      long start = window.getStartMillis() - (window.getStartMillis() % interval);
      long nextFireTimestamp = start + interval;

      ctx.registerTimer(nextFireTimestamp, window);
      fireStamp.set(nextFireTimestamp);
    }

    return TriggerResult.NOOP;
  }

  @Override
  public TriggerResult onTimer(long time, TimeInterval window, TriggerContext ctx) {
    ValueStorage<Long> fireStamp = ctx.getValueStorage(FIRE_TIME_DESCR);

    if (fireStamp.get() == time) {
      long nextTimestamp = time + interval;
      if (nextTimestamp < window.getEndMillis()) {
        ctx.registerTimer(time + interval, window);
        fireStamp.set(time + interval);
      }

      return TriggerResult.FLUSH;
    }

    return TriggerResult.NOOP;
  }

  @Override
  public void onClear(TimeInterval window, TriggerContext ctx) {
    ValueStorage<Long> fireStamp = ctx.getValueStorage(FIRE_TIME_DESCR);
    ctx.deleteTimer(fireStamp.get(), window);
    fireStamp.clear();
  }

  @Override
  public void onMerge(TimeInterval window, TriggerContext.TriggerMergeContext ctx) {
    ctx.mergeStoredState(FIRE_TIME_DESCR);
    // register timer according to merged state
    ValueStorage<Long> fireStamp = ctx.getValueStorage(FIRE_TIME_DESCR);
    ctx.registerTimer(fireStamp.get(), window);
  }
}
