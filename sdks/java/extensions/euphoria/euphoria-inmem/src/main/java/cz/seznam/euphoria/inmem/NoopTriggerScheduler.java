/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.inmem;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;

public class NoopTriggerScheduler<W extends Window, K>
    implements TriggerScheduler<W, K> {

  private volatile long currentWatermark;

  @Override
  public boolean scheduleAt(long stamp, KeyedWindow<W, K> window, Triggerable<W, K> trigger) {
    if (currentWatermark < stamp) {
      currentWatermark = stamp;
    }
    return true;
  }

  @Override
  public long getCurrentTimestamp() {
    return currentWatermark;
  }

  @Override
  public void cancelAll() {

  }

  @Override
  public void cancel(long stamp, KeyedWindow<W, K> window) {

  }

  @Override
  public void close() {

  }

}
