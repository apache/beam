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
package cz.seznam.euphoria.inmem;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;

/**
 * Schedules and fires registered triggers according to internal time.
 */
public interface TriggerScheduler<W extends Window, K> {

  /**
   * Fire specific trigger on given time.
   * Schedule the given trigger at the given stamp.
   * The trigger will be fired as close to the time as possible.
   *
   * @return true if the triggerable has been scheduled,
   *          false if the time already passed
   */
  boolean scheduleAt(long stamp, KeyedWindow<W, K> window, Triggerable<W, K> trigger);

  /**
   * Retrieve current timestamp this triggering is on.
   * This can be either a real system timestamp or the last
   * timestamp updated by call to `updateStamp'.
   */
  long getCurrentTimestamp();

  /**
   * Cancel all scheduled tasks
   */
  void cancelAll();

  /**
   * Cancel previously registered timer
   */
  void cancel(long stamp, KeyedWindow<W, K> window);

  /**
   * Update the internal timestamp (optional operation).
   */
  default void updateStamp(long stamp) {
    // nop
  }

  /**
   * Close all triggers and destroy the triggering.
   */
  void close();

}
