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
package cz.seznam.euphoria.executor.local;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;

@FunctionalInterface
public interface Triggerable<W extends Window, K> {

  /**
   * This method is invoked with the timestamp for which the trigger was scheduled.
   *
   * <p>If the triggering is delayed for whatever reason (trigger timer was blocked, JVM stalled due
   * to a garbage collection), the timestamp supplied to this function will still be the original
   * timestamp for which the trigger was scheduled.
   *
   * @param timestamp the timestamp for which the trigger event was scheduled.
   * @param window the window this action is invoked for; i.e. the context of the invocation
   */
  void fire(long timestamp, KeyedWindow<W, K> window);
}
