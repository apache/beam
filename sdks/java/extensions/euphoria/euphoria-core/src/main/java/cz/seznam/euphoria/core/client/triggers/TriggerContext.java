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
import cz.seznam.euphoria.core.client.operator.state.StorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;

/** A context is given to {@link Trigger} methods to allow them to register timer callbacks. */
@Audience(Audience.Type.CLIENT)
public interface TriggerContext extends StorageProvider {

  /**
   * Fire specific trigger on given time. Schedule the given trigger at the given stamp. The trigger
   * will be fired as close to the time as possible.
   *
   * @param stamp the timestamp to register a timer for
   * @param window the window to register the timer for
   * @return {@code true} when trigger was successfully scheduled
   */
  boolean registerTimer(long stamp, Window window);

  /**
   * Delete previously registered timer
   *
   * @param stamp the stamp of a previously registered timer
   * @param window the window of the previously registered timer
   * @see #registerTimer(long, Window)
   */
  void deleteTimer(long stamp, Window window);

  /** @return the current timestamp from runtime (may be different from real clock time). */
  long getCurrentTimestamp();

  /**
   * Extension of {@link TriggerContext} that is given to {@link Trigger#onMerge} as an argument.
   */
  interface TriggerMergeContext extends TriggerContext {
    void mergeStoredState(StorageDescriptor storageDescriptor);
  }
}
