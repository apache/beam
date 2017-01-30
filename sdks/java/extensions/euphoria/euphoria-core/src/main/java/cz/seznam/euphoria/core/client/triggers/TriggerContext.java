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
import cz.seznam.euphoria.core.client.operator.state.StorageDescriptorBase;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;

/**
 * A context is given to {@link Trigger} methods to allow them to register
 * timer callbacks.
 */
public interface TriggerContext extends StorageProvider {

  /**
   * Fire specific trigger on given time.
   * Schedule the given trigger at the given stamp.
   * The trigger will be fired as close to the time as possible.
   * @return {@code true} when trigger was successfully scheduled
   */
  boolean registerTimer(long stamp, Window window);

  /**
   * Delete previously registered timer
   */
  void deleteTimer(long stamp, Window window);

  /**
   * Return current timestamp from runtime (may be different from real
   * clock time).
   */
  long getCurrentTimestamp();

  /**
   * Extension of {@link TriggerContext} that is given to
   * {@link Trigger#onMerge} as an argument.
   */
  interface TriggerMergeContext extends TriggerContext {
    void mergeStoredState(StorageDescriptorBase storageDescriptor);
  }
}
