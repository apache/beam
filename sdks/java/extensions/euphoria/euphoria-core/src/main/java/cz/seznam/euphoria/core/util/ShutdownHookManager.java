/**
 * Copyright 2016 Seznam a.s.
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
package cz.seznam.euphoria.core.util;

/**
 * The {@link ShutdownHookManager} enables running shutdown hook
 * in a deterministic order, higher priority first.
 * The JVM runs shutdown hooks in a non-deterministic order or in parallel.
 * This class registers a single JVM shutdown hook and run all the shutdown hooks
 * registered to it (to this class) in order based on their priority.
 */
public interface ShutdownHookManager {

  /**
   * Adds a shutdown hook with a priority, the higher the priority the earlier will run.
   * ShutdownHooks with same priority run in a non-deterministic order.
   */
  void  addShutdownHook(Runnable shutdownHook, int priority);

  /**
   * Removes a shutdown hook.
   * @param shutdownHook
   * @return {@code TRUE} if the shutdownHook was registered and removed, {@code FALSE} otherwise.
   */
  boolean  removeShutdownHook(Runnable shutdownHook);
}
