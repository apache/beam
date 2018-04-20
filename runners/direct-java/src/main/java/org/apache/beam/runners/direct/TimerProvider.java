/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.runners.direct;

import com.google.auto.value.AutoValue;
import java.util.Collection;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.local.StructuralKey;

/** A provider of {@link FiredTimers}. */
interface TimerProvider<ExecutableT> {
  /**
   * Extracts and returns all of the timers which have fired but not yet been returned by a call to
   * this method.
   */
  Collection<FiredTimers<ExecutableT>> extractFiredTimers();

  /**
   * A pair of {@link TimerData} and key which can be delivered to the appropriate {@code
   * ExecutableT}. A timer fires at the executable that set it with a specific key when the time
   * domain in which it lives progresses past a specified time, as determined by the {@link
   * WatermarkManager}.
   */
  @AutoValue
  abstract class FiredTimers<ExecutableT> {
    static <ExecutableT> FiredTimers<ExecutableT> create(
        ExecutableT executable, StructuralKey<?> key, Collection<TimerData> timers) {
      return new AutoValue_TimerProvider_FiredTimers(executable, key, timers);
    }

    /** The executable the timers were set at and will be delivered to. */
    public abstract ExecutableT getExecutable();

    /** The key the timers were set for and will be delivered to. */
    public abstract StructuralKey<?> getKey();

    /**
     * Gets all of the timers that have fired within the provided {@link TimeDomain}. If no timers
     * fired within the provided domain, return an empty collection.
     *
     * <p>Timers within a {@link TimeDomain} are guaranteed to be in order of increasing timestamp.
     */
    public abstract Collection<TimerData> getTimers();
  }
}
