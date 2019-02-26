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
package org.apache.beam.runners.direct.portable;

import org.apache.beam.runners.direct.Clock;
import org.apache.beam.runners.direct.WatermarkManager.TimerUpdate;
import org.apache.beam.runners.direct.WatermarkManager.TransformWatermarks;
import org.apache.beam.runners.local.StructuralKey;

/**
 * State and Timer access for the {@link ReferenceRunner}.
 *
 * <p>This provides per-key, per-stage access to {@link CopyOnAccessInMemoryStateInternals} and
 * {@link DirectTimerInternals} for transforms that require access to state or timers.
 *
 * <p>This implementation is not thread safe. A new {@link DirectStateAndTimers} must be created for
 * each thread that requires it.
 */
class DirectStateAndTimers<K> implements StepStateAndTimers<K> {
  private final StructuralKey<K> key;
  private final CopyOnAccessInMemoryStateInternals existingState;

  private final Clock clock;
  private final TransformWatermarks watermarks;

  private CopyOnAccessInMemoryStateInternals<K> stateInternals;
  private DirectTimerInternals timerInternals;

  DirectStateAndTimers(
      StructuralKey<K> key,
      CopyOnAccessInMemoryStateInternals existingState,
      Clock clock,
      TransformWatermarks watermarks) {
    this.key = key;
    this.existingState = existingState;
    this.clock = clock;
    this.watermarks = watermarks;
  }

  @Override
  public CopyOnAccessInMemoryStateInternals<K> stateInternals() {
    if (stateInternals == null) {
      stateInternals = CopyOnAccessInMemoryStateInternals.withUnderlying(key, existingState);
    }
    return stateInternals;
  }

  @Override
  public DirectTimerInternals timerInternals() {
    if (timerInternals == null) {
      timerInternals = DirectTimerInternals.create(clock, watermarks, TimerUpdate.builder(key));
    }
    return timerInternals;
  }
}
