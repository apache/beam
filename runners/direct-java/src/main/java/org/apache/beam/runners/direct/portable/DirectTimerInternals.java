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

import javax.annotation.Nullable;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.direct.portable.WatermarkManager.TimerUpdate;
import org.apache.beam.runners.direct.portable.WatermarkManager.TimerUpdate.TimerUpdateBuilder;
import org.apache.beam.runners.direct.portable.WatermarkManager.TransformWatermarks;
import org.apache.beam.sdk.state.TimeDomain;
import org.joda.time.Instant;

/**
 * An implementation of {@link TimerInternals} where all relevant data exists in memory.
 */
class DirectTimerInternals implements TimerInternals {
  private final Clock processingTimeClock;
  private final TransformWatermarks watermarks;
  private final TimerUpdateBuilder timerUpdateBuilder;

  public static DirectTimerInternals create(
      Clock clock, TransformWatermarks watermarks, TimerUpdateBuilder timerUpdateBuilder) {
    return new DirectTimerInternals(clock, watermarks, timerUpdateBuilder);
  }

  private DirectTimerInternals(
      Clock clock, TransformWatermarks watermarks, TimerUpdateBuilder timerUpdateBuilder) {
    this.processingTimeClock = clock;
    this.watermarks = watermarks;
    this.timerUpdateBuilder = timerUpdateBuilder;
  }

  @Override
  public void setTimer(StateNamespace namespace, String timerId, Instant target,
      TimeDomain timeDomain) {
    timerUpdateBuilder.setTimer(TimerData.of(timerId, namespace, target, timeDomain));
  }

  /**
   * @deprecated use {@link #setTimer(StateNamespace, String, Instant, TimeDomain)}.
   */
  @Deprecated
  @Override
  public void setTimer(TimerData timerData) {
    timerUpdateBuilder.setTimer(timerData);
  }

  @Override
  public void deleteTimer(StateNamespace namespace, String timerId, TimeDomain timeDomain) {
    throw new UnsupportedOperationException("Canceling of timer by ID is not yet supported.");
  }

  /**
   * @deprecated use {@link #deleteTimer(StateNamespace, String, TimeDomain)}.
   */
  @Deprecated
  @Override
  public void deleteTimer(StateNamespace namespace, String timerId) {
    throw new UnsupportedOperationException("Canceling of timer by ID is not yet supported.");
  }

  /**
   * @deprecated use {@link #deleteTimer(StateNamespace, String, TimeDomain)}.
   */
  @Deprecated
  @Override
  public void deleteTimer(TimerData timerKey) {
    timerUpdateBuilder.deletedTimer(timerKey);
  }

  public TimerUpdate getTimerUpdate() {
    return timerUpdateBuilder.build();
  }

  @Override
  public Instant currentProcessingTime() {
    return processingTimeClock.now();
  }

  @Override
  @Nullable
  public Instant currentSynchronizedProcessingTime() {
    return watermarks.getSynchronizedProcessingInputTime();
  }

  @Override
  public Instant currentInputWatermarkTime() {
    return watermarks.getInputWatermark();
  }

  @Override
  @Nullable
  public Instant currentOutputWatermarkTime() {
    return watermarks.getOutputWatermark();
  }
}
