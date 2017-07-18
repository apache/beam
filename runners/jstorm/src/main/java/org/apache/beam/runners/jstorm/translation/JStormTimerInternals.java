/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.jstorm.translation;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nullable;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.state.TimeDomain;
import org.joda.time.Instant;

/**
 * JStorm implementation of {@link TimerInternals}.
 */
class JStormTimerInternals<K> implements TimerInternals {

  private final K key;
  private final DoFnExecutor<?, ?> doFnExecutor;
  private final TimerService timerService;

  public JStormTimerInternals(
      @Nullable K key, DoFnExecutor<?, ?> doFnExecutor, TimerService timerService) {
    this.key = key;
    this.doFnExecutor = checkNotNull(doFnExecutor, "doFnExecutor");
    this.timerService = checkNotNull(timerService, "timerService");
  }

  @Override
  public void setTimer(
      StateNamespace namespace, String timerId, Instant target, TimeDomain timeDomain) {
    setTimer(TimerData.of(timerId, namespace, target, timeDomain));
  }

  @Override
  @Deprecated
  public void setTimer(TimerData timerData) {
    timerService.setTimer(key, timerData, doFnExecutor);
  }

  @Override
  public void deleteTimer(StateNamespace namespace, String timerId, TimeDomain timeDomain) {
    throw new UnsupportedOperationException(
        "Canceling of a timer is not yet supported.");
  }

  @Override
  @Deprecated
  public void deleteTimer(StateNamespace namespace, String timerId) {
    throw new UnsupportedOperationException(
        "Canceling of a timer is not yet supported.");
  }

  @Override
  @Deprecated
  public void deleteTimer(TimerData timerData) {
    timerService.deleteTimer(timerData);
  }

  @Override
  public Instant currentProcessingTime() {
    return Instant.now();
  }

  @Override
  @Nullable
  public Instant currentSynchronizedProcessingTime() {
    return null;
  }

  @Override
  public Instant currentInputWatermarkTime() {
    return new Instant(timerService.currentInputWatermark());
  }

  @Override
  @Nullable
  public Instant currentOutputWatermarkTime() {
    return new Instant(timerService.currentOutputWatermark());
  }
}
