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

import java.util.Map;
import java.util.NavigableSet;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.direct.WatermarkManager.TimerUpdate;
import org.apache.beam.runners.direct.WatermarkManager.TimerUpdate.TimerUpdateBuilder;
import org.apache.beam.runners.direct.WatermarkManager.TransformWatermarks;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** An implementation of {@link TimerInternals} where all relevant data exists in memory. */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
class DirectTimerInternals implements TimerInternals {
  private final Clock processingTimeClock;
  private final TransformWatermarks watermarks;
  private final TimerUpdateBuilder timerUpdateBuilder;
  private final Map<TimeDomain, NavigableSet<TimerData>> modifiedTimers;
  private final Map<String, TimerData> modifiedTimerIds;

  public static DirectTimerInternals create(
      Clock clock, TransformWatermarks watermarks, TimerUpdateBuilder timerUpdateBuilder) {
    return new DirectTimerInternals(clock, watermarks, timerUpdateBuilder);
  }

  private DirectTimerInternals(
      Clock clock, TransformWatermarks watermarks, TimerUpdateBuilder timerUpdateBuilder) {
    this.processingTimeClock = clock;
    this.watermarks = watermarks;
    this.timerUpdateBuilder = timerUpdateBuilder;
    this.modifiedTimers = Maps.newHashMap();
    this.modifiedTimers.put(TimeDomain.EVENT_TIME, Sets.newTreeSet());
    this.modifiedTimers.put(TimeDomain.PROCESSING_TIME, Sets.newTreeSet());
    this.modifiedTimers.put(TimeDomain.SYNCHRONIZED_PROCESSING_TIME, Sets.newTreeSet());
    this.modifiedTimerIds = Maps.newHashMap();
  }

  @Override
  public void setTimer(
      StateNamespace namespace,
      String timerId,
      String timerFamilyId,
      Instant target,
      Instant outputTimestamp,
      TimeDomain timeDomain) {
    setTimer(TimerData.of(timerId, timerFamilyId, namespace, target, outputTimestamp, timeDomain));
  }

  /**
   * @deprecated use {@link #setTimer(StateNamespace, String, String, Instant, Instant,
   *     TimeDomain)}.
   */
  @Deprecated
  @Override
  public void setTimer(TimerData timerData) {
    timerUpdateBuilder.setTimer(timerData);
    getModifiedTimersOrdered(timerData.getDomain()).add(timerData);
    modifiedTimerIds.put(timerData.stringKey(), timerData);
  }

  @Override
  public void deleteTimer(
      StateNamespace namespace, String timerId, String timerFamilyId, TimeDomain timeDomain) {
    deleteTimer(
        TimerData.of(
            timerId,
            timerFamilyId,
            namespace,
            BoundedWindow.TIMESTAMP_MIN_VALUE,
            BoundedWindow.TIMESTAMP_MAX_VALUE,
            timeDomain));
  }

  /** @deprecated use {@link #deleteTimer(StateNamespace, String, TimeDomain)}. */
  @Deprecated
  @Override
  public void deleteTimer(StateNamespace namespace, String timerId, String timerFamilyId) {
    throw new UnsupportedOperationException("Canceling of timer by ID is not yet supported.");
  }

  /** @deprecated use {@link #deleteTimer(StateNamespace, String, TimeDomain)}. */
  @Deprecated
  @Override
  public void deleteTimer(TimerData timerData) {
    timerUpdateBuilder.deletedTimer(timerData);
    modifiedTimerIds.put(timerData.stringKey(), timerData.deleted());
  }

  public TimerUpdate getTimerUpdate() {
    return timerUpdateBuilder.build();
  }

  public NavigableSet<TimerData> getModifiedTimersOrdered(TimeDomain timeDomain) {
    NavigableSet<TimerData> modified = modifiedTimers.get(timeDomain);
    if (modified == null) {
      throw new IllegalStateException("Unexpected time domain " + timeDomain);
    }
    return modified;
  }

  public Map<String, TimerData> getModifiedTimerIds() {
    return modifiedTimerIds;
  }

  @Override
  public Instant currentProcessingTime() {
    return processingTimeClock.now();
  }

  @Override
  public @Nullable Instant currentSynchronizedProcessingTime() {
    return watermarks.getSynchronizedProcessingInputTime();
  }

  @Override
  public Instant currentInputWatermarkTime() {
    return watermarks.getInputWatermark();
  }

  @Override
  public @Nullable Instant currentOutputWatermarkTime() {
    return watermarks.getOutputWatermark();
  }
}
