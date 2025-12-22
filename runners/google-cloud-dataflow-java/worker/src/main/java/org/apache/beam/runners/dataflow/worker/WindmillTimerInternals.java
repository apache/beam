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
package org.apache.beam.runners.dataflow.worker;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.Timer;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillTagEncoding;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Windmill {@link TimerInternals}.
 *
 * <p>Includes parsing / assembly of timer tags and some extra methods.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class WindmillTimerInternals implements TimerInternals {

  // Map from timer id to its TimerData. If it is to be deleted, we still need
  // its time domain here. Note that TimerData is unique per ID and namespace,
  // though technically in Windmill this is only enforced per ID and namespace
  // and TimeDomain. This TimerInternals is scoped to a step and key, shared
  // across namespaces.
  private final Map<
          Entry<String /*ID*/, StateNamespace>, Entry<TimerData, Boolean /*timer set/unset*/>>
      timerMap = new HashMap<>();

  private final Watermarks watermarks;
  private final Instant processingTime;
  private final String stateFamily;
  private final WindmillNamespacePrefix prefix;
  private final Consumer<TimerData> onTimerModified;
  private final WindmillTagEncoding windmillTagEncoding;

  public WindmillTimerInternals(
      String stateFamily, // unique identifies a step
      WindmillNamespacePrefix prefix, // partitions user and system namespaces into "/u" and "/s"
      Instant processingTime,
      Watermarks watermarks,
      WindmillTagEncoding windmillTagEncoding,
      Consumer<TimerData> onTimerModified) {
    this.watermarks = watermarks;
    this.processingTime = checkNotNull(processingTime);
    this.stateFamily = stateFamily;
    this.prefix = prefix;
    this.windmillTagEncoding = windmillTagEncoding;
    this.onTimerModified = onTimerModified;
  }

  public WindmillTimerInternals withPrefix(WindmillNamespacePrefix prefix) {
    return new WindmillTimerInternals(
        stateFamily, prefix, processingTime, watermarks, windmillTagEncoding, onTimerModified);
  }

  @Override
  public void setTimer(TimerData timerKey) {
    String timerDataKey = getTimerDataKey(timerKey.getTimerId(), timerKey.getTimerFamilyId());
    timerMap.put(
        new SimpleEntry<>(timerDataKey, timerKey.getNamespace()),
        new SimpleEntry<>(timerKey, true));
    onTimerModified.accept(timerKey);
  }

  @Override
  public void setTimer(
      StateNamespace namespace,
      String timerId,
      String timerFamilyId,
      Instant timestamp,
      Instant outputTimestamp,
      TimeDomain timeDomain) {
    TimerData timer =
        TimerData.of(timerId, timerFamilyId, namespace, timestamp, outputTimestamp, timeDomain);
    setTimer(timer);
  }

  public static String getTimerDataKey(TimerData timerData) {
    return getTimerDataKey(timerData.getTimerId(), timerData.getTimerFamilyId());
  }

  private static String getTimerDataKey(String timerId, String timerFamilyId) {
    // Identifies timer uniquely with timerFamilyId
    return timerId + '+' + timerFamilyId;
  }

  @Override
  public void deleteTimer(TimerData timerKey) {
    String timerDataKey = getTimerDataKey(timerKey.getTimerId(), timerKey.getTimerFamilyId());
    timerMap.put(
        new SimpleEntry<>(timerDataKey, timerKey.getNamespace()),
        new SimpleEntry<>(timerKey, false));
    onTimerModified.accept(timerKey.deleted());
  }

  @Override
  public void deleteTimer(StateNamespace namespace, String timerId, String timerFamilyId) {
    throw new UnsupportedOperationException("Canceling a timer by ID is not yet supported.");
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

  @Override
  public Instant currentProcessingTime() {
    Instant now = Instant.now();
    return processingTime.isAfter(now) ? processingTime : now;
  }

  @Override
  public @Nullable Instant currentSynchronizedProcessingTime() {
    return watermarks.synchronizedProcessingTime();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Note that this value may be arbitrarily behind the global input watermark. Windmill simply
   * reports the last known input watermark value at the time the GetWork response was constructed.
   * However, if an element in a GetWork request has a timestamp at or ahead of the local input
   * watermark then Windmill will not allow the local input watermark to advance until that element
   * has been committed.
   */
  @Override
  public Instant currentInputWatermarkTime() {
    return watermarks.inputDataWatermark();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Note that Windmill will provisionally hold the output watermark to the timestamp of the
   * earliest element in a computation's GetWork response. (Elements with timestamps already behind
   * the output watermark at the point the GetWork response is constructed will have no influence on
   * the output watermark). The provisional hold will last until this work item is committed. It is
   * the responsibility of the harness to impose any persistent holds it needs.
   */
  @Override
  public @Nullable Instant currentOutputWatermarkTime() {
    return watermarks.outputDataWatermark();
  }

  public void persistTo(Windmill.WorkItemCommitRequest.Builder outputBuilder) {
    for (Entry<TimerData, Boolean> value : timerMap.values()) {
      // Regardless of whether it is set or not, it must have some TimerData stored so we
      // can know its time domain
      TimerData timerData = value.getKey();

      Timer.Builder timer =
          windmillTagEncoding.buildWindmillTimerFromTimerData(
              stateFamily, prefix, timerData, outputBuilder.addOutputTimersBuilder());

      if (value.getValue()) {
        // Setting the timer. If it is a user timer, set a hold.

        // Only set a hold if it's needed and if the hold is before the end of the global window.
        if (needsWatermarkHold(timerData)) {
          if (timerData
              .getOutputTimestamp()
              .isBefore(GlobalWindow.INSTANCE.maxTimestamp().plus(Duration.millis(1)))) {
            // Setting a timer, clear any prior hold and set to the new value
            outputBuilder
                .addWatermarkHoldsBuilder()
                .setTag(windmillTagEncoding.timerHoldTag(prefix, timerData, timer.getTag()))
                .setStateFamily(stateFamily)
                .setReset(true)
                .addTimestamps(
                    WindmillTimeUtils.harnessToWindmillTimestamp(timerData.getOutputTimestamp()));
          } else {
            // Clear the hold in case a previous iteration of this timer set one.
            outputBuilder
                .addWatermarkHoldsBuilder()
                .setTag(windmillTagEncoding.timerHoldTag(prefix, timerData, timer.getTag()))
                .setStateFamily(stateFamily)
                .setReset(true);
          }
        }
      } else {
        // Deleting a timer. If it is a user timer, clear the hold
        timer.clearTimestamp();
        timer.clearMetadataTimestamp();
        // Clear the hold even if it's the end of the global window in order to maintain update
        // compatibility.
        if (needsWatermarkHold(timerData)) {
          // We are deleting timer; clear the hold
          outputBuilder
              .addWatermarkHoldsBuilder()
              .setTag(windmillTagEncoding.timerHoldTag(prefix, timerData, timer.getTag()))
              .setStateFamily(stateFamily)
              .setReset(true);
        }
      }
    }

    // Wipe the unpersisted state
    timerMap.clear();
  }

  private boolean needsWatermarkHold(TimerData timerData) {
    // If it is a user timer or a system timer with outputTimestamp different than timestamp
    return WindmillNamespacePrefix.USER_NAMESPACE_PREFIX.equals(prefix)
        || !timerData.getTimestamp().isEqual(timerData.getOutputTimestamp());
  }

  public static boolean isSystemTimer(Windmill.Timer timer) {
    return timer.getTag().startsWith(WindmillNamespacePrefix.SYSTEM_NAMESPACE_PREFIX.byteString());
  }

  public static boolean isUserTimer(Windmill.Timer timer) {
    return timer.getTag().startsWith(WindmillNamespacePrefix.USER_NAMESPACE_PREFIX.byteString());
  }
}
