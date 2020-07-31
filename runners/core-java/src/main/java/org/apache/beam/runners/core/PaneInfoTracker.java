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
package org.apache.beam.runners.core;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.PaneInfoCoder;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing;
import org.apache.beam.sdk.util.WindowTracing;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Instant;

/**
 * Determine the timing and other properties of a new pane for a given computation, key and window.
 * Incorporates any previous pane, whether the pane has been produced because an on-time {@link
 * AfterWatermark} trigger firing, and the relation between the element's timestamp and the current
 * output watermark.
 */
public class PaneInfoTracker {
  private TimerInternals timerInternals;

  public PaneInfoTracker(TimerInternals timerInternals) {
    this.timerInternals = timerInternals;
  }

  @VisibleForTesting
  static final StateTag<ValueState<PaneInfo>> PANE_INFO_TAG =
      StateTags.makeSystemTagInternal(StateTags.value("pane", PaneInfoCoder.INSTANCE));

  public void clear(StateAccessor<?> state) {
    state.access(PANE_INFO_TAG).clear();
  }

  public void prefetchPaneInfo(ReduceFn<?, ?, ?, ?>.Context context) {
    context.state().access(PaneInfoTracker.PANE_INFO_TAG).readLater();
  }

  /**
   * Return a ({@link ReadableState} for) the pane info appropriate for {@code context}. The pane
   * info includes the timing for the pane, who's calculation is quite subtle.
   *
   * @param isFinal should be {@code true} only if the triggering machinery can guarantee no further
   *     firings for the
   */
  public ReadableState<PaneInfo> getNextPaneInfo(
      ReduceFn<?, ?, ?, ?>.Context context, final boolean isFinal) {
    final Object key = context.key();
    final ReadableState<PaneInfo> previousPaneFuture =
        context.state().access(PaneInfoTracker.PANE_INFO_TAG);
    final Instant windowMaxTimestamp = context.window().maxTimestamp();

    return new ReadableState<PaneInfo>() {
      @Override
      @SuppressFBWarnings(
          "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT") // just prefetch calls to readLater
      public ReadableState<PaneInfo> readLater() {
        previousPaneFuture.readLater();
        return this;
      }

      @Override
      public PaneInfo read() {
        PaneInfo previousPane = previousPaneFuture.read();
        return describePane(key, windowMaxTimestamp, previousPane, isFinal);
      }
    };
  }

  public void storeCurrentPaneInfo(ReduceFn<?, ?, ?, ?>.Context context, PaneInfo currentPane) {
    context.state().access(PANE_INFO_TAG).write(currentPane);
  }

  private <W> PaneInfo describePane(
      Object key, Instant windowMaxTimestamp, PaneInfo previousPane, boolean isFinal) {
    boolean isFirst = previousPane == null;
    Timing previousTiming = isFirst ? null : previousPane.getTiming();
    long index = isFirst ? 0 : previousPane.getIndex() + 1;
    long nonSpeculativeIndex = isFirst ? 0 : previousPane.getNonSpeculativeIndex() + 1;
    Instant outputWM = timerInternals.currentOutputWatermarkTime();
    Instant inputWM = timerInternals.currentInputWatermarkTime();

    // True if it is not possible to assign the element representing this pane a timestamp
    // which will make an ON_TIME pane for any following computation.
    // Ie true if the element's latest possible timestamp is before the current output watermark.
    boolean isLateForOutput = outputWM != null && windowMaxTimestamp.isBefore(outputWM);

    // True if all emitted panes (if any) were EARLY panes.
    // Once the ON_TIME pane has fired, all following panes must be considered LATE even
    // if the output watermark is behind the end of the window.
    boolean onlyEarlyPanesSoFar = previousTiming == null || previousTiming == Timing.EARLY;

    // True is the input watermark hasn't passed the window's max timestamp.
    boolean isEarlyForInput = !inputWM.isAfter(windowMaxTimestamp);

    Timing timing;
    if (isLateForOutput || !onlyEarlyPanesSoFar) {
      // The output watermark has already passed the end of this window, or we have already
      // emitted a non-EARLY pane. Irrespective of how this pane was triggered we must
      // consider this pane LATE.
      timing = Timing.LATE;
    } else if (isEarlyForInput) {
      // This is an EARLY firing.
      timing = Timing.EARLY;
      nonSpeculativeIndex = -1;
    } else {
      // This is the unique ON_TIME firing for the window.
      timing = Timing.ON_TIME;
    }

    WindowTracing.debug(
        "describePane: {} pane (prev was {}) for key:{}; windowMaxTimestamp:{}; "
            + "inputWatermark:{}; outputWatermark:{}; isLateForOutput:{}",
        timing,
        previousTiming,
        key,
        windowMaxTimestamp,
        inputWM,
        outputWM,
        isLateForOutput);

    if (previousPane != null) {
      // Timing transitions should follow EARLY* ON_TIME? LATE*
      switch (previousTiming) {
        case EARLY:
          checkState(
              timing == Timing.EARLY || timing == Timing.ON_TIME || timing == Timing.LATE,
              "EARLY cannot transition to %s",
              timing);
          break;
        case ON_TIME:
          checkState(timing == Timing.LATE, "ON_TIME cannot transition to %s", timing);
          break;
        case LATE:
          checkState(timing == Timing.LATE, "LATE cannot transtion to %s", timing);
          break;
        case UNKNOWN:
          break;
      }
      checkState(!previousPane.isLast(), "Last pane was not last after all.");
    }

    return PaneInfo.createPane(isFirst, isFinal, timing, index, nonSpeculativeIndex);
  }
}
