/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo.PaneInfoCoder;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo.Timing;
import com.google.cloud.dataflow.sdk.util.ReduceFn.StateContext;
import com.google.cloud.dataflow.sdk.util.state.StateContents;
import com.google.cloud.dataflow.sdk.util.state.StateTag;
import com.google.cloud.dataflow.sdk.util.state.StateTags;
import com.google.cloud.dataflow.sdk.util.state.ValueState;
import com.google.common.annotations.VisibleForTesting;

import org.joda.time.Instant;

/**
 * Encapsulates the logic for tracking the current {@link PaneInfo} and producing new PaneInfo for
 * a trigger firing.
 */
public class PaneInfoTracker {

  private TimerManager timerManager;

  public PaneInfoTracker(TimerManager timerManager) {
    this.timerManager = timerManager;
  }

  @VisibleForTesting static final StateTag<ValueState<PaneInfo>> PANE_INFO_TAG =
      StateTags.value("__pane_info", PaneInfoCoder.INSTANCE);

  public void clear(StateContext state) {
    state.access(PANE_INFO_TAG).clear();
  }

  public StateContents<PaneInfo> getNextPaneInfo(
      ReduceFn<?, ?, ?, ?>.Context context, final boolean isFinal) {
    final StateContents<PaneInfo> previousPaneFuture =
        context.state().access(PaneInfoTracker.PANE_INFO_TAG).get();
    final Instant endOfWindow = context.window().maxTimestamp();
    final StateContext state = context.state();

    return new StateContents<PaneInfo>() {
      private PaneInfo result = null;

      @Override
      public PaneInfo read() {
        if (result == null) {
          PaneInfo previousPane = previousPaneFuture.read();
          result = describePane(endOfWindow, previousPane, isFinal);
          state.access(PANE_INFO_TAG).set(result);
        }
        return result;
      }
    };
  }

  private <W> PaneInfo describePane(Instant endOfWindow, PaneInfo previousPane, boolean isFinal) {
    boolean isSpeculative = endOfWindow.isAfter(timerManager.currentWatermarkTime());
    boolean isFirst = (previousPane == null);

    Timing timing = Timing.EARLY;
    if (!isSpeculative) {
      boolean firstNonSpeculative =
          previousPane == null || previousPane.getTiming() == Timing.EARLY;
      timing = firstNonSpeculative ? Timing.ON_TIME : Timing.LATE;
    }

    return PaneInfo.createPane(isFirst, isFinal, timing);
  }
}
