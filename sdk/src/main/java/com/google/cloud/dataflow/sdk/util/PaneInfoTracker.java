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

  private TimerInternals timerInternals;

  public PaneInfoTracker(TimerInternals timerInternals) {
    this.timerInternals = timerInternals;
  }

  @VisibleForTesting static final StateTag<ValueState<PaneInfo>> PANE_INFO_TAG =
      StateTags.makeSystemTagInternal(StateTags.value("pane", PaneInfoCoder.INSTANCE));

  public void clear(StateContext state) {
    state.access(PANE_INFO_TAG).clear();
  }


  public StateContents<PaneInfo> getNextPaneInfo(ReduceFn<?, ?, ?, ?>.Context context,
      final boolean isForWatermarkTrigger, final boolean isFinal) {
    final StateContents<PaneInfo> previousPaneFuture =
        context.state().access(PaneInfoTracker.PANE_INFO_TAG).get();
    final Instant endOfWindow = context.window().maxTimestamp();

    return new StateContents<PaneInfo>() {
      @Override
      public PaneInfo read() {
        PaneInfo previousPane = previousPaneFuture.read();
        return describePane(endOfWindow, previousPane, isForWatermarkTrigger, isFinal);
      }
    };
  }

  public void storeCurrentPaneInfo(ReduceFn<?, ?, ?, ?>.Context context, PaneInfo currentPane) {
    context.state().access(PANE_INFO_TAG).set(currentPane);
  }

  private <W> PaneInfo describePane(Instant endOfWindow, PaneInfo prevPane,
      boolean isForWatermarkTrigger, boolean isFinal) {
    boolean isSpeculative = endOfWindow.isAfter(timerInternals.currentWatermarkTime());
    boolean isFirst = (prevPane == null);

    long index = isFirst ? 0 : prevPane.getIndex() + 1;
    long nonSpeculativeIndex;
    Timing timing;
    if (isSpeculative) {
      timing = Timing.EARLY;
      nonSpeculativeIndex = -1;
    } else {
      boolean firstNonSpeculative = prevPane == null || prevPane.getTiming() == Timing.EARLY;
      timing = (isForWatermarkTrigger && firstNonSpeculative) ? Timing.ON_TIME : Timing.LATE;
      nonSpeculativeIndex = firstNonSpeculative ? 0 : prevPane.getNonSpeculativeIndex() + 1;
    }

    return PaneInfo.createPane(isFirst, isFinal, timing, index, nonSpeculativeIndex);
  }
}
