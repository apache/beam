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

import com.google.cloud.dataflow.sdk.coders.VarLongCoder;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy.AccumulationMode;
import com.google.cloud.dataflow.sdk.util.state.CombiningValueState;
import com.google.cloud.dataflow.sdk.util.state.StateContents;
import com.google.cloud.dataflow.sdk.util.state.StateTag;
import com.google.cloud.dataflow.sdk.util.state.StateTags;

/**
 * Tracks which windows have non-empty panes. Specifically, which windows have new elements since
 * their last triggering.
 *
 * @param <W> The kind of windows being tracked.
 */
public abstract class NonEmptyPanes<W extends BoundedWindow> {

  public static <W extends BoundedWindow> NonEmptyPanes<W> create(
      WindowingStrategy<?, W> strategy, ReduceFn<?, ?, ?, W> reduceFn) {
    if (strategy.getMode() == AccumulationMode.DISCARDING_FIRED_PANES) {
      return new DiscardingModeNonEmptyPanes<>(reduceFn);
    } else {
      return new GeneralNonEmptyPanes<>();
    }
  }

  /**
   * Record that some content has been added to the window in {@code context}, and therefore the
   * current pane is not empty.
   */
  public abstract void recordContent(ReduceFn<?, ?, ?, W>.Context context);

  /**
   * Record that the given pane is empty.
   */
  public abstract void clearPane(ReduceFn<?, ?, ?, W>.Context context);

  /**
   * Return true if the current pane for the window in {@code context} is non-empty.
   */
  public abstract StateContents<Boolean> isEmpty(ReduceFn<?, ?, ?, W>.Context context);

  /**
   * An implementation of {@code NonEmptyPanes} optimized for use with discarding mode. Uses the
   * presence of data in the accumulation buffer to record non-empty panes.
   */
  private static class DiscardingModeNonEmptyPanes<W extends BoundedWindow>
      extends NonEmptyPanes<W> {

    private ReduceFn<?, ?, ?, W> reduceFn;

    private DiscardingModeNonEmptyPanes(ReduceFn<?, ?, ?, W> reduceFn) {
      this.reduceFn = reduceFn;
    }

    @Override
    public StateContents<Boolean> isEmpty(ReduceFn<?, ?, ?, W>.Context context) {
      return reduceFn.isEmpty(context.state());
    }

    @Override
    public void recordContent(ReduceFn<?, ?, ?, W>.Context context) {
      // Nothing to do -- the reduceFn is tracking contents
    }

    @Override
    public void clearPane(ReduceFn<?, ?, ?, W>.Context context) {
      // Nothing to do -- the reduceFn is tracking contents
    }
  }

  /**
   * An implementation of {@code NonEmptyPanes} for general use.
   */
  private static class GeneralNonEmptyPanes<W extends BoundedWindow> extends NonEmptyPanes<W> {

    private static final StateTag<CombiningValueState<Long, Long>> PANE_ADDITIONS_TAG =
        StateTags.makeSystemTagInternal(StateTags.combiningValueFromInputInternal(
            "count", VarLongCoder.of(), new Sum.SumLongFn()));

    @Override
    public void recordContent(ReduceFn<?, ?, ?, W>.Context context) {
      context.state().access(PANE_ADDITIONS_TAG).add(1L);
    }

    @Override
    public void clearPane(ReduceFn<?, ?, ?, W>.Context context) {
      context.state().accessAcrossMergedWindows(PANE_ADDITIONS_TAG).clear();
    }

    @Override
    public StateContents<Boolean> isEmpty(ReduceFn<?, ?, ?, W>.Context context) {
      return context.state().accessAcrossMergedWindows(PANE_ADDITIONS_TAG).isEmpty();
    }
  }
}

