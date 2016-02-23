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
import com.google.cloud.dataflow.sdk.util.state.AccumulatorCombiningState;
import com.google.cloud.dataflow.sdk.util.state.MergingStateAccessor;
import com.google.cloud.dataflow.sdk.util.state.ReadableState;
import com.google.cloud.dataflow.sdk.util.state.StateAccessor;
import com.google.cloud.dataflow.sdk.util.state.StateMerging;
import com.google.cloud.dataflow.sdk.util.state.StateTag;
import com.google.cloud.dataflow.sdk.util.state.StateTags;

/**
 * Tracks which windows have non-empty panes. Specifically, which windows have new elements since
 * their last triggering.
 *
 * @param <W> The kind of windows being tracked.
 */
public abstract class NonEmptyPanes<K, W extends BoundedWindow> {

  static <K, W extends BoundedWindow> NonEmptyPanes<K, W> create(
      WindowingStrategy<?, W> strategy, ReduceFn<K, ?, ?, W> reduceFn) {
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
  public abstract void recordContent(StateAccessor<K> context);

  /**
   * Record that the given pane is empty.
   */
  public abstract void clearPane(StateAccessor<K> state);

  /**
   * Return true if the current pane for the window in {@code context} is empty.
   */
  public abstract ReadableState<Boolean> isEmpty(StateAccessor<K> context);

  /**
   * Prefetch in preparation for merging.
   */
  public abstract void prefetchOnMerge(MergingStateAccessor<K, W> state);

  /**
   * Eagerly merge backing state.
   */
  public abstract void onMerge(MergingStateAccessor<K, W> context);

  /**
   * An implementation of {@code NonEmptyPanes} optimized for use with discarding mode. Uses the
   * presence of data in the accumulation buffer to record non-empty panes.
   */
  private static class DiscardingModeNonEmptyPanes<K, W extends BoundedWindow>
      extends NonEmptyPanes<K, W> {

    private ReduceFn<K, ?, ?, W> reduceFn;

    private DiscardingModeNonEmptyPanes(ReduceFn<K, ?, ?, W> reduceFn) {
      this.reduceFn = reduceFn;
    }

    @Override
    public ReadableState<Boolean> isEmpty(StateAccessor<K> state) {
      return reduceFn.isEmpty(state);
    }

    @Override
    public void recordContent(StateAccessor<K> state) {
      // Nothing to do -- the reduceFn is tracking contents
    }

    @Override
    public void clearPane(StateAccessor<K> state) {
      // Nothing to do -- the reduceFn is tracking contents
    }

    @Override
    public void prefetchOnMerge(MergingStateAccessor<K, W> state) {
      // Nothing to do -- the reduceFn is tracking contents
    }

    @Override
    public void onMerge(MergingStateAccessor<K, W> context) {
      // Nothing to do -- the reduceFn is tracking contents
    }
  }

  /**
   * An implementation of {@code NonEmptyPanes} for general use.
   */
  private static class GeneralNonEmptyPanes<K, W extends BoundedWindow>
      extends NonEmptyPanes<K, W> {

    private static final StateTag<Object, AccumulatorCombiningState<Long, long[], Long>>
        PANE_ADDITIONS_TAG =
        StateTags.makeSystemTagInternal(StateTags.combiningValueFromInputInternal(
            "count", VarLongCoder.of(), new Sum.SumLongFn()));

    @Override
    public void recordContent(StateAccessor<K> state) {
      state.access(PANE_ADDITIONS_TAG).add(1L);
    }

    @Override
    public void clearPane(StateAccessor<K> state) {
      state.access(PANE_ADDITIONS_TAG).clear();
    }

    @Override
    public ReadableState<Boolean> isEmpty(StateAccessor<K> state) {
      return state.access(PANE_ADDITIONS_TAG).isEmpty();
    }

    @Override
    public void prefetchOnMerge(MergingStateAccessor<K, W> state) {
      StateMerging.prefetchCombiningValues(state, PANE_ADDITIONS_TAG);
    }

    @Override
    public void onMerge(MergingStateAccessor<K, W> context) {
      StateMerging.mergeCombiningValues(context, PANE_ADDITIONS_TAG);
    }
  }
}
