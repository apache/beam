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

import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.DefaultTrigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger;
import com.google.cloud.dataflow.sdk.util.state.StateTag;
import com.google.cloud.dataflow.sdk.util.state.StateTags;
import com.google.cloud.dataflow.sdk.util.state.ValueState;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.BitSet;
import java.util.Map;

/**
 * Executes a trigger within the context provided by {@link ReduceFnRunner}.
 *
 * <p>This is responsible for:
 *
 * <ul>
 * <li> Tracking the finished bits for the trigger tree, included whether the root trigger
 * has finished.
 * <li> Ensuring that the timer and state associated with each trigger node is separate.
 * </ul>
 *
 * @param <W> The kind of windows being processed.
 */
public class TriggerRunner<W extends BoundedWindow> {
  @VisibleForTesting
  static final StateTag<ValueState<BitSet>> FINISHED_BITS_TAG =
      StateTags.makeSystemTagInternal(StateTags.value("closed", BitSetCoder.of()));

  private final ExecutableTrigger<W> rootTrigger;
  private final TriggerContextFactory<W> contextFactory;

  public TriggerRunner(ExecutableTrigger<W> rootTrigger, TriggerContextFactory<W> contextFactory) {
    Preconditions.checkState(rootTrigger.getTriggerIndex() == 0);
    this.rootTrigger = rootTrigger;
    this.contextFactory = contextFactory;
  }

  private FinishedTriggersBitSet readFinishedBits(ValueState<BitSet> state) {
    if (!isFinishedSetNeeded()) {
      // If no trigger in the tree will ever have finished bits, then we don't need to read them.
      // So that the code can be agnostic to that fact, we create a BitSet that is all 0 (not
      // finished) for each trigger in the tree.
      return FinishedTriggersBitSet.emptyWithCapacity(rootTrigger.getFirstIndexAfterSubtree());
    }

    BitSet bitSet = state.get().read();
    return bitSet == null
        ? FinishedTriggersBitSet.emptyWithCapacity(rootTrigger.getFirstIndexAfterSubtree())
            : FinishedTriggersBitSet.fromBitSet(bitSet);
  }

  /** Return true if the trigger is closed in the window corresponding to the specified state. */
  public boolean isClosed(ReduceFn.StateContext state) {
    return readFinishedBits(state.access(FINISHED_BITS_TAG)).isFinished(rootTrigger);
  }

  public void prefetchForValue(ReduceFn.StateContext state) {
    if (isFinishedSetNeeded()) {
      state.access(FINISHED_BITS_TAG).get();
    }
    rootTrigger.getSpec().prefetchOnElement(state);
  }

  public void prefetchOnFire(ReduceFn.StateContext state) {
    if (isFinishedSetNeeded()) {
      state.access(FINISHED_BITS_TAG).get();
    }
    rootTrigger.getSpec().prefetchOnFire(state);
  }

  public void prefetchShouldFire(ReduceFn.StateContext state) {
    if (isFinishedSetNeeded()) {
      state.access(FINISHED_BITS_TAG).get();
    }
    rootTrigger.getSpec().prefetchShouldFire(state);
  }

  /**
   * Run the trigger logic to deal with a new value.
   */
  public void processValue(ReduceFn<?, ?, ?, W>.ProcessValueContext c) throws Exception {
    // Clone so that we can detect changes and so that changes here don't pollute merging.
    FinishedTriggersBitSet finishedSet =
        readFinishedBits(c.state().access(FINISHED_BITS_TAG)).copy();
    Trigger<W>.OnElementContext triggerContext = contextFactory.createOnElementContext(
        c.window(), c.timers(), c.timestamp(), rootTrigger, finishedSet);
    rootTrigger.invokeOnElement(triggerContext);
    persistFinishedSet(c.state(), finishedSet);
  }

  public void prefetchForMerge(ReduceFn.MergingStateContext state) {
    if (isFinishedSetNeeded()) {
      for (ValueState<?> value :
          state.mergingAccessInEachMergingWindow(FINISHED_BITS_TAG).values()) {
        value.get();
      }
    }
    rootTrigger.getSpec().prefetchOnMerge(state);
  }

  /**
   * Run the trigger merging logic as part of executing the specified merge.
   */
  public void onMerge(ReduceFn<?, ?, ?, W>.OnMergeContext c) throws Exception {
    // Clone so that we can detect changes and so that changes here don't pollute merging.
    FinishedTriggersBitSet finishedSet =
        readFinishedBits(c.state().access(FINISHED_BITS_TAG)).copy();

    // And read the finished bits in each merging window.
    ImmutableMap.Builder<W, FinishedTriggers> mergingFinishedSets = ImmutableMap.builder();
    Map<BoundedWindow, ValueState<BitSet>> mergingFinishedSetState =
        c.state().mergingAccessInEachMergingWindow(FINISHED_BITS_TAG);
    for (W window : c.mergingWindows()) {
      // Don't need to clone these, since the trigger context doesn't allow modification
      mergingFinishedSets.put(window, readFinishedBits(mergingFinishedSetState.get(window)));
    }

    Trigger<W>.OnMergeContext mergeContext = contextFactory.createOnMergeContext(
            c.window(), c.timers(), c.mergingWindows(), rootTrigger,
            finishedSet, mergingFinishedSets.build());

    // Run the merge from the trigger
    rootTrigger.invokeOnMerge(mergeContext);

    persistFinishedSet(c.state(), finishedSet);
  }

  public boolean shouldFire(ReduceFn<?, ?, ?, W>.Context c) throws Exception {
    FinishedTriggers finishedSet = readFinishedBits(c.state().access(FINISHED_BITS_TAG)).copy();
    Trigger<W>.TriggerContext context = contextFactory.base(c.window(), c.timers(),
        rootTrigger, finishedSet);
    return rootTrigger.invokeShouldFire(context);
  }

  public void onFire(ReduceFn<?, ?, ?, W>.Context c) throws Exception {
    FinishedTriggersBitSet finishedSet =
        readFinishedBits(c.state().access(FINISHED_BITS_TAG)).copy();
    Trigger<W>.TriggerContext context = contextFactory.base(c.window(), c.timers(),
        rootTrigger, finishedSet);
    rootTrigger.invokeOnFire(context);
    persistFinishedSet(c.state(), finishedSet);
  }

  private void persistFinishedSet(
      ReduceFn.StateContext state, FinishedTriggersBitSet modifiedFinishedSet) {
    if (!isFinishedSetNeeded()) {
      return;
    }

    ValueState<BitSet> finishedSet = state.access(FINISHED_BITS_TAG);
    if (!finishedSet.get().equals(modifiedFinishedSet)) {
      if (modifiedFinishedSet.getBitSet().isEmpty()) {
        finishedSet.clear();
      } else {
        finishedSet.set(modifiedFinishedSet.getBitSet());
      }
    }
  }

  /**
   * Clear the state used for executing triggers, but leave the finished set to indicate
   * the window is closed.
   */
  public void clearState(ReduceFn<?, ?, ?, W>.Context c) throws Exception {
    // Don't need to clone, because we'll be clearing the finished bits anyways.
    FinishedTriggers finishedSet = readFinishedBits(c.state().access(FINISHED_BITS_TAG));
    rootTrigger.invokeClear(contextFactory.base(
        c.window(), c.timers(), rootTrigger, finishedSet));
  }

  /**
   * Clear all the state for executing triggers, including the finished bits. This should only be
   * called after the allowed lateness has elapsed, so that the window will never be recreated.
   */
  public void clearEverything(ReduceFn<?, ?, ?, W>.Context c) throws Exception {
    clearState(c);
    if (isFinishedSetNeeded()) {
      c.state().access(FINISHED_BITS_TAG).clear();
    }
  }

  private boolean isFinishedSetNeeded() {
    // TODO: If we know that no trigger in the tree will ever finish, we don't need to do the
    // lookup. Right now, we special case this for the DefaultTrigger.
    return !(rootTrigger.getSpec() instanceof DefaultTrigger);
  }
}
