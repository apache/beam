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
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.MergeResult;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerResult;
import com.google.cloud.dataflow.sdk.util.TimerInternals.TimerData;
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
 * <p> This is responsible for:
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

  /**
   * Result of trigger execution.
   *
   * <p> This includes the actual {@link TriggerResult} as well as an updated set of finished bits.
   * The bits should typically be committed, but if the trigger fired we want to merge and apply
   * the merging logic on the old finished bits, hence the need to delay committing these results.
   */
  public static class Result {

    private final TriggerResult result;
    private final boolean isFinishedSetUsed;
    private final BitSet modifiedFinishedSet;

    private Result(
        TriggerResult result,
        boolean isFinishedSetUsed,
        BitSet modifiedFinishedSet) {
      this.result = result;
      this.isFinishedSetUsed = isFinishedSetUsed;
      this.modifiedFinishedSet = modifiedFinishedSet;
    }

    public boolean isFire() {
      return result.isFire();
    }

    public boolean isFinish() {
      return result.isFinish();
    }

    public void persistFinishedSet(ReduceFn.StateContext state) {
      if (!isFinishedSetUsed) {
        return;
      }

      ValueState<BitSet> finishedSet = state.access(FINISHED_BITS_TAG);
      if (!finishedSet.get().equals(modifiedFinishedSet)) {
        if (modifiedFinishedSet.isEmpty()) {
          finishedSet.clear();
        } else {
          finishedSet.set(modifiedFinishedSet);
        }
      }
    }
  }

  @VisibleForTesting static final StateTag<ValueState<BitSet>> FINISHED_BITS_TAG =
      StateTags.makeSystemTagInternal(StateTags.value("closed", BitSetCoder.of()));

  private final ExecutableTrigger<W> rootTrigger;
  private final TriggerContextFactory<W> contextFactory;

  public TriggerRunner(ExecutableTrigger<W> rootTrigger, TriggerContextFactory<W> contextFactory) {
    Preconditions.checkState(rootTrigger.getTriggerIndex() == 0);
    this.rootTrigger = rootTrigger;
    this.contextFactory = contextFactory;
  }

  private BitSet readFinishedBits(ValueState<BitSet> state) {
    if (!isFinishedSetNeeded()) {
      // If no trigger in the tree will ever have finished bits, then we don't need to read them.
      // So that the code can be agnostic to that fact, we create a BitSet that is all 0 (not
      // finished) for each trigger in the tree.
      return new BitSet(rootTrigger.getFirstIndexAfterSubtree());
    }

    BitSet bitSet = state.get().read();
    return bitSet == null ? new BitSet(rootTrigger.getFirstIndexAfterSubtree()) : bitSet;
  }

  /** Return true if the trigger is closed in the window corresponding to the specified state. */
  public boolean isClosed(ReduceFn.StateContext state) {
    return readFinishedBits(state.access(FINISHED_BITS_TAG)).get(0);
  }

  /**
   * Run the trigger logic to deal with a new value.
   */
  public Result processValue(ReduceFn<?, ?, ?, W>.ProcessValueContext c) throws Exception {
    // Clone so that we can detect changes and so that changes here don't pollute merging.
    BitSet finishedSet = (BitSet) readFinishedBits(c.state().access(FINISHED_BITS_TAG)).clone();
    Trigger<W>.OnElementContext triggerContext = contextFactory.create(c, rootTrigger, finishedSet);
    TriggerResult result = rootTrigger.invokeElement(triggerContext);
    return new Result(result, isFinishedSetNeeded(), finishedSet);
  }

  /**
   * Run the trigger merging logic as part of executing the specified merge.
   */
  public Result onMerge(ReduceFn<?, ?, ?, W>.OnMergeContext c) throws Exception {
    // Clone so that we can detect changes and so that changes here don't pollute merging.
    BitSet finishedSet = (BitSet) readFinishedBits(c.state().access(FINISHED_BITS_TAG)).clone();

    // And read the finished bits in each merging window.
    ImmutableMap.Builder<W, BitSet> mergingFinishedSets = ImmutableMap.builder();
    Map<BoundedWindow, ValueState<BitSet>> mergingFinishedSetState =
        c.state().accessInEachMergingWindow(FINISHED_BITS_TAG);
    for (W window : c.mergingWindows()) {
      // Don't need to clone these, since the trigger context doesn't allow modification
      mergingFinishedSets.put(window,
          readFinishedBits(mergingFinishedSetState.get(window)));
    }

    Trigger<W>.OnMergeContext mergeContext =
        contextFactory.create(c, rootTrigger, finishedSet, mergingFinishedSets.build());

    // Run the merge from the trigger
    MergeResult result = rootTrigger.invokeMerge(mergeContext);
    if (MergeResult.ALREADY_FINISHED.equals(result)) {
      throw new IllegalStateException("Root trigger returned MergeResult.ALREADY_FINISHED.");
    }

    return new Result(result.getTriggerResult(), isFinishedSetNeeded(), finishedSet);
  }

  /**
   * Run the trigger logic appropriate for receiving a timer with the specified destination ID.
   */
  public Result onTimer(ReduceFn<?, ?, ?, W>.Context c, TimerData timer) throws Exception {
    // Clone so that we can detect changes and so that changes here don't pollute merging.
    BitSet finishedSet = (BitSet) readFinishedBits(c.state().access(FINISHED_BITS_TAG)).clone();
    Trigger<W>.OnTimerContext triggerContext =
        contextFactory.create(c, rootTrigger, finishedSet, timer.getTimestamp(), timer.getDomain());
    TriggerResult result = rootTrigger.invokeTimer(triggerContext);
    return new Result(result, isFinishedSetNeeded(), finishedSet);
  }

  /**
   * Clear the state used for executing triggers, but leave the finished set to indicate
   * the window is closed.
   */
  public void clearState(ReduceFn<?, ?, ?, W>.Context c) throws Exception {
    // Don't need to clone, because we'll be clearing the finished bits anyways.
    BitSet finishedSet = readFinishedBits(c.state().access(FINISHED_BITS_TAG));
    rootTrigger.invokeClear(contextFactory.base(c, rootTrigger, finishedSet));
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

  public void prefetchForValue(ReduceFn.StateContext state) {
    if (isFinishedSetNeeded()) {
      state.access(FINISHED_BITS_TAG).get();
    }
    rootTrigger.getSpec().prefetchOnElement(state);
  }

  public void prefetchForMerge(ReduceFn.MergingStateContext state) {
    if (isFinishedSetNeeded()) {
      for (ValueState<?> value : state.accessInEachMergingWindow(FINISHED_BITS_TAG).values()) {
        value.get();
      }
    }
    rootTrigger.getSpec().prefetchOnMerge(state);
  }

  public void prefetchForTimer(ReduceFn.StateContext state) {
    if (isFinishedSetNeeded()) {
      state.access(FINISHED_BITS_TAG).get();
    }
    rootTrigger.getSpec().prefetchOnElement(state);
  }

  private boolean isFinishedSetNeeded() {
    // TODO: If we know that no trigger in the tree will ever finish, we don't need to do the
    // lookup. Right now, we special case this for the DefaultTrigger.
    return !(rootTrigger.getSpec() instanceof DefaultTrigger);
  }
}
