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
import com.google.cloud.dataflow.sdk.util.state.MergingStateContext;
import com.google.cloud.dataflow.sdk.util.state.StateContext;
import com.google.cloud.dataflow.sdk.util.state.StateTag;
import com.google.cloud.dataflow.sdk.util.state.StateTags;
import com.google.cloud.dataflow.sdk.util.state.ValueState;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.BitSet;
import java.util.Collection;
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
  static final StateTag<Object, ValueState<BitSet>> FINISHED_BITS_TAG =
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
  public boolean isClosed(StateContext<?> state) {
    return readFinishedBits(state.access(FINISHED_BITS_TAG)).isFinished(rootTrigger);
  }

  public void prefetchForValue(W window, StateContext<?> state) {
    if (isFinishedSetNeeded()) {
      state.access(FINISHED_BITS_TAG).get();
    }
    rootTrigger.getSpec().prefetchOnElement(contextFactory.createStateContext(window, rootTrigger));
  }

  public void prefetchOnFire(W window, StateContext<?> state) {
    if (isFinishedSetNeeded()) {
      state.access(FINISHED_BITS_TAG).get();
    }
    rootTrigger.getSpec().prefetchOnFire(contextFactory.createStateContext(window, rootTrigger));
  }

  public void prefetchShouldFire(W window, StateContext<?> state) {
    if (isFinishedSetNeeded()) {
      state.access(FINISHED_BITS_TAG).get();
    }
    rootTrigger.getSpec().prefetchShouldFire(
        contextFactory.createStateContext(window, rootTrigger));
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

  public void prefetchForMerge(
      W window, Collection<W> mergingWindows, MergingStateContext<?, W> state) {
    if (isFinishedSetNeeded()) {
      for (ValueState<?> value : state.accessInEachMergingWindow(FINISHED_BITS_TAG).values()) {
        value.get();
      }
    }
    rootTrigger.getSpec().prefetchOnMerge(contextFactory.createMergingStateContext(
        window, mergingWindows, rootTrigger));
  }

  /**
   * Run the trigger merging logic as part of executing the specified merge.
   */
  public void onMerge(ReduceFn<?, ?, ?, W>.OnMergeContext c) throws Exception {
    // Clone so that we can detect changes and so that changes here don't pollute merging.
    FinishedTriggersBitSet finishedSet =
        readFinishedBits(c.state().access(FINISHED_BITS_TAG)).copy();

    // And read the finished bits in each merging window.
    ImmutableMap.Builder<W, FinishedTriggers> builder = ImmutableMap.builder();
    for (Map.Entry<W, ValueState<BitSet>> entry :
        c.state().accessInEachMergingWindow(FINISHED_BITS_TAG).entrySet()) {
      // Don't need to clone these, since the trigger context doesn't allow modification
      builder.put(entry.getKey(), readFinishedBits(entry.getValue()));
    }
    ImmutableMap<W, FinishedTriggers> mergingFinishedSets = builder.build();

    Trigger<W>.OnMergeContext mergeContext = contextFactory.createOnMergeContext(
        c.window(), c.timers(), rootTrigger, finishedSet, mergingFinishedSets);

    // Run the merge from the trigger
    rootTrigger.invokeOnMerge(mergeContext);

    persistFinishedSet(c.state(), finishedSet);

    // Clear the finished bits.
    clearFinished(c);
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
      StateContext<?> state, FinishedTriggersBitSet modifiedFinishedSet) {
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
   * Clear finished bits.
   */
  public void clearFinished(ReduceFn<?, ?, ?, W>.Context c) {
    if (isFinishedSetNeeded()) {
      c.state().access(FINISHED_BITS_TAG).clear();
    }
  }

  /**
   * Clear the state used for executing triggers, but leave the finished set to indicate
   * the window is closed.
   */
  public void clearState(ReduceFn<?, ?, ?, W>.Context c) throws Exception {
    // Don't need to clone, because we'll be clearing the finished bits anyways.
    FinishedTriggers finishedSet = readFinishedBits(c.state().access(FINISHED_BITS_TAG));
    rootTrigger.invokeClear(contextFactory.base(c.window(), c.timers(), rootTrigger, finishedSet));
  }

  private boolean isFinishedSetNeeded() {
    // TODO: If we know that no trigger in the tree will ever finish, we don't need to do the
    // lookup. Right now, we special case this for the DefaultTrigger.
    return !(rootTrigger.getSpec() instanceof DefaultTrigger);
  }
}
