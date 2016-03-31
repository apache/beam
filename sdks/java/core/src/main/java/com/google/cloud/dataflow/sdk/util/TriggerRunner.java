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
package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.DefaultTrigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger;
import com.google.cloud.dataflow.sdk.util.state.MergingStateAccessor;
import com.google.cloud.dataflow.sdk.util.state.StateAccessor;
import com.google.cloud.dataflow.sdk.util.state.StateTag;
import com.google.cloud.dataflow.sdk.util.state.StateTags;
import com.google.cloud.dataflow.sdk.util.state.ValueState;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import org.joda.time.Instant;

import java.util.BitSet;
import java.util.Collection;
import java.util.Map;

/**
 * Executes a trigger while managing persistence of information about which subtriggers are
 * finished. Subtriggers include all recursive trigger expressions as well as the entire trigger.
 *
 * <p>Specifically, the responsibilities are:
 *
 * <ul>
 *   <li>Invoking the trigger's methods via its {@link ExecutableTrigger} wrapper by
 *       constructing the appropriate trigger contexts.</li>
 *   <li>Committing a record of which subtriggers are finished to persistent state.</li>
 *   <li>Restoring the record of which subtriggers are finished from persistent state.</li>
 *   <li>Clearing out the persisted finished set when a caller indicates
 *       (via {#link #clearFinished}) that it is no longer needed.</li>
 * </ul>
 *
 * <p>These responsibilities are intertwined: trigger contexts include mutable information about
 * which subtriggers are finished. This class provides the information when building the contexts
 * and commits the information when the method of the {@link ExecutableTrigger} returns.
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

    BitSet bitSet = state.read();
    return bitSet == null
        ? FinishedTriggersBitSet.emptyWithCapacity(rootTrigger.getFirstIndexAfterSubtree())
            : FinishedTriggersBitSet.fromBitSet(bitSet);
  }

  /** Return true if the trigger is closed in the window corresponding to the specified state. */
  public boolean isClosed(StateAccessor<?> state) {
    return readFinishedBits(state.access(FINISHED_BITS_TAG)).isFinished(rootTrigger);
  }

  public void prefetchForValue(W window, StateAccessor<?> state) {
    if (isFinishedSetNeeded()) {
      state.access(FINISHED_BITS_TAG).readLater();
    }
    rootTrigger.getSpec().prefetchOnElement(
        contextFactory.createStateAccessor(window, rootTrigger));
  }

  public void prefetchOnFire(W window, StateAccessor<?> state) {
    if (isFinishedSetNeeded()) {
      state.access(FINISHED_BITS_TAG).readLater();
    }
    rootTrigger.getSpec().prefetchOnFire(contextFactory.createStateAccessor(window, rootTrigger));
  }

  public void prefetchShouldFire(W window, StateAccessor<?> state) {
    if (isFinishedSetNeeded()) {
      state.access(FINISHED_BITS_TAG).readLater();
    }
    rootTrigger.getSpec().prefetchShouldFire(
        contextFactory.createStateAccessor(window, rootTrigger));
  }

  /**
   * Run the trigger logic to deal with a new value.
   */
  public void processValue(W window, Instant timestamp, Timers timers, StateAccessor<?> state)
      throws Exception {
    // Clone so that we can detect changes and so that changes here don't pollute merging.
    FinishedTriggersBitSet finishedSet =
        readFinishedBits(state.access(FINISHED_BITS_TAG)).copy();
    Trigger<W>.OnElementContext triggerContext = contextFactory.createOnElementContext(
        window, timers, timestamp, rootTrigger, finishedSet);
    rootTrigger.invokeOnElement(triggerContext);
    persistFinishedSet(state, finishedSet);
  }

  public void prefetchForMerge(
      W window, Collection<W> mergingWindows, MergingStateAccessor<?, W> state) {
    if (isFinishedSetNeeded()) {
      for (ValueState<?> value : state.accessInEachMergingWindow(FINISHED_BITS_TAG).values()) {
        value.readLater();
      }
    }
    rootTrigger.getSpec().prefetchOnMerge(contextFactory.createMergingStateAccessor(
        window, mergingWindows, rootTrigger));
  }

  /**
   * Run the trigger merging logic as part of executing the specified merge.
   */
  public void onMerge(W window, Timers timers, MergingStateAccessor<?, W> state) throws Exception {
    // Clone so that we can detect changes and so that changes here don't pollute merging.
    FinishedTriggersBitSet finishedSet =
        readFinishedBits(state.access(FINISHED_BITS_TAG)).copy();

    // And read the finished bits in each merging window.
    ImmutableMap.Builder<W, FinishedTriggers> builder = ImmutableMap.builder();
    for (Map.Entry<W, ValueState<BitSet>> entry :
        state.accessInEachMergingWindow(FINISHED_BITS_TAG).entrySet()) {
      // Don't need to clone these, since the trigger context doesn't allow modification
      builder.put(entry.getKey(), readFinishedBits(entry.getValue()));
    }
    ImmutableMap<W, FinishedTriggers> mergingFinishedSets = builder.build();

    Trigger<W>.OnMergeContext mergeContext = contextFactory.createOnMergeContext(
        window, timers, rootTrigger, finishedSet, mergingFinishedSets);

    // Run the merge from the trigger
    rootTrigger.invokeOnMerge(mergeContext);

    persistFinishedSet(state, finishedSet);

    // Clear the finished bits.
    clearFinished(state);
  }

  public boolean shouldFire(W window, Timers timers, StateAccessor<?> state) throws Exception {
    FinishedTriggers finishedSet = readFinishedBits(state.access(FINISHED_BITS_TAG)).copy();
    Trigger<W>.TriggerContext context = contextFactory.base(window, timers,
        rootTrigger, finishedSet);
    return rootTrigger.invokeShouldFire(context);
  }

  public void onFire(W window, Timers timers, StateAccessor<?> state) throws Exception {
    // shouldFire should be false.
    // However it is too expensive to assert.
    FinishedTriggersBitSet finishedSet =
        readFinishedBits(state.access(FINISHED_BITS_TAG)).copy();
    Trigger<W>.TriggerContext context = contextFactory.base(window, timers,
        rootTrigger, finishedSet);
    rootTrigger.invokeOnFire(context);
    persistFinishedSet(state, finishedSet);
  }

  private void persistFinishedSet(
      StateAccessor<?> state, FinishedTriggersBitSet modifiedFinishedSet) {
    if (!isFinishedSetNeeded()) {
      return;
    }

    ValueState<BitSet> finishedSetState = state.access(FINISHED_BITS_TAG);
    if (!readFinishedBits(finishedSetState).equals(modifiedFinishedSet)) {
      if (modifiedFinishedSet.getBitSet().isEmpty()) {
        finishedSetState.clear();
      } else {
        finishedSetState.write(modifiedFinishedSet.getBitSet());
      }
    }
  }

  /**
   * Clear finished bits.
   */
  public void clearFinished(StateAccessor<?> state) {
    if (isFinishedSetNeeded()) {
      state.access(FINISHED_BITS_TAG).clear();
    }
  }

  /**
   * Clear the state used for executing triggers, but leave the finished set to indicate
   * the window is closed.
   */
  public void clearState(W window, Timers timers, StateAccessor<?> state) throws Exception {
    // Don't need to clone, because we'll be clearing the finished bits anyways.
    FinishedTriggers finishedSet = readFinishedBits(state.access(FINISHED_BITS_TAG));
    rootTrigger.invokeClear(contextFactory.base(window, timers, rootTrigger, finishedSet));
  }

  private boolean isFinishedSetNeeded() {
    // TODO: If we know that no trigger in the tree will ever finish, we don't need to do the
    // lookup. Right now, we special case this for the DefaultTrigger.
    return !(rootTrigger.getSpec() instanceof DefaultTrigger);
  }
}
