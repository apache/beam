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
package org.apache.beam.runners.core.triggers;

import java.util.Objects;
import org.apache.beam.runners.core.MergingStateAccessor;
import org.apache.beam.runners.core.StateAccessor;
import org.apache.beam.runners.core.StateMerging;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.transforms.Sum;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link TriggerStateMachine}s that fire based on properties of the elements in the current pane.
 */
public class AfterPaneStateMachine extends TriggerStateMachine {

  private static final StateTag<CombiningState<Long, long[], Long>> ELEMENTS_IN_PANE_TAG =
      StateTags.makeSystemTagInternal(
          StateTags.combiningValueFromInputInternal("count", VarLongCoder.of(), Sum.ofLongs()));

  private final int countElems;

  private AfterPaneStateMachine(int countElems) {
    super(null);
    this.countElems = countElems;
  }

  /** The number of elements after which this trigger may fire. */
  public int getElementCount() {
    return countElems;
  }

  /** Creates a trigger that fires when the pane contains at least {@code countElems} elements. */
  public static AfterPaneStateMachine elementCountAtLeast(int countElems) {
    return new AfterPaneStateMachine(countElems);
  }

  @Override
  public void onElement(OnElementContext c) throws Exception {
    c.state().access(ELEMENTS_IN_PANE_TAG).add(1L);
  }

  @Override
  public void prefetchOnMerge(MergingStateAccessor<?, ?> state) {
    super.prefetchOnMerge(state);
    StateMerging.prefetchCombiningValues(state, ELEMENTS_IN_PANE_TAG);
  }

  @Override
  public void onMerge(OnMergeContext context) throws Exception {
    // If we've already received enough elements and finished in some window,
    // then this trigger is just finished.
    if (context.trigger().finishedInAnyMergingWindow()) {
      context.trigger().setFinished(true);
      StateMerging.clear(context.state(), ELEMENTS_IN_PANE_TAG);
      return;
    }

    // Otherwise, compute the sum of elements in all the active panes.
    StateMerging.mergeCombiningValues(context.state(), ELEMENTS_IN_PANE_TAG);
  }

  @Override
  public void prefetchShouldFire(StateAccessor<?> state) {
    state.access(ELEMENTS_IN_PANE_TAG).readLater();
  }

  @Override
  public boolean shouldFire(TriggerStateMachine.TriggerContext context) throws Exception {
    long count = context.state().access(ELEMENTS_IN_PANE_TAG).read();
    return count >= countElems;
  }

  @Override
  public void clear(TriggerContext c) throws Exception {
    c.state().access(ELEMENTS_IN_PANE_TAG).clear();
  }

  @Override
  public boolean isCompatible(TriggerStateMachine other) {
    return this.equals(other);
  }

  @Override
  public String toString() {
    return "AfterPane.elementCountAtLeast(" + countElems + ")";
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof AfterPaneStateMachine)) {
      return false;
    }
    AfterPaneStateMachine that = (AfterPaneStateMachine) obj;
    return this.countElems == that.countElems;
  }

  @Override
  public int hashCode() {
    return Objects.hash(countElems);
  }

  @Override
  public void onFire(TriggerStateMachine.TriggerContext context) throws Exception {
    clear(context);
    context.trigger().setFinished(true);
  }
}
