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

package com.google.cloud.dataflow.sdk.transforms.windowing;

import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.coders.VarLongCoder;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnceTrigger;
import com.google.cloud.dataflow.sdk.util.ReduceFn.MergingStateContext;
import com.google.cloud.dataflow.sdk.util.ReduceFn.StateContext;
import com.google.cloud.dataflow.sdk.util.state.CombiningValueState;
import com.google.cloud.dataflow.sdk.util.state.StateContents;
import com.google.cloud.dataflow.sdk.util.state.StateTag;
import com.google.cloud.dataflow.sdk.util.state.StateTags;

import org.joda.time.Instant;

import java.util.List;
import java.util.Objects;

/**
 * {@link Trigger}s that fire based on properties of the elements in the current pane.
 *
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
 *            {@link Trigger}
 */
@Experimental(Experimental.Kind.TRIGGER)
public class AfterPane<W extends BoundedWindow> extends OnceTrigger<W>{

  private static final long serialVersionUID = 0L;

  private static final StateTag<CombiningValueState<Long, Long>> ELEMENTS_IN_PANE_TAG =
      StateTags.makeSystemTagInternal(StateTags.combiningValueFromInputInternal(
          "count", VarLongCoder.of(), new Sum.SumLongFn()));

  private final int countElems;

  private AfterPane(int countElems) {
    super(null);
    this.countElems = countElems;
  }

  /**
   * Creates a trigger that fires when the pane contains at least {@code countElems} elements.
   */
  public static <W extends BoundedWindow> AfterPane<W> elementCountAtLeast(int countElems) {
    return new AfterPane<>(countElems);
  }

  @Override
  public TriggerResult onElement(OnElementContext c) throws Exception {
    CombiningValueState<Long, Long> elementsInPane = c.state().access(ELEMENTS_IN_PANE_TAG);
    StateContents<Long> countContents = elementsInPane.get();
    elementsInPane.add(1L);

    // TODO: Consider waiting to read the value until the end of a bundle, since we don't need to
    // fire immediately when the count exceeds countElems.
    long count = countContents.read();
    return count >= countElems ? TriggerResult.FIRE_AND_FINISH : TriggerResult.CONTINUE;
  }

  @Override
  public MergeResult onMerge(OnMergeContext c) throws Exception {
    // If we've already received enough elements and finished in some window, then this trigger
    // is just finished.
    if (c.trigger().finishedInAnyMergingWindow()) {
      return MergeResult.ALREADY_FINISHED;
    }

    // Otherwise, compute the sum of elements in all the active panes
    CombiningValueState<Long, Long> elementsInPane =
        c.state().accessAcrossMergingWindows(ELEMENTS_IN_PANE_TAG);
    long count = elementsInPane.get().read();
    return count >= countElems ? MergeResult.FIRE_AND_FINISH : MergeResult.CONTINUE;
  }

  @Override
  public TriggerResult onTimer(OnTimerContext c) {
    return TriggerResult.CONTINUE;
  }

  @Override
  public void prefetchOnElement(StateContext state) {
    state.access(ELEMENTS_IN_PANE_TAG).get();
  }

  @Override
  public void prefetchOnMerge(MergingStateContext state) {
    state.accessAcrossMergingWindows(ELEMENTS_IN_PANE_TAG).get();
  }

  @Override
  public void prefetchOnTimer(StateContext state) {
  }

  @Override
  public void clear(TriggerContext c) throws Exception {
    c.state().access(ELEMENTS_IN_PANE_TAG).clear();
  }

  @Override
  public boolean isCompatible(Trigger<?> other) {
    return this.equals(other);
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(W window) {
    return BoundedWindow.TIMESTAMP_MAX_VALUE;
  }

  @Override
  public OnceTrigger<W> getContinuationTrigger(List<Trigger<W>> continuationTriggers) {
    return AfterPane.elementCountAtLeast(1);
  }

  @Override
  public String toString() {
    return "AfterPane.elementCountAtLeast(" + countElems + ")";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof AfterPane)) {
      return false;
    }
    AfterPane<?> that = (AfterPane<?>) obj;
    return this.countElems == that.countElems;
  }

  @Override
  public int hashCode() {
    return Objects.hash(countElems);
  }
}
