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

import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.AtMostOnceTrigger;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.common.base.Preconditions;

import org.joda.time.Instant;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

/**
 * Create a {@link CompositeTrigger} that fires once after all of its sub-triggers have fired. If
 * any of the sub-triggers finish without firing, the {@code AfterAll.of(...)} will also finish
 * without firing.
 *
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
 *            {@code Trigger}
 */
public class AfterAll<W extends BoundedWindow>
    extends CompositeTrigger<W> implements AtMostOnceTrigger<W> {

  private static final long serialVersionUID = 0L;

  private static final CodedTupleTag<BitSet> SUBTRIGGERS_FIRED_SET_TAG =
      CodedTupleTag.of("fired", new BitSetCoder());

  private AfterAll(List<Trigger<W>> subTriggers) {
    super(subTriggers);
    Preconditions.checkArgument(subTriggers.size() > 1);
  }

  @SafeVarargs
  public static <W extends BoundedWindow> AtMostOnceTrigger<W> of(
      AtMostOnceTrigger<W>... triggers) {
    return new AfterAll<W>(Arrays.<Trigger<W>>asList(triggers));
  }

  private TriggerResult wrapResult(TriggerContext<W> c, W window,
      BitSet firedSet, SubTriggerExecutor subExecutor) throws IOException {
    // If all children have fired, fire and finish.
    if (firedSet.cardinality() == subTriggers.size()) {
      return TriggerResult.FIRE_AND_FINISH;
    }

    // If we have any triggers that have finished without firing, we should finish:
    BitSet finishedWithoutFiring = subExecutor.getFinishedSet();
    finishedWithoutFiring.andNot(firedSet);
    if (finishedWithoutFiring.cardinality() > 0) {
      return TriggerResult.FINISH;
    }

    // Otherwise, store the FIRED set and continue
    c.store(SUBTRIGGERS_FIRED_SET_TAG, window, firedSet);

    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onElement(TriggerContext<W> c, OnElementEvent<W> e) throws Exception {
    BitSet firedSet = c.lookup(SUBTRIGGERS_FIRED_SET_TAG, e.window());
    if (firedSet == null) {
      firedSet = new BitSet(subTriggers.size());
    }

    SubTriggerExecutor subExecutor = subExecutor(c, e.window());
    for (int i : subExecutor.getUnfinishedTriggers()) {
      if (subExecutor.onElement(c, i, e).isFire()) {
        firedSet.set(i);
      }
    }

    return wrapResult(c, e.window(), firedSet, subExecutor);
  }

  @Override
  public void clear(TriggerContext<W> c, W window) throws Exception {
    super.clear(c, window);
    c.remove(SUBTRIGGERS_FIRED_SET_TAG, window);
  }

  @Override
  public TriggerResult onMerge(TriggerContext<W> c, OnMergeEvent<W> e) throws Exception {
    // First check to see if we've fired in the set of merged triggers
    BitSet newFiredSet = new BitSet(subTriggers.size());
    for (BitSet oldFiredSet : c.lookup(SUBTRIGGERS_FIRED_SET_TAG, e.oldWindows()).values()) {
      if (oldFiredSet != null) {
        newFiredSet.or(oldFiredSet);
      }
    }

    SubTriggerExecutor subExecutor = subExecutor(c, e);

    // Before evaluating the merge of the underlying trigger, see if we can finish early.
    TriggerResult earlyResult = wrapResult(c, e.newWindow(), newFiredSet, subExecutor);
    if (earlyResult.isFinish()) {
      return earlyResult;
    }

    for (int i : subExecutor.getUnfinishedTriggers()) {
      if (subExecutor.onMerge(c, i, e).isFire()) {
        newFiredSet.set(i);
      }
    }

    return wrapResult(c, e.newWindow(), newFiredSet, subExecutor);
  }

  @Override
  public TriggerResult afterChildTimer(
      TriggerContext<W> c, W window, int childIdx, TriggerResult result) throws Exception {
    SubTriggerExecutor subExecutor = subExecutor(c, window);
    BitSet firedSet = c.lookup(SUBTRIGGERS_FIRED_SET_TAG, window);
    if (firedSet == null) {
      firedSet = new BitSet(subTriggers.size());
    }

    if (result.isFire()) {
      firedSet.set(childIdx);
    }

    return wrapResult(c, window, firedSet, subExecutor);
  }

  @Override
  public boolean willNeverFinish() {
    // even if one of the triggers never finishes, the AfterAll could finish if it FIREs.
    return false;
  }

  @Override
  public Instant getWatermarkCutoff(W window) {
    // This trigger will fire after the latest of its sub-triggers.
    Instant deadline = BoundedWindow.TIMESTAMP_MIN_VALUE;
    for (Trigger<W> subTrigger : subTriggers) {
      Instant subDeadline = subTrigger.getWatermarkCutoff(window);
      if (deadline.isBefore(subDeadline)) {
        deadline = subDeadline;
      }
    }
    return deadline;
  }
}
