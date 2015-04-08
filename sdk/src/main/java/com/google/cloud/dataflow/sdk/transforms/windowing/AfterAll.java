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
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnceTrigger;
import com.google.common.base.Preconditions;

import org.joda.time.Instant;

import java.util.Arrays;
import java.util.List;

/**
 * Create a {@link Trigger} that fires and finishes once after all of its sub-triggers have fired.
 *
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
 *            {@code Trigger}
 */
@Experimental(Experimental.Kind.TRIGGER)
public class AfterAll<W extends BoundedWindow> extends OnceTrigger<W> {

  private static final long serialVersionUID = 0L;
  private List<Trigger<W>> subTriggers;

  private AfterAll(List<Trigger<W>> subTriggers) {
    this.subTriggers = subTriggers;
    Preconditions.checkArgument(subTriggers.size() > 1);
  }

  @SafeVarargs
  public static <W extends BoundedWindow> OnceTrigger<W> of(
      OnceTrigger<W>... triggers) {
    return new AfterAll<W>(Arrays.<Trigger<W>>asList(triggers));
  }

  private TriggerResult wrapResult(SubTriggerExecutor<W> subExecutor) {
    // If all children have finished, then they must have each fired at least once.
    if (subExecutor.allFinished()) {
      return TriggerResult.FIRE_AND_FINISH;
    }

    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onElement(TriggerContext<W> c, OnElementEvent<W> e) throws Exception {
    SubTriggerExecutor<W> subExecutor = SubTriggerExecutor.forWindow(subTriggers, c, e.window());
    for (int i : subExecutor.getUnfinishedTriggers()) {
      // Mark any fired triggers as finished.
      if (subExecutor.onElement(i, e).isFire()) {
        subExecutor.markFinished(c, i);
      }
    }

    return wrapResult(subExecutor);
  }

  @Override
  public TriggerResult onMerge(TriggerContext<W> c, OnMergeEvent<W> e) throws Exception {
    SubTriggerExecutor<W> subExecutor = SubTriggerExecutor.forMerge(subTriggers, c, e);

    // If after merging the set of fire & finished sub-triggers, we're done, we can
    // FIRE_AND_FINISH early.
    if (subExecutor.allFinished()) {
      return TriggerResult.FIRE_AND_FINISH;
    }

    // Otherwise, merge all of the unfinished triggers.
    for (int i : subExecutor.getUnfinishedTriggers()) {
      if (subExecutor.onMerge(i, e).isFire()) {
        subExecutor.markFinished(c, i);
      }
    }

    return wrapResult(subExecutor);
  }

  @Override
  public TriggerResult onTimer(TriggerContext<W> c, OnTimerEvent<W> e) throws Exception {
    if (e.isForCurrentLayer()) {
      throw new IllegalStateException("AfterAll shouldn't receive any timers.");
    }

    int childIdx = e.getChildIndex();
    SubTriggerExecutor<W> subExecutor = SubTriggerExecutor.forWindow(subTriggers, c, e.window());

    // We take at-most-once triggers, so the result of the timer on the child should be either
    // CONTINUE or FIRE_AND_FINISH. The subexecutor already tracks finishing of children, so we just
    // need to know that we fire and finish if all of the children have finished.
    subExecutor.onTimer(childIdx, e);
    return wrapResult(subExecutor);
  }


  @Override
  public void clear(Trigger.TriggerContext<W> c, W window) throws Exception {
    SubTriggerExecutor.forWindow(subTriggers, c, window).clear();
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

  @Override
  public boolean isCompatible(Trigger<?> other) {
    if (!(other instanceof AfterAll)) {
      return false;
    }

    AfterAll<?> that = (AfterAll<?>) other;
    if (this.subTriggers.size() != that.subTriggers.size()) {
      return false;
    }

    for (int i = 0; i < this.subTriggers.size(); i++) {
      if (!this.subTriggers.get(i).isCompatible(that.subTriggers.get(i))) {
        return false;
      }
    }

    return true;
  }
}
