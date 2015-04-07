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
import com.google.common.base.Preconditions;

import org.joda.time.Instant;

import java.util.Arrays;
import java.util.List;

/**
 * Create a {@link CompositeTrigger} that fires once after at least one of its sub-triggers have
 * fired. If all of the sub-triggers finish without firing, the {@code AfterFirst.of(...)} will also
 * finish without firing.
 *
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
 *            {@code Trigger}
 */
public class AfterFirst<W extends BoundedWindow>
    extends CompositeTrigger<W> implements AtMostOnceTrigger<W> {

  private static final long serialVersionUID = 0L;

  private AfterFirst(List<Trigger<W>> subTriggers) {
    super(subTriggers);
    Preconditions.checkArgument(subTriggers.size() > 1);
  }

  @SafeVarargs
  public static <W extends BoundedWindow> AtMostOnceTrigger<W> of(
      AtMostOnceTrigger<W>... triggers) {
    return new AfterFirst<W>(Arrays.<Trigger<W>>asList(triggers));
  }

  @Override
  public TriggerResult onElement(TriggerContext<W> c, OnElementEvent<W> e) throws Exception {
    // If all the sub-triggers have finished, we should have already finished, so we know there is
    // at least one unfinished trigger.

    SubTriggerExecutor subStates = subExecutor(c, e.window());
    for (int i = 0; i < subTriggers.size(); i++) {
      if (subStates.onElement(c, i, e).isFire()) {
        return TriggerResult.FIRE_AND_FINISH;
      }
    }

    if (subStates.allFinished()) {
      throw new IllegalStateException("AfterFirst should have fired earlier.");
    }
    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onMerge(TriggerContext<W> c, OnMergeEvent<W> e) throws Exception {
    SubTriggerExecutor subStates = subExecutor(c, e);
    for (int i = 0; i < subTriggers.size(); i++) {
      if (subStates.onMerge(c, i, e).isFire()) {
        return TriggerResult.FIRE_AND_FINISH;
      }
    }

    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult afterChildTimer(
      TriggerContext<W> c, W window, int childIdx, TriggerResult result) throws Exception {
    if (result.isFire()) {
      return TriggerResult.FIRE_AND_FINISH;
    }

    return TriggerResult.CONTINUE;
  }

  @Override
  public boolean willNeverFinish() {
    // The only case an AfterAll will never finish, is if some trigger never fires. But, we can't
    // statically determine if (or when) a trigger might fire.
    return false;
  }

  @Override
  public Instant getWatermarkCutoff(W window) {
    // This trigger will fire after the earliest of its sub-triggers.
    Instant deadline = BoundedWindow.TIMESTAMP_MAX_VALUE;
    for (Trigger<W> subTrigger : subTriggers) {
      Instant subDeadline = subTrigger.getWatermarkCutoff(window);
      if (deadline.isAfter(subDeadline)) {
        deadline = subDeadline;
      }
    }
    return deadline;
  }
}
