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

import com.google.common.base.Preconditions;

import org.joda.time.Instant;

import java.util.Arrays;
import java.util.List;

/**
 * A composite trigger that executes its sub-triggers in order. Only one sub-trigger is executing at
 * a time, and any time it fires the {@code AfterEach} fires. When the currently executing
 * sub-trigger finishes, the {@code AfterEach} starts executing the next sub-trigger.
 *
 * <p> {@code AfterEach.inOrder(t1, t2, ...)} finishes when all of the sub-triggers have finished.
 *
 * <p> The following properties hold:
 * <ul>
 *   <li> {@code AfterEach.inOrder(AfterEach.inOrder(a, b), c)} behaves the same as
 *   {@code AfterEach.inOrder(a, b, c)}
 *   <li> {@code AfterEach.inOrder(Repeatedly.forever(a), b)} behaves the same as
 *   {@code Repeatedly.forever(a)}, since the repeated trigger never finishes.
 * </ul>
 *
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
 *            {@code Trigger}
 */
public class AfterEach<W extends BoundedWindow> extends CompositeTrigger<W> {

  private static final long serialVersionUID = 0L;

  private AfterEach(List<Trigger<W>> subTriggers) {
    super(subTriggers);
    Preconditions.checkArgument(subTriggers.size() > 1);
  }

  @SafeVarargs
  public static <W extends BoundedWindow> Trigger<W> inOrder(Trigger<W>... triggers) {
    return new AfterEach<W>(Arrays.<Trigger<W>>asList(triggers));
  }

  private TriggerResult result(
      TriggerResult subResult, SubTriggerExecutor subexecutor)
      throws Exception {

    if (subResult.isFire()) {
      return subexecutor.allFinished() ? TriggerResult.FIRE_AND_FINISH : TriggerResult.FIRE;
    } else {
      return TriggerResult.CONTINUE;
    }
  }

  @Override
  public TriggerResult onElement(TriggerContext<W> c, OnElementEvent<W> e) throws Exception {
    // If all the sub-triggers have finished, we should have already finished, so we know there is
    // at least one unfinished trigger.

    SubTriggerExecutor subexecutor = subExecutor(c, e.window());

    // There must be at least one unfinished, because otherwise we would have finished the root.
    int current = subexecutor.firstUnfinished();
    return result(subexecutor.onElement(c, current, e), subexecutor);
  }

  @Override
  public TriggerResult onMerge(TriggerContext<W> c, OnMergeEvent<W> e) throws Exception {
    SubTriggerExecutor subexecutor = subExecutor(c, e);

    // There must be at least one unfinished, because otherwise we would have finished the root.
    int current = subexecutor.firstUnfinished();
    return result(subexecutor.onMerge(c, current, e), subexecutor);
  }

  @Override
  public TriggerResult afterChildTimer(
      TriggerContext<W> c, W window, int childIdx, TriggerResult result) throws Exception {
    SubTriggerExecutor subExecutor = subExecutor(c, window);
    if (childIdx != subExecutor.firstUnfinished()) {
      // If we aren't currently executing the given sub-trigger, it shouldn't have been able to send
      // a timer at all. We record its finishing, but ignore it otherwise.
      return TriggerResult.CONTINUE;
    }

    return result(result, subExecutor);
  }

  @Override
  public boolean willNeverFinish() {
    for (Trigger<W> trigger : subTriggers) {
      if (trigger.willNeverFinish()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Instant getWatermarkCutoff(W window) {
    // This trigger will fire at least once when the first trigger in the sequence
    // fires at least once.
    return subTriggers.get(0).getWatermarkCutoff(window);
  }
}
