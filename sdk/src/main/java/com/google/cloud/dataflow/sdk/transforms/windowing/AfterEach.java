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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class AfterEach<W extends BoundedWindow> extends Trigger<W> {

  private static final Logger LOG = LoggerFactory.getLogger(AfterEach.class);

  private static final long serialVersionUID = 0L;
  private List<Trigger<W>> subTriggers;

  private AfterEach(List<Trigger<W>> subTriggers) {
    this.subTriggers = subTriggers;
    Preconditions.checkArgument(subTriggers.size() > 1);
  }

  @SafeVarargs
  public static <W extends BoundedWindow> Trigger<W> inOrder(Trigger<W>... triggers) {
    return new AfterEach<W>(Arrays.<Trigger<W>>asList(triggers));
  }

  private TriggerResult wrapResult(
      TriggerResult subResult, SubTriggerExecutor<W> subexecutor)
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

    SubTriggerExecutor<W> subexecutor = SubTriggerExecutor.forWindow(subTriggers, c, e.window());

    // There must be at least one unfinished, because otherwise we would have finished the root.
    int current = subexecutor.firstUnfinished();
    return wrapResult(subexecutor.onElement(current, e), subexecutor);
  }

  @Override
  public TriggerResult onMerge(TriggerContext<W> c, OnMergeEvent<W> e) throws Exception {
    SubTriggerExecutor<W> subexecutor = SubTriggerExecutor.forMerge(subTriggers, c, e);

    // There must be at least one unfinished, because otherwise we would have finished the root.
    int current = subexecutor.firstUnfinished();
    return wrapResult(subexecutor.onMerge(current, e), subexecutor);
  }

  @Override
  public TriggerResult onTimer(TriggerContext<W> c, OnTimerEvent<W> e) throws Exception {
    if (e.isForCurrentLayer()) {
      throw new IllegalStateException("AfterAll shouldn't receive any timers.");
    }

    int childIdx = e.getChildIndex();
    SubTriggerExecutor<W> subExecutor = SubTriggerExecutor.forWindow(subTriggers, c, e.window());

    if (childIdx != subExecutor.firstUnfinished()) {
      LOG.warn("AfterEach received timer for non-current sub-trigger {}", childIdx);
      return TriggerResult.CONTINUE;
    }

    return wrapResult(subExecutor.onTimer(childIdx, e), subExecutor);
  }

  @Override
  public void clear(Trigger.TriggerContext<W> c, W window) throws Exception {
    SubTriggerExecutor.forWindow(subTriggers, c, window).clear();
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

  @Override
  public boolean isCompatible(Trigger<?> other) {
    if (!(other instanceof AfterEach)) {
      return false;
    }

    AfterEach<?> that = (AfterEach<?>) other;
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
