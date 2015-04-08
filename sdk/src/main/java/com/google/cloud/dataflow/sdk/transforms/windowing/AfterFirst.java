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
 * Create a composite {@link Trigger} that fires once after at least one of its sub-triggers have
 * fired.
 *
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
 *            {@code Trigger}
 */
@Experimental(Experimental.Kind.TRIGGER)
public class AfterFirst<W extends BoundedWindow> extends OnceTrigger<W> {

  private static final long serialVersionUID = 0L;
  private List<Trigger<W>> subTriggers;

  private AfterFirst(List<Trigger<W>> subTriggers) {
    this.subTriggers = subTriggers;
    Preconditions.checkArgument(subTriggers.size() > 1);
  }

  @SafeVarargs
  public static <W extends BoundedWindow> OnceTrigger<W> of(
      OnceTrigger<W>... triggers) {
    return new AfterFirst<W>(Arrays.<Trigger<W>>asList(triggers));
  }

  @Override
  public TriggerResult onElement(TriggerContext<W> c, OnElementEvent<W> e) throws Exception {
    for (int i = 0; i < subTriggers.size(); i++) {
      if (subTriggers.get(i).onElement(c.forChild(i), e).isFire()) {
        return TriggerResult.FIRE_AND_FINISH;
      }
    }
    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onMerge(TriggerContext<W> c, OnMergeEvent<W> e) throws Exception {
    for (int i = 0; i < subTriggers.size(); i++) {
      if (subTriggers.get(i).onMerge(c.forChild(i), e).isFire()) {
        return TriggerResult.FIRE_AND_FINISH;
      }
    }
    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onTimer(TriggerContext<W> c, OnTimerEvent<W> e) throws Exception {
    if (e.isForCurrentLayer()) {
      throw new IllegalStateException("AfterFirst shouldn't receive any timers.");
    }

    int childIdx = e.getChildIndex();
    return subTriggers.get(childIdx).onTimer(c.forChild(childIdx), e.withoutOuterTrigger()).isFire()
        ? TriggerResult.FIRE_AND_FINISH
        : TriggerResult.CONTINUE;
  }

  @Override
  public void clear(Trigger.TriggerContext<W> c, W window) throws Exception {
    SubTriggerExecutor.forWindow(subTriggers, c, window).clear();
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

  @Override
  public boolean isCompatible(Trigger<?> other) {
    if (!(other instanceof AfterFirst)) {
      return false;
    }

    AfterFirst<?> that = (AfterFirst<?>) other;
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
