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
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.MergeResult;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnElementEvent;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnMergeEvent;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnTimerEvent;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnceTrigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerContext;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerResult;
import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A wrapper around a trigger used during execution. While an actual trigger may appear multiple
 * times (both in the same trigger expression and in other trigger expressions), the
 * {@code ExecutableTrigger} wrapped around them forms a tree (only one occurrence).
 *
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used.
 */
public class ExecutableTrigger<W extends BoundedWindow> implements Serializable {

  private static final long serialVersionUID = 0L;

  /** Store the index assigned to this trigger. */
  private final int triggerIndex;
  private final int firstIndexAfterSubtree;
  private final List<ExecutableTrigger<W>> subTriggers = new ArrayList<>();
  private final Trigger<W> trigger;

  public static <W extends BoundedWindow> ExecutableTrigger<W> create(Trigger<W> trigger) {
    return create(trigger, 0);
  }

  private static <W extends BoundedWindow> ExecutableTrigger<W> create(
      Trigger<W> trigger, int nextUnusedIndex) {
    if (trigger instanceof OnceTrigger) {
      return new ExecutableOnceTrigger<W>((OnceTrigger<W>) trigger, nextUnusedIndex);
    } else {
      return new ExecutableTrigger<W>(trigger, nextUnusedIndex);
    }
  }

  public static <W extends BoundedWindow> ExecutableTrigger<W> createForOnceTrigger(
      OnceTrigger<W> trigger, int nextUnusedIndex) {
    return new ExecutableOnceTrigger<W>(trigger, nextUnusedIndex);
  }

  private ExecutableTrigger(Trigger<W> trigger, int nextUnusedIndex) {
    this.trigger = trigger;
    this.triggerIndex = nextUnusedIndex++;

    if (trigger.subTriggers() != null) {
      for (Trigger<W> subTrigger : trigger.subTriggers()) {
        ExecutableTrigger<W> subExecutable = create(subTrigger, nextUnusedIndex);
        subTriggers.add(subExecutable);
        nextUnusedIndex = subExecutable.firstIndexAfterSubtree;
      }
    }
    firstIndexAfterSubtree = nextUnusedIndex;
  }

  public List<ExecutableTrigger<W>> subTriggers() {
    return subTriggers;
  }

  @Override
  public String toString() {
    return trigger.toString();
  }

  /**
   * Return the underlying trigger specification corresponding to this {@code ExecutableTrigger}.
   */
  public Trigger<W> getSpec() {
    return trigger;
  }

  public int getTriggerIndex() {
    return triggerIndex;
  }

  public final int getFirstIndexAfterSubtree() {
    return firstIndexAfterSubtree;
  }

  public boolean isCompatible(ExecutableTrigger<W> other) {
    return trigger.isCompatible(other.trigger);
  }

  public ExecutableTrigger<W> getSubTriggerContaining(int index) {
    Preconditions.checkNotNull(subTriggers);
    Preconditions.checkState(index > triggerIndex && index < firstIndexAfterSubtree,
        "Cannot find sub-trigger containing index not in this tree.");
    ExecutableTrigger<W> previous = null;
    for (ExecutableTrigger<W> subTrigger : subTriggers) {
      if (index < subTrigger.triggerIndex) {
        return previous;
      }
      previous = subTrigger;
    }
    return previous;
  }

  /**
   * Invoke the {@link Trigger#onElement} method for this trigger, ensuring that the bits are
   * properly updated if the trigger finishes.
   */
  public TriggerResult invokeElement(TriggerContext<W> c, OnElementEvent<W> e) throws Exception {
    TriggerContext<W> subContext = c.forTrigger(this);
    if (subContext.isFinished()) {
      throw new IllegalStateException("Shouldn't invokeElement on finished triggers.");
    }

    Trigger.TriggerResult result = trigger.onElement(subContext, e);

    if (result.isFinish()) {
      subContext.setFinished(true);
    }

    return result;
  }

  /**
   * Invoke the {@link Trigger#onTimer} method for this trigger, ensuring that the bits are properly
   * updated if the trigger finishes.
   */
  public TriggerResult invokeTimer(TriggerContext<W> c, OnTimerEvent<W> e) throws Exception {
    TriggerContext<W> subContext = c.forTrigger(this);
    if (subContext.isFinished()) {
      throw new IllegalStateException("Shouldn't invokeTimer on finished triggers.");
    }

    Trigger.TriggerResult result = trigger.onTimer(subContext, e);
    if (result.isFinish()) {
      subContext.setFinished(true);
    }
    return result;
  }

  /**
   * Invoke the {@link Trigger#onMerge} method for this trigger, ensuring that the bits are properly
   * updated.
   */
  public MergeResult invokeMerge(TriggerContext<W> c, OnMergeEvent<W> e) throws Exception {
    Trigger.TriggerContext<W> subContext = c.forTrigger(this);
    Trigger.MergeResult result = trigger.onMerge(subContext, e);
    subContext.setFinished(result.isFinish());
    return result;
  }

  /**
   * Invoke clear for the current this trigger.
   */
  public void invokeClear(Trigger.TriggerContext<W> c, W window) throws Exception {
    trigger.clear(c.forTrigger(this), window);
  }

  /**
   * {@link ExecutableTrigger} that enforces the fact that the trigger should always FIRE_AND_FINISH
   * and never just FIRE.
   */
  private static class ExecutableOnceTrigger<W extends BoundedWindow> extends ExecutableTrigger<W> {

    private static final long serialVersionUID = 0L;

    public ExecutableOnceTrigger(OnceTrigger<W> trigger, int nextUnusedIndex) {
      super(trigger, nextUnusedIndex);
    }

    @Override
    public TriggerResult invokeElement(TriggerContext<W> c, OnElementEvent<W> e) throws Exception {
      TriggerResult result = super.invokeElement(c, e);
      if (TriggerResult.FIRE.equals(result)) {
        throw new IllegalStateException("TriggerResult.FIRE returned from once trigger");
      }
      return result;
    }

    @Override
    public TriggerResult invokeTimer(TriggerContext<W> c, OnTimerEvent<W> e) throws Exception {
      TriggerResult result = super.invokeTimer(c, e);
      if (TriggerResult.FIRE.equals(result)) {
        throw new IllegalStateException("TriggerResult.FIRE returned from once trigger");
      }
      return result;
    }

    @Override
    public MergeResult invokeMerge(TriggerContext<W> c, OnMergeEvent<W> e) throws Exception {
      MergeResult result = super.invokeMerge(c, e);
      if (MergeResult.FIRE.equals(result)) {
        throw new IllegalStateException("MergeResult.FIRE returned from once trigger");
      }
      return result;
    }
  }
}
