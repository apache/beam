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
package org.apache.beam.sdk.util;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Trigger.OnceTrigger;

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A wrapper around a trigger used during execution. While an actual trigger may appear multiple
 * times (both in the same trigger expression and in other trigger expressions), the
 * {@code ExecutableTrigger} wrapped around them forms a tree (only one occurrence).
 */
public class ExecutableTrigger implements Serializable {

  /** Store the index assigned to this trigger. */
  private final int triggerIndex;
  private final int firstIndexAfterSubtree;
  private final List<ExecutableTrigger> subTriggers = new ArrayList<>();
  private final Trigger trigger;

  public static <W extends BoundedWindow> ExecutableTrigger create(Trigger trigger) {
    return create(trigger, 0);
  }

  private static <W extends BoundedWindow> ExecutableTrigger create(
      Trigger trigger, int nextUnusedIndex) {
    if (trigger instanceof OnceTrigger) {
      return new ExecutableOnceTrigger((OnceTrigger) trigger, nextUnusedIndex);
    } else {
      return new ExecutableTrigger(trigger, nextUnusedIndex);
    }
  }

  public static <W extends BoundedWindow> ExecutableTrigger createForOnceTrigger(
      OnceTrigger trigger, int nextUnusedIndex) {
    return new ExecutableOnceTrigger(trigger, nextUnusedIndex);
  }

  private ExecutableTrigger(Trigger trigger, int nextUnusedIndex) {
    this.trigger = Preconditions.checkNotNull(trigger, "trigger must not be null");
    this.triggerIndex = nextUnusedIndex++;

    if (trigger.subTriggers() != null) {
      for (Trigger subTrigger : trigger.subTriggers()) {
        ExecutableTrigger subExecutable = create(subTrigger, nextUnusedIndex);
        subTriggers.add(subExecutable);
        nextUnusedIndex = subExecutable.firstIndexAfterSubtree;
      }
    }
    firstIndexAfterSubtree = nextUnusedIndex;
  }

  public List<ExecutableTrigger> subTriggers() {
    return subTriggers;
  }

  @Override
  public String toString() {
    return trigger.toString();
  }

  /**
   * Return the underlying trigger specification corresponding to this {@code ExecutableTrigger}.
   */
  public Trigger getSpec() {
    return trigger;
  }

  public int getTriggerIndex() {
    return triggerIndex;
  }

  public final int getFirstIndexAfterSubtree() {
    return firstIndexAfterSubtree;
  }

  public boolean isCompatible(ExecutableTrigger other) {
    return trigger.isCompatible(other.trigger);
  }

  public ExecutableTrigger getSubTriggerContaining(int index) {
    Preconditions.checkNotNull(subTriggers);
    Preconditions.checkState(index > triggerIndex && index < firstIndexAfterSubtree,
        "Cannot find sub-trigger containing index not in this tree.");
    ExecutableTrigger previous = null;
    for (ExecutableTrigger subTrigger : subTriggers) {
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
  public void invokeOnElement(Trigger.OnElementContext c) throws Exception {
    trigger.onElement(c.forTrigger(this));
  }

  /**
   * Invoke the {@link Trigger#onMerge} method for this trigger, ensuring that the bits are properly
   * updated.
   */
  public void invokeOnMerge(Trigger.OnMergeContext c) throws Exception {
    Trigger.OnMergeContext subContext = c.forTrigger(this);
    trigger.onMerge(subContext);
  }

  public boolean invokeShouldFire(Trigger.TriggerContext c) throws Exception {
    return trigger.shouldFire(c.forTrigger(this));
  }

  public void invokeOnFire(Trigger.TriggerContext c) throws Exception {
    trigger.onFire(c.forTrigger(this));
  }

  /**
   * Invoke clear for the current this trigger.
   */
  public void invokeClear(Trigger.TriggerContext c) throws Exception {
    trigger.clear(c.forTrigger(this));
  }

  /**
   * {@link ExecutableTrigger} that enforces the fact that the trigger should always FIRE_AND_FINISH
   * and never just FIRE.
   */
  private static class ExecutableOnceTrigger extends ExecutableTrigger {

    public ExecutableOnceTrigger(OnceTrigger trigger, int nextUnusedIndex) {
      super(trigger, nextUnusedIndex);
    }
  }
}
