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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

/**
 * A wrapper around a trigger used during execution. While an actual trigger may appear multiple
 * times (both in the same trigger expression and in other trigger expressions), the {@code
 * ExecutableTrigger} wrapped around them forms a tree (only one occurrence).
 */
public class ExecutableTriggerStateMachine implements Serializable {

  /** Store the index assigned to this trigger. */
  private final int triggerIndex;

  private final int firstIndexAfterSubtree;
  private final List<ExecutableTriggerStateMachine> subTriggers = new ArrayList<>();
  private final TriggerStateMachine trigger;

  public static <W extends BoundedWindow> ExecutableTriggerStateMachine create(
      TriggerStateMachine trigger) {
    return create(trigger, 0);
  }

  private static <W extends BoundedWindow> ExecutableTriggerStateMachine create(
      TriggerStateMachine trigger, int nextUnusedIndex) {

    return new ExecutableTriggerStateMachine(trigger, nextUnusedIndex);
  }

  public static <W extends BoundedWindow> ExecutableTriggerStateMachine createForOnceTrigger(
      TriggerStateMachine trigger, int nextUnusedIndex) {
    return new ExecutableTriggerStateMachine(trigger, nextUnusedIndex);
  }

  private ExecutableTriggerStateMachine(TriggerStateMachine trigger, int nextUnusedIndex) {
    this.trigger = checkNotNull(trigger, "trigger must not be null");
    this.triggerIndex = nextUnusedIndex++;

    if (trigger.subTriggers() != null) {
      for (TriggerStateMachine subTrigger : trigger.subTriggers()) {
        ExecutableTriggerStateMachine subExecutable = create(subTrigger, nextUnusedIndex);
        subTriggers.add(subExecutable);
        nextUnusedIndex = subExecutable.firstIndexAfterSubtree;
      }
    }
    firstIndexAfterSubtree = nextUnusedIndex;
  }

  public List<ExecutableTriggerStateMachine> subTriggers() {
    return subTriggers;
  }

  @Override
  public String toString() {
    return trigger.toString();
  }

  /**
   * Return the underlying trigger specification corresponding to this {@code ExecutableTrigger}.
   */
  public TriggerStateMachine getSpec() {
    return trigger;
  }

  public int getTriggerIndex() {
    return triggerIndex;
  }

  public final int getFirstIndexAfterSubtree() {
    return firstIndexAfterSubtree;
  }

  public boolean isCompatible(ExecutableTriggerStateMachine other) {
    return trigger.isCompatible(other.trigger);
  }

  public ExecutableTriggerStateMachine getSubTriggerContaining(int index) {
    checkNotNull(subTriggers);
    checkState(
        index > triggerIndex && index < firstIndexAfterSubtree,
        "Cannot find sub-trigger containing index not in this tree.");
    ExecutableTriggerStateMachine previous = null;
    for (ExecutableTriggerStateMachine subTrigger : subTriggers) {
      if (index < subTrigger.triggerIndex) {
        return previous;
      }
      previous = subTrigger;
    }
    return previous;
  }

  /**
   * Invoke the {@link TriggerStateMachine#onElement} method for this trigger, ensuring that the
   * bits are properly updated if the trigger finishes.
   */
  public void invokeOnElement(TriggerStateMachine.OnElementContext c) throws Exception {
    trigger.onElement(c.forTrigger(this));
  }

  /**
   * Invoke the {@link TriggerStateMachine#onMerge} method for this trigger, ensuring that the bits
   * are properly updated.
   */
  public void invokeOnMerge(TriggerStateMachine.OnMergeContext c) throws Exception {
    TriggerStateMachine.OnMergeContext subContext = c.forTrigger(this);
    trigger.onMerge(subContext);
  }

  public boolean invokeShouldFire(TriggerStateMachine.TriggerContext c) throws Exception {
    return trigger.shouldFire(c.forTrigger(this));
  }

  public void invokeOnFire(TriggerStateMachine.TriggerContext c) throws Exception {
    trigger.onFire(c.forTrigger(this));
  }

  /** Invoke clear for the current this trigger. */
  public void invokeClear(TriggerStateMachine.TriggerContext c) throws Exception {
    trigger.clear(c.forTrigger(this));
  }
}
