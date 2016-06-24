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
package org.apache.beam.runners.core.reactors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.core.reactors.TriggerReactor.OnceTriggerReactor;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

/**
 * A wrapper around a trigger used during execution. While an actual trigger may appear multiple
 * times (both in the same trigger expression and in other trigger expressions), the
 * {@code ExecutableTrigger} wrapped around them forms a tree (only one occurrence).
 */
public class ExecutableTriggerReactor implements Serializable {

  /** Store the index assigned to this trigger. */
  private final int triggerIndex;
  private final int firstIndexAfterSubtree;
  private final List<ExecutableTriggerReactor> subTriggers = new ArrayList<>();
  private final TriggerReactor trigger;

  public static <W extends BoundedWindow> ExecutableTriggerReactor create(TriggerReactor trigger) {
    return create(trigger, 0);
  }

  private static <W extends BoundedWindow> ExecutableTriggerReactor create(
      TriggerReactor trigger, int nextUnusedIndex) {
    if (trigger instanceof OnceTriggerReactor) {
      return new ExecutableOnceTriggerReactor((OnceTriggerReactor) trigger, nextUnusedIndex);
    } else {
      return new ExecutableTriggerReactor(trigger, nextUnusedIndex);
    }
  }

  public static <W extends BoundedWindow> ExecutableTriggerReactor createForOnceTrigger(
      OnceTriggerReactor trigger, int nextUnusedIndex) {
    return new ExecutableOnceTriggerReactor(trigger, nextUnusedIndex);
  }

  private ExecutableTriggerReactor(TriggerReactor trigger, int nextUnusedIndex) {
    this.trigger = checkNotNull(trigger, "trigger must not be null");
    this.triggerIndex = nextUnusedIndex++;

    if (trigger.subTriggers() != null) {
      for (TriggerReactor subTrigger : trigger.subTriggers()) {
        ExecutableTriggerReactor subExecutable = create(subTrigger, nextUnusedIndex);
        subTriggers.add(subExecutable);
        nextUnusedIndex = subExecutable.firstIndexAfterSubtree;
      }
    }
    firstIndexAfterSubtree = nextUnusedIndex;
  }

  public List<ExecutableTriggerReactor> subTriggers() {
    return subTriggers;
  }

  @Override
  public String toString() {
    return trigger.toString();
  }

  /**
   * Return the underlying trigger specification corresponding to this {@code ExecutableTrigger}.
   */
  public TriggerReactor getSpec() {
    return trigger;
  }

  public int getTriggerIndex() {
    return triggerIndex;
  }

  public final int getFirstIndexAfterSubtree() {
    return firstIndexAfterSubtree;
  }

  public boolean isCompatible(ExecutableTriggerReactor other) {
    return trigger.isCompatible(other.trigger);
  }

  public ExecutableTriggerReactor getSubTriggerContaining(int index) {
    checkNotNull(subTriggers);
    checkState(index > triggerIndex && index < firstIndexAfterSubtree,
        "Cannot find sub-trigger containing index not in this tree.");
    ExecutableTriggerReactor previous = null;
    for (ExecutableTriggerReactor subTrigger : subTriggers) {
      if (index < subTrigger.triggerIndex) {
        return previous;
      }
      previous = subTrigger;
    }
    return previous;
  }

  /**
   * Invoke the {@link TriggerReactor#onElement} method for this trigger, ensuring that the bits are
   * properly updated if the trigger finishes.
   */
  public void invokeOnElement(TriggerReactor.OnElementContext c) throws Exception {
    trigger.onElement(c.forTrigger(this));
  }

  /**
   * Invoke the {@link TriggerReactor#onMerge} method for this trigger, ensuring that the bits are
   * properly updated.
   */
  public void invokeOnMerge(TriggerReactor.OnMergeContext c) throws Exception {
    TriggerReactor.OnMergeContext subContext = c.forTrigger(this);
    trigger.onMerge(subContext);
  }

  public boolean invokeShouldFire(TriggerReactor.TriggerContext c) throws Exception {
    return trigger.shouldFire(c.forTrigger(this));
  }

  public void invokeOnFire(TriggerReactor.TriggerContext c) throws Exception {
    trigger.onFire(c.forTrigger(this));
  }

  /**
   * Invoke clear for the current this trigger.
   */
  public void invokeClear(TriggerReactor.TriggerContext c) throws Exception {
    trigger.clear(c.forTrigger(this));
  }

  /**
   * {@link ExecutableTriggerReactor} that enforces the fact that the trigger should always
   * FIRE_AND_FINISH and never just FIRE.
   */
  private static class ExecutableOnceTriggerReactor extends ExecutableTriggerReactor {

    public ExecutableOnceTriggerReactor(OnceTriggerReactor trigger, int nextUnusedIndex) {
      super(trigger, nextUnusedIndex);
    }
  }
}
