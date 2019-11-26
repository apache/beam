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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/**
 * A composite {@link TriggerStateMachine} that executes its sub-triggers in order. Only one
 * sub-trigger is executing at a time, and any time it fires the {@code AfterEach} fires. When the
 * currently executing sub-trigger finishes, the {@code AfterEach} starts executing the next
 * sub-trigger.
 *
 * <p>{@code AfterEach.inOrder(t1, t2, ...)} finishes when all of the sub-triggers have finished.
 *
 * <p>The following properties hold:
 *
 * <ul>
 *   <li>{@code AfterEach.inOrder(AfterEach.inOrder(a, b), c)} behaves the same as {@code
 *       AfterEach.inOrder(a, b, c)} and {@code AfterEach.inOrder(a, AfterEach.inOrder(b, c)}.
 *   <li>{@code AfterEach.inOrder(Repeatedly.forever(a), b)} behaves the same as {@code
 *       Repeatedly.forever(a)}, since the repeated trigger never finishes.
 * </ul>
 */
public class AfterEachStateMachine extends TriggerStateMachine {

  private AfterEachStateMachine(List<TriggerStateMachine> subTriggers) {
    super(subTriggers);
    checkArgument(subTriggers.size() > 1);
  }

  /** Returns an {@code AfterEach} {@code Trigger} with the given subtriggers. */
  @SafeVarargs
  public static TriggerStateMachine inOrder(TriggerStateMachine... triggers) {
    return new AfterEachStateMachine(Arrays.asList(triggers));
  }

  public static TriggerStateMachine inOrder(Iterable<? extends TriggerStateMachine> triggers) {
    return new AfterEachStateMachine(ImmutableList.copyOf(triggers));
  }

  @Override
  public void onElement(OnElementContext c) throws Exception {
    if (!c.trigger().isMerging()) {
      // If merges are not possible, we need only run the first unfinished subtrigger
      c.trigger().firstUnfinishedSubTrigger().invokeOnElement(c);
    } else {
      // If merges are possible, we need to run all subtriggers in parallel
      for (ExecutableTriggerStateMachine subTrigger : c.trigger().subTriggers()) {
        // Even if the subTrigger is done, it may be revived via merging and must have
        // adequate state.
        subTrigger.invokeOnElement(c);
      }
    }
  }

  @Override
  public void onMerge(OnMergeContext context) throws Exception {
    // If merging makes a subtrigger no-longer-finished, it will automatically
    // begin participating in shouldFire and onFire appropriately.

    // All the following triggers are retroactively "not started" but that is
    // also automatic because they are cleared whenever this trigger
    // fires.
    boolean priorTriggersAllFinished = true;
    for (ExecutableTriggerStateMachine subTrigger : context.trigger().subTriggers()) {
      if (priorTriggersAllFinished) {
        subTrigger.invokeOnMerge(context);
        priorTriggersAllFinished &= context.forTrigger(subTrigger).trigger().isFinished();
      } else {
        subTrigger.invokeClear(context);
      }
    }
    updateFinishedState(context);
  }

  @Override
  public boolean shouldFire(TriggerStateMachine.TriggerContext context) throws Exception {
    ExecutableTriggerStateMachine firstUnfinished = context.trigger().firstUnfinishedSubTrigger();
    return firstUnfinished.invokeShouldFire(context);
  }

  @Override
  public void onFire(TriggerStateMachine.TriggerContext context) throws Exception {
    context.trigger().firstUnfinishedSubTrigger().invokeOnFire(context);

    // Reset all subtriggers if in a merging context; any may be revived by merging so they are
    // all run in parallel for each pending pane.
    if (context.trigger().isMerging()) {
      for (ExecutableTriggerStateMachine subTrigger : context.trigger().subTriggers()) {
        subTrigger.invokeClear(context);
      }
    }

    updateFinishedState(context);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("AfterEach.inOrder(");
    Joiner.on(", ").appendTo(builder, subTriggers);
    builder.append(")");

    return builder.toString();
  }

  private void updateFinishedState(TriggerContext context) {
    context.trigger().setFinished(context.trigger().firstUnfinishedSubTrigger() == null);
  }
}
