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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.util.ExecutableTrigger;

import org.joda.time.Instant;

import java.util.Arrays;
import java.util.List;

/**
 * A composite {@link Trigger} that executes its sub-triggers in order.
 * Only one sub-trigger is executing at a time,
 * and any time it fires the {@code AfterEach} fires. When the currently executing
 * sub-trigger finishes, the {@code AfterEach} starts executing the next sub-trigger.
 *
 * <p>{@code AfterEach.inOrder(t1, t2, ...)} finishes when all of the sub-triggers have finished.
 *
 * <p>The following properties hold:
 * <ul>
 *   <li> {@code AfterEach.inOrder(AfterEach.inOrder(a, b), c)} behaves the same as
 *   {@code AfterEach.inOrder(a, b, c)} and {@code AfterEach.inOrder(a, AfterEach.inOrder(b, c)}.
 *   <li> {@code AfterEach.inOrder(Repeatedly.forever(a), b)} behaves the same as
 *   {@code Repeatedly.forever(a)}, since the repeated trigger never finishes.
 * </ul>
 *
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
 *            {@code Trigger}
 */
@Experimental(Experimental.Kind.TRIGGER)
public class AfterEach<W extends BoundedWindow> extends Trigger<W> {

  private AfterEach(List<Trigger<W>> subTriggers) {
    super(subTriggers);
    checkArgument(subTriggers.size() > 1);
  }

  /**
   * Returns an {@code AfterEach} {@code Trigger} with the given subtriggers.
   */
  @SafeVarargs
  public static <W extends BoundedWindow> Trigger<W> inOrder(Trigger<W>... triggers) {
    return new AfterEach<W>(Arrays.<Trigger<W>>asList(triggers));
  }

  @Override
  public void onElement(OnElementContext c) throws Exception {
    if (!c.trigger().isMerging()) {
      // If merges are not possible, we need only run the first unfinished subtrigger
      c.trigger().firstUnfinishedSubTrigger().invokeOnElement(c);
    } else {
      // If merges are possible, we need to run all subtriggers in parallel
      for (ExecutableTrigger<W> subTrigger :  c.trigger().subTriggers()) {
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
    for (ExecutableTrigger<W> subTrigger : context.trigger().subTriggers()) {
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
  public Instant getWatermarkThatGuaranteesFiring(W window) {
    // This trigger will fire at least once when the first trigger in the sequence
    // fires at least once.
    return subTriggers.get(0).getWatermarkThatGuaranteesFiring(window);
  }

  @Override
  public Trigger<W> getContinuationTrigger(List<Trigger<W>> continuationTriggers) {
    return Repeatedly.forever(new AfterFirst<W>(continuationTriggers));
  }

  @Override
  public boolean shouldFire(Trigger<W>.TriggerContext context) throws Exception {
    ExecutableTrigger<W> firstUnfinished = context.trigger().firstUnfinishedSubTrigger();
    return firstUnfinished.invokeShouldFire(context);
  }

  @Override
  public void onFire(Trigger<W>.TriggerContext context) throws Exception {
    context.trigger().firstUnfinishedSubTrigger().invokeOnFire(context);

    // Reset all subtriggers if in a merging context; any may be revived by merging so they are
    // all run in parallel for each pending pane.
    if (context.trigger().isMerging()) {
      for (ExecutableTrigger<W> subTrigger : context.trigger().subTriggers()) {
        subTrigger.invokeClear(context);
      }
    }

    updateFinishedState(context);
  }

  private void updateFinishedState(TriggerContext context) {
    context.trigger().setFinished(context.trigger().firstUnfinishedSubTrigger() == null);
  }
}
