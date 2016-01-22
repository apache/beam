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
import com.google.cloud.dataflow.sdk.util.ExecutableTrigger;
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

  AfterFirst(List<Trigger<W>> subTriggers) {
    super(subTriggers);
    Preconditions.checkArgument(subTriggers.size() > 1);
  }

  /**
   * Returns an {@code AfterFirst} {@code Trigger} with the given subtriggers.
   */
  @SafeVarargs
  public static <W extends BoundedWindow> OnceTrigger<W> of(
      OnceTrigger<W>... triggers) {
    return new AfterFirst<W>(Arrays.<Trigger<W>>asList(triggers));
  }

  @Override
  public void onElement(OnElementContext c) throws Exception {
    for (ExecutableTrigger<W> subTrigger : c.trigger().subTriggers()) {
      subTrigger.invokeOnElement(c);
    }
  }

  @Override
  public void onMerge(OnMergeContext c) throws Exception {
    for (ExecutableTrigger<W> subTrigger : c.trigger().subTriggers()) {
      subTrigger.invokeOnMerge(c);
    }
    updateFinishedStatus(c);
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(W window) {
    // This trigger will fire after the earliest of its sub-triggers.
    Instant deadline = BoundedWindow.TIMESTAMP_MAX_VALUE;
    for (Trigger<W> subTrigger : subTriggers) {
      Instant subDeadline = subTrigger.getWatermarkThatGuaranteesFiring(window);
      if (deadline.isAfter(subDeadline)) {
        deadline = subDeadline;
      }
    }
    return deadline;
  }

  @Override
  public OnceTrigger<W> getContinuationTrigger(List<Trigger<W>> continuationTriggers) {
    return new AfterFirst<W>(continuationTriggers);
  }

  @Override
  public boolean shouldFire(Trigger<W>.TriggerContext context) throws Exception {
    for (ExecutableTrigger<W> subtrigger : context.trigger().subTriggers()) {
      if (context.forTrigger(subtrigger).trigger().isFinished()
          || subtrigger.invokeShouldFire(context)) {
        return true;
      }
    }
    return false;
  }

  @Override
  protected void onOnlyFiring(TriggerContext context) throws Exception {
    for (ExecutableTrigger<W> subtrigger : context.trigger().subTriggers()) {
      TriggerContext subContext = context.forTrigger(subtrigger);
      if (subtrigger.invokeShouldFire(subContext)) {
        // If the trigger is ready to fire, then do whatever it needs to do.
        subtrigger.invokeOnFire(subContext);
      } else {
        // If the trigger is not ready to fire, it is nonetheless true that whatever
        // pending pane it was tracking is now gone.
        subtrigger.invokeClear(subContext);
      }
    }
  }

  private void updateFinishedStatus(TriggerContext c) {
    boolean anyFinished = false;
    for (ExecutableTrigger<W> subTrigger : c.trigger().subTriggers()) {
      anyFinished |= c.forTrigger(subTrigger).trigger().isFinished();
    }
    c.trigger().setFinished(anyFinished);
  }
}
