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
 * Create a {@link Trigger} that fires and finishes once after all of its sub-triggers have fired.
 *
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
 *            {@code Trigger}
 */
@Experimental(Experimental.Kind.TRIGGER)
public class AfterAll<W extends BoundedWindow> extends OnceTrigger<W> {

  private AfterAll(List<Trigger<W>> subTriggers) {
    super(subTriggers);
    Preconditions.checkArgument(subTriggers.size() > 1);
  }

  /**
   * Returns an {@code AfterAll} {@code Trigger} with the given subtriggers.
   */
  @SafeVarargs
  public static <W extends BoundedWindow> OnceTrigger<W> of(
      OnceTrigger<W>... triggers) {
    return new AfterAll<W>(Arrays.<Trigger<W>>asList(triggers));
  }

  @Override
  public void onElement(OnElementContext c) throws Exception {
    for (ExecutableTrigger<W> subTrigger : c.trigger().unfinishedSubTriggers()) {
      // Since subTriggers are all OnceTriggers, they must either CONTINUE or FIRE_AND_FINISH.
      // invokeElement will automatically mark the finish bit if they return FIRE_AND_FINISH.
      subTrigger.invokeOnElement(c);
    }
  }

  @Override
  public void onMerge(OnMergeContext c) throws Exception {
    for (ExecutableTrigger<W> subTrigger : c.trigger().subTriggers()) {
      subTrigger.invokeOnMerge(c);
    }
    boolean allFinished = true;
    for (ExecutableTrigger<W> subTrigger1 : c.trigger().subTriggers()) {
      allFinished &= c.forTrigger(subTrigger1).trigger().isFinished();
    }
    c.trigger().setFinished(allFinished);
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(W window) {
    // This trigger will fire after the latest of its sub-triggers.
    Instant deadline = BoundedWindow.TIMESTAMP_MIN_VALUE;
    for (Trigger<W> subTrigger : subTriggers) {
      Instant subDeadline = subTrigger.getWatermarkThatGuaranteesFiring(window);
      if (deadline.isBefore(subDeadline)) {
        deadline = subDeadline;
      }
    }
    return deadline;
  }

  @Override
  public OnceTrigger<W> getContinuationTrigger(List<Trigger<W>> continuationTriggers) {
    return new AfterAll<W>(continuationTriggers);
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code true} if all subtriggers return {@code true}.
   */
  @Override
  public boolean shouldFire(TriggerContext context) throws Exception {
    for (ExecutableTrigger<W> subtrigger : context.trigger().subTriggers()) {
      if (!context.forTrigger(subtrigger).trigger().isFinished()
          && !subtrigger.invokeShouldFire(context)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Invokes {@link #onFire} for all subtriggers, eliding redundant calls to {@link #shouldFire}
   * because they all must be ready to fire.
   */
  @Override
  public void onOnlyFiring(TriggerContext context) throws Exception {
    for (ExecutableTrigger<W> subtrigger : context.trigger().subTriggers()) {
      subtrigger.invokeOnFire(context);
    }
  }
}
