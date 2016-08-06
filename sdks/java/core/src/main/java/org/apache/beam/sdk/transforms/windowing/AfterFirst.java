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
package org.apache.beam.sdk.transforms.windowing;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.windowing.Trigger.OnceTrigger;
import org.apache.beam.sdk.util.ExecutableTrigger;

import com.google.common.base.Joiner;

import org.joda.time.Instant;

import java.util.Arrays;
import java.util.List;

/**
 * Create a composite {@link Trigger} that fires once after at least one of its sub-triggers have
 * fired.
 */
@Experimental(Experimental.Kind.TRIGGER)
public class AfterFirst extends OnceTrigger {

  AfterFirst(List<Trigger> subTriggers) {
    super(subTriggers);
    checkArgument(subTriggers.size() > 1);
  }

  /**
   * Returns an {@code AfterFirst} {@code Trigger} with the given subtriggers.
   */
  public static OnceTrigger of(OnceTrigger... triggers) {
    return new AfterFirst(Arrays.<Trigger>asList(triggers));
  }

  @Override
  public void onElement(OnElementContext c) throws Exception {
    for (ExecutableTrigger subTrigger : c.trigger().subTriggers()) {
      subTrigger.invokeOnElement(c);
    }
  }

  @Override
  public void onMerge(OnMergeContext c) throws Exception {
    for (ExecutableTrigger subTrigger : c.trigger().subTriggers()) {
      subTrigger.invokeOnMerge(c);
    }
    updateFinishedStatus(c);
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(BoundedWindow window) {
    // This trigger will fire after the earliest of its sub-triggers.
    Instant deadline = BoundedWindow.TIMESTAMP_MAX_VALUE;
    for (Trigger subTrigger : subTriggers) {
      Instant subDeadline = subTrigger.getWatermarkThatGuaranteesFiring(window);
      if (deadline.isAfter(subDeadline)) {
        deadline = subDeadline;
      }
    }
    return deadline;
  }

  @Override
  public OnceTrigger getContinuationTrigger(List<Trigger> continuationTriggers) {
    return new AfterFirst(continuationTriggers);
  }

  @Override
  public boolean shouldFire(Trigger.TriggerContext context) throws Exception {
    for (ExecutableTrigger subtrigger : context.trigger().subTriggers()) {
      if (context.forTrigger(subtrigger).trigger().isFinished()
          || subtrigger.invokeShouldFire(context)) {
        return true;
      }
    }
    return false;
  }

  @Override
  protected void onOnlyFiring(TriggerContext context) throws Exception {
    for (ExecutableTrigger subtrigger : context.trigger().subTriggers()) {
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

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("AfterFirst.of(");
    Joiner.on(", ").appendTo(builder, subTriggers);
    builder.append(")");

    return builder.toString();
  }

  private void updateFinishedStatus(TriggerContext c) {
    boolean anyFinished = false;
    for (ExecutableTrigger subTrigger : c.trigger().subTriggers()) {
      anyFinished |= c.forTrigger(subTrigger).trigger().isFinished();
    }
    c.trigger().setFinished(anyFinished);
  }
}
