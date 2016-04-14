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

import org.apache.beam.sdk.util.ExecutableTrigger;

import com.google.common.annotations.VisibleForTesting;

import org.joda.time.Instant;

import java.util.Arrays;
import java.util.List;

/**
 * Executes the {@code actual} trigger until it finishes or until the {@code until} trigger fires.
 */
class OrFinallyTrigger extends Trigger {

  private static final int ACTUAL = 0;
  private static final int UNTIL = 1;

  @VisibleForTesting OrFinallyTrigger(Trigger actual, Trigger.OnceTrigger until) {
    super(Arrays.asList(actual, until));
  }

  @Override
  public void onElement(OnElementContext c) throws Exception {
    c.trigger().subTrigger(ACTUAL).invokeOnElement(c);
    c.trigger().subTrigger(UNTIL).invokeOnElement(c);
  }

  @Override
  public void onMerge(OnMergeContext c) throws Exception {
    for (ExecutableTrigger subTrigger : c.trigger().subTriggers()) {
      subTrigger.invokeOnMerge(c);
    }
    updateFinishedState(c);
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(BoundedWindow window) {
    // This trigger fires once either the trigger or the until trigger fires.
    Instant actualDeadline = subTriggers.get(ACTUAL).getWatermarkThatGuaranteesFiring(window);
    Instant untilDeadline = subTriggers.get(UNTIL).getWatermarkThatGuaranteesFiring(window);
    return actualDeadline.isBefore(untilDeadline) ? actualDeadline : untilDeadline;
  }

  @Override
  public Trigger getContinuationTrigger(List<Trigger> continuationTriggers) {
    // Use OrFinallyTrigger instead of AfterFirst because the continuation of ACTUAL
    // may not be a OnceTrigger.
    return Repeatedly.forever(
        new OrFinallyTrigger(
            continuationTriggers.get(ACTUAL),
            (Trigger.OnceTrigger) continuationTriggers.get(UNTIL)));
  }

  @Override
  public boolean shouldFire(Trigger.TriggerContext context) throws Exception {
    return context.trigger().subTrigger(ACTUAL).invokeShouldFire(context)
        || context.trigger().subTrigger(UNTIL).invokeShouldFire(context);
  }

  @Override
  public void onFire(Trigger.TriggerContext context) throws Exception {
    ExecutableTrigger actualSubtrigger = context.trigger().subTrigger(ACTUAL);
    ExecutableTrigger untilSubtrigger = context.trigger().subTrigger(UNTIL);

    if (untilSubtrigger.invokeShouldFire(context)) {
      untilSubtrigger.invokeOnFire(context);
      actualSubtrigger.invokeClear(context);
    } else {
      // If until didn't fire, then the actual must have (or it is forbidden to call
      // onFire) so we are done only if actual is done.
      actualSubtrigger.invokeOnFire(context);
      // Do not clear the until trigger, because it tracks data cross firings.
    }
    updateFinishedState(context);
  }

  private void updateFinishedState(TriggerContext c) throws Exception {
    boolean anyStillFinished = false;
    for (ExecutableTrigger subTrigger : c.trigger().subTriggers()) {
      anyStillFinished |= c.forTrigger(subTrigger).trigger().isFinished();
    }
    c.trigger().setFinished(anyStillFinished);
  }
}
