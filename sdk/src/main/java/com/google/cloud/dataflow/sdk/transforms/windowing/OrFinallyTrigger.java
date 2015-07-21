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

import com.google.common.annotations.VisibleForTesting;

import org.joda.time.Instant;

import java.util.Arrays;
import java.util.List;

/**
 * Executes the {@code actual} trigger until it finishes or until the {@code until} trigger fires.
 */
class OrFinallyTrigger<W extends BoundedWindow> extends Trigger<W> {

  private static final int ACTUAL = 0;
  private static final int UNTIL = 1;

  private static final long serialVersionUID = 0L;

  @VisibleForTesting OrFinallyTrigger(Trigger<W> actual, Trigger.OnceTrigger<W> until) {
    super(Arrays.asList(actual, until));
  }

  @Override
  public Trigger.TriggerResult onElement(OnElementContext c) throws Exception {
    Trigger.TriggerResult untilResult = c.trigger().subTrigger(UNTIL).invokeElement(c);
    if (untilResult != TriggerResult.CONTINUE) {
      return TriggerResult.FIRE_AND_FINISH;
    }

    return c.trigger().subTrigger(ACTUAL).invokeElement(c);
  }

  @Override
  public Trigger.MergeResult onMerge(OnMergeContext c) throws Exception {
    Trigger.MergeResult untilResult = c.trigger().subTrigger(UNTIL).invokeMerge(c);
    if (untilResult == MergeResult.ALREADY_FINISHED) {
      return MergeResult.ALREADY_FINISHED;
    } else if (untilResult.isFire()) {
      return MergeResult.FIRE_AND_FINISH;
    } else {
      // was CONTINUE -- so merge the underlying trigger
      return c.trigger().subTrigger(ACTUAL).invokeMerge(c);
    }
  }

  @Override
  public Trigger.TriggerResult onTimer(OnTimerContext c) throws Exception {
    Trigger.TriggerResult untilResult = c.trigger().subTrigger(UNTIL).invokeTimer(c);
    if (untilResult != TriggerResult.CONTINUE) {
      return TriggerResult.FIRE_AND_FINISH;
    }

    return c.trigger().subTrigger(ACTUAL).invokeTimer(c);
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(W window) {
    // This trigger fires once either the trigger or the until trigger fires.
    Instant actualDeadline = subTriggers.get(ACTUAL).getWatermarkThatGuaranteesFiring(window);
    Instant untilDeadline = subTriggers.get(UNTIL).getWatermarkThatGuaranteesFiring(window);
    return actualDeadline.isBefore(untilDeadline) ? actualDeadline : untilDeadline;
  }

  @Override
  public Trigger<W> getContinuationTrigger(List<Trigger<W>> continuationTriggers) {
    return new OrFinallyTrigger<W>(
        continuationTriggers.get(ACTUAL),
        (Trigger.OnceTrigger<W>) continuationTriggers.get(UNTIL));
  }
}
