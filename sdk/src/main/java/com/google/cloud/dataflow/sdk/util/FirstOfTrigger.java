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

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.Trigger.TriggerResult;

import java.util.List;

/**
 * Create a {@link CompositeTrigger} that fires once the first time any of its sub-triggers fire.
 *
 * @param <W> The type of windows this trigger operates on.
 */
public class FirstOfTrigger<W extends BoundedWindow> extends CompositeTrigger<W> {

  public FirstOfTrigger(List<Trigger<W>> subTriggers) {
    super(subTriggers);
  }

  @Override
  public TriggerResult onElement(TriggerContext<W> c, Object value, W window, WindowStatus status)
      throws Exception {
    // If all the sub-triggers have finished, we should have already finished, so we know there is
    // at least one unfinished trigger.

    SubTriggerExecutor subStates = subExecutor(c, window);
    for (int i : subStates.getUnfinishedTriggers()) {
      if (subStates.onElement(c, i, value, window, status).isFire()) {
        return TriggerResult.FIRE_AND_FINISH;
      }
    }

    return subStates.allFinished() ? TriggerResult.FINISH : TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onMerge(TriggerContext<W> c, Iterable<W> oldWindows, W newWindow)
      throws Exception {
    SubTriggerExecutor subStates = subExecutor(c, oldWindows, newWindow);
    if (subStates.allFinished()) {
      return TriggerResult.FINISH;
    }

    for (int i : subStates.getUnfinishedTriggers()) {
      if (subStates.onMerge(c, i, oldWindows, newWindow).isFire()) {
        return TriggerResult.FIRE_AND_FINISH;
      }
    }

    return subStates.allFinished() ? TriggerResult.FINISH : TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult afterChildTimer(
      TriggerContext<W> c, W window, int childIdx, TriggerResult result) throws Exception {
    if (result.isFire()) {
      return TriggerResult.FIRE_AND_FINISH;
    } else if (result.isFinish()) {
      // If the given child finished, we may need to mark final completion if there are no more
      // unfinished children.
      SubTriggerExecutor subStates = subExecutor(c, window);
      if (subStates.allFinished()) {
        return TriggerResult.FINISH;
      }
    }

    return TriggerResult.CONTINUE;
  }
}
