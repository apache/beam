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
import com.google.common.base.Preconditions;

import java.util.List;

/**
 * Creates a trigger that executes each trigger in sequence. Any time the current trigger fires, the
 * sequence will fire. It moves on to the next trigger in the sequence after the current trigger
 * finishes.
 *
 * @param <W> The type of windows this trigger operates on.
 */
public class SequenceOfTrigger<W extends BoundedWindow> extends CompositeTrigger<W> {

  public SequenceOfTrigger(List<Trigger<W>> subTriggers) {
    super(subTriggers);
    Preconditions.checkArgument(subTriggers.size() > 1);
  }

  private TriggerResult result(TriggerResult subResult, SubTriggerExecutor subexecutor)
      throws Exception {
    return TriggerResult.valueOf(subResult.isFire(), subexecutor.allFinished());
  }

  @Override
  public TriggerResult onElement(TriggerContext<W> c, Object value, W window, WindowStatus status)
      throws Exception {
    // If all the sub-triggers have finished, we should have already finished, so we know there is
    // at least one unfinished trigger.

    SubTriggerExecutor subexecutor = subExecutor(c, window);

    // There must be at least one unfinished, because otherwise we would have finished the root.
    int current = subexecutor.firstUnfinished();
    return result(subexecutor.onElement(c, current, value, window, status), subexecutor);
  }

  @Override
  public TriggerResult onMerge(TriggerContext<W> c, Iterable<W> oldWindows, W newWindow)
      throws Exception {
    SubTriggerExecutor subexecutor = subExecutor(c, oldWindows, newWindow);

    // There must be at least one unfinished, because otherwise we would have finished the root.
    int current = subexecutor.firstUnfinished();
    return result(subexecutor.onMerge(c, current, oldWindows, newWindow), subexecutor);
  }

  @Override
  public TriggerResult afterChildTimer(
      TriggerContext<W> c, W window, int childIdx, TriggerResult result) throws Exception {
    SubTriggerExecutor subExecutor = subExecutor(c, window);
    if (childIdx != subExecutor.firstUnfinished()) {
      return TriggerResult.CONTINUE;
    }

    return result(result, subExecutor);
  }
}
