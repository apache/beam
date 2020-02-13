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

import java.util.Arrays;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

/**
 * Executes the {@code actual} trigger until it finishes or until the {@code until} trigger fires.
 */
class OrFinallyStateMachine extends TriggerStateMachine {

  private static final int ACTUAL = 0;
  private static final int UNTIL = 1;

  @VisibleForTesting
  OrFinallyStateMachine(TriggerStateMachine actual, TriggerStateMachine until) {
    super(Arrays.asList(actual, until));
  }

  @Override
  public void onElement(OnElementContext c) throws Exception {
    c.trigger().subTrigger(ACTUAL).invokeOnElement(c);
    c.trigger().subTrigger(UNTIL).invokeOnElement(c);
  }

  @Override
  public void onMerge(OnMergeContext c) throws Exception {
    for (ExecutableTriggerStateMachine subTrigger : c.trigger().subTriggers()) {
      subTrigger.invokeOnMerge(c);
    }
    updateFinishedState(c);
  }

  @Override
  public boolean shouldFire(TriggerStateMachine.TriggerContext context) throws Exception {
    return context.trigger().subTrigger(ACTUAL).invokeShouldFire(context)
        || context.trigger().subTrigger(UNTIL).invokeShouldFire(context);
  }

  @Override
  public void onFire(TriggerStateMachine.TriggerContext context) throws Exception {
    ExecutableTriggerStateMachine actualSubtrigger = context.trigger().subTrigger(ACTUAL);
    ExecutableTriggerStateMachine untilSubtrigger = context.trigger().subTrigger(UNTIL);

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

  @Override
  public String toString() {
    return String.format("%s.orFinally(%s)", subTriggers.get(ACTUAL), subTriggers.get(UNTIL));
  }

  private void updateFinishedState(TriggerContext c) throws Exception {
    boolean anyStillFinished = false;
    for (ExecutableTriggerStateMachine subTrigger : c.trigger().subTriggers()) {
      anyStillFinished |= c.forTrigger(subTrigger).trigger().isFinished();
    }
    c.trigger().setFinished(anyStillFinished);
  }
}
