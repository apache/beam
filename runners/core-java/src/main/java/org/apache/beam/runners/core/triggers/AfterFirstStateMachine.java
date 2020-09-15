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
 * Create a composite {@link TriggerStateMachine} that fires once after at least one of its
 * sub-triggers have fired.
 */
public class AfterFirstStateMachine extends TriggerStateMachine {

  AfterFirstStateMachine(List<TriggerStateMachine> subTriggers) {
    super(subTriggers);
    checkArgument(subTriggers.size() > 1);
  }

  /** Returns an {@code AfterFirst} {@code Trigger} with the given subtriggers. */
  @SafeVarargs
  public static TriggerStateMachine of(TriggerStateMachine... triggers) {
    return new AfterFirstStateMachine(Arrays.asList(triggers));
  }

  public static TriggerStateMachine of(Iterable<? extends TriggerStateMachine> triggers) {
    return new AfterFirstStateMachine(ImmutableList.copyOf(triggers));
  }

  @Override
  public void onElement(OnElementContext c) throws Exception {
    for (ExecutableTriggerStateMachine subTrigger : c.trigger().subTriggers()) {
      subTrigger.invokeOnElement(c);
    }
  }

  @Override
  public void onMerge(OnMergeContext c) throws Exception {
    for (ExecutableTriggerStateMachine subTrigger : c.trigger().subTriggers()) {
      subTrigger.invokeOnMerge(c);
    }
    updateFinishedStatus(c);
  }

  @Override
  public boolean shouldFire(TriggerStateMachine.TriggerContext context) throws Exception {
    for (ExecutableTriggerStateMachine subtrigger : context.trigger().subTriggers()) {
      if (context.forTrigger(subtrigger).trigger().isFinished()
          || subtrigger.invokeShouldFire(context)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void onFire(TriggerContext context) throws Exception {
    for (ExecutableTriggerStateMachine subTrigger : context.trigger().subTriggers()) {
      TriggerContext subContext = context.forTrigger(subTrigger);
      if (subTrigger.invokeShouldFire(subContext)) {
        // If the trigger is ready to fire, then do whatever it needs to do.
        subTrigger.invokeOnFire(subContext);
      } else {
        // If the trigger is not ready to fire, it is nonetheless true that whatever
        // pending pane it was tracking is now gone.
        subTrigger.invokeClear(subContext);
      }
    }
    context.trigger().setFinished(true);
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
    for (ExecutableTriggerStateMachine subTrigger : c.trigger().subTriggers()) {
      anyFinished |= c.forTrigger(subTrigger).trigger().isFinished();
    }
    c.trigger().setFinished(anyFinished);
  }
}
