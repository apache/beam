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
 * A {@link TriggerStateMachine} that fires and finishes once after all of its sub-triggers have
 * fired.
 */
public class AfterAllStateMachine extends TriggerStateMachine {

  private AfterAllStateMachine(List<TriggerStateMachine> subTriggers) {
    super(subTriggers);
    checkArgument(subTriggers.size() > 1);
  }

  /** Returns an {@code AfterAll} {@code Trigger} with the given subtriggers. */
  @SafeVarargs
  public static TriggerStateMachine of(TriggerStateMachine... triggers) {
    return new AfterAllStateMachine(Arrays.asList(triggers));
  }

  public static TriggerStateMachine of(Iterable<? extends TriggerStateMachine> triggers) {
    return new AfterAllStateMachine(ImmutableList.copyOf(triggers));
  }

  @Override
  public void onElement(OnElementContext c) throws Exception {
    for (ExecutableTriggerStateMachine subTrigger : c.trigger().unfinishedSubTriggers()) {
      // Since subTriggers are all OnceTriggers, they must either CONTINUE or FIRE_AND_FINISH.
      // invokeElement will automatically mark the finish bit if they return FIRE_AND_FINISH.
      subTrigger.invokeOnElement(c);
    }
  }

  @Override
  public void onMerge(OnMergeContext c) throws Exception {
    for (ExecutableTriggerStateMachine subTrigger : c.trigger().subTriggers()) {
      subTrigger.invokeOnMerge(c);
    }
    boolean allFinished = true;
    for (ExecutableTriggerStateMachine subTrigger1 : c.trigger().subTriggers()) {
      allFinished &= c.forTrigger(subTrigger1).trigger().isFinished();
    }
    c.trigger().setFinished(allFinished);
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code true} if all subtriggers return {@code true}.
   */
  @Override
  public boolean shouldFire(TriggerContext context) throws Exception {
    for (ExecutableTriggerStateMachine subTrigger : context.trigger().subTriggers()) {
      if (!context.forTrigger(subTrigger).trigger().isFinished()
          && !subTrigger.invokeShouldFire(context)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void onFire(TriggerContext context) throws Exception {
    for (ExecutableTriggerStateMachine subTrigger : context.trigger().subTriggers()) {
      subTrigger.invokeOnFire(context);
    }
    context.trigger().setFinished(true);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("AfterAll.of(");
    Joiner.on(", ").appendTo(builder, subTriggers);
    builder.append(")");
    return builder.toString();
  }
}
