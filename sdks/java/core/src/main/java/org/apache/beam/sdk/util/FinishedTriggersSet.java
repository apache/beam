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
package org.apache.beam.sdk.util;

import com.google.common.collect.Sets;
import java.util.Set;

/**
 * An implementation of {@link FinishedTriggers} atop a user-provided mutable {@link Set}.
 */
public class FinishedTriggersSet implements FinishedTriggers {

  private final Set<ExecutableTrigger> finishedTriggers;

  private FinishedTriggersSet(Set<ExecutableTrigger> finishedTriggers) {
    this.finishedTriggers = finishedTriggers;
  }

  public static FinishedTriggersSet fromSet(Set<ExecutableTrigger> finishedTriggers) {
    return new FinishedTriggersSet(finishedTriggers);
  }

  /**
   * Returns a mutable {@link Set} of the underlying triggers that are finished.
   */
  public Set<ExecutableTrigger> getFinishedTriggers() {
    return finishedTriggers;
  }

  @Override
  public boolean isFinished(ExecutableTrigger trigger) {
    return finishedTriggers.contains(trigger);
  }

  @Override
  public void setFinished(ExecutableTrigger trigger, boolean value) {
    if (value) {
      finishedTriggers.add(trigger);
    } else {
      finishedTriggers.remove(trigger);
    }
  }

  @Override
  public void clearRecursively(ExecutableTrigger trigger) {
    finishedTriggers.remove(trigger);
    for (ExecutableTrigger subTrigger : trigger.subTriggers()) {
      clearRecursively(subTrigger);
    }
  }

  @Override
  public FinishedTriggersSet copy() {
    return fromSet(Sets.newHashSet(finishedTriggers));
  }

}
