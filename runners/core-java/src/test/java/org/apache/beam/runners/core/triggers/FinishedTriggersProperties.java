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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Generalized tests for {@link FinishedTriggers} implementations. */
public class FinishedTriggersProperties {
  /**
   * Tests that for the provided trigger and {@link FinishedTriggers}, when the trigger is set
   * finished, it is correctly reported as finished.
   */
  public static void verifyGetAfterSet(
      FinishedTriggers finishedSet, ExecutableTriggerStateMachine trigger) {
    assertFalse(finishedSet.isFinished(trigger));
    finishedSet.setFinished(trigger, true);
    assertTrue(finishedSet.isFinished(trigger));
  }

  /**
   * For a few arbitrary triggers, tests that when the trigger is set finished it is correctly
   * reported as finished.
   */
  public static void verifyGetAfterSet(FinishedTriggers finishedSet) {
    ExecutableTriggerStateMachine trigger =
        ExecutableTriggerStateMachine.create(
            AfterAllStateMachine.of(
                AfterFirstStateMachine.of(
                    AfterPaneStateMachine.elementCountAtLeast(3),
                    AfterWatermarkStateMachine.pastEndOfWindow()),
                AfterAllStateMachine.of(
                    AfterPaneStateMachine.elementCountAtLeast(10),
                    AfterProcessingTimeStateMachine.pastFirstElementInPane())));

    verifyGetAfterSet(finishedSet, trigger);
    verifyGetAfterSet(finishedSet, trigger.subTriggers().get(0).subTriggers().get(1));
    verifyGetAfterSet(finishedSet, trigger.subTriggers().get(0));
    verifyGetAfterSet(finishedSet, trigger.subTriggers().get(1));
    verifyGetAfterSet(finishedSet, trigger.subTriggers().get(1).subTriggers().get(1));
    verifyGetAfterSet(finishedSet, trigger.subTriggers().get(1).subTriggers().get(0));
  }

  /**
   * Tests that clearing a trigger recursively clears all of that triggers subTriggers, but no
   * others.
   */
  public static void verifyClearRecursively(FinishedTriggers finishedSet) {
    ExecutableTriggerStateMachine trigger =
        ExecutableTriggerStateMachine.create(
            AfterAllStateMachine.of(
                AfterFirstStateMachine.of(
                    AfterPaneStateMachine.elementCountAtLeast(3),
                    AfterWatermarkStateMachine.pastEndOfWindow()),
                AfterAllStateMachine.of(
                    AfterPaneStateMachine.elementCountAtLeast(10),
                    AfterProcessingTimeStateMachine.pastFirstElementInPane())));

    // Set them all finished. This method is not on a trigger as it makes no sense outside tests.
    setFinishedRecursively(finishedSet, trigger);
    assertTrue(finishedSet.isFinished(trigger));
    assertTrue(finishedSet.isFinished(trigger.subTriggers().get(0)));
    assertTrue(finishedSet.isFinished(trigger.subTriggers().get(0).subTriggers().get(0)));
    assertTrue(finishedSet.isFinished(trigger.subTriggers().get(0).subTriggers().get(1)));

    // Clear just the second AfterAll
    finishedSet.clearRecursively(trigger.subTriggers().get(1));

    // Check that the first and all that are still finished
    assertTrue(finishedSet.isFinished(trigger));
    verifyFinishedRecursively(finishedSet, trigger.subTriggers().get(0));
    verifyUnfinishedRecursively(finishedSet, trigger.subTriggers().get(1));
  }

  private static void setFinishedRecursively(
      FinishedTriggers finishedSet, ExecutableTriggerStateMachine trigger) {
    finishedSet.setFinished(trigger, true);
    for (ExecutableTriggerStateMachine subTrigger : trigger.subTriggers()) {
      setFinishedRecursively(finishedSet, subTrigger);
    }
  }

  private static void verifyFinishedRecursively(
      FinishedTriggers finishedSet, ExecutableTriggerStateMachine trigger) {
    assertTrue(finishedSet.isFinished(trigger));
    for (ExecutableTriggerStateMachine subTrigger : trigger.subTriggers()) {
      verifyFinishedRecursively(finishedSet, subTrigger);
    }
  }

  private static void verifyUnfinishedRecursively(
      FinishedTriggers finishedSet, ExecutableTriggerStateMachine trigger) {
    assertFalse(finishedSet.isFinished(trigger));
    for (ExecutableTriggerStateMachine subTrigger : trigger.subTriggers()) {
      verifyUnfinishedRecursively(finishedSet, subTrigger);
    }
  }
}
