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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import org.apache.beam.runners.core.triggers.TriggerStateMachine.OnceTriggerStateMachine;
import org.apache.beam.sdk.transforms.windowing.AfterAll;
import org.apache.beam.sdk.transforms.windowing.AfterDelayFromFirstElement;
import org.apache.beam.sdk.transforms.windowing.AfterEach;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.Never;
import org.apache.beam.sdk.transforms.windowing.Never.NeverTrigger;
import org.apache.beam.sdk.transforms.windowing.OrFinallyTrigger;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Trigger.OnceTrigger;
import org.apache.beam.sdk.util.TimeDomain;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests the {@link TriggerStateMachines} static utility methods. */
@RunWith(JUnit4.class)
public class TriggerStateMachinesTest {

  //
  // Tests for leaf trigger translation
  //

  @Test
  public void testStateMachineForAfterPane() {
    int count = 37;
    AfterPane trigger = AfterPane.elementCountAtLeast(count);
    AfterPaneStateMachine machine =
        (AfterPaneStateMachine) TriggerStateMachines.stateMachineForOnceTrigger(trigger);

    assertThat(machine.getElementCount(), equalTo(trigger.getElementCount()));
  }

  @Test
  public void testStateMachineForAfterProcessingTime() {
    Duration minutes = Duration.standardMinutes(94);
    Duration hours = Duration.standardHours(13);

    AfterDelayFromFirstElement trigger =
        AfterProcessingTime.pastFirstElementInPane().plusDelayOf(minutes).alignedTo(hours);

    AfterDelayFromFirstElementStateMachine machine =
        (AfterDelayFromFirstElementStateMachine)
            TriggerStateMachines.stateMachineForOnceTrigger(trigger);

    assertThat(machine.getTimeDomain(), equalTo(TimeDomain.PROCESSING_TIME));

    // This equality is function equality, but due to the structure of the code (no serialization)
    // it is OK to check
    assertThat(machine.getTimestampMappers(), equalTo(trigger.getTimestampMappers()));
  }

  @Test
  public void testStateMachineForAfterWatermark() {
    AfterWatermark.FromEndOfWindow trigger = AfterWatermark.pastEndOfWindow();
    AfterWatermarkStateMachine.FromEndOfWindow machine =
        (AfterWatermarkStateMachine.FromEndOfWindow)
            TriggerStateMachines.stateMachineForOnceTrigger(trigger);
    // No parameters, so if it doesn't crash, we win!
  }

  @Test
  public void testDefaultTriggerTranslation() {
    DefaultTrigger trigger = DefaultTrigger.of();
    DefaultTriggerStateMachine machine =
        (DefaultTriggerStateMachine)
            checkNotNull(TriggerStateMachines.stateMachineForTrigger(trigger));
    // No parameters, so if it doesn't crash, we win!
  }

  @Test
  public void testNeverTranslation() {
    NeverTrigger trigger = Never.ever();
    NeverStateMachine machine =
        (NeverStateMachine) checkNotNull(TriggerStateMachines.stateMachineForTrigger(trigger));
    // No parameters, so if it doesn't crash, we win!
  }

  //
  // Tests for composite trigger translation
  //
  // These check just that translation was invoked recursively using somewhat random
  // leaf subtriggers; by induction it all holds together. Beyond this, explicit tests
  // of particular triggers will suffice.

  private static final int ELEM_COUNT = 472;
  private static final Duration DELAY = Duration.standardSeconds(95673);

  private final OnceTrigger subtrigger1 = AfterPane.elementCountAtLeast(ELEM_COUNT);
  private final OnceTrigger subtrigger2 =
      AfterProcessingTime.pastFirstElementInPane().plusDelayOf(DELAY);

  private final OnceTriggerStateMachine submachine1 =
      TriggerStateMachines.stateMachineForOnceTrigger(subtrigger1);
  private final OnceTriggerStateMachine submachine2 =
      TriggerStateMachines.stateMachineForOnceTrigger(subtrigger2);

  @Test
  public void testAfterEachTranslation() {
    AfterEach trigger = AfterEach.inOrder(subtrigger1, subtrigger2);
    AfterEachStateMachine machine =
        (AfterEachStateMachine) TriggerStateMachines.stateMachineForTrigger(trigger);

    assertThat(machine, equalTo(AfterEachStateMachine.inOrder(submachine1, submachine2)));
  }

  @Test
  public void testAfterFirstTranslation() {
    AfterFirst trigger = AfterFirst.of(subtrigger1, subtrigger2);
    AfterFirstStateMachine machine =
        (AfterFirstStateMachine) TriggerStateMachines.stateMachineForTrigger(trigger);

    assertThat(machine, equalTo(AfterFirstStateMachine.of(submachine1, submachine2)));
  }

  @Test
  public void testAfterAllTranslation() {
    AfterAll trigger = AfterAll.of(subtrigger1, subtrigger2);
    AfterAllStateMachine machine =
        (AfterAllStateMachine) TriggerStateMachines.stateMachineForTrigger(trigger);

    assertThat(machine, equalTo(AfterAllStateMachine.of(submachine1, submachine2)));
  }

  @Test
  public void testAfterWatermarkEarlyTranslation() {
    AfterWatermark.AfterWatermarkEarlyAndLate trigger =
        AfterWatermark.pastEndOfWindow().withEarlyFirings(subtrigger1);
    AfterWatermarkStateMachine.AfterWatermarkEarlyAndLate machine =
        (AfterWatermarkStateMachine.AfterWatermarkEarlyAndLate)
            TriggerStateMachines.stateMachineForTrigger(trigger);

    assertThat(
        machine,
        equalTo(AfterWatermarkStateMachine.pastEndOfWindow().withEarlyFirings(submachine1)));
  }

  @Test
  public void testAfterWatermarkEarlyLateTranslation() {
    AfterWatermark.AfterWatermarkEarlyAndLate trigger =
        AfterWatermark.pastEndOfWindow().withEarlyFirings(subtrigger1).withLateFirings(subtrigger2);
    AfterWatermarkStateMachine.AfterWatermarkEarlyAndLate machine =
        (AfterWatermarkStateMachine.AfterWatermarkEarlyAndLate)
            TriggerStateMachines.stateMachineForTrigger(trigger);

    assertThat(
        machine,
        equalTo(
            AfterWatermarkStateMachine.pastEndOfWindow()
                .withEarlyFirings(submachine1)
                .withLateFirings(submachine2)));
  }

  @Test
  public void testOrFinallyTranslation() {
    OrFinallyTrigger trigger = subtrigger1.orFinally(subtrigger2);
    OrFinallyStateMachine machine =
        (OrFinallyStateMachine) TriggerStateMachines.stateMachineForTrigger(trigger);

    assertThat(machine, equalTo(submachine1.orFinally(submachine2)));
  }

  @Test
  public void testRepeatedlyTranslation() {
    Repeatedly trigger = Repeatedly.forever(subtrigger1);
    RepeatedlyStateMachine machine =
        (RepeatedlyStateMachine) TriggerStateMachines.stateMachineForTrigger(trigger);

    assertThat(machine, equalTo(RepeatedlyStateMachine.forever(submachine1)));
  }
}
