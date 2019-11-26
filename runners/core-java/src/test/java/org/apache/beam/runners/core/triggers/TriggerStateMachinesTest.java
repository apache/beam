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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.state.TimeDomain;
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
    RunnerApi.Trigger trigger =
        RunnerApi.Trigger.newBuilder()
            .setElementCount(RunnerApi.Trigger.ElementCount.newBuilder().setElementCount(count))
            .build();

    AfterPaneStateMachine machine =
        (AfterPaneStateMachine) TriggerStateMachines.stateMachineForTrigger(trigger);

    assertThat(machine.getElementCount(), equalTo(trigger.getElementCount().getElementCount()));
  }

  // TODO: make these all build the proto
  @Test
  public void testStateMachineForAfterProcessingTime() {
    Duration minutes = Duration.standardMinutes(94);
    Duration hours = Duration.standardHours(13);

    RunnerApi.Trigger trigger =
        RunnerApi.Trigger.newBuilder()
            .setAfterProcessingTime(
                RunnerApi.Trigger.AfterProcessingTime.newBuilder()
                    .addTimestampTransforms(
                        RunnerApi.TimestampTransform.newBuilder()
                            .setDelay(
                                RunnerApi.TimestampTransform.Delay.newBuilder()
                                    .setDelayMillis(minutes.getMillis())))
                    .addTimestampTransforms(
                        RunnerApi.TimestampTransform.newBuilder()
                            .setAlignTo(
                                RunnerApi.TimestampTransform.AlignTo.newBuilder()
                                    .setPeriod(hours.getMillis()))))
            .build();

    AfterDelayFromFirstElementStateMachine machine =
        (AfterDelayFromFirstElementStateMachine)
            TriggerStateMachines.stateMachineForTrigger(trigger);

    assertThat(machine.getTimeDomain(), equalTo(TimeDomain.PROCESSING_TIME));
  }

  @Test
  public void testStateMachineForAfterWatermark() {
    RunnerApi.Trigger trigger =
        RunnerApi.Trigger.newBuilder()
            .setAfterEndOfWindow(RunnerApi.Trigger.AfterEndOfWindow.getDefaultInstance())
            .build();
    AfterWatermarkStateMachine.FromEndOfWindow machine =
        (AfterWatermarkStateMachine.FromEndOfWindow)
            TriggerStateMachines.stateMachineForTrigger(trigger);

    assertThat(
        TriggerStateMachines.stateMachineForTrigger(trigger),
        instanceOf(AfterWatermarkStateMachine.FromEndOfWindow.class));
  }

  @Test
  public void testDefaultTriggerTranslation() {
    RunnerApi.Trigger trigger =
        RunnerApi.Trigger.newBuilder()
            .setDefault(RunnerApi.Trigger.Default.getDefaultInstance())
            .build();

    assertThat(
        TriggerStateMachines.stateMachineForTrigger(trigger),
        instanceOf(DefaultTriggerStateMachine.class));
  }

  @Test
  public void testNeverTranslation() {
    RunnerApi.Trigger trigger =
        RunnerApi.Trigger.newBuilder()
            .setNever(RunnerApi.Trigger.Never.getDefaultInstance())
            .build();
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

  private final RunnerApi.Trigger subtrigger1 =
      RunnerApi.Trigger.newBuilder()
          .setElementCount(RunnerApi.Trigger.ElementCount.newBuilder().setElementCount(ELEM_COUNT))
          .build();
  private final RunnerApi.Trigger subtrigger2 =
      RunnerApi.Trigger.newBuilder()
          .setAfterProcessingTime(
              RunnerApi.Trigger.AfterProcessingTime.newBuilder()
                  .addTimestampTransforms(
                      RunnerApi.TimestampTransform.newBuilder()
                          .setDelay(
                              RunnerApi.TimestampTransform.Delay.newBuilder()
                                  .setDelayMillis(DELAY.getMillis()))))
          .build();

  private final TriggerStateMachine submachine1 =
      TriggerStateMachines.stateMachineForTrigger(subtrigger1);
  private final TriggerStateMachine submachine2 =
      TriggerStateMachines.stateMachineForTrigger(subtrigger2);

  @Test
  public void testAfterEachTranslation() {
    RunnerApi.Trigger trigger =
        RunnerApi.Trigger.newBuilder()
            .setAfterEach(
                RunnerApi.Trigger.AfterEach.newBuilder()
                    .addSubtriggers(subtrigger1)
                    .addSubtriggers(subtrigger2))
            .build();
    AfterEachStateMachine machine =
        (AfterEachStateMachine) TriggerStateMachines.stateMachineForTrigger(trigger);

    assertThat(machine, equalTo(AfterEachStateMachine.inOrder(submachine1, submachine2)));
  }

  @Test
  public void testAfterFirstTranslation() {
    RunnerApi.Trigger trigger =
        RunnerApi.Trigger.newBuilder()
            .setAfterAny(
                RunnerApi.Trigger.AfterAny.newBuilder()
                    .addSubtriggers(subtrigger1)
                    .addSubtriggers(subtrigger2))
            .build();
    AfterFirstStateMachine machine =
        (AfterFirstStateMachine) TriggerStateMachines.stateMachineForTrigger(trigger);

    assertThat(machine, equalTo(AfterFirstStateMachine.of(submachine1, submachine2)));
  }

  @Test
  public void testAfterAllTranslation() {
    RunnerApi.Trigger trigger =
        RunnerApi.Trigger.newBuilder()
            .setAfterAll(
                RunnerApi.Trigger.AfterAll.newBuilder()
                    .addSubtriggers(subtrigger1)
                    .addSubtriggers(subtrigger2))
            .build();
    AfterAllStateMachine machine =
        (AfterAllStateMachine) TriggerStateMachines.stateMachineForTrigger(trigger);

    assertThat(machine, equalTo(AfterAllStateMachine.of(submachine1, submachine2)));
  }

  @Test
  public void testAfterWatermarkEarlyTranslation() {
    RunnerApi.Trigger trigger =
        RunnerApi.Trigger.newBuilder()
            .setAfterEndOfWindow(
                RunnerApi.Trigger.AfterEndOfWindow.newBuilder().setEarlyFirings(subtrigger1))
            .build();
    AfterWatermarkStateMachine.AfterWatermarkEarlyAndLate machine =
        (AfterWatermarkStateMachine.AfterWatermarkEarlyAndLate)
            TriggerStateMachines.stateMachineForTrigger(trigger);

    assertThat(
        machine,
        equalTo(AfterWatermarkStateMachine.pastEndOfWindow().withEarlyFirings(submachine1)));
  }

  @Test
  public void testAfterWatermarkEarlyLateTranslation() {
    RunnerApi.Trigger trigger =
        RunnerApi.Trigger.newBuilder()
            .setAfterEndOfWindow(
                RunnerApi.Trigger.AfterEndOfWindow.newBuilder()
                    .setEarlyFirings(subtrigger1)
                    .setLateFirings(subtrigger2))
            .build();
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
    RunnerApi.Trigger trigger =
        RunnerApi.Trigger.newBuilder()
            .setOrFinally(
                RunnerApi.Trigger.OrFinally.newBuilder()
                    .setMain(subtrigger1)
                    .setFinally(subtrigger2))
            .build();
    OrFinallyStateMachine machine =
        (OrFinallyStateMachine) TriggerStateMachines.stateMachineForTrigger(trigger);

    assertThat(machine, equalTo(submachine1.orFinally(submachine2)));
  }

  @Test
  public void testRepeatedlyTranslation() {
    RunnerApi.Trigger trigger =
        RunnerApi.Trigger.newBuilder()
            .setRepeat(RunnerApi.Trigger.Repeat.newBuilder().setSubtrigger(subtrigger1))
            .build();
    RepeatedlyStateMachine machine =
        (RepeatedlyStateMachine) TriggerStateMachines.stateMachineForTrigger(trigger);

    assertThat(machine, equalTo(RepeatedlyStateMachine.forever(submachine1)));
  }
}
