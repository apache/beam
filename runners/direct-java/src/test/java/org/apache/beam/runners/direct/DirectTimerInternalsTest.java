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
package org.apache.beam.runners.direct;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.direct.WatermarkManager.TimerUpdate;
import org.apache.beam.runners.direct.WatermarkManager.TimerUpdate.TimerUpdateBuilder;
import org.apache.beam.runners.direct.WatermarkManager.TransformWatermarks;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.state.TimeDomain;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link DirectTimerInternals}. */
@RunWith(JUnit4.class)
public class DirectTimerInternalsTest {
  private MockClock clock;
  @Mock private TransformWatermarks watermarks;

  private TimerUpdateBuilder timerUpdateBuilder;

  private DirectTimerInternals internals;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    clock = MockClock.fromInstant(new Instant(0));

    timerUpdateBuilder = TimerUpdate.builder(StructuralKey.of(1234, VarIntCoder.of()));

    internals = DirectTimerInternals.create(clock, watermarks, timerUpdateBuilder);
  }

  @Test
  public void setTimerAddsToBuilder() {
    TimerData eventTimer =
        TimerData.of(
            StateNamespaces.global(),
            new Instant(20145L),
            new Instant(20145L),
            TimeDomain.EVENT_TIME);
    TimerData processingTimer =
        TimerData.of(
            StateNamespaces.global(),
            new Instant(125555555L),
            new Instant(125555555L),
            TimeDomain.PROCESSING_TIME);
    TimerData synchronizedProcessingTimer =
        TimerData.of(
            StateNamespaces.global(),
            new Instant(98745632189L),
            new Instant(98745632189L),
            TimeDomain.SYNCHRONIZED_PROCESSING_TIME);
    internals.setTimer(eventTimer);
    internals.setTimer(processingTimer);
    internals.setTimer(synchronizedProcessingTimer);

    assertThat(
        internals.getTimerUpdate().getSetTimers(),
        containsInAnyOrder(eventTimer, synchronizedProcessingTimer, processingTimer));
  }

  @Test
  public void deleteTimerDeletesOnBuilder() {
    TimerData eventTimer =
        TimerData.of(
            StateNamespaces.global(),
            new Instant(20145L),
            new Instant(20145L),
            TimeDomain.EVENT_TIME);
    TimerData processingTimer =
        TimerData.of(
            StateNamespaces.global(),
            new Instant(125555555L),
            new Instant(125555555L),
            TimeDomain.PROCESSING_TIME);
    TimerData synchronizedProcessingTimer =
        TimerData.of(
            StateNamespaces.global(),
            new Instant(98745632189L),
            new Instant(98745632189L),
            TimeDomain.SYNCHRONIZED_PROCESSING_TIME);
    internals.deleteTimer(eventTimer);
    internals.deleteTimer(processingTimer);
    internals.deleteTimer(synchronizedProcessingTimer);

    assertThat(
        internals.getTimerUpdate().getDeletedTimers(),
        containsInAnyOrder(eventTimer, synchronizedProcessingTimer, processingTimer));
  }

  @Test
  public void getProcessingTimeIsClockNow() {
    assertThat(internals.currentProcessingTime(), equalTo(clock.now()));
    Instant oldProcessingTime = internals.currentProcessingTime();

    clock.advance(Duration.standardHours(12));

    assertThat(internals.currentProcessingTime(), equalTo(clock.now()));
    assertThat(
        internals.currentProcessingTime(),
        equalTo(oldProcessingTime.plus(Duration.standardHours(12))));
  }

  @Test
  public void getSynchronizedProcessingTimeIsWatermarkSynchronizedInputTime() {
    when(watermarks.getSynchronizedProcessingInputTime()).thenReturn(new Instant(12345L));
    assertThat(internals.currentSynchronizedProcessingTime(), equalTo(new Instant(12345L)));
  }

  @Test
  public void getInputWatermarkTimeUsesWatermarkTime() {
    when(watermarks.getInputWatermark()).thenReturn(new Instant(8765L));
    assertThat(internals.currentInputWatermarkTime(), equalTo(new Instant(8765L)));
  }

  @Test
  public void getOutputWatermarkTimeUsesWatermarkTime() {
    when(watermarks.getOutputWatermark()).thenReturn(new Instant(25525L));
    assertThat(internals.currentOutputWatermarkTime(), equalTo(new Instant(25525L)));
  }
}
