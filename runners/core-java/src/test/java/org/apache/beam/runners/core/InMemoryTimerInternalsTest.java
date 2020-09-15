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
package org.apache.beam.runners.core;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.sdk.state.TimeDomain;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link InMemoryTimerInternals}. */
@RunWith(JUnit4.class)
public class InMemoryTimerInternalsTest {

  private static final StateNamespace NS1 = new StateNamespaceForTest("NS1");
  private static final String ID1 = "id1";
  private static final String ID2 = "id2";

  @Test
  public void testFiringEventTimers() throws Exception {
    InMemoryTimerInternals underTest = new InMemoryTimerInternals();
    TimerData eventTimer1 =
        TimerData.of(ID1, NS1, new Instant(19), new Instant(19), TimeDomain.EVENT_TIME);
    TimerData eventTimer2 =
        TimerData.of(ID2, NS1, new Instant(29), new Instant(29), TimeDomain.EVENT_TIME);

    underTest.setTimer(eventTimer1);
    underTest.setTimer(eventTimer2);

    underTest.advanceInputWatermark(new Instant(20));
    assertThat(underTest.removeNextEventTimer(), equalTo(eventTimer1));
    assertThat(underTest.removeNextEventTimer(), nullValue());

    // Advancing just a little shouldn't refire
    underTest.advanceInputWatermark(new Instant(21));
    assertThat(underTest.removeNextEventTimer(), nullValue());

    // Adding the timer and advancing a little should refire
    underTest.setTimer(eventTimer1);
    assertThat(underTest.removeNextEventTimer(), equalTo(eventTimer1));
    assertThat(underTest.removeNextEventTimer(), nullValue());

    // And advancing the rest of the way should still have the other timer
    underTest.advanceInputWatermark(new Instant(30));
    assertThat(underTest.removeNextEventTimer(), equalTo(eventTimer2));
    assertThat(underTest.removeNextEventTimer(), nullValue());
  }

  @Test
  public void testResetById() throws Exception {
    InMemoryTimerInternals underTest = new InMemoryTimerInternals();
    Instant earlyTimestamp = new Instant(13);
    Instant laterTimestamp = new Instant(42);

    underTest.advanceInputWatermark(new Instant(0));
    underTest.setTimer(NS1, ID1, "", earlyTimestamp, earlyTimestamp, TimeDomain.EVENT_TIME);
    underTest.setTimer(NS1, ID1, "", laterTimestamp, laterTimestamp, TimeDomain.EVENT_TIME);
    underTest.advanceInputWatermark(earlyTimestamp.plus(1L));
    assertThat(underTest.removeNextEventTimer(), nullValue());

    underTest.advanceInputWatermark(laterTimestamp.plus(1L));
    assertThat(
        underTest.removeNextEventTimer(),
        equalTo(TimerData.of(ID1, "", NS1, laterTimestamp, laterTimestamp, TimeDomain.EVENT_TIME)));
  }

  @Test
  public void testDeletionIdempotent() throws Exception {
    InMemoryTimerInternals underTest = new InMemoryTimerInternals();
    Instant timestamp = new Instant(42);
    underTest.setTimer(NS1, ID1, ID1, timestamp, timestamp, TimeDomain.EVENT_TIME);
    underTest.deleteTimer(NS1, ID1, ID1);
    underTest.deleteTimer(NS1, ID1, ID1);
  }

  @Test
  public void testDeletionById() throws Exception {
    InMemoryTimerInternals underTest = new InMemoryTimerInternals();
    Instant timestamp = new Instant(42);

    underTest.advanceInputWatermark(new Instant(0));
    underTest.setTimer(NS1, ID1, ID1, timestamp, timestamp, TimeDomain.EVENT_TIME);
    underTest.deleteTimer(NS1, ID1, ID1);
    underTest.advanceInputWatermark(new Instant(43));

    assertThat(underTest.removeNextEventTimer(), nullValue());
  }

  @Test
  public void testFiringProcessingTimeTimers() throws Exception {
    InMemoryTimerInternals underTest = new InMemoryTimerInternals();
    TimerData processingTime1 =
        TimerData.of(NS1, new Instant(19), new Instant(19), TimeDomain.PROCESSING_TIME);
    TimerData processingTime2 =
        TimerData.of(NS1, new Instant(29), new Instant(29), TimeDomain.PROCESSING_TIME);

    underTest.setTimer(processingTime1);
    underTest.setTimer(processingTime2);

    underTest.advanceProcessingTime(new Instant(20));
    assertThat(underTest.removeNextProcessingTimer(), equalTo(processingTime1));
    assertThat(underTest.removeNextProcessingTimer(), nullValue());

    // Advancing just a little shouldn't refire
    underTest.advanceProcessingTime(new Instant(21));
    assertThat(underTest.removeNextProcessingTimer(), nullValue());

    // Adding the timer and advancing a little should fire again
    underTest.setTimer(processingTime1);
    underTest.advanceProcessingTime(new Instant(21));
    assertThat(underTest.removeNextProcessingTimer(), equalTo(processingTime1));
    assertThat(underTest.removeNextProcessingTimer(), nullValue());

    // And advancing the rest of the way should still have the other timer
    underTest.advanceProcessingTime(new Instant(30));
    assertThat(underTest.removeNextProcessingTimer(), equalTo(processingTime2));
    assertThat(underTest.removeNextProcessingTimer(), nullValue());
  }

  @Test
  public void testTimerOrdering() throws Exception {
    InMemoryTimerInternals underTest = new InMemoryTimerInternals();
    TimerData eventTime1 =
        TimerData.of(NS1, new Instant(19), new Instant(19), TimeDomain.EVENT_TIME);
    TimerData processingTime1 =
        TimerData.of(NS1, new Instant(19), new Instant(19), TimeDomain.PROCESSING_TIME);
    TimerData synchronizedProcessingTime1 =
        TimerData.of(
            NS1, new Instant(19), new Instant(19), TimeDomain.SYNCHRONIZED_PROCESSING_TIME);
    TimerData eventTime2 =
        TimerData.of(NS1, new Instant(29), new Instant(29), TimeDomain.EVENT_TIME);
    TimerData processingTime2 =
        TimerData.of(NS1, new Instant(29), new Instant(29), TimeDomain.PROCESSING_TIME);
    TimerData synchronizedProcessingTime2 =
        TimerData.of(
            NS1, new Instant(29), new Instant(29), TimeDomain.SYNCHRONIZED_PROCESSING_TIME);

    underTest.setTimer(processingTime1);
    underTest.setTimer(eventTime1);
    underTest.setTimer(synchronizedProcessingTime1);
    underTest.setTimer(processingTime2);
    underTest.setTimer(eventTime2);
    underTest.setTimer(synchronizedProcessingTime2);

    assertThat(underTest.removeNextEventTimer(), nullValue());
    underTest.advanceInputWatermark(new Instant(30));
    assertThat(underTest.removeNextEventTimer(), equalTo(eventTime1));
    assertThat(underTest.removeNextEventTimer(), equalTo(eventTime2));
    assertThat(underTest.removeNextEventTimer(), nullValue());

    assertThat(underTest.removeNextProcessingTimer(), nullValue());
    underTest.advanceProcessingTime(new Instant(30));
    assertThat(underTest.removeNextProcessingTimer(), equalTo(processingTime1));
    assertThat(underTest.removeNextProcessingTimer(), equalTo(processingTime2));
    assertThat(underTest.removeNextProcessingTimer(), nullValue());

    assertThat(underTest.removeNextSynchronizedProcessingTimer(), nullValue());
    underTest.advanceSynchronizedProcessingTime(new Instant(30));
    assertThat(
        underTest.removeNextSynchronizedProcessingTimer(), equalTo(synchronizedProcessingTime1));
    assertThat(
        underTest.removeNextSynchronizedProcessingTimer(), equalTo(synchronizedProcessingTime2));
    assertThat(underTest.removeNextProcessingTimer(), nullValue());
  }

  @Test
  public void testDeduplicate() throws Exception {
    InMemoryTimerInternals underTest = new InMemoryTimerInternals();
    TimerData eventTime =
        TimerData.of(NS1, new Instant(19), new Instant(19), TimeDomain.EVENT_TIME);
    TimerData processingTime =
        TimerData.of(NS1, new Instant(19), new Instant(19), TimeDomain.PROCESSING_TIME);
    underTest.setTimer(eventTime);
    underTest.setTimer(eventTime);
    underTest.setTimer(processingTime);
    underTest.setTimer(processingTime);
    underTest.advanceProcessingTime(new Instant(20));
    underTest.advanceInputWatermark(new Instant(20));

    assertThat(underTest.removeNextProcessingTimer(), equalTo(processingTime));
    assertThat(underTest.removeNextProcessingTimer(), nullValue());
    assertThat(underTest.removeNextEventTimer(), equalTo(eventTime));
    assertThat(underTest.removeNextEventTimer(), nullValue());
  }
}
