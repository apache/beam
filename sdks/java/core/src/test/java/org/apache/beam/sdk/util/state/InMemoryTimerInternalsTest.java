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
package org.apache.beam.sdk.util.state;

import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.TimerInternals.TimerData;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link InMemoryTimerInternals}.
 */
@RunWith(JUnit4.class)
public class InMemoryTimerInternalsTest {

  private static final StateNamespace NS1 = new StateNamespaceForTest("NS1");

  @Mock
  private TimerCallback timerCallback;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testFiringTimers() throws Exception {
    InMemoryTimerInternals underTest = new InMemoryTimerInternals();
    TimerData processingTime1 = TimerData.of(NS1, new Instant(19), TimeDomain.PROCESSING_TIME);
    TimerData processingTime2 = TimerData.of(NS1, new Instant(29), TimeDomain.PROCESSING_TIME);

    underTest.setTimer(processingTime1);
    underTest.setTimer(processingTime2);

    underTest.advanceProcessingTime(timerCallback, new Instant(20));
    Mockito.verify(timerCallback).onTimer(processingTime1);
    Mockito.verifyNoMoreInteractions(timerCallback);

    // Advancing just a little shouldn't refire
    underTest.advanceProcessingTime(timerCallback, new Instant(21));
    Mockito.verifyNoMoreInteractions(timerCallback);

    // Adding the timer and advancing a little should refire
    underTest.setTimer(processingTime1);
    Mockito.verify(timerCallback).onTimer(processingTime1);
    underTest.advanceProcessingTime(timerCallback, new Instant(21));
    Mockito.verifyNoMoreInteractions(timerCallback);

    // And advancing the rest of the way should still have the other timer
    underTest.advanceProcessingTime(timerCallback, new Instant(30));
    Mockito.verify(timerCallback).onTimer(processingTime2);
    Mockito.verifyNoMoreInteractions(timerCallback);
  }

  @Test
  public void testTimerOrdering() throws Exception {
    InMemoryTimerInternals underTest = new InMemoryTimerInternals();
    TimerData watermarkTime1 = TimerData.of(NS1, new Instant(19), TimeDomain.EVENT_TIME);
    TimerData processingTime1 = TimerData.of(NS1, new Instant(19), TimeDomain.PROCESSING_TIME);
    TimerData watermarkTime2 = TimerData.of(NS1, new Instant(29), TimeDomain.EVENT_TIME);
    TimerData processingTime2 = TimerData.of(NS1, new Instant(29), TimeDomain.PROCESSING_TIME);

    underTest.setTimer(processingTime1);
    underTest.setTimer(watermarkTime1);
    underTest.setTimer(processingTime2);
    underTest.setTimer(watermarkTime2);

    underTest.advanceInputWatermark(timerCallback, new Instant(30));
    Mockito.verify(timerCallback).onTimer(watermarkTime1);
    Mockito.verify(timerCallback).onTimer(watermarkTime2);
    Mockito.verifyNoMoreInteractions(timerCallback);

    underTest.advanceProcessingTime(timerCallback, new Instant(30));
    Mockito.verify(timerCallback).onTimer(processingTime1);
    Mockito.verify(timerCallback).onTimer(processingTime2);
    Mockito.verifyNoMoreInteractions(timerCallback);
  }

  @Test
  public void testDeduplicate() throws Exception {
    InMemoryTimerInternals underTest = new InMemoryTimerInternals();
    TimerData watermarkTime = TimerData.of(NS1, new Instant(19), TimeDomain.EVENT_TIME);
    TimerData processingTime = TimerData.of(NS1, new Instant(19), TimeDomain.PROCESSING_TIME);
    underTest.setTimer(watermarkTime);
    underTest.setTimer(watermarkTime);
    underTest.setTimer(processingTime);
    underTest.setTimer(processingTime);
    underTest.advanceProcessingTime(timerCallback, new Instant(20));
    underTest.advanceInputWatermark(timerCallback, new Instant(20));

    Mockito.verify(timerCallback).onTimer(processingTime);
    Mockito.verify(timerCallback).onTimer(watermarkTime);
    Mockito.verifyNoMoreInteractions(timerCallback);
  }
}
