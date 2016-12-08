/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.util;

import static org.mockito.Matchers.argThat;

import com.google.cloud.dataflow.sdk.util.TimerInternals.TimerData;
import com.google.cloud.dataflow.sdk.util.state.StateNamespace;
import com.google.cloud.dataflow.sdk.util.state.StateNamespaceForTest;

import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for {@link BatchTimerInternals}.
 */
@RunWith(JUnit4.class)
public class BatchTimerInternalsTest {

  private static final StateNamespace NS1 = new StateNamespaceForTest("NS1");

  @Mock
  private ReduceFnRunner<?, ?, ?, ?> mockRunner;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  private static class TimersAre extends ArgumentMatcher<Iterable<TimerData>> {
    final List<TimerData> expectedTimers;
    TimersAre(List<TimerData> timers) {
      expectedTimers = timers;
    }

    @Override
    public boolean matches(Object actual) {
      if (actual == null || !(actual instanceof Iterable)) {
        return false;
      }
      @SuppressWarnings("unchecked")
      Iterable<TimerData> timers = (Iterable<TimerData>) actual;

      List<TimerData> actualTimers = new ArrayList();
      for (TimerData timer : timers) {
        actualTimers.add(timer);
      }
      return expectedTimers.equals(actualTimers);
    }

    @Override
    public String toString() {
      return "ordered timers " + expectedTimers.toString();
    }
  }

  private static TimersAre timersAre(TimerData... timers) {
    return new TimersAre(Arrays.asList(timers));
  }

  @Test
  public void testFiringTimers() throws Exception {
    BatchTimerInternals underTest = new BatchTimerInternals(new Instant(0));
    TimerData processingTime1 = TimerData.of(NS1, new Instant(19), TimeDomain.PROCESSING_TIME);
    TimerData processingTime2 = TimerData.of(NS1, new Instant(29), TimeDomain.PROCESSING_TIME);

    underTest.setTimer(processingTime1);
    underTest.setTimer(processingTime2);

    underTest.advanceProcessingTime(mockRunner, new Instant(20));
    Mockito.verify(mockRunner).onTimers(argThat(timersAre(processingTime1)));
    Mockito.verifyNoMoreInteractions(mockRunner);

    // Advancing just a little shouldn't refire
    underTest.advanceProcessingTime(mockRunner, new Instant(21));
    Mockito.verifyNoMoreInteractions(mockRunner);

    // Adding the timer and advancing a little should refire
    underTest.setTimer(processingTime1);
    underTest.advanceProcessingTime(mockRunner, new Instant(21));
    Mockito.verify(mockRunner, Mockito.times(2)).onTimers(argThat(timersAre(processingTime1)));
    Mockito.verifyNoMoreInteractions(mockRunner);

    // And advancing the rest of the way should still have the other timer
    underTest.advanceProcessingTime(mockRunner, new Instant(30));
    Mockito.verify(mockRunner).onTimers(argThat(timersAre(processingTime2)));
    Mockito.verifyNoMoreInteractions(mockRunner);
  }

  @Test
  public void testTimerOrdering() throws Exception {
    BatchTimerInternals underTest = new BatchTimerInternals(new Instant(0));
    TimerData watermarkTime1 = TimerData.of(NS1, new Instant(19), TimeDomain.EVENT_TIME);
    TimerData processingTime1 = TimerData.of(NS1, new Instant(19), TimeDomain.PROCESSING_TIME);
    TimerData watermarkTime2 = TimerData.of(NS1, new Instant(29), TimeDomain.EVENT_TIME);
    TimerData processingTime2 = TimerData.of(NS1, new Instant(29), TimeDomain.PROCESSING_TIME);

    underTest.setTimer(processingTime1);
    underTest.setTimer(watermarkTime1);
    underTest.setTimer(processingTime2);
    underTest.setTimer(watermarkTime2);

    underTest.advanceInputWatermark(mockRunner, new Instant(30));
    Mockito.verify(mockRunner).onTimers(argThat(timersAre(watermarkTime1, watermarkTime2)));
    Mockito.verifyNoMoreInteractions(mockRunner);

    underTest.advanceProcessingTime(mockRunner, new Instant(30));
    Mockito.verify(mockRunner).onTimers(argThat(timersAre(processingTime1, processingTime2)));
    Mockito.verifyNoMoreInteractions(mockRunner);
  }

  @Test
  public void testDeduplicate() throws Exception {
    BatchTimerInternals underTest = new BatchTimerInternals(new Instant(0));
    TimerData watermarkTime = TimerData.of(NS1, new Instant(19), TimeDomain.EVENT_TIME);
    TimerData processingTime = TimerData.of(NS1, new Instant(19), TimeDomain.PROCESSING_TIME);
    underTest.setTimer(watermarkTime);
    underTest.setTimer(watermarkTime);
    underTest.setTimer(processingTime);
    underTest.setTimer(processingTime);
    underTest.advanceProcessingTime(mockRunner, new Instant(20));
    Mockito.verify(mockRunner).onTimers(argThat(timersAre(processingTime)));
    underTest.advanceInputWatermark(mockRunner, new Instant(20));
    Mockito.verify(mockRunner).onTimers(argThat(timersAre(watermarkTime)));
    Mockito.verifyNoMoreInteractions(mockRunner);
  }
}
