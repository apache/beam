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
package org.apache.beam.runners.spark.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.spark.stateful.SparkTimerInternals;
import org.apache.beam.runners.spark.translation.AbstractInOutIterator;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link TimerUtils}. */
public class TimerUtilsTest {

  @Mock private SparkTimerInternals mockTimerInternals;
  @Mock private WindowingStrategy<?, IntervalWindow> mockWindowingStrategy;
  @Mock private AbstractInOutIterator<?, ?, ?> mockIterator;
  @Mock private TimerInternals.TimerData expiredTimer;
  @Mock private TimerInternals.TimerData activeTimer;
  @Mock private IntervalWindow mockWindow;

  private static final Instant NOW = new Instant(1000L);
  private static final Duration ALLOWED_LATENESS = Duration.standardMinutes(5);

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    StateNamespace namespace = StateNamespaces.window(IntervalWindow.getCoder(), mockWindow);

    // Set up an expired event-time timer (timestamp + allowed lateness < watermark)
    when(expiredTimer.getTimestamp())
        .thenReturn(NOW.minus(ALLOWED_LATENESS.plus(Duration.standardMinutes(1))));
    when(expiredTimer.getDomain()).thenReturn(TimeDomain.EVENT_TIME);
    when(expiredTimer.getNamespace()).thenReturn(namespace);

    // Set up a non-expired event-time timer (timestamp + allowed lateness > watermark)
    when(activeTimer.getTimestamp()).thenReturn(NOW);
    when(activeTimer.getDomain()).thenReturn(TimeDomain.EVENT_TIME);
    when(activeTimer.getNamespace()).thenReturn(namespace);

    // Configure the mocks
    when(mockWindowingStrategy.getAllowedLateness()).thenReturn(ALLOWED_LATENESS);
    when(mockTimerInternals.currentInputWatermarkTime()).thenReturn(NOW);
    when(mockTimerInternals.currentProcessingTime()).thenReturn(NOW);
  }

  @Test
  public void testTriggerExpiredTimers() {
    // Set up the mock timer internals to return both timers
    when(mockTimerInternals.getTimers()).thenReturn(Arrays.asList(expiredTimer, activeTimer));

    // Call the method under test
    TimerUtils.triggerExpiredTimers(mockTimerInternals, mockWindowingStrategy, mockIterator);

    // Verify that fireTimer was called only for the expired timer
    verify(mockIterator).fireTimer(expiredTimer);
    verify(mockIterator, never()).fireTimer(activeTimer);
  }

  @Test
  public void testTriggerExpiredTimersWithNoExpiredTimers() {
    // Set up the mock timer internals to return only the active timer
    when(mockTimerInternals.getTimers()).thenReturn(Collections.singletonList(activeTimer));

    // Call the method under test
    TimerUtils.triggerExpiredTimers(mockTimerInternals, mockWindowingStrategy, mockIterator);

    // Verify that fireTimer was not called for any timer
    verify(mockIterator, never()).fireTimer(any());
  }

  @Test
  public void testTriggerExpiredTimersWithEmptyTimers() {
    // Set up the mock timer internals to return an empty list
    when(mockTimerInternals.getTimers()).thenReturn(Collections.emptyList());

    // Call the method under test
    TimerUtils.triggerExpiredTimers(mockTimerInternals, mockWindowingStrategy, mockIterator);

    // Verify that fireTimer was not called
    verify(mockIterator, never()).fireTimer(any());
  }

  @Test
  public void testTriggerExpiredTimersWithProcessingTimeDomain() {
    // Set up a processing-time timer
    when(expiredTimer.getDomain()).thenReturn(TimeDomain.PROCESSING_TIME);

    // Set up the mock timer internals to return only the expired timer
    when(mockTimerInternals.getTimers()).thenReturn(Collections.singletonList(expiredTimer));

    // Call the method under test
    TimerUtils.triggerExpiredTimers(mockTimerInternals, mockWindowingStrategy, mockIterator);

    // Verify that fireTimer was called for the expired timer
    verify(mockIterator).fireTimer(expiredTimer);
  }
}
