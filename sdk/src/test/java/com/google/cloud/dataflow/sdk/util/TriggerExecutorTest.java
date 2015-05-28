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

import static com.google.cloud.dataflow.sdk.WindowMatchers.isSingleWindowedValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Sessions;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.MergeResult;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnElementEvent;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnMergeEvent;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnTimerEvent;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerContext;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerResult;
import com.google.cloud.dataflow.sdk.util.TimerManager.TimeDomain;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy.AccumulationMode;

import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link TriggerExecutor}.
 */
@RunWith(JUnit4.class)
public class TriggerExecutorTest {

  @Mock private Trigger<IntervalWindow> mockTrigger;
  private TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester;
  private IntervalWindow firstWindow;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    firstWindow = new IntervalWindow(new Instant(0), new Instant(10));
  }

  @SuppressWarnings("unchecked")
  private TriggerContext<IntervalWindow> isTriggerContext() {
    return Mockito.isA(TriggerContext.class);
  }

  private void injectElement(int element, TriggerResult result)
      throws Exception {
    when(mockTrigger.onElement(
        isTriggerContext(), Mockito.<OnElementEvent<IntervalWindow>>any()))
        .thenReturn(result);
    tester.injectElement(element, new Instant(element));
  }

  @Test
  public void testOnElementBufferingDiscarding() throws Exception {
    // Test basic execution of a trigger using a non-combining window set and discarding mode.
    tester = TriggerTester.nonCombining(
        FixedWindows.of(Duration.millis(10)),
        mockTrigger,
        AccumulationMode.DISCARDING_FIRED_PANES,
        Duration.millis(100));

    injectElement(1, TriggerResult.CONTINUE);
    assertTrue(tester.isWindowActive(firstWindow));

    injectElement(2, TriggerResult.FIRE);
    assertFalse(tester.isWindowActive(firstWindow));

    injectElement(3, TriggerResult.FIRE_AND_FINISH);

    // This element shouldn't be seen, because the trigger has finished
    injectElement(4, null);

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 0, 10),
        isSingleWindowedValue(Matchers.containsInAnyOrder(3), 3, 0, 10)));
    assertTrue(tester.isMarkedFinished(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(tester.finishedSet(firstWindow)));
    assertFalse(tester.isWindowActive(firstWindow));
  }

  @Test
  public void testOnElementBufferingAccumulating() throws Exception {
 // Test basic execution of a trigger using a non-combining window set and accumulating mode.
    tester = TriggerTester.nonCombining(
        FixedWindows.of(Duration.millis(10)),
        mockTrigger,
        AccumulationMode.ACCUMULATING_FIRED_PANES,
        Duration.millis(100));

    injectElement(1, TriggerResult.CONTINUE);
    assertTrue(tester.isWindowActive(firstWindow));

    injectElement(2, TriggerResult.FIRE);
    assertTrue(tester.isWindowActive(firstWindow));

    injectElement(3, TriggerResult.FIRE_AND_FINISH);

    // This element shouldn't be seen, because the trigger has finished
    injectElement(4, null);

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 0, 10),
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2, 3), 3, 0, 10)));
    assertTrue(tester.isMarkedFinished(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(tester.finishedSet(firstWindow)));
    assertFalse(tester.isWindowActive(firstWindow));
  }

  @Test
  public void testWatermarkHoldAndLateData() throws Exception {
    // Test handling of late data. Specifically, ensure the watermark hold is correct.
    tester = TriggerTester.nonCombining(
        FixedWindows.of(Duration.millis(10)),
        mockTrigger,
        AccumulationMode.ACCUMULATING_FIRED_PANES,
        Duration.millis(10));

    // All on time data, verify watermark hold.
    injectElement(1, TriggerResult.CONTINUE);
    injectElement(3, TriggerResult.CONTINUE);
    assertEquals(new Instant(1), tester.getWatermarkHold());
    injectElement(2, TriggerResult.FIRE);
    assertEquals(null, tester.getWatermarkHold());

    // Some late, some on time. Verify that we only hold to the minimum of on-time.
    tester.advanceWatermark(new Instant(4));
    injectElement(2, TriggerResult.CONTINUE);
    injectElement(3, TriggerResult.CONTINUE);
    assertEquals(new Instant(19), tester.getWatermarkHold());
    injectElement(5, TriggerResult.CONTINUE);
    assertEquals(new Instant(5), tester.getWatermarkHold());
    injectElement(4, TriggerResult.FIRE);

    // All late -- output at end of window timestamp.
    tester.advanceWatermark(new Instant(8));
    injectElement(6, TriggerResult.CONTINUE);
    injectElement(5, TriggerResult.CONTINUE);
    assertEquals(new Instant(19), tester.getWatermarkHold());
    injectElement(4, TriggerResult.FIRE);

    // This is "pending" at the time the watermark makes it way-late.
    // Because we're about to expire the window, we output it.
    injectElement(8, TriggerResult.CONTINUE);

    // All very late -- gets dropped.
    tester.advanceWatermark(new Instant(50));
    injectElement(2, TriggerResult.FIRE);
    assertEquals(null, tester.getWatermarkHold());

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2, 3), 1, 0, 10),
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2, 3, 2, 3, 4, 5), 4, 0, 10),
        // Output time is end of the window, because all the new data was late
        isSingleWindowedValue(Matchers.containsInAnyOrder(
            1, 2, 3, 2, 3, 4, 5, 4, 5, 6), 9, 0, 10),
        // Output time is not end of the window, because the new data (8) wasn't late
        isSingleWindowedValue(Matchers.containsInAnyOrder(
            1, 2, 3, 2, 3, 4, 5, 4, 5, 6, 8), 8, 0, 10)));

    // And because we're past the end of window + allowed lateness, everything should be cleaned up.
    assertFalse(tester.isMarkedFinished(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.emptyIterable());
    assertFalse(tester.isWindowActive(firstWindow));
  }

  @Test
  public void testMergeBeforeFinalizing() throws Exception {
    // Verify that we merge windows before producing output so users don't see undesired
    // unmerged windows.
    tester = TriggerTester.nonCombining(
        Sessions.withGapDuration(Duration.millis(10)),
        mockTrigger,
        AccumulationMode.DISCARDING_FIRED_PANES,
        Duration.millis(0));

    when(mockTrigger.onMerge(
        Mockito.<TriggerContext<IntervalWindow>>any(),
        Mockito.<OnMergeEvent<IntervalWindow>>any()))
        .thenReturn(MergeResult.CONTINUE);

    when(mockTrigger.onTimer(
        Mockito.<TriggerContext<IntervalWindow>>any(),
        Mockito.<OnTimerEvent<IntervalWindow>>any()))
        .thenReturn(TriggerResult.FIRE);

    // All on time data, verify watermark hold.
    injectElement(1, TriggerResult.CONTINUE); // [1-11)
    injectElement(10, TriggerResult.CONTINUE); // [10-20)

    // Create a fake timer to fire
    tester.setTimer(
        new IntervalWindow(new Instant(1), new Instant(20)), new Instant(20),
        TimeDomain.EVENT_TIME, tester.getTrigger());
    tester.advanceWatermark(new Instant(100));
    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 10), 1, 1, 20)));
  }
}
