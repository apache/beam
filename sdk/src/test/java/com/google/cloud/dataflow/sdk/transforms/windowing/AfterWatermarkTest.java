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

package com.google.cloud.dataflow.sdk.transforms.windowing;

import static com.google.cloud.dataflow.sdk.WindowMatchers.isSingleWindowedValue;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.WindowMatchers;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnceTrigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerResult;
import com.google.cloud.dataflow.sdk.util.TimeDomain;
import com.google.cloud.dataflow.sdk.util.TriggerTester;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy.AccumulationMode;
import com.google.cloud.dataflow.sdk.values.TimestampedValue;

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

import java.util.List;

/**
 * Tests the {@link AfterWatermark} triggers.
 */
@RunWith(JUnit4.class)
public class AfterWatermarkTest {

  @Mock
  private OnceTrigger<IntervalWindow> mockEarly;
  @Mock
  private OnceTrigger<IntervalWindow> mockLate;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testFirstInPaneWithFixedWindow() throws Exception {
    Duration windowDuration = Duration.millis(10);
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        FixedWindows.of(windowDuration),
        AfterWatermark.<IntervalWindow>pastFirstElementInPane().plusDelayOf(Duration.millis(5)),
        AccumulationMode.DISCARDING_FIRED_PANES,
        Duration.millis(100));

    tester.injectElements(
        TimestampedValue.of(1, new Instant(1))); // first in window [0, 10), timer set for 6
    tester.advanceWatermark(new Instant(5));
    tester.injectElements(
        TimestampedValue.of(2, new Instant(9)),
        TimestampedValue.of(3, new Instant(8)));

    tester.advanceWatermark(new Instant(6));
    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.isSingleWindowedValue(containsInAnyOrder(1, 2, 3), 1, 0, 10)));

    // This element belongs in the window that has already fired. It should not be re-output because
    // that trigger (which was one-time) has already gone off.
    tester.injectElements(
        TimestampedValue.of(6, new Instant(2)));
    assertThat(tester.extractOutput(), Matchers.emptyIterable());

    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(0), new Instant(10))));
    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(10), new Instant(20))));
    tester.assertHasOnlyGlobalAndFinishedSetsFor(
        new IntervalWindow(new Instant(0), new Instant(10)));
  }

  @Test
  public void testAlignAndDelay() throws Exception {
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        FixedWindows.of(Duration.standardMinutes(1)),
        AfterWatermark.<IntervalWindow>pastFirstElementInPane()
            .alignedTo(Duration.standardMinutes(1))
            .plusDelayOf(Duration.standardMinutes(5)),
        AccumulationMode.DISCARDING_FIRED_PANES,

        // Don't drop right away at the end of the window, since we have a delay.
        Duration.standardMinutes(10));

    Instant zero = new Instant(0);

    // first in window [0, 1m), timer set for 6m
    tester.injectElements(
        TimestampedValue.of(1, zero.plus(Duration.standardSeconds(1))),
        TimestampedValue.of(2, zero.plus(Duration.standardSeconds(5))),
        TimestampedValue.of(3, zero.plus(Duration.standardSeconds(55))));

    // Advance almost to 6m, but not quite. No output should be produced.
    tester.advanceWatermark(zero.plus(Duration.standardMinutes(6)).minus(1));
    assertThat(tester.extractOutput(), Matchers.emptyIterable());

    // Advance to 6m and see our output
    tester.advanceWatermark(zero.plus(Duration.standardMinutes(6)));
    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.isSingleWindowedValue(
            containsInAnyOrder(1, 2, 3),
            zero.plus(Duration.standardSeconds(1)).getMillis(),
            zero.getMillis(), zero.plus(Duration.standardMinutes(1)).getMillis())));
  }

  @Test
  public void testFirstInPaneWithMerging() throws Exception {
    Duration windowDuration = Duration.millis(10);
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        Sessions.withGapDuration(windowDuration),
        AfterWatermark.<IntervalWindow>pastFirstElementInPane().plusDelayOf(Duration.millis(5)),
        AccumulationMode.DISCARDING_FIRED_PANES,
        Duration.millis(100));

    tester.advanceWatermark(new Instant(1));

    tester.injectElements(
        TimestampedValue.of(1, new Instant(1)),  // in [1, 11), timer for 6
        TimestampedValue.of(2, new Instant(2))); // in [2, 12), timer for 7
    tester.advanceWatermark(new Instant(6));

    // We merged, and updated the watermark timer to the earliest timer, which was still 6.
    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.isSingleWindowedValue(containsInAnyOrder(1, 2), 1, 1, 12)));

    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(1), new Instant(12))));

    tester.assertHasOnlyGlobalAndFinishedSetsFor(
        new IntervalWindow(new Instant(1), new Instant(12)));
  }

  @Test
  public void testEndOfWindowFixedWindow() throws Exception {
    Duration windowDuration = Duration.millis(10);
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        FixedWindows.of(windowDuration),
        AfterWatermark.<IntervalWindow>pastEndOfWindow(),
        AccumulationMode.DISCARDING_FIRED_PANES,
        Duration.millis(100));

    tester.injectElements(
        TimestampedValue.of(1, new Instant(1))); // first in window [0, 10), timer set for 9
    tester.advanceWatermark(new Instant(8));
    assertThat(tester.extractOutput(), Matchers.emptyIterable());
    tester.injectElements(
        TimestampedValue.of(2, new Instant(9)),
        TimestampedValue.of(3, new Instant(8)));

    tester.advanceWatermark(new Instant(9));
    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.isSingleWindowedValue(containsInAnyOrder(1, 2, 3), 1, 0, 10)));

    // This element belongs in the window that has already fired. It should not be re-output because
    // that trigger (which was one-time) has already gone off.
    tester.injectElements(
        TimestampedValue.of(6, new Instant(2)));
    assertThat(tester.extractOutput(), Matchers.emptyIterable());

    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(0), new Instant(10))));
    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(10), new Instant(20))));

    tester.assertHasOnlyGlobalAndFinishedSetsFor(
        new IntervalWindow(new Instant(0), new Instant(10)));
  }

  @Test
  public void testEndOfWindowWithMerging() throws Exception {
    Duration windowDuration = Duration.millis(10);
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        Sessions.withGapDuration(windowDuration),
        AfterWatermark.<IntervalWindow>pastEndOfWindow(),
        AccumulationMode.DISCARDING_FIRED_PANES,
        Duration.millis(100));

    tester.advanceWatermark(new Instant(1));

    tester.injectElements(
        TimestampedValue.of(1, new Instant(1)),  // in [1, 11), timer for 10
        TimestampedValue.of(2, new Instant(2))); // in [2, 12), timer for 11
    tester.advanceWatermark(new Instant(10));

    // We merged, and updated the watermark timer to the end of the new window.
    assertThat(tester.extractOutput(), Matchers.emptyIterable());

    tester.injectElements(
        TimestampedValue.of(3, new Instant(1))); // in [1, 11), timer for 10
    tester.advanceWatermark(new Instant(11));

    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.isSingleWindowedValue(containsInAnyOrder(1, 2, 3), 1, 1, 12)));

    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(1), new Instant(12))));

    tester.assertHasOnlyGlobalAndFinishedSetsFor(
        new IntervalWindow(new Instant(1), new Instant(12)));
  }

  @Test
  public void testEndOfWindowIgnoresTimer() throws Exception {
    Duration windowDuration = Duration.millis(10);
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        FixedWindows.of(windowDuration),
        AfterWatermark.<IntervalWindow>pastEndOfWindow(),
        AccumulationMode.DISCARDING_FIRED_PANES,
        Duration.millis(100));

    tester.fireTimer(new IntervalWindow(new Instant(0), new Instant(10)),
        new Instant(15), TimeDomain.EVENT_TIME);
    tester.injectElements(
        TimestampedValue.of(1, new Instant(1)));
    tester.fireTimer(new IntervalWindow(new Instant(0), new Instant(10)),
        new Instant(9), TimeDomain.EVENT_TIME);
  }

  @Test
  public void testFireDeadline() throws Exception {
    BoundedWindow window = new IntervalWindow(new Instant(0), new Instant(10));

    assertEquals(new Instant(9), AfterWatermark.pastEndOfWindow()
        .getWatermarkThatGuaranteesFiring(window));
    assertEquals(GlobalWindow.INSTANCE.maxTimestamp(),
        AfterWatermark.pastEndOfWindow().getWatermarkThatGuaranteesFiring(GlobalWindow.INSTANCE));
    assertEquals(new Instant(19),
        AfterWatermark
            .pastFirstElementInPane()
            .plusDelayOf(Duration.millis(10)).getWatermarkThatGuaranteesFiring(window));
  }

  @Test
  public void testContinuation() throws Exception {
    Trigger<?> endOfWindow = AfterWatermark.pastEndOfWindow();
    assertEquals(endOfWindow, endOfWindow.getContinuationTrigger());
    assertEquals(
        endOfWindow,
        endOfWindow.getContinuationTrigger().getContinuationTrigger());

    Trigger<?> firstElementAligned =
        AfterWatermark.pastFirstElementInPane().alignedTo(Duration.standardDays(1));
    assertEquals(
        firstElementAligned,
        firstElementAligned.getContinuationTrigger());
    assertEquals(
        firstElementAligned,
        firstElementAligned.getContinuationTrigger().getContinuationTrigger());
  }

  @Test
  public void testEarlyAndAtWatermarkProcessElement() throws Exception {
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        FixedWindows.of(Duration.millis(100)),
        AfterWatermark.<IntervalWindow>pastEndOfWindow()
            .withEarlyFirings(mockEarly),
        AccumulationMode.DISCARDING_FIRED_PANES,
        Duration.millis(100));

    when(mockEarly.onElement(Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
        .thenReturn(TriggerResult.CONTINUE)
        .thenReturn(TriggerResult.FIRE_AND_FINISH)
        .thenReturn(TriggerResult.CONTINUE);
    tester.injectElements(
        TimestampedValue.of(1, new Instant(5L)),
        TimestampedValue.of(2, new Instant(10L))); // Fires due to early trigger
    tester.injectElements(TimestampedValue.of(3, new Instant(15L)));

    tester.advanceWatermark(new Instant(100L)); // Fires due to AtWatermark
    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output, Matchers.contains(
        isSingleWindowedValue(containsInAnyOrder(1, 2), 5, 0, 100),
        isSingleWindowedValue(containsInAnyOrder(3), 15, 0, 100)));
    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(0), new Instant(100))));
  }

  @Test
  public void testLateAndAtWatermarkProcessElement() throws Exception {
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        FixedWindows.of(Duration.millis(100)),
        AfterWatermark.<IntervalWindow>pastEndOfWindow()
            .withLateFirings(mockLate),
        AccumulationMode.DISCARDING_FIRED_PANES,
        Duration.millis(100));

    tester.injectElements(
        TimestampedValue.of(1, new Instant(5L)),
        TimestampedValue.of(2, new Instant(10L)),
        TimestampedValue.of(3, new Instant(15L)));

    tester.advanceWatermark(new Instant(100L)); // Fires due to AtWatermark

    when(mockLate.onElement(Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
        .thenReturn(TriggerResult.CONTINUE)
        .thenReturn(TriggerResult.CONTINUE)
        .thenReturn(TriggerResult.FIRE_AND_FINISH)
        .thenReturn(TriggerResult.CONTINUE)
        .thenReturn(TriggerResult.FIRE_AND_FINISH);

    tester.injectElements(
        TimestampedValue.of(4, new Instant(20L)),
        TimestampedValue.of(5, new Instant(25L)),
        TimestampedValue.of(6, new Instant(30L)));  // Fires due to late trigger
    tester.injectElements(
        TimestampedValue.of(7, new Instant(35L)),
        TimestampedValue.of(8, new Instant(40L))); // Fires due to late trigger

    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output, Matchers.contains(
        isSingleWindowedValue(containsInAnyOrder(1, 2, 3), 5, 0, 100),
        isSingleWindowedValue(containsInAnyOrder(4, 5, 6), 99, 0, 100),
        isSingleWindowedValue(containsInAnyOrder(7, 8), 99, 0, 100)));

    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(0), new Instant(100))));
  }

  @Test
  public void testEarlyLateAndAtWatermarkProcessElement() throws Exception {
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        FixedWindows.of(Duration.millis(100)),
        AfterWatermark.<IntervalWindow>pastEndOfWindow()
            .withEarlyFirings(mockEarly)
            .withLateFirings(mockLate),
        AccumulationMode.DISCARDING_FIRED_PANES,
        Duration.millis(100));

    when(mockEarly.onElement(Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
        .thenReturn(TriggerResult.CONTINUE)
        .thenReturn(TriggerResult.FIRE_AND_FINISH)
        .thenReturn(TriggerResult.CONTINUE);
    tester.injectElements(
        TimestampedValue.of(1, new Instant(5L)),
        TimestampedValue.of(2, new Instant(10L))); // Fires due to early trigger
    tester.injectElements(
        TimestampedValue.of(3, new Instant(15L)));

    tester.advanceWatermark(new Instant(100L)); // Fires due to AtWatermark

    when(mockLate.onElement(Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
        .thenReturn(TriggerResult.CONTINUE)
        .thenReturn(TriggerResult.CONTINUE)
        .thenReturn(TriggerResult.FIRE_AND_FINISH)
        .thenReturn(TriggerResult.CONTINUE)
        .thenReturn(TriggerResult.FIRE_AND_FINISH);
    tester.injectElements(
        TimestampedValue.of(4, new Instant(20L)),
        TimestampedValue.of(5, new Instant(25L)),
        TimestampedValue.of(6, new Instant(30L)));  // Fires due to late trigger
    tester.injectElements(
        TimestampedValue.of(7, new Instant(35L)),
        TimestampedValue.of(8, new Instant(40L))); // Fires due to late

    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output, Matchers.contains(
        isSingleWindowedValue(containsInAnyOrder(1, 2), 5, 0, 100),
        isSingleWindowedValue(containsInAnyOrder(3), 15, 0, 100),
        isSingleWindowedValue(containsInAnyOrder(4, 5, 6), 99, 0, 100),
        isSingleWindowedValue(containsInAnyOrder(7, 8), 99, 0, 100)));

    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(0), new Instant(100))));
  }

  @Test
  public void testEarlyAndAtWatermarkSessions() throws Exception {
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        Sessions.withGapDuration(Duration.millis(20)),
        AfterWatermark.<IntervalWindow>pastEndOfWindow()
            .withEarlyFirings(AfterPane.<IntervalWindow>elementCountAtLeast(2)),
        AccumulationMode.ACCUMULATING_FIRED_PANES,
        Duration.millis(100));

    tester.injectElements(
        TimestampedValue.of(1, new Instant(5L)),
        TimestampedValue.of(2, new Instant(20L)));

    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(5), new Instant(40))));
    tester.injectElements(TimestampedValue.of(3, new Instant(6L)));

    tester.advanceWatermark(new Instant(100L)); // Fires due to AtWatermark
    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output, Matchers.contains(
        isSingleWindowedValue(containsInAnyOrder(1, 2), 5, 5, 40),
        isSingleWindowedValue(containsInAnyOrder(1, 2, 3), 6, 5, 40)));
    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(5), new Instant(40))));
  }

  @Test
  public void testLateAndAtWatermarkSessionsProcessingTime() throws Exception {
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        Sessions.withGapDuration(Duration.millis(20)),
        AfterWatermark.<IntervalWindow>pastEndOfWindow()
            .withLateFirings(AfterProcessingTime
                .<IntervalWindow>pastFirstElementInPane().plusDelayOf(Duration.millis(10))),
        AccumulationMode.ACCUMULATING_FIRED_PANES,
        Duration.millis(100));

    tester.advanceProcessingTime(new Instant(0L));
    tester.injectElements(
        TimestampedValue.of(1, new Instant(5L)),
        TimestampedValue.of(2, new Instant(20L)));

    tester.advanceWatermark(new Instant(70L)); // Fires due to AtWatermark

    tester.injectElements(TimestampedValue.of(3, new Instant(6L)));

    tester.advanceProcessingTime(new Instant(100L)); // Fires the late trigger

    tester.injectElements(TimestampedValue.of(4, new Instant(9L)));

    tester.advanceProcessingTime(new Instant(100L)); // Fires the late trigger again

    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output, Matchers.contains(
        isSingleWindowedValue(containsInAnyOrder(1, 2), 5, 5, 40),
        isSingleWindowedValue(containsInAnyOrder(1, 2, 3), 39, 5, 40)));
    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(5), new Instant(40))));
  }

  @Test
  public void testLateAndAtWatermarkSessions() throws Exception {
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        Sessions.withGapDuration(Duration.millis(20)),
        AfterWatermark.<IntervalWindow>pastEndOfWindow()
            .withLateFirings(AfterPane.<IntervalWindow>elementCountAtLeast(2)),
        AccumulationMode.ACCUMULATING_FIRED_PANES,
        Duration.millis(100));

    tester.injectElements(
        TimestampedValue.of(1, new Instant(5L)),
        TimestampedValue.of(2, new Instant(20L)));

    tester.advanceWatermark(new Instant(70L)); // Fires due to AtWatermark

    IntervalWindow window = new IntervalWindow(new Instant(5), new Instant(40));
    assertFalse(tester.isMarkedFinished(window));

    tester.injectElements(
        TimestampedValue.of(3, new Instant(7L)),
        TimestampedValue.of(4, new Instant(8L)),
        TimestampedValue.of(5, new Instant(9L)));  // Fires because we have at least 5 items

    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output, Matchers.contains(
        isSingleWindowedValue(containsInAnyOrder(1, 2), 5, 5, 40),
        isSingleWindowedValue(containsInAnyOrder(1, 2, 3, 4, 5), 39, 5, 40)));
    assertFalse(tester.isMarkedFinished(window));
  }

  @Test
  public void testEarlyLateAndAtWatermarkSessions() throws Exception {
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        Sessions.withGapDuration(Duration.millis(20)),
        AfterWatermark.<IntervalWindow>pastEndOfWindow()
            .withEarlyFirings(AfterProcessingTime.<IntervalWindow>pastFirstElementInPane()
                .plusDelayOf(Duration.millis(50)))
            .withLateFirings(AfterPane.<IntervalWindow>elementCountAtLeast(2)),
        AccumulationMode.ACCUMULATING_FIRED_PANES,
        Duration.millis(100));

    tester.advanceProcessingTime(new Instant(0));
    tester.injectElements(
        TimestampedValue.of(1, new Instant(5L)),
        TimestampedValue.of(2, new Instant(20L)));

    tester.advanceProcessingTime(new Instant(55)); // Fires due to early trigger

    tester.advanceWatermark(new Instant(70L)); // Fires due to AtWatermark

    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(5), new Instant(40))));

    tester.injectElements(
        TimestampedValue.of(3, new Instant(6L)),
        TimestampedValue.of(4, new Instant(7L)), // Should fire due to late trigger
        TimestampedValue.of(5, new Instant(8L)));

    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output, Matchers.contains(
        isSingleWindowedValue(containsInAnyOrder(1, 2), 5, 5, 40),
        // We get an empty on time pane
        isSingleWindowedValue(containsInAnyOrder(1, 2), 39, 5, 40),
        isSingleWindowedValue(containsInAnyOrder(1, 2, 3, 4, 5), 39, 5, 40)));
    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(5), new Instant(40))));
  }
}
