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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.WindowMatchers;
import com.google.cloud.dataflow.sdk.util.TimeDomain;
import com.google.cloud.dataflow.sdk.util.TriggerTester;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy.AccumulationMode;

import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests the {@link AfterWatermark} triggers.
 */
@RunWith(JUnit4.class)
public class AfterWatermarkTest {

  @Test
  public void testFirstInPaneWithFixedWindow() throws Exception {
    Duration windowDuration = Duration.millis(10);
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        FixedWindows.of(windowDuration),
        AfterWatermark.<IntervalWindow>pastFirstElementInPane().plusDelayOf(Duration.millis(5)),
        AccumulationMode.DISCARDING_FIRED_PANES,
        Duration.millis(100));

    tester.injectElement(1, new Instant(1)); // first in window [0, 10), timer set for 6
    tester.advanceWatermark(new Instant(5));
    tester.injectElement(2, new Instant(9));
    tester.injectElement(3, new Instant(8));

    tester.advanceWatermark(new Instant(6));
    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2, 3), 1, 0, 10)));

    // This element belongs in the window that has already fired. It should not be re-output because
    // that trigger (which was one-time) has already gone off.
    tester.injectElement(6, new Instant(2));
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
    tester.injectElement(1, zero.plus(Duration.standardSeconds(1)));
    tester.injectElement(2, zero.plus(Duration.standardSeconds(5)));
    tester.injectElement(3, zero.plus(Duration.standardSeconds(55)));

    // Advance almost to 6m, but not quite. No output should be produced.
    tester.advanceWatermark(zero.plus(Duration.standardMinutes(6)).minus(1));
    assertThat(tester.extractOutput(), Matchers.emptyIterable());

    // Advance to 6m and see our output
    tester.advanceWatermark(zero.plus(Duration.standardMinutes(6)));
    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.isSingleWindowedValue(
            Matchers.containsInAnyOrder(1, 2, 3),
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

    tester.injectElement(1, new Instant(1)); // in [1, 11), timer for 6
    tester.injectElement(2, new Instant(2)); // in [2, 12), timer for 7
    tester.advanceWatermark(new Instant(6));

    // We merged, and updated the watermark timer to the earliest timer, which was still 6.
    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 1, 12)));

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

    tester.injectElement(1, new Instant(1)); // first in window [0, 10), timer set for 9
    tester.advanceWatermark(new Instant(8));
    assertThat(tester.extractOutput(), Matchers.emptyIterable());
    tester.injectElement(2, new Instant(9));
    tester.injectElement(3, new Instant(8));

    tester.advanceWatermark(new Instant(9));
    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2, 3), 1, 0, 10)));

    // This element belongs in the window that has already fired. It should not be re-output because
    // that trigger (which was one-time) has already gone off.
    tester.injectElement(6, new Instant(2));
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

    tester.injectElement(1, new Instant(1)); // in [1, 11), timer for 10
    tester.injectElement(2, new Instant(2)); // in [2, 12), timer for 11
    tester.advanceWatermark(new Instant(10));

    // We merged, and updated the watermark timer to the end of the new window.
    assertThat(tester.extractOutput(), Matchers.emptyIterable());

    tester.injectElement(3, new Instant(1)); // in [1, 11), timer for 10
    tester.advanceWatermark(new Instant(11));

    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2, 3), 1, 1, 12)));

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
    tester.injectElement(1, new Instant(1));
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
}
