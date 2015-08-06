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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.util.TriggerTester;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy.AccumulationMode;

import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

/**
 * Tests the {@link DefaultTrigger} in a variety of windowing modes.
 */
@RunWith(JUnit4.class)
public class DefaultTriggerTest {

  @Test
  public void testDefaultTriggerWithFixedWindow() throws Exception {
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        FixedWindows.of(Duration.millis(10)),
        DefaultTrigger.<IntervalWindow>of(),
        AccumulationMode.DISCARDING_FIRED_PANES,
        Duration.millis(100));

    tester.injectElement(1, new Instant(1));
    tester.injectElement(2, new Instant(9));
    tester.injectElement(3, new Instant(15));
    tester.injectElement(4, new Instant(19));
    tester.injectElement(5, new Instant(30));

    // Advance the watermark almost to the end of the first window.
    tester.advanceProcessingTime(new Instant(500));
    tester.advanceWatermark(new Instant(8));
    assertThat(tester.extractOutput(), Matchers.emptyIterable());

    // Advance watermark to 9 (the exact end of the window), which causes the first fixed window to
    // be emitted
    tester.advanceWatermark(new Instant(9));
    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 0, 10)));

    // Advance watermark to 100, which causes the remaining two windows to be emitted.
    // Since their timers were at different timestamps, they should fire in order.
    tester.advanceWatermark(new Instant(100));
    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(3, 4), 15, 10, 20),
        isSingleWindowedValue(Matchers.contains(5), 30, 30, 40)));
    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(30), new Instant(40))));
    tester.assertHasOnlyGlobalAndPaneInfoFor(
        new IntervalWindow(new Instant(0), new Instant(10)),
        new IntervalWindow(new Instant(10), new Instant(20)),
        new IntervalWindow(new Instant(30), new Instant(40)));
  }

  @Test
  public void testDefaultTriggerWithSessionWindow() throws Exception {
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        Sessions.withGapDuration(Duration.millis(10)),
        DefaultTrigger.<IntervalWindow>of(),
        AccumulationMode.DISCARDING_FIRED_PANES,
        Duration.millis(100));

    tester.injectElement(1, new Instant(1));
    tester.injectElement(2, new Instant(9));

    // no output, because we merged into the [9-19) session
    tester.advanceWatermark(new Instant(10));
    assertThat(tester.extractOutput(), Matchers.emptyIterable());

    tester.injectElement(3, new Instant(15));
    tester.injectElement(4, new Instant(30));

    tester.advanceWatermark(new Instant(100));
    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2, 3), 1, 1, 25),
        isSingleWindowedValue(Matchers.contains(4), 30, 30, 40)));
    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(1), new Instant(25))));
    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(30), new Instant(40))));
    tester.assertHasOnlyGlobalAndPaneInfoFor(
        new IntervalWindow(new Instant(1), new Instant(25)),
        new IntervalWindow(new Instant(30), new Instant(40)));
  }

  @Test
  public void testDefaultTriggerWithSlidingWindow() throws Exception {
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        SlidingWindows.of(Duration.millis(10)).every(Duration.millis(5)),
        DefaultTrigger.<IntervalWindow>of(),
        AccumulationMode.DISCARDING_FIRED_PANES,
        Duration.millis(100));

    tester.injectElement(1, new Instant(1));
    tester.injectElement(2, new Instant(4));
    tester.injectElement(3, new Instant(9));

    tester.advanceWatermark(new Instant(100));
    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, -5, 5),
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2, 3), 5, 0, 10),
        isSingleWindowedValue(Matchers.containsInAnyOrder(3), 10, 5, 15)));

    // This data is late, so it will hold the watermark to 109
    tester.injectElement(4, new Instant(8));

    tester.advanceWatermark(new Instant(101));
    assertThat(tester.getWatermarkHold(), Matchers.equalTo(new Instant(109)));
    tester.advanceWatermark(new Instant(120));
    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output, Matchers.contains(
        isSingleWindowedValue(Matchers.contains(4), 9, 0, 10),
        isSingleWindowedValue(Matchers.contains(4), 14, 5, 15)));

    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(1), new Instant(10))));
    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(5), new Instant(15))));
    tester.assertHasOnlyGlobalState();
  }

  @Test
  public void testDefaultTriggerWithContainedSessionWindow() throws Exception {
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        Sessions.withGapDuration(Duration.millis(10)),
        DefaultTrigger.<IntervalWindow>of(),
        AccumulationMode.DISCARDING_FIRED_PANES,
        Duration.millis(100));

    tester.injectElement(1, new Instant(1));
    tester.injectElement(2, new Instant(9));
    tester.injectElement(3, new Instant(7));

    tester.advanceWatermark(new Instant(20));
    Iterable<WindowedValue<Iterable<Integer>>> extractOutput = tester.extractOutput();
    assertThat(extractOutput, Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2, 3), 1, 1, 19)));
    tester.assertHasOnlyGlobalAndPaneInfoFor(
        new IntervalWindow(new Instant(1), new Instant(19)));
  }

  @Test
  public void testFireDeadline() throws Exception {
    assertEquals(new Instant(9), DefaultTrigger.of().getWatermarkThatGuaranteesFiring(
        new IntervalWindow(new Instant(0), new Instant(10))));
    assertEquals(GlobalWindow.INSTANCE.maxTimestamp(),
        DefaultTrigger.of().getWatermarkThatGuaranteesFiring(GlobalWindow.INSTANCE));
  }

  @Test
  public void testContinuation() throws Exception {
    assertEquals(DefaultTrigger.of(), DefaultTrigger.of().getContinuationTrigger());
  }
}
