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
        AccumulationMode.DISCARDING_FIRED_PANES);

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
    assertFalse(tester.isDone(new IntervalWindow(new Instant(30), new Instant(40))));
    assertThat(tester.getKeyedStateInUse(), Matchers.emptyIterable());
  }

  @Test
  public void testDefaultTriggerWithSessionWindow() throws Exception {
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        Sessions.withGapDuration(Duration.millis(10)),
        DefaultTrigger.<IntervalWindow>of(),
        AccumulationMode.DISCARDING_FIRED_PANES);

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
    assertFalse(tester.isDone(new IntervalWindow(new Instant(1), new Instant(25))));
    assertFalse(tester.isDone(new IntervalWindow(new Instant(30), new Instant(40))));
    assertThat(tester.getKeyedStateInUse(), Matchers.emptyIterable());
  }

  @Test
  public void testDefaultTriggerWithSlidingWindow() throws Exception {
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        SlidingWindows.of(Duration.millis(10)).every(Duration.millis(5)),
        DefaultTrigger.<IntervalWindow>of(),
        AccumulationMode.DISCARDING_FIRED_PANES);

    tester.injectElement(1, new Instant(1));
    tester.injectElement(2, new Instant(4));
    tester.injectElement(3, new Instant(9));

    tester.advanceWatermark(new Instant(100));
    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, -5, 5),
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2, 3), 1, 0, 10),
        isSingleWindowedValue(Matchers.containsInAnyOrder(3), 9, 5, 15)));

    tester.injectElement(4, new Instant(8));

    // Late data means the merge tree might be empty
    tester.advanceWatermark(new Instant(101));
    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(4), 8, 0, 10),
        isSingleWindowedValue(Matchers.containsInAnyOrder(4), 8, 5, 15)));

    assertFalse(tester.isDone(new IntervalWindow(new Instant(1), new Instant(10))));
    assertFalse(tester.isDone(new IntervalWindow(new Instant(5), new Instant(15))));
    assertThat(tester.getKeyedStateInUse(), Matchers.emptyIterable());
  }


  @Test
  public void testDefaultTriggerWithContainedSessionWindow() throws Exception {
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        Sessions.withGapDuration(Duration.millis(10)),
        DefaultTrigger.<IntervalWindow>of(),
        AccumulationMode.DISCARDING_FIRED_PANES);

    tester.injectElement(1, new Instant(1));
    tester.injectElement(2, new Instant(9));
    tester.injectElement(3, new Instant(7));

    tester.advanceWatermark(new Instant(20));
    Iterable<WindowedValue<Iterable<Integer>>> extractOutput = tester.extractOutput();
    assertThat(extractOutput, Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2, 3), 1, 1, 19)));
    assertThat(tester.getKeyedStateInUse(), Matchers.emptyIterable());
  }

  @Test
  public void testFireDeadline() throws Exception {
    assertEquals(new Instant(9), DefaultTrigger.of().getWatermarkCutoff(
        new IntervalWindow(new Instant(0), new Instant(10))));
    assertEquals(GlobalWindow.INSTANCE.maxTimestamp(),
        DefaultTrigger.of().getWatermarkCutoff(GlobalWindow.INSTANCE));
  }
}
