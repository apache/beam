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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerResult;
import com.google.cloud.dataflow.sdk.util.TriggerTester;
import com.google.cloud.dataflow.sdk.util.TriggerTester.SimpleTriggerTester;

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
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        DefaultTrigger.<IntervalWindow>of(),
        FixedWindows.of(Duration.millis(10)));

    tester.injectElements(1, 9, 15, 19, 30);

    // Advance the watermark almost to the end of the first window.
    tester.advanceProcessingTime(new Instant(500));
    tester.advanceInputWatermark(new Instant(8));
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.CONTINUE));

    // Advance watermark to 10 (past end of the window), which causes the first fixed window to
    // be emitted
    tester.advanceInputWatermark(new Instant(10));
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));

    // Advance watermark to 100, which causes the remaining two windows to be emitted.
    // Since their timers were at different timestamps, they should fire in order.
    tester.advanceInputWatermark(new Instant(100));
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));
    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(30), new Instant(40))));
  }

  @Test
  public void testDefaultTriggerWithSessionWindow() throws Exception {
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        DefaultTrigger.<IntervalWindow>of(),
        Sessions.withGapDuration(Duration.millis(10)));

    tester.injectElements(1, 9);

    // no output, because we merged into the [9-19) session
    tester.advanceInputWatermark(new Instant(10));
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.CONTINUE));

    tester.injectElements(15, 30);

    tester.advanceInputWatermark(new Instant(100));
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));
    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(1), new Instant(25))));
    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(30), new Instant(40))));
  }

  @Test
  public void testDefaultTriggerWithSlidingWindow() throws Exception {
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        DefaultTrigger.<IntervalWindow>of(),
        SlidingWindows.of(Duration.millis(10)).every(Duration.millis(5)));

    tester.injectElements(1, 4, 9);
    assertThat(tester.getResultSequence(), everyItem(equalTo(TriggerResult.CONTINUE)));
    tester.clearResultSequence();

    tester.advanceInputWatermark(new Instant(5));
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));
    tester.advanceInputWatermark(new Instant(10));
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));
    tester.advanceInputWatermark(new Instant(15));
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));

    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(1), new Instant(10))));
    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(5), new Instant(15))));
  }

  @Test
  public void testDefaultTriggerWithContainedSessionWindow() throws Exception {
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        DefaultTrigger.<IntervalWindow>of(),
        Sessions.withGapDuration(Duration.millis(10)));

    tester.injectElements(1, 9, 7);

    tester.advanceInputWatermark(new Instant(20));
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));
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
