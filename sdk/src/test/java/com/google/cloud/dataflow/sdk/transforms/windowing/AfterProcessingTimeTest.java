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

import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerResult;
import com.google.cloud.dataflow.sdk.util.TimeDomain;
import com.google.cloud.dataflow.sdk.util.TriggerTester;
import com.google.cloud.dataflow.sdk.util.TriggerTester.SimpleTriggerTester;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests the {@link AfterProcessingTime}.
 */
@RunWith(JUnit4.class)
public class AfterProcessingTimeTest {
  @Test
  public void testAfterProcessingTimeIgnoresTimer() throws Exception {
    Duration windowDuration = Duration.millis(10);
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        AfterProcessingTime
            .<IntervalWindow>pastFirstElementInPane()
            .plusDelayOf(Duration.millis(5)),
        FixedWindows.of(windowDuration));

    tester.fireTimer(new IntervalWindow(new Instant(0), new Instant(10)),
        new Instant(15), TimeDomain.PROCESSING_TIME);
    tester.injectElements(1);
    tester.fireTimer(new IntervalWindow(new Instant(0), new Instant(10)),
        new Instant(5), TimeDomain.PROCESSING_TIME);
  }

  @Test
  public void testAfterProcessingTimeWithFixedWindow() throws Exception {
    Duration windowDuration = Duration.millis(10);
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        AfterProcessingTime
            .<IntervalWindow>pastFirstElementInPane()
            .plusDelayOf(Duration.millis(5)),
        FixedWindows.of(windowDuration));

    tester.advanceProcessingTime(new Instant(10));

    // Timer at 15
    tester.injectElements(1);
    tester.advanceProcessingTime(new Instant(12));

    // Load up elements in the next window, timer at 17 for them
    tester.injectElements(11, 12, 13);

    tester.advanceProcessingTime(new Instant(14));

    // Timer at 19 for these; it should be ignored since the 15 will fire first
    tester.injectElements(2, 3);

    // We should not have fired yet
    assertThat(tester.getResultSequence(), everyItem(equalTo(TriggerResult.CONTINUE)));

    // Advance past the first timer and fire, finishing the first window
    tester.advanceProcessingTime(new Instant(16));
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE_AND_FINISH));
    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(0), new Instant(10))));

    // This element belongs in the window that has already fired; it should not
    // be passed to onElement, so there should be no resultSequence to observe
    // (thus necessarily no timer)
    tester.clearResultSequence();
    tester.injectElements(2);
    assertThat(tester.getResultSequence(), emptyIterable());

    // The next window fires and finishes
    tester.advanceProcessingTime(new Instant(18));
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE_AND_FINISH));
    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(10), new Instant(20))));

    // The timer for the finished window, from later elements, should also not do anything
    tester.clearResultSequence();
    tester.advanceProcessingTime(new Instant(20));
    assertThat(tester.getResultSequence(), emptyIterable());
  }

  @Test
  public void testClear() throws Exception {
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        AfterProcessingTime
            .<IntervalWindow>pastFirstElementInPane()
            .plusDelayOf(Duration.millis(5)),
        FixedWindows.of(Duration.millis(10)));

    tester.injectElements(1, 2, 3);
    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(10));
    tester.clearState(window);
    tester.assertCleared(window);
  }

  @Test
  public void testAfterProcessingTimeWithMergingWindow() throws Exception {
    Duration windowDuration = Duration.millis(10);
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        AfterProcessingTime
            .<IntervalWindow>pastFirstElementInPane()
            .plusDelayOf(Duration.millis(5)),
        Sessions.withGapDuration(windowDuration));

    tester.advanceProcessingTime(new Instant(10));
    tester.injectElements(1); // in [1, 11), timer for 15
    tester.advanceProcessingTime(new Instant(11));
    tester.injectElements(2); // in [2, 12), timer for 16

    tester.advanceProcessingTime(new Instant(16));
    // This fires, because the earliest element in [1, 12) arrived at time 10
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE_AND_FINISH));
    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(1), new Instant(11))));
  }

  @Test
  public void testFireDeadline() throws Exception {
    assertEquals(BoundedWindow.TIMESTAMP_MAX_VALUE,
        AfterProcessingTime.pastFirstElementInPane().getWatermarkThatGuaranteesFiring(
            new IntervalWindow(new Instant(0), new Instant(10))));
  }

  @Test
  public void testContinuation() throws Exception {
    TimeTrigger<?> firstElementPlus1 =
        AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardHours(1));
    assertEquals(
        new AfterSynchronizedProcessingTime<>(),
        firstElementPlus1.getContinuationTrigger());
  }
}
