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
 * Tests the {@link AfterSynchronizedProcessingTime}.
 */
@RunWith(JUnit4.class)
public class AfterSynchronizedProcessingTimeTest {

  private Trigger<IntervalWindow> underTest =
      new AfterSynchronizedProcessingTime<IntervalWindow>();

  @Test
  public void testAfterProcessingTimeWithFixedWindow() throws Exception {
    Duration windowDuration = Duration.millis(10);
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        underTest, FixedWindows.of(windowDuration));

    tester.advanceProcessingTime(new Instant(10));

    // synchronized timers for 10
    tester.injectElements(1, 9, 8);
    assertThat(tester.getResultSequence(), everyItem(equalTo(TriggerResult.CONTINUE)));

    tester.advanceProcessingTime(new Instant(11));
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE_AND_FINISH));
    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(0), new Instant(10))));

    // This element belongs in the window that has already fired. It should not be
    // processed.
    tester.clearResultSequence();
    tester.injectElements(2);
    assertThat(tester.getResultSequence(), emptyIterable());
  }

  @Test
  public void testAfterProcessingTimeWithMergingWindow() throws Exception {
    Duration windowDuration = Duration.millis(10);
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        underTest,
        Sessions.withGapDuration(windowDuration));

    tester.advanceProcessingTime(new Instant(10));
    tester.injectElements(
        1,   // in [1, 11), synchronized timer for 10
        2);  // in [2, 12), synchronized timer for 10
    tester.mergeWindows();
    tester.clearResultSequence();

    // The timers for the pre-merged windows should be ignored.
    tester.advanceProcessingTime(new Instant(11));
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE_AND_FINISH));
    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(1), new Instant(12))));
  }

  @Test
  public void testAfterSynchronizedProcessingTimeIgnoresTimer() throws Exception {
    Duration windowDuration = Duration.millis(10);
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        new AfterSynchronizedProcessingTime<IntervalWindow>(),
        FixedWindows.of(windowDuration));

    tester.fireTimer(new IntervalWindow(new Instant(0), new Instant(10)),
        new Instant(15), TimeDomain.SYNCHRONIZED_PROCESSING_TIME);
    tester.advanceProcessingTime(new Instant(5));
    tester.injectElements(1);
    tester.fireTimer(new IntervalWindow(new Instant(0), new Instant(10)),
        new Instant(0), TimeDomain.SYNCHRONIZED_PROCESSING_TIME);
  }

  @Test
  public void testFireDeadline() throws Exception {
    assertEquals(BoundedWindow.TIMESTAMP_MAX_VALUE,
        underTest.getWatermarkThatGuaranteesFiring(
            new IntervalWindow(new Instant(0), new Instant(10))));
  }

  @Test
  public void testContinuation() throws Exception {
    assertEquals(underTest, underTest.getContinuationTrigger());
  }
}
