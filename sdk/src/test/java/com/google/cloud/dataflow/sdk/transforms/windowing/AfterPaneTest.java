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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.MergeResult;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerResult;
import com.google.cloud.dataflow.sdk.util.TriggerTester;
import com.google.cloud.dataflow.sdk.util.TriggerTester.SimpleTriggerTester;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link AfterPane}.
 */
@RunWith(JUnit4.class)
public class AfterPaneTest {
  @Test
  public void testAfterPaneWithGlobalWindowsAndCombining() throws Exception {
    Duration windowDuration = Duration.millis(10);
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        AfterPane.<IntervalWindow>elementCountAtLeast(2),
        FixedWindows.of(windowDuration));

    tester.injectElements(1, 9);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE_AND_FINISH));
    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(0), new Instant(10))));
    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(10), new Instant(20))));
  }

  @Test
  public void testAfterPaneWithFixedWindow() throws Exception {
    Duration windowDuration = Duration.millis(10);
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        AfterPane.<IntervalWindow>elementCountAtLeast(2),
        FixedWindows.of(windowDuration));

    tester.injectElements(1, 9);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE_AND_FINISH));
    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(0), new Instant(10))));
    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(10), new Instant(20))));
  }

  @Test
  public void testClear() throws Exception {
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        AfterPane.<IntervalWindow>elementCountAtLeast(2),
        FixedWindows.of(Duration.millis(10)));

    tester.injectElements(1, 2, 3);
    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(10));
    tester.clearState(window);
    tester.assertCleared(window);
  }

  @Test
  public void testAfterPaneWithMerging() throws Exception {
    Duration windowDuration = Duration.millis(10);
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        AfterPane.<IntervalWindow>elementCountAtLeast(2),
        Sessions.withGapDuration(windowDuration));

    tester.injectElements(
        1, // in [1, 11)
        2); // in [2, 12)
    tester.mergeWindows();

    assertThat(tester.getLatestMergeResult(), equalTo(MergeResult.FIRE_AND_FINISH));
    tester.clearLatestMergeResult();

    // Because we closed the previous window, we don't have it around to merge with. So there
    // will be a new FIRE_AND_FINISH result.
    tester.injectElements(
        7,  // in [7, 17)
        9); // in [8, 18)

    assertThat(tester.getLatestMergeResult(), equalTo(MergeResult.FIRE_AND_FINISH));

    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(1), new Instant(12))));
  }

  @Test
  public void testFireDeadline() throws Exception {
    assertEquals(BoundedWindow.TIMESTAMP_MAX_VALUE,
        AfterPane.elementCountAtLeast(1).getWatermarkThatGuaranteesFiring(
            new IntervalWindow(new Instant(0), new Instant(10))));
  }

  @Test
  public void testContinuation() throws Exception {
    assertEquals(
        AfterPane.elementCountAtLeast(1),
        AfterPane.elementCountAtLeast(100).getContinuationTrigger());
    assertEquals(
        AfterPane.elementCountAtLeast(1),
        AfterPane.elementCountAtLeast(100).getContinuationTrigger().getContinuationTrigger());
  }
}
