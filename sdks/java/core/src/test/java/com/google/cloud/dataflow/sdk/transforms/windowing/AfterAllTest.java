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
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnceTrigger;
import com.google.cloud.dataflow.sdk.util.TriggerTester;
import com.google.cloud.dataflow.sdk.util.TriggerTester.SimpleTriggerTester;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link AfterAll}.
 */
@RunWith(JUnit4.class)
public class AfterAllTest {

  private SimpleTriggerTester<IntervalWindow> tester;

  @Test
  public void testT1FiresFirst() throws Exception {
    tester = TriggerTester.forTrigger(
        AfterAll.of(
            AfterPane.<IntervalWindow>elementCountAtLeast(1),
            AfterPane.<IntervalWindow>elementCountAtLeast(2)),
        FixedWindows.of(Duration.millis(100)));

    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(100));

    tester.injectElements(1);
    assertFalse(tester.shouldFire(window));

    tester.injectElements(2);
    assertTrue(tester.shouldFire(window));
    tester.fireIfShouldFire(window);
    assertTrue(tester.isMarkedFinished(window));
  }

  @Test
  public void testT2FiresFirst() throws Exception {
    tester = TriggerTester.forTrigger(
        AfterAll.of(
            AfterPane.<IntervalWindow>elementCountAtLeast(2),
            AfterPane.<IntervalWindow>elementCountAtLeast(1)),
        FixedWindows.of(Duration.millis(100)));

    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(100));

    tester.injectElements(1);
    assertFalse(tester.shouldFire(window));

    tester.injectElements(2);
    assertTrue(tester.shouldFire(window));
    tester.fireIfShouldFire(window);
    assertTrue(tester.isMarkedFinished(window));
  }

  /**
   * Tests that the AfterAll properly unsets finished bits when a merge causing it to become
   * unfinished.
   */
  @Test
  public void testOnMergeRewinds() throws Exception {
    tester = TriggerTester.forTrigger(
        AfterEach.inOrder(
            AfterAll.of(
                AfterWatermark.<IntervalWindow>pastEndOfWindow(),
                AfterPane.<IntervalWindow>elementCountAtLeast(1)),
            Repeatedly.forever(AfterPane.<IntervalWindow>elementCountAtLeast(1))),
        Sessions.withGapDuration(Duration.millis(10)));

    tester.injectElements(1);
    IntervalWindow firstWindow = new IntervalWindow(new Instant(1), new Instant(11));

    tester.injectElements(5);
    IntervalWindow secondWindow = new IntervalWindow(new Instant(5), new Instant(15));

    // Finish the AfterAll in the first window
    tester.advanceInputWatermark(new Instant(11));
    assertTrue(tester.shouldFire(firstWindow));
    assertFalse(tester.shouldFire(secondWindow));
    tester.fireIfShouldFire(firstWindow);

    // Merge them; the AfterAll should not be finished
    tester.mergeWindows();
    IntervalWindow mergedWindow = new IntervalWindow(new Instant(1), new Instant(15));
    assertFalse(tester.isMarkedFinished(mergedWindow));

    // Confirm that we are back on the first trigger by probing that it is not ready to fire
    // after an element (with merging)
    tester.injectElements(3);
    tester.mergeWindows();
    assertFalse(tester.shouldFire(mergedWindow));

    // Fire the AfterAll in the merged window
    tester.advanceInputWatermark(new Instant(15));
    assertTrue(tester.shouldFire(mergedWindow));
    tester.fireIfShouldFire(mergedWindow);

    // Confirm that we are on the second trigger by probing
    tester.injectElements(2);
    tester.mergeWindows();
    assertTrue(tester.shouldFire(mergedWindow));
    tester.fireIfShouldFire(mergedWindow);
    tester.injectElements(2);
    tester.mergeWindows();
    assertTrue(tester.shouldFire(mergedWindow));
    tester.fireIfShouldFire(mergedWindow);
  }

  @Test
  public void testFireDeadline() throws Exception {
    BoundedWindow window = new IntervalWindow(new Instant(0), new Instant(10));

    assertEquals(BoundedWindow.TIMESTAMP_MAX_VALUE,
        AfterAll.of(AfterWatermark.pastEndOfWindow(), AfterPane.elementCountAtLeast(1))
            .getWatermarkThatGuaranteesFiring(window));
  }

  @Test
  public void testContinuation() throws Exception {
    OnceTrigger<IntervalWindow> trigger1 = AfterProcessingTime.pastFirstElementInPane();
    OnceTrigger<IntervalWindow> trigger2 = AfterWatermark.pastEndOfWindow();
    Trigger<IntervalWindow> afterAll = AfterAll.of(trigger1, trigger2);
    assertEquals(
        AfterAll.of(trigger1.getContinuationTrigger(), trigger2.getContinuationTrigger()),
        afterAll.getContinuationTrigger());
  }

}
