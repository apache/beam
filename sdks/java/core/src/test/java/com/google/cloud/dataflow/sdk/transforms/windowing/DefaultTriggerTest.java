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

import com.google.cloud.dataflow.sdk.util.TriggerTester;
import com.google.cloud.dataflow.sdk.util.TriggerTester.SimpleTriggerTester;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests the {@link DefaultTrigger}, which should be equivalent to
 * {@code Repeatedly.forever(AfterWatermark.pastEndOfWindow())}.
 */
@RunWith(JUnit4.class)
public class DefaultTriggerTest {

  SimpleTriggerTester<IntervalWindow> tester;

  @Test
  public void testDefaultTriggerFixedWindows() throws Exception {
    tester = TriggerTester.forTrigger(
        DefaultTrigger.<IntervalWindow>of(),
        FixedWindows.of(Duration.millis(100)));

    tester.injectElements(
        1, // [0, 100)
        101); // [100, 200)

    IntervalWindow firstWindow = new IntervalWindow(new Instant(0), new Instant(100));
    IntervalWindow secondWindow = new IntervalWindow(new Instant(100), new Instant(200));

    // Advance the watermark almost to the end of the first window.
    tester.advanceInputWatermark(new Instant(99));
    assertFalse(tester.shouldFire(firstWindow));
    assertFalse(tester.shouldFire(secondWindow));

    // Advance watermark past end of the first window, which is then ready
    tester.advanceInputWatermark(new Instant(100));
    assertTrue(tester.shouldFire(firstWindow));
    assertFalse(tester.shouldFire(secondWindow));

    // Fire, but the first window is still allowed to fire
    tester.fireIfShouldFire(firstWindow);
    assertTrue(tester.shouldFire(firstWindow));
    assertFalse(tester.shouldFire(secondWindow));

    // Advance watermark to 200, then both are ready
    tester.advanceInputWatermark(new Instant(200));
    assertTrue(tester.shouldFire(firstWindow));
    assertTrue(tester.shouldFire(secondWindow));

    assertFalse(tester.isMarkedFinished(firstWindow));
    assertFalse(tester.isMarkedFinished(secondWindow));
  }

  @Test
  public void testDefaultTriggerSlidingWindows() throws Exception {
    tester = TriggerTester.forTrigger(
        DefaultTrigger.<IntervalWindow>of(),
        SlidingWindows.of(Duration.millis(100)).every(Duration.millis(50)));

    tester.injectElements(
        1, // [-50, 50), [0, 100)
        50); // [0, 100), [50, 150)

    IntervalWindow firstWindow = new IntervalWindow(new Instant(-50), new Instant(50));
    IntervalWindow secondWindow = new IntervalWindow(new Instant(0), new Instant(100));
    IntervalWindow thirdWindow = new IntervalWindow(new Instant(50), new Instant(150));

    assertFalse(tester.shouldFire(firstWindow));
    assertFalse(tester.shouldFire(secondWindow));
    assertFalse(tester.shouldFire(thirdWindow));

    // At 50, the first becomes ready; it stays ready after firing
    tester.advanceInputWatermark(new Instant(50));
    assertTrue(tester.shouldFire(firstWindow));
    assertFalse(tester.shouldFire(secondWindow));
    assertFalse(tester.shouldFire(thirdWindow));
    tester.fireIfShouldFire(firstWindow);
    assertTrue(tester.shouldFire(firstWindow));
    assertFalse(tester.shouldFire(secondWindow));
    assertFalse(tester.shouldFire(thirdWindow));

    // At 99, the first is still the only one ready
    tester.advanceInputWatermark(new Instant(99));
    assertTrue(tester.shouldFire(firstWindow));
    assertFalse(tester.shouldFire(secondWindow));
    assertFalse(tester.shouldFire(thirdWindow));

    // At 100, the first and second are ready
    tester.advanceInputWatermark(new Instant(100));
    assertTrue(tester.shouldFire(firstWindow));
    assertTrue(tester.shouldFire(secondWindow));
    assertFalse(tester.shouldFire(thirdWindow));
    tester.fireIfShouldFire(firstWindow);

    assertFalse(tester.isMarkedFinished(firstWindow));
    assertFalse(tester.isMarkedFinished(secondWindow));
    assertFalse(tester.isMarkedFinished(thirdWindow));
  }

  @Test
  public void testDefaultTriggerSessions() throws Exception {
    tester = TriggerTester.forTrigger(
        DefaultTrigger.<IntervalWindow>of(),
        Sessions.withGapDuration(Duration.millis(100)));

    tester.injectElements(
        1, // [1, 101)
        50); // [50, 150)
    tester.mergeWindows();

    IntervalWindow firstWindow = new IntervalWindow(new Instant(1), new Instant(101));
    IntervalWindow secondWindow = new IntervalWindow(new Instant(50), new Instant(150));
    IntervalWindow mergedWindow = new IntervalWindow(new Instant(1), new Instant(150));

    // Not ready in any window yet
    tester.advanceInputWatermark(new Instant(100));
    assertFalse(tester.shouldFire(firstWindow));
    assertFalse(tester.shouldFire(secondWindow));
    assertFalse(tester.shouldFire(mergedWindow));

    // The first window is "ready": the caller owns knowledge of which windows are merged away
    tester.advanceInputWatermark(new Instant(149));
    assertTrue(tester.shouldFire(firstWindow));
    assertFalse(tester.shouldFire(secondWindow));
    assertFalse(tester.shouldFire(mergedWindow));

    // Now ready on all windows
    tester.advanceInputWatermark(new Instant(150));
    assertTrue(tester.shouldFire(firstWindow));
    assertTrue(tester.shouldFire(secondWindow));
    assertTrue(tester.shouldFire(mergedWindow));

    // Ensure it repeats
    tester.fireIfShouldFire(mergedWindow);
    assertTrue(tester.shouldFire(mergedWindow));

    assertFalse(tester.isMarkedFinished(mergedWindow));
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
