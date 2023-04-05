/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.core.triggers;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.beam.runners.core.triggers.TriggerStateMachineTester.SimpleTriggerStateMachineTester;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests the {@link AfterSynchronizedProcessingTimeStateMachine}. */
@RunWith(JUnit4.class)
public class AfterSynchronizedProcessingTimeStateMachineTest {

  @Test
  public void testAfterProcessingTimeWithFixedWindows() throws Exception {
    Duration windowDuration = Duration.millis(10);
    SimpleTriggerStateMachineTester<IntervalWindow> tester =
        TriggerStateMachineTester.forTrigger(
            AfterProcessingTimeStateMachine.pastFirstElementInPane()
                .plusDelayOf(Duration.millis(5)),
            FixedWindows.of(windowDuration));

    tester.advanceProcessingTime(new Instant(10));

    // Timer at 15
    tester.injectElements(1);
    IntervalWindow firstWindow = new IntervalWindow(new Instant(0), new Instant(10));
    tester.advanceProcessingTime(new Instant(12));
    assertFalse(tester.shouldFire(firstWindow));

    // Load up elements in the next window, timer at 17 for them
    tester.injectElements(11, 12, 13);
    IntervalWindow secondWindow = new IntervalWindow(new Instant(10), new Instant(20));
    assertFalse(tester.shouldFire(secondWindow));

    // Not quite time to fire
    tester.advanceProcessingTime(new Instant(14));
    assertFalse(tester.shouldFire(firstWindow));
    assertFalse(tester.shouldFire(secondWindow));

    // Timer at 19 for these in the first window; it should be ignored since the 15 will fire first
    tester.injectElements(2, 3);

    // Advance past the first timer and fire, finishing the first window
    tester.advanceProcessingTime(new Instant(16));
    assertTrue(tester.shouldFire(firstWindow));
    assertFalse(tester.shouldFire(secondWindow));
    tester.fireIfShouldFire(firstWindow);
    assertTrue(tester.isMarkedFinished(firstWindow));

    // The next window fires and finishes now
    tester.advanceProcessingTime(new Instant(18));
    assertTrue(tester.shouldFire(secondWindow));
    tester.fireIfShouldFire(secondWindow);
    assertTrue(tester.isMarkedFinished(secondWindow));
  }

  @Test
  public void testAfterProcessingTimeWithMergingWindow() throws Exception {
    Duration windowDuration = Duration.millis(10);
    SimpleTriggerStateMachineTester<IntervalWindow> tester =
        TriggerStateMachineTester.forTrigger(
            AfterProcessingTimeStateMachine.pastFirstElementInPane()
                .plusDelayOf(Duration.millis(5)),
            Sessions.withGapDuration(windowDuration));

    tester.advanceProcessingTime(new Instant(10));
    tester.injectElements(1); // in [1, 11), timer for 15
    IntervalWindow firstWindow = new IntervalWindow(new Instant(1), new Instant(11));
    assertFalse(tester.shouldFire(firstWindow));

    tester.advanceProcessingTime(new Instant(12));
    tester.injectElements(3); // in [3, 13), timer for 17
    IntervalWindow secondWindow = new IntervalWindow(new Instant(3), new Instant(13));
    assertFalse(tester.shouldFire(secondWindow));

    tester.mergeWindows();
    IntervalWindow mergedWindow = new IntervalWindow(new Instant(1), new Instant(13));

    tester.advanceProcessingTime(new Instant(16));
    assertTrue(tester.shouldFire(mergedWindow));
  }
}
