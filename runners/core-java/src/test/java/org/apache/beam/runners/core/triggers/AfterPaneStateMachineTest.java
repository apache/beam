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

import static org.junit.Assert.assertEquals;
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

/** Tests for {@link AfterPaneStateMachine}. */
@RunWith(JUnit4.class)
public class AfterPaneStateMachineTest {

  SimpleTriggerStateMachineTester<IntervalWindow> tester;
  /**
   * Tests that the trigger does fire when enough elements are in a window, and that it only fires
   * that window (no leakage).
   */
  @Test
  public void testAfterPaneElementCountFixedWindows() throws Exception {
    tester =
        TriggerStateMachineTester.forTrigger(
            AfterPaneStateMachine.elementCountAtLeast(2), FixedWindows.of(Duration.millis(10)));

    tester.injectElements(1); // [0, 10)
    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(10));
    assertFalse(tester.shouldFire(window));

    tester.injectElements(2); // [0, 10)
    tester.injectElements(11); // [10, 20)

    assertTrue(tester.shouldFire(window)); // ready to fire
    tester.fireIfShouldFire(window); // and finished
    assertTrue(tester.isMarkedFinished(window));

    // But don't finish the other window
    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(10), new Instant(20))));
  }

  @Test
  public void testClear() throws Exception {
    SimpleTriggerStateMachineTester<IntervalWindow> tester =
        TriggerStateMachineTester.forTrigger(
            AfterPaneStateMachine.elementCountAtLeast(2), FixedWindows.of(Duration.millis(10)));

    tester.injectElements(1, 2, 3);
    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(10));
    tester.clearState(window);
    tester.assertCleared(window);
  }

  @Test
  public void testAfterPaneElementCountSessions() throws Exception {
    tester =
        TriggerStateMachineTester.forTrigger(
            AfterPaneStateMachine.elementCountAtLeast(2),
            Sessions.withGapDuration(Duration.millis(10)));

    tester.injectElements(
        1, // in [1, 11)
        2); // in [2, 12)

    assertFalse(tester.shouldFire(new IntervalWindow(new Instant(1), new Instant(11))));
    assertFalse(tester.shouldFire(new IntervalWindow(new Instant(2), new Instant(12))));

    tester.mergeWindows();

    IntervalWindow mergedWindow = new IntervalWindow(new Instant(1), new Instant(12));
    assertTrue(tester.shouldFire(mergedWindow));
    tester.fireIfShouldFire(mergedWindow);
    assertTrue(tester.isMarkedFinished(mergedWindow));

    // Because we closed the previous window, we don't have it around to merge with. So there
    // will be a new FIRE_AND_FINISH result.
    tester.injectElements(
        7, // in [7, 17)
        9); // in [9, 19)

    tester.mergeWindows();

    IntervalWindow newMergedWindow = new IntervalWindow(new Instant(7), new Instant(19));
    assertTrue(tester.shouldFire(newMergedWindow));
    tester.fireIfShouldFire(newMergedWindow);
    assertTrue(tester.isMarkedFinished(newMergedWindow));
  }

  @Test
  public void testToString() {
    TriggerStateMachine trigger = AfterPaneStateMachine.elementCountAtLeast(5);
    assertEquals("AfterPane.elementCountAtLeast(5)", trigger.toString());
  }
}
