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

/** Tests for {@link OrFinallyStateMachine}. */
@RunWith(JUnit4.class)
public class OrFinallyStateMachineTest {

  private SimpleTriggerStateMachineTester<IntervalWindow> tester;

  /**
   * Tests that for {@code OrFinally(actual, ...)} when {@code actual} fires and finishes, the
   * {@code OrFinally} also fires and finishes.
   */
  @Test
  public void testActualFiresAndFinishes() throws Exception {
    tester =
        TriggerStateMachineTester.forTrigger(
            new OrFinallyStateMachine(
                AfterPaneStateMachine.elementCountAtLeast(2),
                AfterPaneStateMachine.elementCountAtLeast(100)),
            FixedWindows.of(Duration.millis(100)));

    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(100));

    // Not yet firing
    tester.injectElements(1);
    assertFalse(tester.shouldFire(window));
    assertFalse(tester.isMarkedFinished(window));

    // The actual fires and finishes
    tester.injectElements(2);
    assertTrue(tester.shouldFire(window));
    tester.fireIfShouldFire(window);
    assertTrue(tester.isMarkedFinished(window));
  }

  /**
   * Tests that for {@code OrFinally(actual, ...)} when {@code actual} fires but does not finish,
   * the {@code OrFinally} also fires and also does not finish.
   */
  @Test
  public void testActualFiresOnly() throws Exception {
    tester =
        TriggerStateMachineTester.forTrigger(
            new OrFinallyStateMachine(
                RepeatedlyStateMachine.forever(AfterPaneStateMachine.elementCountAtLeast(2)),
                AfterPaneStateMachine.elementCountAtLeast(100)),
            FixedWindows.of(Duration.millis(100)));

    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(100));

    // Not yet firing
    tester.injectElements(1);
    assertFalse(tester.shouldFire(window));
    assertFalse(tester.isMarkedFinished(window));

    // The actual fires but does not finish
    tester.injectElements(2);
    assertTrue(tester.shouldFire(window));
    tester.fireIfShouldFire(window);
    assertFalse(tester.isMarkedFinished(window));

    // And again
    tester.injectElements(3, 4);
    assertTrue(tester.shouldFire(window));
    tester.fireIfShouldFire(window);
    assertFalse(tester.isMarkedFinished(window));
  }

  /**
   * Tests that if the first trigger rewinds to be non-finished in the merged window, then it
   * becomes the currently active trigger again, with real triggers.
   */
  @Test
  public void testShouldFireAfterMerge() throws Exception {
    tester =
        TriggerStateMachineTester.forTrigger(
            AfterEachStateMachine.inOrder(
                AfterPaneStateMachine.elementCountAtLeast(5)
                    .orFinally(AfterWatermarkStateMachine.pastEndOfWindow()),
                RepeatedlyStateMachine.forever(AfterPaneStateMachine.elementCountAtLeast(1))),
            Sessions.withGapDuration(Duration.millis(10)));

    // Finished the orFinally in the first window
    tester.injectElements(1);
    IntervalWindow firstWindow = new IntervalWindow(new Instant(1), new Instant(11));
    assertFalse(tester.shouldFire(firstWindow));
    tester.advanceInputWatermark(new Instant(11));
    assertTrue(tester.shouldFire(firstWindow));
    tester.fireIfShouldFire(firstWindow);

    // Set up second window where it is not done
    tester.injectElements(5);
    IntervalWindow secondWindow = new IntervalWindow(new Instant(5), new Instant(15));
    assertFalse(tester.shouldFire(secondWindow));

    // Merge them, if the merged window were on the second trigger, it would be ready
    tester.mergeWindows();
    IntervalWindow mergedWindow = new IntervalWindow(new Instant(1), new Instant(15));
    assertFalse(tester.shouldFire(mergedWindow));

    // Now adding 3 more makes the main trigger ready to fire
    tester.injectElements(1, 2, 3, 4, 5);
    tester.mergeWindows();
    assertTrue(tester.shouldFire(mergedWindow));
  }

  /**
   * Tests that for {@code OrFinally(actual, until)} when {@code actual} fires but does not finish,
   * then {@code until} fires and finishes, the whole thing fires and finished.
   */
  @Test
  public void testActualFiresButUntilFinishes() throws Exception {
    tester =
        TriggerStateMachineTester.forTrigger(
            new OrFinallyStateMachine(
                RepeatedlyStateMachine.forever(AfterPaneStateMachine.elementCountAtLeast(2)),
                AfterPaneStateMachine.elementCountAtLeast(3)),
            FixedWindows.of(Duration.millis(10)));

    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(10));

    // Before any firing
    tester.injectElements(1);
    assertFalse(tester.shouldFire(window));
    assertFalse(tester.isMarkedFinished(window));

    // The actual fires but doesn't finish
    tester.injectElements(2);
    assertTrue(tester.shouldFire(window));
    tester.fireIfShouldFire(window);
    assertFalse(tester.isMarkedFinished(window));

    // The until fires and finishes; the trigger is finished
    tester.injectElements(3);
    assertTrue(tester.shouldFire(window));
    tester.fireIfShouldFire(window);
    assertTrue(tester.isMarkedFinished(window));
  }

  @Test
  public void testToString() {
    TriggerStateMachine trigger =
        StubTriggerStateMachine.named("t1").orFinally(StubTriggerStateMachine.named("t2"));
    assertEquals("t1.orFinally(t2)", trigger.toString());
  }
}
