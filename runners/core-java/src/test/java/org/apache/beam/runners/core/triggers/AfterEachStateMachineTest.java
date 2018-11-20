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
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockitoAnnotations;

/** Tests for {@link AfterEachStateMachine}. */
@RunWith(JUnit4.class)
public class AfterEachStateMachineTest {

  private SimpleTriggerStateMachineTester<IntervalWindow> tester;

  @Before
  public void initMocks() {
    MockitoAnnotations.initMocks(this);
  }

  /**
   * Tests that the {@link AfterEachStateMachine} trigger fires and finishes the first trigger then
   * the second.
   */
  @Test
  public void testAfterEachInSequence() throws Exception {
    tester =
        TriggerStateMachineTester.forTrigger(
            AfterEachStateMachine.inOrder(
                RepeatedlyStateMachine.forever(AfterPaneStateMachine.elementCountAtLeast(2))
                    .orFinally(AfterPaneStateMachine.elementCountAtLeast(3)),
                RepeatedlyStateMachine.forever(AfterPaneStateMachine.elementCountAtLeast(5))
                    .orFinally(AfterWatermarkStateMachine.pastEndOfWindow())),
            FixedWindows.of(Duration.millis(10)));

    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(10));

    // AfterCount(2) not ready
    tester.injectElements(1);
    assertFalse(tester.shouldFire(window));

    // AfterCount(2) ready, not finished
    tester.injectElements(2);
    assertTrue(tester.shouldFire(window));
    tester.fireIfShouldFire(window);
    assertFalse(tester.isMarkedFinished(window));

    // orFinally(AfterCount(3)) ready and will finish the first
    tester.injectElements(1, 2, 3);
    assertTrue(tester.shouldFire(window));
    tester.fireIfShouldFire(window);
    assertFalse(tester.isMarkedFinished(window));

    // Now running as the second trigger
    assertFalse(tester.shouldFire(window));
    // This quantity of elements would fire and finish if it were erroneously still the first
    tester.injectElements(1, 2, 3, 4);
    assertFalse(tester.shouldFire(window));

    // Now fire once
    tester.injectElements(5);
    assertTrue(tester.shouldFire(window));
    tester.fireIfShouldFire(window);
    assertFalse(tester.isMarkedFinished(window));

    // This time advance the watermark to finish the whole mess.
    tester.advanceInputWatermark(new Instant(10));
    assertTrue(tester.shouldFire(window));
    tester.fireIfShouldFire(window);
    assertTrue(tester.isMarkedFinished(window));
  }

  @Test
  public void testToString() {
    TriggerStateMachine trigger =
        AfterEachStateMachine.inOrder(
            StubTriggerStateMachine.named("t1"),
            StubTriggerStateMachine.named("t2"),
            StubTriggerStateMachine.named("t3"));

    assertEquals("AfterEach.inOrder(t1, t2, t3)", trigger.toString());
  }
}
