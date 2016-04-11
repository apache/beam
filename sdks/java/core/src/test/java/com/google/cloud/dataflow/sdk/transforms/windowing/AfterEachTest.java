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
package com.google.cloud.dataflow.sdk.transforms.windowing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnceTrigger;
import com.google.cloud.dataflow.sdk.util.TriggerTester;
import com.google.cloud.dataflow.sdk.util.TriggerTester.SimpleTriggerTester;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link AfterEach}.
 */
@RunWith(JUnit4.class)
public class AfterEachTest {

  private SimpleTriggerTester<IntervalWindow> tester;

  @Before
  public void initMocks() {
    MockitoAnnotations.initMocks(this);
  }

  /**
   * Tests that the {@link AfterEach} trigger fires and finishes the first trigger then the second.
   */
  @Test
  public void testAfterEachInSequence() throws Exception {
    tester = TriggerTester.forTrigger(
        AfterEach.inOrder(
            Repeatedly.forever(AfterPane.<IntervalWindow>elementCountAtLeast(2))
                .orFinally(AfterPane.<IntervalWindow>elementCountAtLeast(3)),
            Repeatedly.forever(AfterPane.<IntervalWindow>elementCountAtLeast(5))
                .orFinally(AfterWatermark.<IntervalWindow>pastEndOfWindow())),
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
  public void testFireDeadline() throws Exception {
    BoundedWindow window = new IntervalWindow(new Instant(0), new Instant(10));

    assertEquals(new Instant(9),
        AfterEach.inOrder(AfterWatermark.pastEndOfWindow(),
                          AfterPane.elementCountAtLeast(4))
            .getWatermarkThatGuaranteesFiring(window));

    assertEquals(BoundedWindow.TIMESTAMP_MAX_VALUE,
        AfterEach.inOrder(AfterPane.elementCountAtLeast(2), AfterWatermark.pastEndOfWindow())
            .getWatermarkThatGuaranteesFiring(window));
  }

  @Test
  public void testContinuation() throws Exception {
    OnceTrigger trigger1 = AfterProcessingTime.pastFirstElementInPane();
    OnceTrigger trigger2 = AfterWatermark.pastEndOfWindow();
    Trigger afterEach = AfterEach.inOrder(trigger1, trigger2);
    assertEquals(
        Repeatedly.forever(AfterFirst.of(
            trigger1.getContinuationTrigger(), trigger2.getContinuationTrigger())),
        afterEach.getContinuationTrigger());
  }
}
