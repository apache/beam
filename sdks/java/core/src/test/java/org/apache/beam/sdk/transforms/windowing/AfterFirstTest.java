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
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnceTrigger;
import com.google.cloud.dataflow.sdk.util.TriggerTester;
import com.google.cloud.dataflow.sdk.util.TriggerTester.SimpleTriggerTester;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link AfterFirst}.
 */
@RunWith(JUnit4.class)
public class AfterFirstTest {

  @Mock private OnceTrigger mockTrigger1;
  @Mock private OnceTrigger mockTrigger2;
  private SimpleTriggerTester<IntervalWindow> tester;
  private static Trigger.TriggerContext anyTriggerContext() {
    return Mockito.<Trigger.TriggerContext>any();
  }

  @Before
  public void initMocks() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testNeitherShouldFireFixedWindows() throws Exception {
    tester = TriggerTester.forTrigger(
        AfterFirst.of(mockTrigger1, mockTrigger2), FixedWindows.of(Duration.millis(10)));

    tester.injectElements(1);
    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(10));

    when(mockTrigger1.shouldFire(anyTriggerContext())).thenReturn(false);
    when(mockTrigger2.shouldFire(anyTriggerContext())).thenReturn(false);

    assertFalse(tester.shouldFire(window)); // should not fire
    assertFalse(tester.isMarkedFinished(window)); // not finished
  }

  @Test
  public void testOnlyT1ShouldFireFixedWindows() throws Exception {
    tester = TriggerTester.forTrigger(
        AfterFirst.of(mockTrigger1, mockTrigger2), FixedWindows.of(Duration.millis(10)));
    tester.injectElements(1);
    IntervalWindow window = new IntervalWindow(new Instant(1), new Instant(11));

    when(mockTrigger1.shouldFire(anyTriggerContext())).thenReturn(true);
    when(mockTrigger2.shouldFire(anyTriggerContext())).thenReturn(false);

    assertTrue(tester.shouldFire(window)); // should fire

    tester.fireIfShouldFire(window);
    assertTrue(tester.isMarkedFinished(window));
  }

  @Test
  public void testOnlyT2ShouldFireFixedWindows() throws Exception {
    tester = TriggerTester.forTrigger(
    AfterFirst.of(mockTrigger1, mockTrigger2), FixedWindows.of(Duration.millis(10)));
    tester.injectElements(1);
    IntervalWindow window = new IntervalWindow(new Instant(1), new Instant(11));

    when(mockTrigger1.shouldFire(anyTriggerContext())).thenReturn(false);
    when(mockTrigger2.shouldFire(anyTriggerContext())).thenReturn(true);
    assertTrue(tester.shouldFire(window)); // should fire

    tester.fireIfShouldFire(window); // now finished
    assertTrue(tester.isMarkedFinished(window));
  }

  @Test
  public void testBothShouldFireFixedWindows() throws Exception {
    tester = TriggerTester.forTrigger(
    AfterFirst.of(mockTrigger1, mockTrigger2), FixedWindows.of(Duration.millis(10)));
    tester.injectElements(1);
    IntervalWindow window = new IntervalWindow(new Instant(1), new Instant(11));

    when(mockTrigger1.shouldFire(anyTriggerContext())).thenReturn(true);
    when(mockTrigger2.shouldFire(anyTriggerContext())).thenReturn(true);
    assertTrue(tester.shouldFire(window)); // should fire

    tester.fireIfShouldFire(window);
    assertTrue(tester.isMarkedFinished(window));
  }

  /**
   * Tests that if the first trigger rewinds to be non-finished in the merged window,
   * then it becomes the currently active trigger again, with real triggers.
   */
  @Test
  public void testShouldFireAfterMerge() throws Exception {
    tester = TriggerTester.forTrigger(
        AfterEach.inOrder(
            AfterFirst.of(AfterPane.<IntervalWindow>elementCountAtLeast(5),
                AfterWatermark.<IntervalWindow>pastEndOfWindow()),
            Repeatedly.forever(AfterPane.<IntervalWindow>elementCountAtLeast(1))),
        Sessions.withGapDuration(Duration.millis(10)));

    // Finished the AfterFirst in the first window
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

    // Now adding 3 more makes the AfterFirst ready to fire
    tester.injectElements(1, 2, 3, 4, 5);
    tester.mergeWindows();
    assertTrue(tester.shouldFire(mergedWindow));
  }

  @Test
  public void testFireDeadline() throws Exception {
    BoundedWindow window = new IntervalWindow(new Instant(0), new Instant(10));

    assertEquals(new Instant(9),
        AfterFirst.of(AfterWatermark.pastEndOfWindow(), AfterPane.elementCountAtLeast(4))
            .getWatermarkThatGuaranteesFiring(window));
    assertEquals(BoundedWindow.TIMESTAMP_MAX_VALUE,
        AfterFirst.of(AfterPane.elementCountAtLeast(2), AfterPane.elementCountAtLeast(1))
            .getWatermarkThatGuaranteesFiring(window));
  }

  @Test
  public void testContinuation() throws Exception {
    OnceTrigger trigger1 = AfterProcessingTime.pastFirstElementInPane();
    OnceTrigger trigger2 = AfterWatermark.pastEndOfWindow();
    Trigger afterFirst = AfterFirst.of(trigger1, trigger2);
    assertEquals(
        AfterFirst.of(trigger1.getContinuationTrigger(), trigger2.getContinuationTrigger()),
        afterFirst.getContinuationTrigger());
  }
}
