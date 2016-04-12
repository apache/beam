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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.util.TriggerTester;
import com.google.cloud.dataflow.sdk.util.TriggerTester.SimpleTriggerTester;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link Repeatedly}.
 */
@RunWith(JUnit4.class)
public class RepeatedlyTest {

  @Mock private Trigger mockTrigger;
  private SimpleTriggerTester<IntervalWindow> tester;
  private static Trigger.TriggerContext anyTriggerContext() {
    return Mockito.<Trigger.TriggerContext>any();
  }

  public void setUp(WindowFn<Object, IntervalWindow> windowFn) throws Exception {
    MockitoAnnotations.initMocks(this);
    tester = TriggerTester.forTrigger(Repeatedly.forever(mockTrigger), windowFn);
  }

  /**
   * Tests that onElement correctly passes the data on to the subtrigger.
   */
  @Test
  public void testOnElement() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));
    tester.injectElements(37);
    verify(mockTrigger).onElement(Mockito.<Trigger.OnElementContext>any());
  }

  /**
   * Tests that the repeatedly is ready to fire whenever the subtrigger is ready.
   */
  @Test
  public void testShouldFire() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    assertTrue(tester.shouldFire(new IntervalWindow(new Instant(0), new Instant(10))));

    when(mockTrigger.shouldFire(Mockito.<Trigger.TriggerContext>any()))
        .thenReturn(false);
    assertFalse(tester.shouldFire(new IntervalWindow(new Instant(0), new Instant(10))));
  }

  /**
   * Tests that the watermark that guarantees firing is that of the subtrigger.
   */
  @Test
  public void testFireDeadline() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));
    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(10));
    Instant arbitraryInstant = new Instant(34957849);

    when(mockTrigger.getWatermarkThatGuaranteesFiring(Mockito.<IntervalWindow>any()))
        .thenReturn(arbitraryInstant);

    assertThat(
        Repeatedly.forever(mockTrigger).getWatermarkThatGuaranteesFiring(window),
        equalTo(arbitraryInstant));
  }

  @Test
  public void testContinuation() throws Exception {
    Trigger trigger = AfterProcessingTime.pastFirstElementInPane();
    Trigger repeatedly = Repeatedly.forever(trigger);
    assertEquals(
        Repeatedly.forever(trigger.getContinuationTrigger()), repeatedly.getContinuationTrigger());
    assertEquals(
        Repeatedly.forever(trigger.getContinuationTrigger().getContinuationTrigger()),
        repeatedly.getContinuationTrigger().getContinuationTrigger());
  }

  @Test
  public void testShouldFireAfterMerge() throws Exception {
    tester = TriggerTester.forTrigger(
        Repeatedly.forever(AfterPane.<IntervalWindow>elementCountAtLeast(2)),
        Sessions.withGapDuration(Duration.millis(10)));

    tester.injectElements(1);
    IntervalWindow firstWindow = new IntervalWindow(new Instant(1), new Instant(11));
    assertFalse(tester.shouldFire(firstWindow));

    tester.injectElements(5);
    IntervalWindow secondWindow = new IntervalWindow(new Instant(5), new Instant(15));
    assertFalse(tester.shouldFire(secondWindow));

    // Merge them, if the merged window were on the second trigger, it would be ready
    tester.mergeWindows();
    IntervalWindow mergedWindow = new IntervalWindow(new Instant(1), new Instant(15));
    assertTrue(tester.shouldFire(mergedWindow));
  }

  @Test
  public void testRepeatedlyAfterFirstElementCount() throws Exception {
    SimpleTriggerTester<GlobalWindow> tester =
        TriggerTester.forTrigger(
            Repeatedly.forever(
                AfterFirst.<GlobalWindow>of(
                    AfterProcessingTime.<GlobalWindow>pastFirstElementInPane()
                        .plusDelayOf(Duration.standardMinutes(15)),
                    AfterPane.<GlobalWindow>elementCountAtLeast(5))),
            new GlobalWindows());

    GlobalWindow window = GlobalWindow.INSTANCE;

    tester.injectElements(1);
    assertFalse(tester.shouldFire(window));

    tester.injectElements(2, 3, 4, 5);
    assertTrue(tester.shouldFire(window));
    tester.fireIfShouldFire(window);
    assertFalse(tester.shouldFire(window));
  }

  @Test
  public void testRepeatedlyAfterFirstProcessingTime() throws Exception {
    SimpleTriggerTester<GlobalWindow> tester =
        TriggerTester.forTrigger(
            Repeatedly.forever(
                AfterFirst.<GlobalWindow>of(
                    AfterProcessingTime.<GlobalWindow>pastFirstElementInPane()
                        .plusDelayOf(Duration.standardMinutes(15)),
                    AfterPane.<GlobalWindow>elementCountAtLeast(5))),
            new GlobalWindows());

    GlobalWindow window = GlobalWindow.INSTANCE;

    tester.injectElements(1);
    assertFalse(tester.shouldFire(window));

    tester.advanceProcessingTime(new Instant(0).plus(Duration.standardMinutes(15)));
    assertTrue(tester.shouldFire(window));
    tester.fireIfShouldFire(window);
    assertFalse(tester.shouldFire(window));
  }

  @Test
  public void testRepeatedlyElementCount() throws Exception {
    SimpleTriggerTester<GlobalWindow> tester =
        TriggerTester.forTrigger(
            Repeatedly.forever(AfterPane.<GlobalWindow>elementCountAtLeast(5)),
            new GlobalWindows());

    GlobalWindow window = GlobalWindow.INSTANCE;

    tester.injectElements(1);
    assertFalse(tester.shouldFire(window));

    tester.injectElements(2, 3, 4, 5);
    assertTrue(tester.shouldFire(window));
    tester.fireIfShouldFire(window);
    assertFalse(tester.shouldFire(window));
  }

  @Test
  public void testRepeatedlyProcessingTime() throws Exception {
    SimpleTriggerTester<GlobalWindow> tester =
        TriggerTester.forTrigger(
            Repeatedly.forever(
                    AfterProcessingTime.<GlobalWindow>pastFirstElementInPane()
                        .plusDelayOf(Duration.standardMinutes(15))),
            new GlobalWindows());

    GlobalWindow window = GlobalWindow.INSTANCE;

    tester.injectElements(1);
    assertFalse(tester.shouldFire(window));

    tester.advanceProcessingTime(new Instant(0).plus(Duration.standardMinutes(15)));
    assertTrue(tester.shouldFire(window));
    tester.fireIfShouldFire(window);
    assertFalse(tester.shouldFire(window));
  }

}
