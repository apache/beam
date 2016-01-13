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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.MergeResult;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnTimerContext;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnceTrigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerResult;
import com.google.cloud.dataflow.sdk.util.TimeDomain;
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
 * Tests for {@link AfterEach}.
 */
@RunWith(JUnit4.class)
public class AfterEachTest {

  @Mock private Trigger<IntervalWindow> mockTrigger1;
  @Mock private Trigger<IntervalWindow> mockTrigger2;

  private SimpleTriggerTester<IntervalWindow> tester;
  private IntervalWindow firstWindow;

  public void setUp(WindowFn<?, IntervalWindow> windowFn) throws Exception {
    MockitoAnnotations.initMocks(this);
    tester = TriggerTester.forTrigger(
        AfterEach.inOrder(mockTrigger1, mockTrigger2), windowFn);
    firstWindow = new IntervalWindow(new Instant(0), new Instant(10));
  }

  private void injectElement(int element, TriggerResult result1, TriggerResult result2)
      throws Exception {
    if (result1 != null) {
      when(mockTrigger1.onElement(
          Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
          .thenReturn(result1);
    }
    if (result2 != null) {
      when(mockTrigger2.onElement(
          Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
          .thenReturn(result2);
    }

    tester.injectElements(element);
  }

  /**
   * Tests that the {@link AfterEach} trigger fires and finishes the first trigger then the second.
   */
  @Test
  public void testOnElementT1Fires() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.CONTINUE));

    injectElement(2, TriggerResult.FIRE, null);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));
    tester.clearResultSequence();

    injectElement(3, TriggerResult.FIRE_AND_FINISH, null);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));
    tester.clearResultSequence();

    injectElement(4, null, TriggerResult.FIRE_AND_FINISH);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE_AND_FINISH));
  }

  @Test
  public void testOnElementT2Fires() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.FIRE);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.CONTINUE));
    assertFalse(tester.isMarkedFinished(firstWindow));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOnTimerFire() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.CONTINUE));

    when(mockTrigger1.onTimer(Mockito.isA(OnTimerContext.class)))
        .thenReturn(TriggerResult.FIRE);
    tester.fireTimer(firstWindow, new Instant(11), TimeDomain.EVENT_TIME);

    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));
    assertFalse("Should still be waiting for the second trigger.",
        tester.isMarkedFinished(firstWindow));
  }

  @Test
  public void testOnMergeFinishes() throws Exception {
    setUp(Sessions.withGapDuration(Duration.millis(10)));

    when(mockTrigger1.onElement(Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
        .thenReturn(TriggerResult.CONTINUE);
    when(mockTrigger2.onElement(Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
        .thenReturn(TriggerResult.CONTINUE);
    tester.injectElements(1, 5);

    when(mockTrigger1.onMerge(Mockito.<Trigger<IntervalWindow>.OnMergeContext>any()))
        .thenReturn(MergeResult.ALREADY_FINISHED);
    when(mockTrigger2.onMerge(Mockito.<Trigger<IntervalWindow>.OnMergeContext>any()))
        .thenReturn(MergeResult.FIRE_AND_FINISH);
    tester.injectElements(5);

    tester.mergeWindows();

    assertThat(tester.getLatestMergeResult(), equalTo(MergeResult.FIRE_AND_FINISH));
    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(1), new Instant(15))));
  }

  @Test
  public void testOnMergeFires() throws Exception {
    setUp(Sessions.withGapDuration(Duration.millis(10)));

    when(mockTrigger1.onElement(Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
        .thenReturn(TriggerResult.CONTINUE);
    when(mockTrigger2.onElement(Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
        .thenReturn(TriggerResult.CONTINUE);
    tester.injectElements(1, 5);

    when(mockTrigger1.onMerge(Mockito.<Trigger<IntervalWindow>.OnMergeContext>any()))
        .thenReturn(MergeResult.ALREADY_FINISHED);
    when(mockTrigger2.onMerge(Mockito.<Trigger<IntervalWindow>.OnMergeContext>any()))
        .thenReturn(MergeResult.FIRE);

    tester.mergeWindows();

    assertThat(tester.getLatestMergeResult(), equalTo(MergeResult.FIRE));
  }

  @Test
  public void testFireDeadline() throws Exception {
    BoundedWindow window = new IntervalWindow(new Instant(0), new Instant(10));

    assertEquals(new Instant(9),
        AfterEach.inOrder(AfterWatermark.pastEndOfWindow(),
                          AfterWatermark.pastFirstElementInPane().plusDelayOf(Duration.millis(10)))
            .getWatermarkThatGuaranteesFiring(window));
    assertEquals(BoundedWindow.TIMESTAMP_MAX_VALUE,
        AfterEach.inOrder(AfterPane.elementCountAtLeast(2), AfterWatermark.pastEndOfWindow())
            .getWatermarkThatGuaranteesFiring(window));
  }

  @Test
  public void testSequenceRealTriggersFixedWindow() throws Exception {
    tester = TriggerTester.forTrigger(AfterEach.<IntervalWindow>inOrder(
        AfterPane.<IntervalWindow>elementCountAtLeast(5),
        AfterPane.<IntervalWindow>elementCountAtLeast(5),
        Repeatedly.<IntervalWindow>forever(AfterEach.inOrder(
            AfterPane.<IntervalWindow>elementCountAtLeast(2),
            AfterPane.<IntervalWindow>elementCountAtLeast(2)))
            .orFinally(AfterPane.<IntervalWindow>elementCountAtLeast(7))),
        FixedWindows.of(Duration.millis(50)));

    // Inject a bunch of elements all in the same window
    for (int i = 0; i < 20; i++) {
      tester.injectElements(i);
    }
    assertThat(tester.getResultSequence(), contains(
        TriggerResult.CONTINUE,
        TriggerResult.CONTINUE,
        TriggerResult.CONTINUE,
        TriggerResult.CONTINUE,
        TriggerResult.FIRE, // 5 elements
        TriggerResult.CONTINUE,
        TriggerResult.CONTINUE,
        TriggerResult.CONTINUE,
        TriggerResult.CONTINUE,
        TriggerResult.FIRE, // 5 elements
        TriggerResult.CONTINUE,
        TriggerResult.FIRE, // 2 elements (OrFinally at 2)
        TriggerResult.CONTINUE,
        TriggerResult.FIRE, // 2 elements (OrFinally at 4)
        TriggerResult.CONTINUE,
        TriggerResult.FIRE, // 2 elements (OrFinally at 6)
        TriggerResult.FIRE_AND_FINISH)); // 1 elements (OrFinally at 7)
    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(0), new Instant(50))));
  }

  @Test
  public void testAfterEachMergingWindowSomeFinished() throws Exception {
    Duration windowDuration = Duration.millis(10);
    tester = TriggerTester.forTrigger(
        AfterEach.<IntervalWindow>inOrder(
            AfterProcessingTime.<IntervalWindow>pastFirstElementInPane()
                .plusDelayOf(Duration.millis(5)),
            AfterPane.<IntervalWindow>elementCountAtLeast(5)),
        Sessions.withGapDuration(windowDuration));

    tester.advanceProcessingTime(new Instant(10));
    tester.injectElements(1); // in [1, 11), timer for 15
    tester.advanceProcessingTime(new Instant(16));
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));

    tester.injectElements(
        1,   // in [1, 11) count = 1
        2); // in [2, 12), timer for 16

    // [2, 12) tries to fire, but gets merged; count = 2
    tester.advanceProcessingTime(new Instant(30));

    tester.injectElements(1, 2, 1); // count = 5, but need to call merge fire

    tester.mergeWindows();

    // This fires, because the earliest element in [1, 12) arrived at time 10
    assertThat(tester.getLatestMergeResult(), equalTo(MergeResult.FIRE_AND_FINISH));
    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(1), new Instant(12))));
  }

  @Test
  public void testContinuation() throws Exception {
    OnceTrigger<IntervalWindow> trigger1 = AfterProcessingTime.pastFirstElementInPane();
    OnceTrigger<IntervalWindow> trigger2 = AfterWatermark.pastEndOfWindow();
    Trigger<IntervalWindow> afterEach = AfterEach.inOrder(trigger1, trigger2);
    assertEquals(
        Repeatedly.forever(AfterFirst.of(
            trigger1.getContinuationTrigger(), trigger2.getContinuationTrigger())),
        afterEach.getContinuationTrigger());
  }
}
