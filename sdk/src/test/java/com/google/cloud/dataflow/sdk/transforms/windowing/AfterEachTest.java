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

import static com.google.cloud.dataflow.sdk.WindowMatchers.isSingleWindowedValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.WindowMatchers;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.MergeResult;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnTimerContext;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnceTrigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerResult;
import com.google.cloud.dataflow.sdk.util.TimeDomain;
import com.google.cloud.dataflow.sdk.util.TriggerTester;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy.AccumulationMode;

import org.hamcrest.Matchers;
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

  private TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester;
  private IntervalWindow firstWindow;

  public void setUp(WindowFn<?, IntervalWindow> windowFn) throws Exception {
    MockitoAnnotations.initMocks(this);
    tester = TriggerTester.nonCombining(
        windowFn, AfterEach.inOrder(mockTrigger1, mockTrigger2),
        AccumulationMode.DISCARDING_FIRED_PANES,
        Duration.millis(100));
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
    tester.injectElement(element, new Instant(element));
  }

  @Test
  public void testOnElementT1Fires() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    assertThat(tester.extractOutput(), Matchers.emptyIterable());

    injectElement(2, TriggerResult.FIRE, null);
    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 0, 10)));

    injectElement(3, TriggerResult.FIRE_AND_FINISH, null);
    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(3), 3, 0, 10)));

    injectElement(4, null, TriggerResult.FIRE_AND_FINISH);
    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(4), 4, 0, 10)));
    assertTrue(tester.isMarkedFinished(firstWindow));

    tester.assertHasOnlyGlobalAndFinishedSetsFor(firstWindow);
  }

  @Test
  public void testOnElementT2Fires() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.FIRE);
    assertThat(tester.extractOutput(), Matchers.emptyIterable());
    assertFalse(tester.isMarkedFinished(firstWindow));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOnTimerFire() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);

    when(mockTrigger1.onTimer(Mockito.isA(OnTimerContext.class)))
        .thenReturn(TriggerResult.FIRE);
    tester.fireTimer(firstWindow, new Instant(11), TimeDomain.EVENT_TIME);

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1), 1, 0, 10)));
    assertFalse("Should still be waiting for the second trigger.",
        tester.isMarkedFinished(firstWindow));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOnTimerFinish() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);

    when(mockTrigger1.onTimer(Mockito.isA(OnTimerContext.class)))
        .thenReturn(TriggerResult.FIRE_AND_FINISH);

    tester.advanceWatermark(new Instant(12));
    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1), 1, 0, 10)));

    injectElement(2, null, TriggerResult.FIRE_AND_FINISH);
    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(2), 9, 0, 10)));
    assertTrue(tester.isMarkedFinished(firstWindow));

    tester.assertHasOnlyGlobalAndFinishedSetsFor(firstWindow);
  }

  @Test
  public void testOnMergeFinishes() throws Exception {
    setUp(Sessions.withGapDuration(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    injectElement(12, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    injectElement(5, TriggerResult.CONTINUE, TriggerResult.CONTINUE);

    when(mockTrigger1.onMerge(
        Mockito.<Trigger<IntervalWindow>.OnMergeContext>any()))
        .thenReturn(MergeResult.ALREADY_FINISHED);

    when(mockTrigger2.onMerge(
        Mockito.<Trigger<IntervalWindow>.OnMergeContext>any()))
        .thenReturn(MergeResult.FIRE_AND_FINISH);
    tester.doMerge();

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 5, 12), 1, 1, 22)));
    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(1), new Instant(22))));

    tester.assertHasOnlyGlobalAndFinishedSetsFor(
        new IntervalWindow(new Instant(1), new Instant(22)));
  }

  @Test
  public void testOnMergeFires() throws Exception {
    setUp(Sessions.withGapDuration(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    injectElement(12, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    injectElement(5, TriggerResult.CONTINUE, TriggerResult.CONTINUE);

    when(mockTrigger1.onMerge(
        Mockito.<Trigger<IntervalWindow>.OnMergeContext>any()))
        .thenReturn(MergeResult.ALREADY_FINISHED);
    when(mockTrigger2.onMerge(
        Mockito.<Trigger<IntervalWindow>.OnMergeContext>any())).thenReturn(MergeResult.FIRE);
    tester.doMerge();

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 5, 12), 1, 1, 22)));
    tester.assertHasOnlyGlobalAndFinishedSetsAndPaneInfoFor(
        new IntervalWindow(new Instant(1), new Instant(22)));
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
    tester = TriggerTester.nonCombining(FixedWindows.of(Duration.millis(50)),
        AfterEach.<IntervalWindow>inOrder(
            AfterPane.<IntervalWindow>elementCountAtLeast(5),
            AfterPane.<IntervalWindow>elementCountAtLeast(5),
            Repeatedly.<IntervalWindow>forever(AfterEach.inOrder(
                AfterPane.<IntervalWindow>elementCountAtLeast(2),
                AfterPane.<IntervalWindow>elementCountAtLeast(2)))
                .orFinally(AfterPane.<IntervalWindow>elementCountAtLeast(7))),
        AccumulationMode.DISCARDING_FIRED_PANES,
        Duration.millis(100));

    // Inject a bunch of elements
    for (int i = 0; i < 20; i++) {
      tester.injectElement(i, new Instant(i));
    }
    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(0, 1, 2, 3, 4), 0, 0, 50),
        isSingleWindowedValue(Matchers.containsInAnyOrder(5, 6, 7, 8, 9), 5, 0, 50),
        isSingleWindowedValue(Matchers.containsInAnyOrder(10, 11), 10, 0, 50),
        isSingleWindowedValue(Matchers.containsInAnyOrder(12, 13), 12, 0, 50),
        isSingleWindowedValue(Matchers.containsInAnyOrder(14, 15), 14, 0, 50),
        isSingleWindowedValue(Matchers.containsInAnyOrder(16), 16, 0, 50)));
    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(0), new Instant(50))));
    tester.assertHasOnlyGlobalAndFinishedSetsFor(
        new IntervalWindow(new Instant(0), new Instant(50)));
  }

  @Test
  public void testAfterEachMergingWindowSomeFinished() throws Exception {
    Duration windowDuration = Duration.millis(10);
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        Sessions.withGapDuration(windowDuration),
        AfterEach.<IntervalWindow>inOrder(
            AfterProcessingTime.<IntervalWindow>pastFirstElementInPane()
                .plusDelayOf(Duration.millis(5)),
            AfterPane.<IntervalWindow>elementCountAtLeast(5)),
        AccumulationMode.ACCUMULATING_FIRED_PANES,
        Duration.millis(100));

    tester.advanceProcessingTime(new Instant(10));
    tester.injectElement(1, new Instant(1)); // in [1, 11), timer for 15
    tester.advanceProcessingTime(new Instant(15));
    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.isSingleWindowedValue(Matchers.contains(1), 1, 1, 11)));

    tester.injectElement(2, new Instant(1)); // in [1, 11) count = 1
    tester.injectElement(3, new Instant(2)); // in [2, 12), timer for 16

    // [2, 12) tries to fire, but gets merged; count = 2
    tester.advanceProcessingTime(new Instant(30));

    tester.injectElement(4, new Instant(1));
    tester.injectElement(5, new Instant(2));
    tester.injectElement(6, new Instant(1)); // count = 5, but need to call merge to discover this

    tester.doMerge();

    // This fires, because the earliest element in [1, 12) arrived at time 10
    assertThat(tester.extractOutput(), Matchers.contains(WindowMatchers.isSingleWindowedValue(
        Matchers.containsInAnyOrder(1, 2, 3, 4, 5, 6), 1, 1, 12)));

    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(1), new Instant(12))));
    tester.assertHasOnlyGlobalAndFinishedSetsFor(
        new IntervalWindow(new Instant(1), new Instant(12)));
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
