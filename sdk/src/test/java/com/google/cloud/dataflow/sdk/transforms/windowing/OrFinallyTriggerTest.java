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
import static org.hamcrest.Matchers.everyItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.MergeResult;
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
 * Tests for {@link OrFinallyTrigger}.
 */
@RunWith(JUnit4.class)
public class OrFinallyTriggerTest {
  @Mock private Trigger<IntervalWindow> mockActual;
  @Mock private OnceTrigger<IntervalWindow> mockUntil;

  private SimpleTriggerTester<IntervalWindow> tester;
  private IntervalWindow firstWindow;

  public void setUp(WindowFn<?, IntervalWindow> windowFn) throws Exception {
    MockitoAnnotations.initMocks(this);

    Trigger<IntervalWindow> underTest =
        new OrFinallyTrigger<IntervalWindow>(mockActual, mockUntil);

    tester = TriggerTester.forTrigger(
        underTest, windowFn);
    firstWindow = new IntervalWindow(new Instant(0), new Instant(10));
  }

  private void injectElement(int element, TriggerResult result1, TriggerResult result2)
      throws Exception {
    if (result1 != null) {
      when(mockActual.onElement(
          Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
          .thenReturn(result1);
    }
    if (result2 != null) {
      when(mockUntil.onElement(
          Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
          .thenReturn(result2);
    }
    tester.injectElements(element);
  }

  @Test
  public void testOnElementActualFires() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.FIRE, TriggerResult.CONTINUE);
    injectElement(2, TriggerResult.FIRE_AND_FINISH, TriggerResult.CONTINUE);
    assertThat(tester.getResultSequence(),
        contains(TriggerResult.FIRE, TriggerResult.FIRE_AND_FINISH));

    // This should do nothing (we've already fired and finished)
    injectElement(3, TriggerResult.FIRE, TriggerResult.FIRE_AND_FINISH);
    assertThat(tester.getResultSequence(),
        contains(TriggerResult.FIRE, TriggerResult.FIRE_AND_FINISH));

    assertTrue(tester.isMarkedFinished(firstWindow));
  }

  @Test
  public void testOnElementUntilFires() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.CONTINUE));

    injectElement(2, TriggerResult.CONTINUE, TriggerResult.FIRE_AND_FINISH);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE_AND_FINISH));

    assertTrue(tester.isMarkedFinished(firstWindow));
  }

  @Test
  public void testOnElementUntilFiresAndFinishes() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.CONTINUE));

    injectElement(2, TriggerResult.CONTINUE, TriggerResult.FIRE_AND_FINISH);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE_AND_FINISH));

    assertTrue(tester.isMarkedFinished(firstWindow));
  }

  @Test
  public void testOnTimerFinishesUntil() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);

    // Timer fires for until, which says continue
    when(mockUntil.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
        .thenReturn(TriggerResult.CONTINUE);
    when(mockActual.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
        .thenReturn(TriggerResult.CONTINUE);
    tester.fireTimer(firstWindow, new Instant(11), TimeDomain.EVENT_TIME);

    injectElement(2, TriggerResult.FIRE, TriggerResult.CONTINUE);

    injectElement(3, TriggerResult.CONTINUE, TriggerResult.CONTINUE);

    // Timer fires for until, which says FIRE, so we fire and finish
    when(mockUntil.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
        .thenReturn(TriggerResult.FIRE_AND_FINISH);
    tester.fireTimer(firstWindow, new Instant(12), TimeDomain.EVENT_TIME);

    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE_AND_FINISH));
    assertTrue(tester.isMarkedFinished(firstWindow));
  }

  @Test
  public void testMergeActualFires() throws Exception {
    setUp(Sessions.withGapDuration(Duration.millis(10)));

    when(mockActual.onElement(Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
        .thenReturn(TriggerResult.CONTINUE);
    when(mockUntil.onElement(Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
        .thenReturn(TriggerResult.CONTINUE);

    tester.injectElements(1, 12);

    when(mockActual.onMerge(Mockito.<Trigger<IntervalWindow>.OnMergeContext>any()))
        .thenReturn(MergeResult.FIRE);
    when(mockUntil.onMerge(Mockito.<Trigger<IntervalWindow>.OnMergeContext>any()))
        .thenReturn(MergeResult.CONTINUE);

    tester.injectElements(5);
    tester.mergeWindows();

    assertThat(tester.getLatestMergeResult(), equalTo(MergeResult.FIRE));
  }

  @Test
  public void testMergeUntilFires() throws Exception {
    setUp(Sessions.withGapDuration(Duration.millis(10)));

    when(mockActual.onElement(Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
        .thenReturn(TriggerResult.CONTINUE);
    when(mockUntil.onElement(Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
        .thenReturn(TriggerResult.CONTINUE);
    tester.injectElements(1, 12);

    when(mockActual.onMerge(Mockito.<Trigger<IntervalWindow>.OnMergeContext>any()))
        .thenReturn(MergeResult.CONTINUE);
    when(mockUntil.onMerge(Mockito.<Trigger<IntervalWindow>.OnMergeContext>any()))
        .thenReturn(MergeResult.FIRE_AND_FINISH);

    tester.injectElements(5);
    tester.mergeWindows();

    assertThat(tester.getLatestMergeResult(), equalTo(MergeResult.FIRE_AND_FINISH));
    // the until fired during the merge
    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(1), new Instant(22))));
  }

  @Test
  public void testFireDeadline() throws Exception {
    BoundedWindow window = new IntervalWindow(new Instant(0), new Instant(10));

    assertEquals(new Instant(9),
        Repeatedly.forever(AfterWatermark.pastEndOfWindow())
        .getWatermarkThatGuaranteesFiring(window));
    assertEquals(new Instant(9), Repeatedly.forever(AfterWatermark.pastEndOfWindow())
        .orFinally(AfterPane.elementCountAtLeast(1))
        .getWatermarkThatGuaranteesFiring(window));
    assertEquals(new Instant(9), Repeatedly.forever(AfterPane.elementCountAtLeast(1))
        .orFinally(AfterWatermark.pastEndOfWindow())
        .getWatermarkThatGuaranteesFiring(window));
    assertEquals(new Instant(9),
        AfterPane.elementCountAtLeast(100)
            .orFinally(AfterWatermark.pastEndOfWindow())
            .getWatermarkThatGuaranteesFiring(window));

    assertEquals(BoundedWindow.TIMESTAMP_MAX_VALUE,
        Repeatedly.forever(AfterPane.elementCountAtLeast(1))
        .orFinally(AfterPane.elementCountAtLeast(10))
        .getWatermarkThatGuaranteesFiring(window));
  }

  @Test
  public void testOrFinallyRealTriggersFixedWindow() throws Exception {
    // Test an orFinally with a composite trigger, and make sure it properly resets state, etc.
    tester = TriggerTester.forTrigger(Repeatedly.<IntervalWindow>forever(
        // This element count should never fire because the orFinally fires sooner, every time
        AfterPane.<IntervalWindow>elementCountAtLeast(12)
            .orFinally(AfterAll.<IntervalWindow>of(
                AfterProcessingTime.<IntervalWindow>pastFirstElementInPane()
                    .plusDelayOf(Duration.millis(5)),
                AfterPane.<IntervalWindow>elementCountAtLeast(5)))),
        FixedWindows.of(Duration.millis(50)));

    // First, fire processing time then the 5 element

    tester.advanceProcessingTime(new Instant(0));
    tester.injectElements(0, 0, 1, 1);
    tester.advanceProcessingTime(new Instant(6));
    assertThat(tester.getResultSequence(), everyItem(equalTo(TriggerResult.CONTINUE)));

    tester.injectElements(1);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));

    // Then fire 6 new elements, then processing time
    tester.clearResultSequence();
    tester.injectElements(2, 3, 4, 5, 2, 3);
    assertThat(tester.getResultSequence(), everyItem(equalTo(TriggerResult.CONTINUE)));
    tester.clearResultSequence();
    tester.advanceProcessingTime(new Instant(15));

    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));

    // Finally, fire 3 more elements and verify the base of the orFinally doesn't fire.
    tester.clearResultSequence();
    tester.injectElements(1, 1, 1);
    assertThat(tester.getResultSequence(), everyItem(equalTo(TriggerResult.CONTINUE)));
  }

  @Test
  public void testOrFinallyMergingWindowSomeFinished() throws Exception {
    Duration windowDuration = Duration.millis(10);
    tester = TriggerTester.forTrigger(
        AfterProcessingTime.<IntervalWindow>pastFirstElementInPane()
            .plusDelayOf(Duration.millis(5))
            .orFinally(AfterPane.<IntervalWindow>elementCountAtLeast(5)),
        Sessions.withGapDuration(windowDuration));

    tester.advanceProcessingTime(new Instant(10));
    tester.injectElements(
        1,  // in [1, 11), timer for 15
        1,  // in [1, 11) count = 1
        2); // in [2, 12), timer for 16

    // Enough data comes in for 2 that combined, we should fire
    tester.injectElements(2, 2);
    tester.mergeWindows();

    // This fires, because the earliest element in [1, 12) arrived at time 10
    assertThat(tester.getLatestMergeResult(), equalTo(MergeResult.FIRE_AND_FINISH));

    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(1), new Instant(12))));
  }

  @Test
  public void testContinuation() throws Exception {
    OnceTrigger<IntervalWindow> triggerA = AfterProcessingTime.pastFirstElementInPane();
    OnceTrigger<IntervalWindow> triggerB = AfterWatermark.pastEndOfWindow();
    Trigger<IntervalWindow> aOrFinallyB = triggerA.orFinally(triggerB);
    Trigger<IntervalWindow> bOrFinallyA = triggerB.orFinally(triggerA);
    assertEquals(
        Repeatedly.forever(
            triggerA.getContinuationTrigger().orFinally(triggerB.getContinuationTrigger())),
        aOrFinallyB.getContinuationTrigger());
    assertEquals(
        Repeatedly.forever(
            triggerB.getContinuationTrigger().orFinally(triggerA.getContinuationTrigger())),
        bOrFinallyA.getContinuationTrigger());
  }
}
