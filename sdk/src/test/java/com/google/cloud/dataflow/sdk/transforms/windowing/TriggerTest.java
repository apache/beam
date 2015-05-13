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

import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.MergeResult;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnElementEvent;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnMergeEvent;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnTimerEvent;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnceTrigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OrFinallyTrigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TimeDomain;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerContext;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerResult;
import com.google.cloud.dataflow.sdk.util.ExecutableTrigger;
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
 * Tests for {@link Trigger}.
 */
@RunWith(JUnit4.class)
public class TriggerTest {
  @Mock private Trigger<IntervalWindow> mockActual;
  @Mock private OnceTrigger<IntervalWindow> mockUntil;
  private ExecutableTrigger<IntervalWindow> executableUntil;

  private TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester;
  private IntervalWindow firstWindow;

  public void setUp(WindowFn<?, IntervalWindow> windowFn) throws Exception {
    MockitoAnnotations.initMocks(this);

    Trigger<IntervalWindow> underTest =
        new OrFinallyTrigger<IntervalWindow>(mockActual, mockUntil);

    tester = TriggerTester.nonCombining(
        windowFn, underTest, AccumulationMode.DISCARDING_FIRED_PANES);
    executableUntil = tester.getTrigger().subTriggers().get(1);
    firstWindow = new IntervalWindow(new Instant(0), new Instant(10));
  }

  @SuppressWarnings("unchecked")
  private TriggerContext<IntervalWindow> isTriggerContext() {
    return Mockito.isA(TriggerContext.class);
  }

  private void injectElement(int element, TriggerResult result1, TriggerResult result2)
      throws Exception {
    if (result1 != null) {
      when(mockActual.onElement(
          isTriggerContext(), Mockito.<OnElementEvent<IntervalWindow>>any()))
          .thenReturn(result1);
    }
    if (result2 != null) {
      when(mockUntil.onElement(
          isTriggerContext(), Mockito.<OnElementEvent<IntervalWindow>>any()))
          .thenReturn(result2);
    }
    tester.injectElement(element, new Instant(element));
  }

  @Test
  public void testOnElementActualFires() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.FIRE, TriggerResult.CONTINUE);
    injectElement(2, TriggerResult.FIRE_AND_FINISH, TriggerResult.CONTINUE);

    // This should do nothing (we've already fired and finished)
    injectElement(3, TriggerResult.FIRE, TriggerResult.FIRE_AND_FINISH);

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1), 1, 0, 10),
        isSingleWindowedValue(Matchers.containsInAnyOrder(2), 2, 0, 10)));
    assertTrue(tester.isDone(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(
        // We're storing that the root trigger has finished.
        tester.finishedSet(firstWindow)));
  }

  @Test
  public void testOnElementUntilFires() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    injectElement(2, TriggerResult.CONTINUE, TriggerResult.FIRE_AND_FINISH);

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 0, 10)));
    assertTrue(tester.isDone(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(
        // We're storing that the root trigger has finished.
        tester.finishedSet(firstWindow)));
  }

  @Test
  public void testOnElementUntilFiresAndFinishes() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    injectElement(2, TriggerResult.CONTINUE, TriggerResult.FIRE_AND_FINISH);

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 0, 10)));
    assertTrue(tester.isDone(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(
        // We're storing that the root trigger has finished.
        tester.finishedSet(firstWindow)));
  }

  @Test
  public void testOnTimerFiresWithUntil() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);

    // Timer fires for until, which says continue
    tester.setTimer(firstWindow, new Instant(11), TimeDomain.EVENT_TIME, executableUntil);
    when(mockUntil.onTimer(isTriggerContext(), Mockito.<OnTimerEvent<IntervalWindow>>any()))
        .thenReturn(TriggerResult.CONTINUE);
    tester.advanceWatermark(new Instant(12));

    injectElement(2, TriggerResult.FIRE, TriggerResult.CONTINUE);

    // Timer fires for until, which says fire, so we stop repeating.
    tester.setTimer(firstWindow, new Instant(12), TimeDomain.EVENT_TIME, executableUntil);
    when(mockUntil.onTimer(isTriggerContext(), Mockito.<OnTimerEvent<IntervalWindow>>any()))
        .thenReturn(TriggerResult.FIRE_AND_FINISH);
    tester.advanceWatermark(new Instant(13));

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 0, 10),
        isSingleWindowedValue(Matchers.emptyIterable(), 9, 0, 10)));
    assertTrue(tester.isDone(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(
        tester.finishedSet(firstWindow)));
  }

  @Test
  public void testOnTimerFinishesUntil() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);

    // Timer fires for until, which says continue
    tester.setTimer(firstWindow, new Instant(11), TimeDomain.EVENT_TIME, executableUntil);
    when(mockUntil.onTimer(isTriggerContext(), Mockito.<OnTimerEvent<IntervalWindow>>any()))
        .thenReturn(TriggerResult.CONTINUE);
    tester.advanceWatermark(new Instant(12));

    injectElement(2, TriggerResult.FIRE, TriggerResult.CONTINUE);

    injectElement(3, TriggerResult.CONTINUE, TriggerResult.CONTINUE);

    // Timer fires for until, which says FIRE, so we fire and finish
    tester.setTimer(firstWindow, new Instant(12), TimeDomain.EVENT_TIME, executableUntil);
    when(mockUntil.onTimer(isTriggerContext(), Mockito.<OnTimerEvent<IntervalWindow>>any()))
        .thenReturn(TriggerResult.FIRE_AND_FINISH);
    tester.advanceWatermark(new Instant(13));

    // These timers shouldn't do anything -- at this point we've already finished
    tester.setTimer(firstWindow, new Instant(13), TimeDomain.EVENT_TIME, executableUntil);
    tester.advanceWatermark(new Instant(14));

    tester.setTimer(firstWindow, new Instant(14), TimeDomain.EVENT_TIME, executableUntil);
    tester.advanceWatermark(new Instant(15));

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 0, 10),
        isSingleWindowedValue(Matchers.containsInAnyOrder(3), 3, 0, 10)));
    assertTrue(tester.isDone(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(
        tester.finishedSet(firstWindow)));
  }

  @Test
  public void testMergeActualFires() throws Exception {
    setUp(Sessions.withGapDuration(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    injectElement(12, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    injectElement(5, TriggerResult.CONTINUE, TriggerResult.CONTINUE);

    when(mockActual.onMerge(
        isTriggerContext(),
        Mockito.<OnMergeEvent<IntervalWindow>>any())).thenReturn(MergeResult.FIRE);

    when(mockUntil.onMerge(
        isTriggerContext(),
        Mockito.<OnMergeEvent<IntervalWindow>>any())).thenReturn(MergeResult.CONTINUE);
    tester.doMerge();

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 5, 12), 1, 1, 22)));
    assertFalse(tester.isDone(new IntervalWindow(new Instant(1), new Instant(22))));
    assertThat(tester.getKeyedStateInUse(), Matchers.emptyIterable());
  }

  @Test
  public void testMergeUntilFires() throws Exception {
    setUp(Sessions.withGapDuration(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    injectElement(12, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    injectElement(5, TriggerResult.CONTINUE, TriggerResult.CONTINUE);

    when(mockActual.onMerge(
        isTriggerContext(),
        Mockito.<OnMergeEvent<IntervalWindow>>any())).thenReturn(MergeResult.CONTINUE);

    when(mockUntil.onMerge(
        isTriggerContext(),
        Mockito.<OnMergeEvent<IntervalWindow>>any())).thenReturn(MergeResult.FIRE_AND_FINISH);

    tester.doMerge();

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 5, 12), 1, 1, 22)));
    // the until fired during the merge
    assertTrue(tester.isDone(new IntervalWindow(new Instant(1), new Instant(22))));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(
        // We're storing that the root has finished
        tester.finishedSet(new IntervalWindow(new Instant(1), new Instant(22)))));
  }

  @Test
  public void testFireDeadline() throws Exception {
    BoundedWindow window = new IntervalWindow(new Instant(0), new Instant(10));

    assertEquals(new Instant(9),
        Repeatedly.forever(AfterWatermark.pastEndOfWindow()).getWatermarkCutoff(window));
    assertEquals(new Instant(9), Repeatedly.forever(AfterWatermark.pastEndOfWindow())
        .orFinally(AfterPane.elementCountAtLeast(1))
        .getWatermarkCutoff(window));
    assertEquals(new Instant(9), Repeatedly.forever(AfterPane.elementCountAtLeast(1))
        .orFinally(AfterWatermark.pastEndOfWindow())
        .getWatermarkCutoff(window));
    assertEquals(new Instant(9),
        AfterPane.elementCountAtLeast(100)
            .orFinally(AfterWatermark.pastEndOfWindow())
            .getWatermarkCutoff(window));

    assertEquals(BoundedWindow.TIMESTAMP_MAX_VALUE,
        Repeatedly.forever(AfterPane.elementCountAtLeast(1))
        .orFinally(AfterPane.elementCountAtLeast(10))
        .getWatermarkCutoff(window));
  }

  @Test
  public void testOrFinallyRealTriggersFixedWindow() throws Exception {
    // Test an orFinally with a composite trigger, and make sure it properly resets state, etc.
    tester = TriggerTester.nonCombining(FixedWindows.of(Duration.millis(50)),
        Repeatedly.<IntervalWindow>forever(
            // This element count should never fire because the orFinally fires sooner, every time
            AfterPane.<IntervalWindow>elementCountAtLeast(12)
                .orFinally(AfterAll.<IntervalWindow>of(
                    AfterProcessingTime.<IntervalWindow>pastFirstElementInPane()
                        .plusDelayOf(Duration.millis(5)),
                    AfterPane.<IntervalWindow>elementCountAtLeast(5)))),
        AccumulationMode.DISCARDING_FIRED_PANES);

    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(50));

    // First, fire processing time then the 5 element

    tester.advanceProcessingTime(new Instant(0));
    tester.injectElement(0, new Instant(0));
    tester.injectElement(1, new Instant(0));
    tester.injectElement(2, new Instant(1));
    tester.injectElement(3, new Instant(1));
    tester.advanceProcessingTime(new Instant(5));
    assertThat(tester.extractOutput(), Matchers.emptyIterable());

    assertThat(tester.getKeyedStateInUse(), Matchers.containsInAnyOrder(
        Matchers.equalTo(tester.finishedSet(window)),
        Matchers.equalTo(tester.bufferTag(window)),
        Matchers.containsString("delayed-until"),
        Matchers.containsString("elements-in-pane"),
        Matchers.containsString("elements-in-pane"),
        Matchers.containsString("earliest-element")));

    tester.injectElement(4, new Instant(1));
    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(0, 1, 2, 3, 4), 0, 0, 50)));

    // Then fire 6 new elements, then processing time
    tester.injectElement(6, new Instant(2));
    tester.injectElement(7, new Instant(3));
    tester.injectElement(8, new Instant(4));
    tester.injectElement(9, new Instant(5));
    tester.injectElement(10, new Instant(2));
    tester.injectElement(11, new Instant(3));
    assertThat(tester.extractOutput(), Matchers.emptyIterable());
    tester.advanceProcessingTime(new Instant(15));

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(6, 7, 8, 9, 10, 11), 2, 0, 50)));

    // Finally, fire 3 more elements and verify the base of the orFinally doesn't fire.
    tester.injectElement(100, new Instant(1));
    tester.injectElement(101, new Instant(1));
    tester.injectElement(102, new Instant(1));
    assertThat(tester.extractOutput(), Matchers.emptyIterable());
  }

  @Test
  public void testTriggerToString() throws Exception {
    assertEquals("AfterWatermark.FromEndOfWindow", AfterWatermark.pastEndOfWindow().toString());
    assertEquals("Repeatedly(AfterWatermark.FromEndOfWindow)",
        Repeatedly.forever(AfterWatermark.pastEndOfWindow()).toString());
  }
}
