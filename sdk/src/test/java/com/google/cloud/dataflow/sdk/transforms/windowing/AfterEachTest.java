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
 * Tests for {@link AfterEach}.
 */
@RunWith(JUnit4.class)
public class AfterEachTest {

  @Mock private Trigger<IntervalWindow> mockTrigger1;
  @Mock private Trigger<IntervalWindow> mockTrigger2;
  private ExecutableTrigger<IntervalWindow> executable1;

  private TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester;
  private IntervalWindow firstWindow;

  public void setUp(WindowFn<?, IntervalWindow> windowFn) throws Exception {
    MockitoAnnotations.initMocks(this);
    tester = TriggerTester.nonCombining(
        windowFn, AfterEach.inOrder(mockTrigger1, mockTrigger2),
        AccumulationMode.DISCARDING_FIRED_PANES);
    executable1 = tester.getTrigger().subTriggers().get(0);
    firstWindow = new IntervalWindow(new Instant(0), new Instant(10));
  }

  @SuppressWarnings("unchecked")
  private TriggerContext<IntervalWindow> isTriggerContext() {
    return Mockito.isA(TriggerContext.class);
  }

  private void injectElement(int element, TriggerResult result1, TriggerResult result2)
      throws Exception {
    if (result1 != null) {
      when(mockTrigger1.onElement(
          isTriggerContext(), Mockito.<OnElementEvent<IntervalWindow>>any()))
          .thenReturn(result1);
    }
    if (result2 != null) {
      when(mockTrigger2.onElement(
          isTriggerContext(), Mockito.<OnElementEvent<IntervalWindow>>any()))
          .thenReturn(result2);
    }
    tester.injectElement(element, new Instant(element));
  }

  @Test
  public void testOnElementT1Fires() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, null);
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
    assertTrue(tester.isDone(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(tester.finishedSet(firstWindow)));
  }

  @Test
  public void testOnElementT2Fires() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.FIRE);
    assertThat(tester.extractOutput(), Matchers.emptyIterable());
    assertFalse(tester.isDone(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(
        // Buffering element 1; Ignored the trigger for T2 since we aren't there yet.
        tester.bufferTag(firstWindow),
        // Still holding the earliest element, waiting to fire
        tester.earliestElement(firstWindow)));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOnTimerFire() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, null);

    tester.setTimer(firstWindow, new Instant(11), TimeDomain.EVENT_TIME, executable1);
    when(mockTrigger1.onTimer(isTriggerContext(), Mockito.isA(OnTimerEvent.class)))
        .thenReturn(TriggerResult.FIRE);
    tester.advanceWatermark(new Instant(12));

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1), 1, 0, 10)));
    assertFalse("Should still be waiting for the second trigger.", tester.isDone(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.emptyIterable());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOnTimerFinish() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, null);

    tester.setTimer(firstWindow, new Instant(11), TimeDomain.EVENT_TIME, executable1);
    when(mockTrigger1.onTimer(isTriggerContext(), Mockito.isA(OnTimerEvent.class)))
        .thenReturn(TriggerResult.FIRE_AND_FINISH);

    tester.advanceWatermark(new Instant(12));
    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1), 1, 0, 10)));

    injectElement(2, null, TriggerResult.FIRE_AND_FINISH);
    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(2), 2, 0, 10)));
    assertTrue(tester.isDone(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(tester.finishedSet(firstWindow)));
  }

  @Test
  public void testOnMergeFinishes() throws Exception {
    setUp(Sessions.withGapDuration(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    injectElement(12, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    injectElement(5, TriggerResult.CONTINUE, TriggerResult.CONTINUE);

    when(mockTrigger1.onMerge(
        isTriggerContext(),
        Mockito.<OnMergeEvent<IntervalWindow>>any()))
        .thenReturn(MergeResult.ALREADY_FINISHED);

    when(mockTrigger2.onMerge(
        isTriggerContext(),
        Mockito.<OnMergeEvent<IntervalWindow>>any())).thenReturn(MergeResult.FIRE_AND_FINISH);
    tester.doMerge();

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 5, 12), 1, 1, 22)));
    assertTrue(tester.isDone(new IntervalWindow(new Instant(1), new Instant(22))));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(
        tester.finishedSet(new IntervalWindow(new Instant(1), new Instant(22)))));
  }

  @Test
  public void testOnMergeFires() throws Exception {
    setUp(Sessions.withGapDuration(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    injectElement(12, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    injectElement(5, TriggerResult.CONTINUE, TriggerResult.CONTINUE);

    when(mockTrigger1.onMerge(
        isTriggerContext(),
        Mockito.<OnMergeEvent<IntervalWindow>>any()))
        .thenReturn(MergeResult.ALREADY_FINISHED);
    when(mockTrigger2.onMerge(
        isTriggerContext(),
        Mockito.<OnMergeEvent<IntervalWindow>>any())).thenReturn(MergeResult.FIRE);
    tester.doMerge();

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 5, 12), 1, 1, 22)));
    assertFalse(tester.isDone(new IntervalWindow(new Instant(1), new Instant(22))));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(
        tester.finishedSet(new IntervalWindow(new Instant(1), new Instant(22)))));
  }

  @Test
  public void testFireDeadline() throws Exception {
    BoundedWindow window = new IntervalWindow(new Instant(0), new Instant(10));

    assertEquals(new Instant(9),
        AfterEach.inOrder(AfterWatermark.pastEndOfWindow(),
                      AfterWatermark.pastEndOfWindow().plusDelayOf(Duration.millis(10)))
            .getWatermarkCutoff(window));
    assertEquals(BoundedWindow.TIMESTAMP_MAX_VALUE,
        AfterEach.inOrder(AfterPane.elementCountAtLeast(2), AfterWatermark.pastEndOfWindow())
            .getWatermarkCutoff(window));
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
        AccumulationMode.DISCARDING_FIRED_PANES);

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
    assertTrue(tester.isDone(new IntervalWindow(new Instant(0), new Instant(50))));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(
        tester.finishedSet(new IntervalWindow(new Instant(0), new Instant(50)))));
  }
}
