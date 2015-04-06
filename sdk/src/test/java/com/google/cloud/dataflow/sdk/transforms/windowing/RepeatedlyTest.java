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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.AtMostOnceTrigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnElementEvent;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnMergeEvent;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnTimerEvent;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TimeDomain;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerContext;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerResult;
import com.google.cloud.dataflow.sdk.util.TriggerTester;
import com.google.common.collect.ImmutableList;

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
 * Tests for {@link Repeatedly}.
 */
@RunWith(JUnit4.class)
public class RepeatedlyTest {
  @Mock private Trigger<IntervalWindow> mockTrigger1;
  @Mock private AtMostOnceTrigger<IntervalWindow> mockTrigger2;
  private TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester;
  private IntervalWindow firstWindow;

  public void setUp(WindowFn<?, IntervalWindow> windowFn, boolean until) throws Exception {
    MockitoAnnotations.initMocks(this);
    Trigger<IntervalWindow> underTest = until
        ? Repeatedly.forever(mockTrigger1).until(mockTrigger2)
        : Repeatedly.forever(mockTrigger1);

    tester = TriggerTester.buffering(windowFn, underTest);
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
  public void testOnElementNoUntil() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)), false);

    injectElement(1, TriggerResult.CONTINUE, null);
    injectElement(2, TriggerResult.FIRE_AND_FINISH, null);
    injectElement(3, TriggerResult.FIRE_AND_FINISH, null);
    injectElement(4, TriggerResult.FINISH, null);

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 0, 10),
        isSingleWindowedValue(Matchers.containsInAnyOrder(3), 3, 0, 10)));
    assertFalse(tester.isDone(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(
        tester.bufferTag(firstWindow)));
  }

  @Test
  public void testOnElementUntilFires() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)), true);

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    injectElement(2, TriggerResult.FIRE_AND_FINISH, TriggerResult.FIRE);

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 0, 10)));
    assertTrue(tester.isDone(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(
        // We're storing that the root trigger has finished.
        tester.rootFinished(firstWindow)));
  }

  @Test
  public void testOnElementUntilFinishes() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)), true);

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    injectElement(2, TriggerResult.FIRE_AND_FINISH, TriggerResult.FINISH);
    injectElement(3, TriggerResult.FIRE,
        // until is already finished, so this shouldn't be called, and shouldn't finish the repeat.
        TriggerResult.FIRE);

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 0, 10),
        isSingleWindowedValue(Matchers.containsInAnyOrder(3), 3, 0, 10)));
    assertFalse(tester.isDone(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(
        // We're storing the sub-trigger state for the root trigger.
        tester.subFinished(firstWindow)));
  }

  @Test
  public void testOnElementTimerFiresWithoutUntil() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)), false);

    injectElement(1, TriggerResult.CONTINUE, null);

    tester.setTimer(firstWindow, new Instant(11), TimeDomain.EVENT_TIME, ImmutableList.of(0));
    when(mockTrigger1.onTimer(isTriggerContext(), Mockito.<OnTimerEvent<IntervalWindow>>any()))
        .thenReturn(TriggerResult.FIRE);
    tester.advanceWatermark(new Instant(12));

    injectElement(2, TriggerResult.CONTINUE, null);

    tester.setTimer(firstWindow, new Instant(12), TimeDomain.EVENT_TIME, ImmutableList.of(0));
    when(mockTrigger1.onTimer(isTriggerContext(), Mockito.<OnTimerEvent<IntervalWindow>>any()))
        .thenReturn(TriggerResult.FIRE_AND_FINISH);
    tester.advanceWatermark(new Instant(13));

    injectElement(3, TriggerResult.CONTINUE, null);

    tester.setTimer(firstWindow, new Instant(13), TimeDomain.EVENT_TIME, ImmutableList.of(0));
    when(mockTrigger1.onTimer(isTriggerContext(), Mockito.<OnTimerEvent<IntervalWindow>>any()))
        .thenReturn(TriggerResult.FINISH);
    tester.advanceWatermark(new Instant(14));

    injectElement(4, TriggerResult.CONTINUE, null);

    tester.setTimer(firstWindow, new Instant(14), TimeDomain.EVENT_TIME, ImmutableList.of(0));
    when(mockTrigger1.onTimer(isTriggerContext(), Mockito.<OnTimerEvent<IntervalWindow>>any()))
        .thenReturn(TriggerResult.FIRE);
    tester.advanceWatermark(new Instant(15));

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1), 1, 0, 10),
        isSingleWindowedValue(Matchers.containsInAnyOrder(2), 2, 0, 10),
        isSingleWindowedValue(Matchers.containsInAnyOrder(3, 4), 3, 0, 10)));
    assertFalse(tester.isDone(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.emptyIterable());
  }

  @Test
  public void testOnElementTimerFiresWithUntil() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)), true);

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);

    // Timer fires for until, which says continue
    tester.setTimer(firstWindow, new Instant(11), TimeDomain.EVENT_TIME, ImmutableList.of(1));
    when(mockTrigger2.onTimer(isTriggerContext(), Mockito.<OnTimerEvent<IntervalWindow>>any()))
        .thenReturn(TriggerResult.CONTINUE);
    tester.advanceWatermark(new Instant(12));

    injectElement(2, TriggerResult.FIRE, TriggerResult.CONTINUE);

    // Timer fires for until, which says fire, so we stop repeating.
    tester.setTimer(firstWindow, new Instant(12), TimeDomain.EVENT_TIME, ImmutableList.of(1));
    when(mockTrigger2.onTimer(isTriggerContext(), Mockito.<OnTimerEvent<IntervalWindow>>any()))
        .thenReturn(TriggerResult.FIRE);
    tester.advanceWatermark(new Instant(13));

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 0, 10)));
    assertTrue(tester.isDone(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(
        tester.rootFinished(firstWindow)));
  }

  @Test
  public void testOnElementTimerFinishesUntil() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)), true);

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);

    // Timer fires for until, which says continue
    tester.setTimer(firstWindow, new Instant(11), TimeDomain.EVENT_TIME, ImmutableList.of(1));
    when(mockTrigger2.onTimer(isTriggerContext(), Mockito.<OnTimerEvent<IntervalWindow>>any()))
        .thenReturn(TriggerResult.FINISH);
    tester.advanceWatermark(new Instant(12));

    injectElement(2, TriggerResult.FIRE, TriggerResult.CONTINUE);

    // Timer fires for until, which says finish, so we stop paying attention to it.
    tester.setTimer(firstWindow, new Instant(12), TimeDomain.EVENT_TIME, ImmutableList.of(1));
    when(mockTrigger2.onTimer(isTriggerContext(), Mockito.<OnTimerEvent<IntervalWindow>>any()))
        .thenReturn(TriggerResult.FIRE);
    tester.advanceWatermark(new Instant(13));

    // This timer for the until shouldn't do anything
    tester.setTimer(firstWindow, new Instant(13), TimeDomain.EVENT_TIME, ImmutableList.of(1));
    when(mockTrigger2.onTimer(isTriggerContext(), Mockito.<OnTimerEvent<IntervalWindow>>any()))
        .thenReturn(TriggerResult.FINISH);
    tester.advanceWatermark(new Instant(14));

    // But we should be able to fire trigger 1 still
    injectElement(3, TriggerResult.CONTINUE, null);
    tester.setTimer(firstWindow, new Instant(14), TimeDomain.EVENT_TIME, ImmutableList.of(0));
    when(mockTrigger1.onTimer(isTriggerContext(), Mockito.<OnTimerEvent<IntervalWindow>>any()))
        .thenReturn(TriggerResult.FIRE);
    tester.advanceWatermark(new Instant(15));

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 0, 10),
        isSingleWindowedValue(Matchers.containsInAnyOrder(3), 3, 0, 10)));
    assertFalse(tester.isDone(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(
        // We're storing that the until sub-trigger has finished
        tester.subFinished(firstWindow)));
  }

  @Test
  public void testMergeWithoutUntil() throws Exception {
    setUp(Sessions.withGapDuration(Duration.millis(10)), false);

    injectElement(1, TriggerResult.CONTINUE, null);
    injectElement(12, TriggerResult.FINISH, null);

    when(mockTrigger1.onMerge(
        isTriggerContext(),
        Mockito.<OnMergeEvent<IntervalWindow>>any())).thenReturn(TriggerResult.FIRE_AND_FINISH);

    // The arrival of this element should trigger merging.
    injectElement(5, TriggerResult.CONTINUE, TriggerResult.CONTINUE);

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 5, 12), 1, 1, 22)));
    assertFalse(tester.isDone(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.emptyIterable());
  }

  @Test
  public void testMergeUntilFires() throws Exception {
    setUp(Sessions.withGapDuration(Duration.millis(10)), true);

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    injectElement(12, TriggerResult.FINISH, TriggerResult.CONTINUE);

    when(mockTrigger1.onMerge(
        isTriggerContext(),
        Mockito.<OnMergeEvent<IntervalWindow>>any())).thenReturn(TriggerResult.FIRE_AND_FINISH);

    when(mockTrigger2.onMerge(
        isTriggerContext(),
        Mockito.<OnMergeEvent<IntervalWindow>>any())).thenReturn(TriggerResult.FIRE);

    // The arrival of this element should trigger merging.
    injectElement(5, TriggerResult.CONTINUE, TriggerResult.CONTINUE);

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 5, 12), 1, 1, 22)));
    // the until fired during the merge
    assertTrue(tester.isDone(new IntervalWindow(new Instant(1), new Instant(22))));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(
        // We're storing that the root has finished
        tester.rootFinished(new IntervalWindow(new Instant(1), new Instant(22)))));
  }

  @Test
  public void testMergeRepeatUntilFinished() throws Exception {
    setUp(Sessions.withGapDuration(Duration.millis(10)), true);

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    injectElement(12, TriggerResult.FINISH, TriggerResult.FINISH);
    assertFalse(tester.isDone(new IntervalWindow(new Instant(1), new Instant(11))));
    assertFalse(tester.isDone(new IntervalWindow(new Instant(12), new Instant(22))));

    when(mockTrigger1.onMerge(
        isTriggerContext(),
        Mockito.<OnMergeEvent<IntervalWindow>>any())).thenReturn(TriggerResult.FIRE_AND_FINISH);

    // The arrival of this element should trigger merging.
    injectElement(5, TriggerResult.CONTINUE, TriggerResult.CONTINUE);

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 5, 12), 1, 1, 22)));
    assertFalse(tester.isDone(new IntervalWindow(new Instant(1), new Instant(22))));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(
        // Remembering that we finished the until clause in the 1-22 window.
        tester.subFinished(new IntervalWindow(new Instant(1), new Instant(22)))));
  }

  @Test
  public void testMergeRepeatUntilFired() throws Exception {
    setUp(Sessions.withGapDuration(Duration.millis(10)), true);

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    injectElement(12, TriggerResult.CONTINUE, TriggerResult.FIRE);
    assertFalse(tester.isDone(new IntervalWindow(new Instant(1), new Instant(11))));
    assertTrue(tester.isDone(new IntervalWindow(new Instant(12), new Instant(22))));

    // The arrival of this element would cause a merge (see above), but since we mark this finished
    // in the window from [5, 15), when we merge, the TriggerExecutor finishes things for us.
    injectElement(5, TriggerResult.CONTINUE, TriggerResult.FIRE);

    assertThat(tester.extractOutput(), Matchers.emptyIterable());

    assertTrue(tester.isDone(new IntervalWindow(new Instant(12), new Instant(22))));
    assertTrue(tester.isDone(new IntervalWindow(new Instant(5), new Instant(15))));

    assertThat(tester.getKeyedStateInUse(), Matchers.containsInAnyOrder(
        tester.bufferTag(new IntervalWindow(new Instant(1), new Instant(11))),
        tester.rootFinished(new IntervalWindow(new Instant(5), new Instant(15))),
        tester.rootFinished(new IntervalWindow(new Instant(12), new Instant(22)))));
  }
}
