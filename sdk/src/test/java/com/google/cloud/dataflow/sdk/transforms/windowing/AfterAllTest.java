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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
 * Tests for {@link AfterAll}.
 */
@RunWith(JUnit4.class)
public class AfterAllTest {
  @Mock private OnceTrigger<IntervalWindow> mockTrigger1;
  @Mock private OnceTrigger<IntervalWindow> mockTrigger2;

  private SimpleTriggerTester<IntervalWindow> tester;
  private IntervalWindow firstWindow;

  public void setUp(WindowFn<?, IntervalWindow> windowFn) throws Exception {
    MockitoAnnotations.initMocks(this);
    tester = TriggerTester.forTrigger(
        AfterAll.of(mockTrigger1, mockTrigger2),
        windowFn);
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

  @Test
  public void testOnElementT1FiresFirst() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.CONTINUE));

    injectElement(2, TriggerResult.FIRE_AND_FINISH, TriggerResult.CONTINUE);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.CONTINUE));

    injectElement(3, null, TriggerResult.FIRE_AND_FINISH);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE_AND_FINISH));
    assertTrue(tester.isMarkedFinished(firstWindow));
  }

  @Test
  public void testOnElementT2FiresFirst() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.FIRE_AND_FINISH);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.CONTINUE));

    injectElement(2, TriggerResult.FIRE_AND_FINISH, null);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE_AND_FINISH));

    assertTrue(tester.isMarkedFinished(firstWindow));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOnTimerFire() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.FIRE_AND_FINISH);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.CONTINUE));

    when(mockTrigger1.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
        .thenReturn(TriggerResult.FIRE_AND_FINISH);
    tester.advanceInputWatermark(new Instant(12));

    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE_AND_FINISH));
    assertTrue(tester.isMarkedFinished(firstWindow));
  }

  @Test
  public void testOnTimerFireAndFinish() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.CONTINUE));

    when(mockTrigger1.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
        .thenReturn(TriggerResult.CONTINUE);
    when(mockTrigger2.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
        .thenReturn(TriggerResult.FIRE_AND_FINISH);
    tester.fireTimer(firstWindow, new Instant(11), TimeDomain.EVENT_TIME);

    tester.advanceInputWatermark(new Instant(12));
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.CONTINUE));
    assertFalse(tester.isMarkedFinished(firstWindow));

    injectElement(2, TriggerResult.FIRE_AND_FINISH, null);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE_AND_FINISH));
    assertTrue(tester.isMarkedFinished(firstWindow));
  }

  @Test
  public void testOnMergeFires() throws Exception {
    setUp(Sessions.withGapDuration(Duration.millis(10)));

    when(mockTrigger1.onElement(Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
        .thenReturn(TriggerResult.CONTINUE);
    when(mockTrigger2.onElement(Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
        .thenReturn(TriggerResult.CONTINUE);
    tester.injectElements(1, 5);
    assertThat(tester.getResultSequence(), everyItem(equalTo(TriggerResult.CONTINUE)));

    when(mockTrigger1.onMerge(Mockito.<Trigger<IntervalWindow>.OnMergeContext>any()))
        .thenReturn(MergeResult.ALREADY_FINISHED);
    when(mockTrigger2.onMerge(Mockito.<Trigger<IntervalWindow>.OnMergeContext>any()))
        .thenReturn(MergeResult.FIRE_AND_FINISH);
    tester.mergeWindows();

    assertThat(tester.getLatestMergeResult(), equalTo(MergeResult.FIRE_AND_FINISH));
    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(1), new Instant(15))));
  }

  @Test
  public void testOnMergeFiresNotAlreadyFinished() throws Exception {
    setUp(Sessions.withGapDuration(Duration.millis(10)));

    when(mockTrigger1.onElement(Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
        .thenReturn(TriggerResult.CONTINUE);
    when(mockTrigger2.onElement(Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
        .thenReturn(TriggerResult.CONTINUE);
    tester.injectElements(1, 12);
    assertThat(tester.getResultSequence(), everyItem(equalTo(TriggerResult.CONTINUE)));

    when(mockTrigger1.onMerge(Mockito.<Trigger<IntervalWindow>.OnMergeContext>any()))
        .thenReturn(MergeResult.ALREADY_FINISHED);
    when(mockTrigger2.onMerge(Mockito.<Trigger<IntervalWindow>.OnMergeContext>any()))
        .thenReturn(MergeResult.ALREADY_FINISHED);
    tester.injectElements(5);
    tester.mergeWindows();

    assertThat(tester.getLatestMergeResult(), equalTo(MergeResult.FIRE_AND_FINISH));
    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(1), new Instant(22))));
  }

  @Test
  public void testFireDeadline() throws Exception {
    BoundedWindow window = new IntervalWindow(new Instant(0), new Instant(10));

    assertEquals(new Instant(19),
        AfterAll.of(AfterWatermark.pastEndOfWindow(),
                    AfterWatermark.pastFirstElementInPane().plusDelayOf(Duration.millis(10)))
            .getWatermarkThatGuaranteesFiring(window));
    assertEquals(BoundedWindow.TIMESTAMP_MAX_VALUE,
        AfterAll.of(AfterWatermark.pastEndOfWindow(), AfterPane.elementCountAtLeast(1))
            .getWatermarkThatGuaranteesFiring(window));
  }

  @Test
  public void testAfterAllRealTriggersFixedWindow() throws Exception {
    tester = TriggerTester.forTrigger(Repeatedly.<IntervalWindow>forever(
        AfterAll.<IntervalWindow>of(
            AfterPane.<IntervalWindow>elementCountAtLeast(5),
            AfterProcessingTime.<IntervalWindow>pastFirstElementInPane()
                .plusDelayOf(Duration.millis(5)))),
        FixedWindows.of(Duration.millis(50)));

    tester.advanceProcessingTime(new Instant(0));
    // 6 elements -> after pane fires
    tester.injectElements(0, 0, 1, 1, 1, 2);

    assertThat(tester.getResultSequence(), everyItem(equalTo(TriggerResult.CONTINUE)));
    tester.advanceProcessingTime(new Instant(6));

    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));

    // 4 elements, advance processing time, then deliver the last elem
    tester.clearResultSequence();
    tester.advanceProcessingTime(new Instant(15));
    tester.injectElements(2, 3, 4, 5);
    tester.advanceProcessingTime(new Instant(21));
    assertThat(tester.getResultSequence(), everyItem(equalTo(TriggerResult.CONTINUE)));
    tester.injectElements(6);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));

    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(0), new Instant(50))));
  }

  @Test
  public void testAfterAllMergingWindowSomeFinished() throws Exception {
    Duration windowDuration = Duration.millis(10);
    tester = TriggerTester.forTrigger(
        AfterAll.<IntervalWindow>of(
            AfterProcessingTime.<IntervalWindow>pastFirstElementInPane()
                .plusDelayOf(Duration.millis(5)),
            AfterPane.<IntervalWindow>elementCountAtLeast(5)),
        Sessions.withGapDuration(windowDuration));

    tester.advanceProcessingTime(new Instant(10));
    tester.injectElements(1); // in [1, 11), timer for 15
    tester.advanceProcessingTime(new Instant(16));
    tester.injectElements(
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
    OnceTrigger<IntervalWindow> trigger1 = AfterProcessingTime.pastFirstElementInPane();
    OnceTrigger<IntervalWindow> trigger2 = AfterWatermark.pastEndOfWindow();
    Trigger<IntervalWindow> afterAll = AfterAll.of(trigger1, trigger2);
    assertEquals(
        AfterAll.of(trigger1.getContinuationTrigger(), trigger2.getContinuationTrigger()),
        afterAll.getContinuationTrigger());
  }
}
