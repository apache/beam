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
 * Tests the {@link AfterWatermark} triggers.
 */
@RunWith(JUnit4.class)
public class AfterWatermarkTest {

  @Mock
  private OnceTrigger<IntervalWindow> mockEarly;
  @Mock
  private OnceTrigger<IntervalWindow> mockLate;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testFirstInPaneWithFixedWindow() throws Exception {
    Duration windowDuration = Duration.millis(10);
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        AfterWatermark.<IntervalWindow>pastFirstElementInPane().plusDelayOf(Duration.millis(5)),
        FixedWindows.of(windowDuration));

    tester.injectElements(1); // first in window [0, 10), timer set for 6
    tester.advanceInputWatermark(new Instant(5));
    tester.injectElements(9, 8);
    assertThat(tester.getResultSequence(), everyItem(equalTo(TriggerResult.CONTINUE)));

    tester.advanceInputWatermark(new Instant(7));

    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE_AND_FINISH));
    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(0), new Instant(10))));
    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(10), new Instant(20))));
  }

  @Test
  public void testAlignAndDelay() throws Exception {
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        AfterWatermark.<IntervalWindow>pastFirstElementInPane()
            .alignedTo(Duration.standardMinutes(1))
            .plusDelayOf(Duration.standardMinutes(5)),
        FixedWindows.of(Duration.standardMinutes(1)));

    Instant zero = new Instant(0);

    // first in window [0, 1m), timer set for 6m
    tester.injectElements(1000, 5000, 55000);

    // Advance to 6m. No output should be produced.
    tester.advanceInputWatermark(zero.plus(Duration.standardMinutes(6)));
    assertThat(tester.getResultSequence(), everyItem(equalTo(TriggerResult.CONTINUE)));

    // Advance to 6m+1ms and see our output
    tester.advanceInputWatermark(zero.plus(Duration.standardMinutes(6).plus(1)));
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE_AND_FINISH));
  }

  @Test
  public void testAfterFirstInPaneClear() throws Exception {
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        AfterWatermark.<IntervalWindow>pastFirstElementInPane().plusDelayOf(Duration.millis(5)),
        FixedWindows.of(Duration.millis(10)));

    tester.injectElements(1, 2, 3);
    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(10));
    tester.clearState(window);
    tester.assertCleared(window);
  }

  @Test
  public void testFirstInPaneWithMerging() throws Exception {
    Duration windowDuration = Duration.millis(10);
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        AfterWatermark.<IntervalWindow>pastFirstElementInPane().plusDelayOf(Duration.millis(5)),
        Sessions.withGapDuration(windowDuration));

    tester.advanceInputWatermark(new Instant(1));

    tester.injectElements(
        1,  // in [1, 11), timer for 6
        2); // in [2, 12), timer for 7
    tester.mergeWindows();
    tester.advanceInputWatermark(new Instant(7));

    // We merged, and updated the timer to the earliest timer, which was still 6.
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE_AND_FINISH));
    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(1), new Instant(12))));
  }

  @Test
  public void testEndOfWindowFixedWindow() throws Exception {
    Duration windowDuration = Duration.millis(10);
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        AfterWatermark.<IntervalWindow>pastEndOfWindow(),
        FixedWindows.of(windowDuration));

    tester.injectElements(1); // first in window [0, 10), timer set for 9
    tester.advanceInputWatermark(new Instant(8));
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.CONTINUE));

    tester.injectElements(9, 8);
    tester.advanceInputWatermark(new Instant(10));
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE_AND_FINISH));

    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(0), new Instant(10))));
    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(10), new Instant(20))));
  }

  @Test
  public void testEndOfWindowWithMerging() throws Exception {
    Duration windowDuration = Duration.millis(10);
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        AfterWatermark.<IntervalWindow>pastEndOfWindow(),
        Sessions.withGapDuration(windowDuration));

    tester.advanceInputWatermark(new Instant(1));

    tester.injectElements(
        1, // in [1, 11], timer for 11
        2); // in [2, 12], timer for 12
    tester.mergeWindows();
    tester.advanceInputWatermark(new Instant(10));

    // We merged, and updated the watermark timer to the end of the new window.
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.CONTINUE));

    tester.injectElements(1); // in [1, 11], timer for 11
    tester.mergeWindows();
    tester.advanceInputWatermark(new Instant(12));

    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE_AND_FINISH));

    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(1), new Instant(12))));
  }

  @Test
  public void testFireDeadline() throws Exception {
    BoundedWindow window = new IntervalWindow(new Instant(0), new Instant(10));

    assertEquals(new Instant(9), AfterWatermark.pastEndOfWindow()
        .getWatermarkThatGuaranteesFiring(window));
    assertEquals(GlobalWindow.INSTANCE.maxTimestamp(),
        AfterWatermark.pastEndOfWindow().getWatermarkThatGuaranteesFiring(GlobalWindow.INSTANCE));
    assertEquals(new Instant(19),
        AfterWatermark
            .pastFirstElementInPane()
            .plusDelayOf(Duration.millis(10)).getWatermarkThatGuaranteesFiring(window));
  }

  @Test
  public void testContinuation() throws Exception {
    Trigger<?> endOfWindow = AfterWatermark.pastEndOfWindow();
    assertEquals(endOfWindow, endOfWindow.getContinuationTrigger());
    assertEquals(
        endOfWindow,
        endOfWindow.getContinuationTrigger().getContinuationTrigger());

    Trigger<?> firstElementAligned =
        AfterWatermark.pastFirstElementInPane().alignedTo(Duration.standardDays(1));
    assertEquals(
        firstElementAligned,
        firstElementAligned.getContinuationTrigger());
    assertEquals(
        firstElementAligned,
        firstElementAligned.getContinuationTrigger().getContinuationTrigger());
  }

  @Test
  public void testEarlyAndAtWatermarkProcessElement() throws Exception {
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        AfterWatermark.<IntervalWindow>pastEndOfWindow()
            .withEarlyFirings(mockEarly),
        FixedWindows.of(Duration.millis(100)));

    // Fire the early trigger
    when(mockEarly.onElement(Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
        .thenReturn(TriggerResult.CONTINUE)
        .thenReturn(TriggerResult.FIRE_AND_FINISH)
        .thenReturn(TriggerResult.CONTINUE);
    tester.injectElements(5, 10); // Fires due to early trigger
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));

    // Fire the watermark trigger
    tester.injectElements(15);
    tester.advanceInputWatermark(new Instant(100L)); // Fires due to AtWatermark
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE_AND_FINISH));
    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(0), new Instant(100))));
  }

  @Test
  public void testLateAndAtWatermarkProcessElement() throws Exception {
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        AfterWatermark.<IntervalWindow>pastEndOfWindow()
            .withLateFirings(mockLate),
        FixedWindows.of(Duration.millis(100)));

    tester.injectElements(5, 10, 15);
    assertThat(tester.getResultSequence(), everyItem(equalTo(TriggerResult.CONTINUE)));

    tester.advanceInputWatermark(new Instant(100L)); // Fires due to AtWatermark
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));

    when(mockLate.onElement(Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
        .thenReturn(TriggerResult.CONTINUE)
        .thenReturn(TriggerResult.CONTINUE)
        .thenReturn(TriggerResult.FIRE_AND_FINISH)
        .thenReturn(TriggerResult.CONTINUE)
        .thenReturn(TriggerResult.FIRE_AND_FINISH);

    tester.injectElements(20, 25, 30); // Fires due to late trigger
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));

    tester.injectElements(35, 40); // Fires due to late trigger
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));

    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(0), new Instant(100))));
  }

  @Test
  public void testEarlyLateAndAtWatermarkProcessElement() throws Exception {
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        AfterWatermark.<IntervalWindow>pastEndOfWindow()
            .withEarlyFirings(mockEarly)
            .withLateFirings(mockLate),
        FixedWindows.of(Duration.millis(100)));

    when(mockEarly.onElement(Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
        .thenReturn(TriggerResult.CONTINUE)
        .thenReturn(TriggerResult.FIRE_AND_FINISH)
        .thenReturn(TriggerResult.CONTINUE);

    // These should set a timer for 100
    tester.injectElements(5, 10); // Fires due to early trigger
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));

    tester.injectElements(15);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.CONTINUE));

    tester.advanceInputWatermark(new Instant(99L)); // Checking for off-by-one
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.CONTINUE));

    tester.advanceInputWatermark(new Instant(100L)); // Fires due to AtWatermark
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));

    when(mockLate.onElement(Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
        .thenReturn(TriggerResult.CONTINUE)
        .thenReturn(TriggerResult.CONTINUE)
        .thenReturn(TriggerResult.FIRE_AND_FINISH)
        .thenReturn(TriggerResult.CONTINUE)
        .thenReturn(TriggerResult.FIRE_AND_FINISH);

    // Get the late trigger to fire once
    tester.clearResultSequence();
    tester.injectElements(20, 25);
    assertThat(tester.getResultSequence(), everyItem(equalTo(TriggerResult.CONTINUE)));
    tester.injectElements(30); // Fires due to late trigger
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));

    // Get the late trigger to fire again
    tester.clearResultSequence();
    tester.injectElements(35);
    assertThat(tester.getResultSequence(), everyItem(equalTo(TriggerResult.CONTINUE)));
    tester.injectElements(40); // Fires due to late again
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));

    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(0), new Instant(100))));
  }

  @Test
  public void testEarlyAndAtWatermarkSessions() throws Exception {
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        AfterWatermark.<IntervalWindow>pastEndOfWindow()
            .withEarlyFirings(AfterPane.<IntervalWindow>elementCountAtLeast(2)),
        Sessions.withGapDuration(Duration.millis(20)));

    tester.injectElements(5, 20);
    tester.mergeWindows();
    assertThat(tester.getLatestMergeResult(), equalTo(MergeResult.FIRE));
    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(5), new Instant(40))));

    tester.injectElements(6);
    tester.mergeWindows();
    assertThat(tester.getLatestMergeResult(), equalTo(MergeResult.CONTINUE));

    tester.advanceInputWatermark(new Instant(100L)); // Fires due to AtWatermark
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE_AND_FINISH));
    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(5), new Instant(40))));
  }

  @Test
  public void testLateAndAtWatermarkSessionsProcessingTime() throws Exception {
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        AfterWatermark.<IntervalWindow>pastEndOfWindow()
            .withLateFirings(AfterProcessingTime
                .<IntervalWindow>pastFirstElementInPane().plusDelayOf(Duration.millis(10))),
        Sessions.withGapDuration(Duration.millis(20)));

    tester.advanceProcessingTime(new Instant(0L));
    tester.injectElements(5, 20);
    tester.advanceProcessingTime(new Instant(50L)); // Check that  we don't trigger wrongly
    assertThat(tester.getResultSequence(), everyItem(equalTo(TriggerResult.CONTINUE)));

    tester.advanceInputWatermark(new Instant(70L)); // Fires due to AtWatermark
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));

    tester.injectElements(6);
    tester.mergeWindows(); // merge must be manually triggered to learn we are on late trigger
    tester.advanceProcessingTime(new Instant(100L)); // Fires the late trigger
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));

    tester.injectElements(9);
    tester.mergeWindows(); // merge must be manually triggered to learn we are on late trigger
    tester.advanceProcessingTime(new Instant(150L)); // Fires the late trigger again

    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));
    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(5), new Instant(40))));
  }

  @Test
  public void testLateAndAtWatermarkSessions() throws Exception {
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        AfterWatermark.<IntervalWindow>pastEndOfWindow()
            .withLateFirings(AfterPane.<IntervalWindow>elementCountAtLeast(2)),
        Sessions.withGapDuration(Duration.millis(20)));

    tester.injectElements(5, 20);

    tester.advanceInputWatermark(new Instant(70L)); // Fires due to AtWatermark
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));

    IntervalWindow window = new IntervalWindow(new Instant(5), new Instant(40));
    assertFalse(tester.isMarkedFinished(window));

    tester.injectElements(7, 8, 9); // Fires because we have at least 5 items
    tester.mergeWindows();

    assertThat(tester.getLatestMergeResult(), equalTo(MergeResult.FIRE));
    assertFalse(tester.isMarkedFinished(window));
  }

  @Test
  public void testEarlyLateAndAtWatermarkSessions() throws Exception {
    SimpleTriggerTester<IntervalWindow> tester = TriggerTester.forTrigger(
        AfterWatermark.<IntervalWindow>pastEndOfWindow()
            .withEarlyFirings(AfterProcessingTime.<IntervalWindow>pastFirstElementInPane()
                .plusDelayOf(Duration.millis(50)))
            .withLateFirings(AfterPane.<IntervalWindow>elementCountAtLeast(2)),
        Sessions.withGapDuration(Duration.millis(20)));

    tester.advanceProcessingTime(new Instant(0));
    tester.injectElements(5, 20);

    tester.advanceProcessingTime(new Instant(56)); // Fires due to early trigger
    tester.advanceInputWatermark(new Instant(70L)); // Fires due to AtWatermark

    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(5), new Instant(40))));

    tester.injectElements(
        6,
        7, // Should fire due to late trigger
        8);
    tester.mergeWindows();

    assertThat(tester.getLatestMergeResult(), equalTo(MergeResult.FIRE));
    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(5), new Instant(40))));
  }
}
