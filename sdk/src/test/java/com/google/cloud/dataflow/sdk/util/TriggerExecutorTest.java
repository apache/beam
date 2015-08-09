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

package com.google.cloud.dataflow.sdk.util;

import static com.google.cloud.dataflow.sdk.WindowMatchers.isSingleWindowedValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.WindowMatchers;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterFirst;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterPane;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterProcessingTime;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterWatermark;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo.Timing;
import com.google.cloud.dataflow.sdk.transforms.windowing.Repeatedly;
import com.google.cloud.dataflow.sdk.transforms.windowing.Sessions;
import com.google.cloud.dataflow.sdk.transforms.windowing.SlidingWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.MergeResult;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerResult;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window.ClosingBehavior;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy.AccumulationMode;

import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.List;

/**
 * Tests for {@link TriggerExecutor}.
 */
@RunWith(JUnit4.class)
public class TriggerExecutorTest {

  @Mock private Trigger<IntervalWindow> mockTrigger;
  private IntervalWindow firstWindow;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    firstWindow = new IntervalWindow(new Instant(0), new Instant(10));
  }

  private void injectElement(TriggerTester<Integer, ?, IntervalWindow> tester,
      int element, TriggerResult result)
      throws Exception {
    when(mockTrigger.onElement(
        Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
        .thenReturn(result);
    tester.injectElement(element, new Instant(element));
  }

  @Test
  public void testOnElementBufferingDiscarding() throws Exception {
    // Test basic execution of a trigger using a non-combining window set and discarding mode.
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        FixedWindows.of(Duration.millis(10)),
        mockTrigger,
        AccumulationMode.DISCARDING_FIRED_PANES,
        Duration.millis(100));

    injectElement(tester, 1, TriggerResult.CONTINUE);
    injectElement(tester, 2, TriggerResult.FIRE);

    injectElement(tester, 3, TriggerResult.FIRE_AND_FINISH);

    // This element shouldn't be seen, because the trigger has finished
    injectElement(tester, 4, null);

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 0, 10),
        isSingleWindowedValue(Matchers.containsInAnyOrder(3), 3, 0, 10)));
    assertTrue(tester.isMarkedFinished(firstWindow));
    tester.assertHasOnlyGlobalAndFinishedSetsFor(firstWindow);

    assertEquals(1, tester.getElementsDroppedDueToClosedWindow());
    assertEquals(0, tester.getElementsDroppedDueToLateness());
  }

  @Test
  public void testOnElementBufferingAccumulating() throws Exception {
    // Test basic execution of a trigger using a non-combining window set and accumulating mode.
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        FixedWindows.of(Duration.millis(10)),
        mockTrigger,
        AccumulationMode.ACCUMULATING_FIRED_PANES,
        Duration.millis(100));

    injectElement(tester, 1, TriggerResult.CONTINUE);
    injectElement(tester, 2, TriggerResult.FIRE);
    injectElement(tester, 3, TriggerResult.FIRE_AND_FINISH);

    // This element shouldn't be seen, because the trigger has finished
    injectElement(tester, 4, null);

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 0, 10),
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2, 3), 3, 0, 10)));
    assertTrue(tester.isMarkedFinished(firstWindow));
    tester.assertHasOnlyGlobalAndFinishedSetsFor(firstWindow);
  }

  @Test
  public void testOnElementCombiningDiscarding() throws Exception {
    // Test basic execution of a trigger using a non-combining window set and discarding mode.
    TriggerTester<Integer, Integer, IntervalWindow> tester = TriggerTester.combining(
        FixedWindows.of(Duration.millis(10)),
        mockTrigger,
        AccumulationMode.DISCARDING_FIRED_PANES,
        new Sum.SumIntegerFn().<String>asKeyedFn(),
        VarIntCoder.of(),
        Duration.millis(100));

    injectElement(tester, 2, TriggerResult.CONTINUE);
    injectElement(tester, 3, TriggerResult.FIRE);
    injectElement(tester, 4, TriggerResult.FIRE_AND_FINISH);

    // This element shouldn't be seen, because the trigger has finished
    injectElement(tester, 6, null);

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.equalTo(5), 2, 0, 10),
        isSingleWindowedValue(Matchers.equalTo(4), 4, 0, 10)));
    assertTrue(tester.isMarkedFinished(firstWindow));
    tester.assertHasOnlyGlobalAndFinishedSetsFor(firstWindow);
  }

  @Test
  public void testOnElementCombiningAccumulating() throws Exception {
    // Test basic execution of a trigger using a non-combining window set and accumulating mode.
    TriggerTester<Integer, Integer, IntervalWindow> tester = TriggerTester.combining(
        FixedWindows.of(Duration.millis(10)),
        mockTrigger,
        AccumulationMode.ACCUMULATING_FIRED_PANES,
        new Sum.SumIntegerFn().<String>asKeyedFn(),
        VarIntCoder.of(),
        Duration.millis(100));

    injectElement(tester, 1, TriggerResult.CONTINUE);
    injectElement(tester, 2, TriggerResult.FIRE);
    injectElement(tester, 3, TriggerResult.FIRE_AND_FINISH);

    // This element shouldn't be seen, because the trigger has finished
    injectElement(tester, 4, null);

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.equalTo(3), 1, 0, 10),
        isSingleWindowedValue(Matchers.equalTo(6), 3, 0, 10)));
    assertTrue(tester.isMarkedFinished(firstWindow));
    tester.assertHasOnlyGlobalAndFinishedSetsFor(firstWindow);
  }

  @Test
  public void testWatermarkHoldAndLateData() throws Exception {
    // Test handling of late data. Specifically, ensure the watermark hold is correct.
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        FixedWindows.of(Duration.millis(10)),
        mockTrigger,
        AccumulationMode.ACCUMULATING_FIRED_PANES,
        Duration.millis(10));

    // All on time data, verify watermark hold.
    injectElement(tester, 1, TriggerResult.CONTINUE);
    injectElement(tester, 3, TriggerResult.CONTINUE);
    assertEquals(new Instant(1), tester.getWatermarkHold());
    injectElement(tester, 2, TriggerResult.FIRE);

    // Holding for the end-of-window transition.
    assertEquals(new Instant(9), tester.getWatermarkHold());

    assertEquals(0, tester.getElementsDroppedDueToLateness());
    assertEquals(0, tester.getElementsDroppedDueToClosedWindow());

    // Some late, some on time. Verify that we only hold to the minimum of on-time.
    tester.advanceWatermark(new Instant(4));
    injectElement(tester, 2, TriggerResult.CONTINUE);
    injectElement(tester, 3, TriggerResult.CONTINUE);
    assertEquals(new Instant(9), tester.getWatermarkHold());
    injectElement(tester, 5, TriggerResult.CONTINUE);
    assertEquals(new Instant(5), tester.getWatermarkHold());
    injectElement(tester, 4, TriggerResult.FIRE);

    // All late -- output at end of window timestamp.
    when(mockTrigger.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
         .thenReturn(TriggerResult.CONTINUE);
    tester.advanceWatermark(new Instant(8));
    injectElement(tester, 6, TriggerResult.CONTINUE);
    injectElement(tester, 5, TriggerResult.CONTINUE);
    assertEquals(new Instant(9), tester.getWatermarkHold());
    injectElement(tester, 4, TriggerResult.FIRE);

    // This is "pending" at the time the watermark makes it way-late.
    // Because we're about to expire the window, we output it.
    injectElement(tester, 8, TriggerResult.CONTINUE);

    assertEquals(0, tester.getElementsDroppedDueToLateness());
    assertEquals(0, tester.getElementsDroppedDueToClosedWindow());

    // All very late -- gets dropped.
    tester.advanceWatermark(new Instant(50));
    assertEquals(null, tester.getWatermarkHold());
    injectElement(tester, 2, TriggerResult.FIRE);
    assertEquals(null, tester.getWatermarkHold());

    // Late timers are ignored
    tester.fireTimer(new IntervalWindow(new Instant(0), new Instant(10)),
        new Instant(12), TimeDomain.EVENT_TIME);

    assertEquals(1, tester.getElementsDroppedDueToLateness());
    assertEquals(0, tester.getElementsDroppedDueToClosedWindow());

    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output, Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2, 3), 1, 0, 10),
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2, 3, 2, 3, 4, 5), 4, 0, 10),
        // Output time is end of the window, because all the new data was late
        isSingleWindowedValue(Matchers.containsInAnyOrder(
            1, 2, 3, 2, 3, 4, 5, 4, 5, 6), 9, 0, 10),
        // Output time is not end of the window, because the new data (8) wasn't late
        isSingleWindowedValue(Matchers.containsInAnyOrder(
            1, 2, 3, 2, 3, 4, 5, 4, 5, 6, 8), 8, 0, 10)));

    assertThat(
               output.get(0).getPane(),
        Matchers.equalTo(PaneInfo.createPane(true, false, Timing.EARLY)));

    // By the time this firing is produced, the input WM already passed the end of the window.
    assertThat(
        output.get(3).getPane(),
        Matchers.equalTo(PaneInfo.createPane(false, true, Timing.LATE, 3, 0)));

    // And because we're past the end of window + allowed lateness, everything should be cleaned up.
    assertFalse(tester.isMarkedFinished(firstWindow));
    tester.assertHasOnlyGlobalAndFinishedSetsFor();
  }

  @Test
  public void testPaneInfoAllStates() throws Exception {
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        FixedWindows.of(Duration.millis(10)),
        mockTrigger,
        AccumulationMode.DISCARDING_FIRED_PANES,
        Duration.millis(100));

    when(mockTrigger.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
        .thenReturn(TriggerResult.CONTINUE);

    tester.advanceWatermark(new Instant(0));
    injectElement(tester, 1, TriggerResult.FIRE);
    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(true, false, Timing.EARLY))));

    injectElement(tester, 2, TriggerResult.FIRE);
    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, false, Timing.EARLY, 1, -1))));

    tester.advanceWatermark(new Instant(15));
    injectElement(tester, 3, TriggerResult.FIRE);
    assertThat(tester.extractOutput(), Matchers.contains(
        // This is late, because the trigger wasn't waiting for AfterWatermark
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, false, Timing.LATE, 2, 0))));

    injectElement(tester, 4, TriggerResult.FIRE);
    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, false, Timing.LATE, 3, 1))));

    injectElement(tester, 5, TriggerResult.FIRE_AND_FINISH);
    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, true, Timing.LATE, 4, 2))));
  }

  @Test
  public void testPaneInfoAllStatesAfterWatermark() throws Exception {
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)))
            .withTrigger(Repeatedly.<IntervalWindow>forever(
                AfterFirst.<IntervalWindow>of(
                    AfterPane.<IntervalWindow>elementCountAtLeast(2),
                    AfterWatermark.<IntervalWindow>pastEndOfWindow())))
            .withMode(AccumulationMode.DISCARDING_FIRED_PANES)
            .withAllowedLateness(Duration.millis(100))
            .withClosingBehavior(ClosingBehavior.FIRE_ALWAYS));

    tester.advanceWatermark(new Instant(0));
    tester.injectElement(1, new Instant(1));
    tester.injectElement(2, new Instant(2));

    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output, Matchers.contains(
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(true, false, Timing.EARLY, 0, -1))));
    assertThat(output, Matchers.contains(
        WindowMatchers.isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 0, 10)));

    tester.advanceWatermark(new Instant(50));

    // We should get the ON_TIME pane even though it is empty,
    // because we have an AfterWatermark.pastEndOfWindow() trigger.
    output = tester.extractOutput();
    assertThat(output, Matchers.contains(
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, false, Timing.ON_TIME, 1, 0))));
    assertThat(output, Matchers.contains(
        WindowMatchers.isSingleWindowedValue(Matchers.emptyIterable(), 9, 0, 10)));

    // We should get the final pane even though it is empty.
    tester.advanceWatermark(new Instant(150));
    output = tester.extractOutput();
    assertThat(output, Matchers.contains(
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, true, Timing.LATE, 2, 1))));
    assertThat(output, Matchers.contains(
        WindowMatchers.isSingleWindowedValue(Matchers.emptyIterable(), 9, 0, 10)));
  }

  @Test
  public void testPaneInfoAllStatesAfterWatermarkAccumulating() throws Exception {
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)))
            .withTrigger(Repeatedly.<IntervalWindow>forever(
                AfterFirst.<IntervalWindow>of(
                    AfterPane.<IntervalWindow>elementCountAtLeast(2),
                    AfterWatermark.<IntervalWindow>pastEndOfWindow())))
            .withMode(AccumulationMode.ACCUMULATING_FIRED_PANES)
            .withAllowedLateness(Duration.millis(100))
            .withClosingBehavior(ClosingBehavior.FIRE_ALWAYS));

    tester.advanceWatermark(new Instant(0));
    tester.injectElement(1, new Instant(1));
    tester.injectElement(2, new Instant(2));

    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output, Matchers.contains(
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(true, false, Timing.EARLY, 0, -1))));
    assertThat(output, Matchers.contains(
        WindowMatchers.isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 0, 10)));

    tester.advanceWatermark(new Instant(50));

    // We should get the ON_TIME pane even though it is empty,
    // because we have an AfterWatermark.pastEndOfWindow() trigger.
    output = tester.extractOutput();
    assertThat(output, Matchers.contains(
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, false, Timing.ON_TIME, 1, 0))));
    assertThat(output, Matchers.contains(
        WindowMatchers.isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 9, 0, 10)));

    // We should get the final pane even though it is empty.
    tester.advanceWatermark(new Instant(150));
    output = tester.extractOutput();
    assertThat(output, Matchers.contains(
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, true, Timing.LATE, 2, 1))));
    assertThat(output, Matchers.contains(
        WindowMatchers.isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 9, 0, 10)));
  }

  @Test
  public void testPaneInfoFinalAndOnTime() throws Exception {
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)))
            .withTrigger(
                Repeatedly.<IntervalWindow>forever(AfterPane.<IntervalWindow>elementCountAtLeast(2))
                .orFinally(AfterWatermark.<IntervalWindow>pastEndOfWindow()))
            .withMode(AccumulationMode.DISCARDING_FIRED_PANES)
            .withAllowedLateness(Duration.millis(100))
            .withClosingBehavior(ClosingBehavior.FIRE_ALWAYS));

    tester.advanceWatermark(new Instant(0));
    tester.injectElement(1, new Instant(1));
    tester.injectElement(2, new Instant(2));

    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(true, false, Timing.EARLY, 0, -1))));

    tester.advanceWatermark(new Instant(150));
    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, true, Timing.ON_TIME, 1, 0))));
  }

  @Test
  public void testPaneInfoSkipToFinish() throws Exception {
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        FixedWindows.of(Duration.millis(10)),
        mockTrigger,
        AccumulationMode.DISCARDING_FIRED_PANES,
        Duration.millis(100));

    tester.advanceWatermark(new Instant(0));
    injectElement(tester, 1, TriggerResult.FIRE_AND_FINISH);
    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(true, true, Timing.EARLY))));
  }

  @Test
  public void testPaneInfoSkipToNonSpeculativeAndFinish() throws Exception {
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        FixedWindows.of(Duration.millis(10)),
        mockTrigger,
        AccumulationMode.DISCARDING_FIRED_PANES,
        Duration.millis(100));

    tester.advanceWatermark(new Instant(15));
    injectElement(tester, 1, TriggerResult.FIRE_AND_FINISH);
    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(true, true, Timing.LATE))));
  }

  @Test
  public void testMergeBeforeFinalizing() throws Exception {
    // Verify that we merge windows before producing output so users don't see undesired
    // unmerged windows.
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        Sessions.withGapDuration(Duration.millis(10)),
        mockTrigger,
        AccumulationMode.DISCARDING_FIRED_PANES,
        Duration.millis(0));

    // All on time data, verify watermark hold.
    injectElement(tester, 1, TriggerResult.CONTINUE); // [1-11)
    injectElement(tester, 10, TriggerResult.CONTINUE); // [10-20)

    // Finalizing forces us to merge to merge, but we aren't ready to fire yet.
    when(mockTrigger.onMerge(Mockito.<Trigger<IntervalWindow>.OnMergeContext>any()))
        .thenReturn(MergeResult.CONTINUE);

    // Wasn't waiting for the ON_TIME firing
    when(mockTrigger.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
        .thenReturn(TriggerResult.CONTINUE);

    tester.advanceWatermark(new Instant(100));
    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output.size(), Matchers.equalTo(1));
    assertThat(output.get(0), isSingleWindowedValue(Matchers.containsInAnyOrder(1, 10), 1, 1, 20));
    assertThat(output.get(0).getPane(),
        Matchers.equalTo(PaneInfo.createPane(true, true, Timing.LATE, 0, 0)));
  }

  @Test
  public void testDropDataMultipleWindows() throws Exception {
    TriggerTester<Integer, Integer, IntervalWindow> tester = TriggerTester.combining(
        SlidingWindows.of(Duration.millis(100)).every(Duration.millis(30)),
        AfterWatermark.<IntervalWindow>pastEndOfWindow(),
        AccumulationMode.ACCUMULATING_FIRED_PANES,
        new Sum.SumIntegerFn().<String>asKeyedFn(),
        VarIntCoder.of(),
        Duration.millis(20));

    tester.injectElement(10, new Instant(23)); // [-60, 40), [-30, 70), [0, 100)
    tester.injectElement(12, new Instant(40)); // [-30, 70), [0, 100), [30, 130)

    assertEquals(0, tester.getElementsDroppedDueToLateness());
    assertEquals(0, tester.getElementsDroppedDueToClosedWindow());

    tester.advanceWatermark(new Instant(70));
    tester.injectElement(14, new Instant(60)); // [-30, 70) = closed, [0, 100), [30, 130)

    assertEquals(0, tester.getElementsDroppedDueToLateness());
    assertEquals(1, tester.getElementsDroppedDueToClosedWindow());

    tester.injectElement(16, new Instant(40)); // dropped due to lateness, assigned to 3 windows

    assertEquals(3, tester.getElementsDroppedDueToLateness());
    assertEquals(1, tester.getElementsDroppedDueToClosedWindow());
  }

  @Test
  public void testIdempotentEmptyPanes() throws Exception {
    // Test uninteresting (empty) panes don't increment the index or otherwise
    // modify PaneInfo.
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        FixedWindows.of(Duration.millis(10)),
        mockTrigger,
        AccumulationMode.DISCARDING_FIRED_PANES,
        Duration.millis(100));

    // Inject a couple of on-time elements and fire at the window end.
    injectElement(tester, 1, TriggerResult.CONTINUE);
    injectElement(tester, 2, TriggerResult.CONTINUE);
    when(mockTrigger.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
        .thenReturn(TriggerResult.CONTINUE);
    tester.advanceWatermark(new Instant(12));

    when(mockTrigger.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
        .thenReturn(TriggerResult.FIRE);
    tester.fireTimer(firstWindow, new Instant(10), TimeDomain.EVENT_TIME);

    // Fire another timer (with no data, so it's an uninteresting pane).
    when(mockTrigger.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
                    .thenReturn(TriggerResult.FIRE);
    tester.fireTimer(firstWindow, new Instant(10), TimeDomain.EVENT_TIME);

    // Finish it off with another datum.
    injectElement(tester, 3, TriggerResult.FIRE_AND_FINISH);

    // The intermediate trigger firing shouldn't result in any output.
    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output.size(), equalTo(2));

    // The on-time pane is as expected.
    assertThat(output.get(0), isSingleWindowedValue(containsInAnyOrder(1, 2), 1, 0, 10));

    // The late pane has the correct indices.
    assertThat(output.get(1).getValue(), contains(3));
    assertThat(output.get(1).getPane(),
        equalTo(PaneInfo.createPane(false, true, Timing.LATE, 1, 1)));

    assertTrue(tester.isMarkedFinished(firstWindow));
    tester.assertHasOnlyGlobalAndFinishedSetsFor(firstWindow);

    assertEquals(0, tester.getElementsDroppedDueToClosedWindow());
    assertEquals(0, tester.getElementsDroppedDueToLateness());
  }

  @Test
  public void testIdempotentEmptyPanesAccumulating() throws Exception {
    // Test uninteresting (empty) panes don't increment the index or otherwise
    // modify PaneInfo.
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        FixedWindows.of(Duration.millis(10)),
        mockTrigger,
        AccumulationMode.ACCUMULATING_FIRED_PANES,
        Duration.millis(100));

    // Inject a couple of on-time elements and fire at the window end.
    injectElement(tester, 1, TriggerResult.CONTINUE);
    injectElement(tester, 2, TriggerResult.CONTINUE);
    when(mockTrigger.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
        .thenReturn(TriggerResult.CONTINUE);
    tester.advanceWatermark(new Instant(12));

    when(mockTrigger.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
        .thenReturn(TriggerResult.FIRE);
    tester.fireTimer(firstWindow, new Instant(10), TimeDomain.EVENT_TIME);

    // Fire another timer (with no data, so it's an uninteresting pane).
    when(mockTrigger.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
                    .thenReturn(TriggerResult.FIRE);
    tester.fireTimer(firstWindow, new Instant(10), TimeDomain.EVENT_TIME);

    // Finish it off with another datum.
    injectElement(tester, 3, TriggerResult.FIRE_AND_FINISH);

    // The intermediate trigger firing shouldn't result in any output.
    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output.size(), equalTo(2));

    // The on-time pane is as expected.
    assertThat(output.get(0), isSingleWindowedValue(containsInAnyOrder(1, 2), 1, 0, 10));

    // The late pane has the correct indices.
    assertThat(output.get(1).getValue(), containsInAnyOrder(1, 2, 3));
    assertThat(output.get(1).getPane(),
        equalTo(PaneInfo.createPane(false, true, Timing.LATE, 1, 1)));

    assertTrue(tester.isMarkedFinished(firstWindow));
    tester.assertHasOnlyGlobalAndFinishedSetsFor(firstWindow);

    assertEquals(0, tester.getElementsDroppedDueToClosedWindow());
    assertEquals(0, tester.getElementsDroppedDueToLateness());
  }

  private class ResultCaptor<T> implements Answer<T> {
    private T result = null;
    public T get() {
      return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T answer(InvocationOnMock invocationOnMock) throws Throwable {
      result = (T) invocationOnMock.callRealMethod();
      return result;
    }
  }

  @Test
  public void testMultipleTimerTypes() throws Exception {
    Trigger<IntervalWindow> trigger = spy(Repeatedly.forever(
        AfterFirst.of(AfterProcessingTime.<IntervalWindow>pastFirstElementInPane().plusDelayOf(
                          Duration.millis(10)),
            AfterWatermark.<IntervalWindow>pastEndOfWindow())));

    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        TriggerTester.nonCombining(
            FixedWindows.of(Duration.millis(10)),
            trigger,
            AccumulationMode.DISCARDING_FIRED_PANES,
            Duration.standardDays(1));

    tester.injectElement(1, new Instant(1));

    ResultCaptor<TriggerResult> result = new ResultCaptor<>();
    doAnswer(result)
        .when(trigger)
        .onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any());
    tester.advanceWatermark(new Instant(1000));
    assertEquals(TriggerResult.FIRE, result.get());

    tester.advanceProcessingTime(Instant.now().plus(Duration.millis(10)));
    // Verify that the only onTimer call was the one from advancing the watermark.
    verify(trigger).onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any());
  }
}
