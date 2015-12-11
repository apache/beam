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
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterEach;
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
import com.google.cloud.dataflow.sdk.values.TimestampedValue;

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
 * Tests for Trigger execution.
 */
@RunWith(JUnit4.class)
public class TriggerExecutorTest {
  @Mock
  private Trigger<IntervalWindow> mockTrigger;
  private IntervalWindow firstWindow;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(mockTrigger.buildTrigger()).thenReturn(mockTrigger);
    firstWindow = new IntervalWindow(new Instant(0), new Instant(10));
  }

  private void injectElement(ReduceFnTester<Integer, ?, IntervalWindow> tester, int element,
      TriggerResult result) throws Exception {
    when(mockTrigger.onElement(Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
        .thenReturn(result);
    tester.injectElements(TimestampedValue.of(element, new Instant(element)));
  }

  @Test
  public void testOnElementBufferingDiscarding() throws Exception {
    // Test basic execution of a trigger using a non-combining window set and discarding mode.
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(FixedWindows.of(Duration.millis(10)), mockTrigger,
            AccumulationMode.DISCARDING_FIRED_PANES, Duration.millis(100));

    injectElement(tester, 1, TriggerResult.CONTINUE);
    injectElement(tester, 2, TriggerResult.FIRE);

    injectElement(tester, 3, TriggerResult.FIRE_AND_FINISH);

    // This element shouldn't be seen, because the trigger has finished
    injectElement(tester, 4, null);

    assertThat(
        tester.extractOutput(),
        Matchers.contains(
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
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(FixedWindows.of(Duration.millis(10)), mockTrigger,
            AccumulationMode.ACCUMULATING_FIRED_PANES, Duration.millis(100));

    injectElement(tester, 1, TriggerResult.CONTINUE);
    injectElement(tester, 2, TriggerResult.FIRE);
    injectElement(tester, 3, TriggerResult.FIRE_AND_FINISH);

    // This element shouldn't be seen, because the trigger has finished
    injectElement(tester, 4, null);

    assertThat(
        tester.extractOutput(),
        Matchers.contains(
            isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 0, 10),
            isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2, 3), 3, 0, 10)));
    assertTrue(tester.isMarkedFinished(firstWindow));
    tester.assertHasOnlyGlobalAndFinishedSetsFor(firstWindow);
  }

  @Test
  public void testOnElementCombiningDiscarding() throws Exception {
    // Test basic execution of a trigger using a non-combining window set and discarding mode.
    ReduceFnTester<Integer, Integer, IntervalWindow> tester = ReduceFnTester.combining(
        FixedWindows.of(Duration.millis(10)), mockTrigger, AccumulationMode.DISCARDING_FIRED_PANES,
        new Sum.SumIntegerFn().<String>asKeyedFn(), VarIntCoder.of(), Duration.millis(100));

    injectElement(tester, 2, TriggerResult.CONTINUE);
    injectElement(tester, 3, TriggerResult.FIRE);
    injectElement(tester, 4, TriggerResult.FIRE_AND_FINISH);

    // This element shouldn't be seen, because the trigger has finished
    injectElement(tester, 6, null);

    assertThat(
        tester.extractOutput(),
        Matchers.contains(
            isSingleWindowedValue(Matchers.equalTo(5), 2, 0, 10),
            isSingleWindowedValue(Matchers.equalTo(4), 4, 0, 10)));
    assertTrue(tester.isMarkedFinished(firstWindow));
    tester.assertHasOnlyGlobalAndFinishedSetsFor(firstWindow);
  }

  @Test
  public void testOnElementCombiningAccumulating() throws Exception {
    // Test basic execution of a trigger using a non-combining window set and accumulating mode.
    ReduceFnTester<Integer, Integer, IntervalWindow> tester =
        ReduceFnTester.combining(FixedWindows.of(Duration.millis(10)), mockTrigger,
            AccumulationMode.ACCUMULATING_FIRED_PANES, new Sum.SumIntegerFn().<String>asKeyedFn(),
            VarIntCoder.of(), Duration.millis(100));

    injectElement(tester, 1, TriggerResult.CONTINUE);
    injectElement(tester, 2, TriggerResult.FIRE);
    injectElement(tester, 3, TriggerResult.FIRE_AND_FINISH);

    // This element shouldn't be seen, because the trigger has finished
    injectElement(tester, 4, null);

    assertThat(
        tester.extractOutput(),
        Matchers.contains(
            isSingleWindowedValue(Matchers.equalTo(3), 1, 0, 10),
            isSingleWindowedValue(Matchers.equalTo(6), 3, 0, 10)));
    assertTrue(tester.isMarkedFinished(firstWindow));
    tester.assertHasOnlyGlobalAndFinishedSetsFor(firstWindow);
  }

  @Test
  public void testWatermarkHoldAndLateData() throws Exception {
    // Test handling of late data. Specifically, ensure the watermark hold is correct.
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(FixedWindows.of(Duration.millis(10)), mockTrigger,
            AccumulationMode.ACCUMULATING_FIRED_PANES, Duration.millis(10));

    // Input watermark -> null
    assertEquals(null, tester.getWatermarkHold());
    assertEquals(null, tester.getOutputWatermark());

    // All on time data, verify watermark hold.
    injectElement(tester, 1, TriggerResult.CONTINUE);
    injectElement(tester, 3, TriggerResult.CONTINUE);
    assertEquals(new Instant(1), tester.getWatermarkHold());
    injectElement(tester, 2, TriggerResult.FIRE);
    assertEquals(1, tester.getOutputSize());

    // Holding for the end-of-window transition.
    assertEquals(new Instant(9), tester.getWatermarkHold());
    // Nothing dropped.
    assertEquals(0, tester.getElementsDroppedDueToLateness());
    assertEquals(0, tester.getElementsDroppedDueToClosedWindow());

    // Input watermark -> 4
    tester.advanceInputWatermark(new Instant(4));
    assertEquals(new Instant(4), tester.getOutputWatermark());

    // Some late, some on time. Verify that we only hold to the minimum of on-time.
    injectElement(tester, 2, TriggerResult.CONTINUE);
    injectElement(tester, 3, TriggerResult.CONTINUE);
    assertEquals(new Instant(9), tester.getWatermarkHold());
    injectElement(tester, 5, TriggerResult.CONTINUE);
    assertEquals(new Instant(5), tester.getWatermarkHold());
    injectElement(tester, 4, TriggerResult.FIRE);
    assertEquals(2, tester.getOutputSize());

    // All late -- output at end of window timestamp.
    when(mockTrigger.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
        .thenReturn(TriggerResult.CONTINUE);
    // Input watermark -> 8
    tester.advanceInputWatermark(new Instant(8));
    assertEquals(new Instant(8), tester.getOutputWatermark());
    injectElement(tester, 6, TriggerResult.CONTINUE);
    injectElement(tester, 5, TriggerResult.CONTINUE);
    assertEquals(new Instant(9), tester.getWatermarkHold());
    injectElement(tester, 4, TriggerResult.CONTINUE);

    // This is behind both the input and output watermarks, but will still make it
    // into an ON_TIME pane.
    when(mockTrigger.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
        .thenReturn(TriggerResult.FIRE);
    // Input watermark -> 10
    tester.advanceInputWatermark(new Instant(10));
    assertEquals(3, tester.getOutputSize());
    assertEquals(new Instant(10), tester.getOutputWatermark());
    injectElement(tester, 8, TriggerResult.CONTINUE);

    assertEquals(0, tester.getElementsDroppedDueToLateness());
    assertEquals(0, tester.getElementsDroppedDueToClosedWindow());

    // All very late -- gets dropped.
    // Input watermark -> 50
    tester.advanceInputWatermark(new Instant(50));
    assertEquals(new Instant(50), tester.getOutputWatermark());
    assertEquals(null, tester.getWatermarkHold());
    injectElement(tester, 22, TriggerResult.FIRE);
    assertEquals(4, tester.getOutputSize());
    assertEquals(null, tester.getWatermarkHold());

    assertEquals(1, tester.getElementsDroppedDueToLateness());
    assertEquals(0, tester.getElementsDroppedDueToClosedWindow());

    // Late timers are ignored
    tester.fireTimer(new IntervalWindow(new Instant(0), new Instant(10)), new Instant(12),
        TimeDomain.EVENT_TIME);

    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(
        output, Matchers.contains(
                    isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2, 3), 1, 0, 10),
                    isSingleWindowedValue(
                        Matchers.containsInAnyOrder(1, 2, 3, 2, 3, 4, 5), 4, 0, 10),
                    // Output time is end of the window, because all the new data was late
                    isSingleWindowedValue(
                        Matchers.containsInAnyOrder(1, 2, 3, 2, 3, 4, 5, 4, 5, 6), 9, 0, 10),
                    // Output time is still end of the window, because the new data (8) was behind
                    // the output watermark.
                    isSingleWindowedValue(
                        Matchers.containsInAnyOrder(1, 2, 3, 2, 3, 4, 5, 4, 5, 6, 8), 9, 0, 10)));

    assertThat(
        output.get(0).getPane(),
        Matchers.equalTo(PaneInfo.createPane(true, false, Timing.EARLY, 0, -1)));

    assertThat(
        output.get(1).getPane(),
        Matchers.equalTo(PaneInfo.createPane(false, false, Timing.EARLY, 1, -1)));

    assertThat(
        output.get(2).getPane(),
        Matchers.equalTo(PaneInfo.createPane(false, false, Timing.ON_TIME, 2, 0)));

    assertThat(
        output.get(3).getPane(),
        Matchers.equalTo(PaneInfo.createPane(false, true, Timing.LATE, 3, 1)));

    // And because we're past the end of window + allowed lateness, everything should be cleaned up.
    assertFalse(tester.isMarkedFinished(firstWindow));
    tester.assertHasOnlyGlobalAndFinishedSetsFor();
  }

  @Test
  public void testPaneInfoAllStates() throws Exception {
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(FixedWindows.of(Duration.millis(10)), mockTrigger,
            AccumulationMode.DISCARDING_FIRED_PANES, Duration.millis(100));

    when(mockTrigger.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
        .thenReturn(TriggerResult.CONTINUE);

    tester.advanceInputWatermark(new Instant(0));
    injectElement(tester, 1, TriggerResult.FIRE);
    assertThat(
        tester.extractOutput(),
        Matchers.contains(
            WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(true, false, Timing.EARLY))));

    injectElement(tester, 2, TriggerResult.FIRE);
    assertThat(
        tester.extractOutput(),
        Matchers.contains(WindowMatchers.valueWithPaneInfo(
            PaneInfo.createPane(false, false, Timing.EARLY, 1, -1))));

    tester.advanceInputWatermark(new Instant(15));
    injectElement(tester, 3, TriggerResult.FIRE);
    assertThat(
        tester.extractOutput(),
        Matchers.contains(WindowMatchers.valueWithPaneInfo(
            PaneInfo.createPane(false, false, Timing.EARLY, 2, -1))));

    injectElement(tester, 4, TriggerResult.FIRE);
    assertThat(
        tester.extractOutput(),
        Matchers.contains(WindowMatchers.valueWithPaneInfo(
            PaneInfo.createPane(false, false, Timing.EARLY, 3, -1))));

    injectElement(tester, 5, TriggerResult.FIRE_AND_FINISH);
    assertThat(
        tester.extractOutput(),
        Matchers.contains(WindowMatchers.valueWithPaneInfo(
            PaneInfo.createPane(false, true, Timing.EARLY, 4, -1))));
  }

  @Test
  public void testPaneInfoAllStatesAfterWatermark() throws Exception {
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester = ReduceFnTester.nonCombining(
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)))
            .withTrigger(Repeatedly.<IntervalWindow>forever(AfterFirst.<IntervalWindow>of(
                AfterPane.<IntervalWindow>elementCountAtLeast(2),
                AfterWatermark.<IntervalWindow>pastEndOfWindow())))
            .withMode(AccumulationMode.DISCARDING_FIRED_PANES)
            .withAllowedLateness(Duration.millis(100))
            .withClosingBehavior(ClosingBehavior.FIRE_ALWAYS));

    tester.advanceInputWatermark(new Instant(0));
    tester.injectElements(
        TimestampedValue.of(1, new Instant(1)), TimestampedValue.of(2, new Instant(2)));

    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(
        output,
        Matchers.contains(WindowMatchers.valueWithPaneInfo(
            PaneInfo.createPane(true, false, Timing.EARLY, 0, -1))));
    assertThat(
        output,
        Matchers.contains(
            WindowMatchers.isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 0, 10)));

    tester.advanceInputWatermark(new Instant(50));

    // We should get the ON_TIME pane even though it is empty,
    // because we have an AfterWatermark.pastEndOfWindow() trigger.
    output = tester.extractOutput();
    assertThat(
        output,
        Matchers.contains(WindowMatchers.valueWithPaneInfo(
            PaneInfo.createPane(false, false, Timing.ON_TIME, 1, 0))));
    assertThat(
        output,
        Matchers.contains(
            WindowMatchers.isSingleWindowedValue(Matchers.emptyIterable(), 9, 0, 10)));

    // We should get the final pane even though it is empty.
    tester.advanceInputWatermark(new Instant(150));
    output = tester.extractOutput();
    assertThat(
        output,
        Matchers.contains(
            WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, true, Timing.LATE, 2, 1))));
    assertThat(
        output,
        Matchers.contains(
            WindowMatchers.isSingleWindowedValue(Matchers.emptyIterable(), 9, 0, 10)));
  }

  @Test
  public void testPaneInfoAllStatesAfterWatermarkAccumulating() throws Exception {
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester = ReduceFnTester.nonCombining(
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)))
            .withTrigger(Repeatedly.<IntervalWindow>forever(AfterFirst.<IntervalWindow>of(
                AfterPane.<IntervalWindow>elementCountAtLeast(2),
                AfterWatermark.<IntervalWindow>pastEndOfWindow())))
            .withMode(AccumulationMode.ACCUMULATING_FIRED_PANES)
            .withAllowedLateness(Duration.millis(100))
            .withClosingBehavior(ClosingBehavior.FIRE_ALWAYS));

    tester.advanceInputWatermark(new Instant(0));
    tester.injectElements(
        TimestampedValue.of(1, new Instant(1)), TimestampedValue.of(2, new Instant(2)));

    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(
        output,
        Matchers.contains(WindowMatchers.valueWithPaneInfo(
            PaneInfo.createPane(true, false, Timing.EARLY, 0, -1))));
    assertThat(
        output,
        Matchers.contains(
            WindowMatchers.isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 0, 10)));

    tester.advanceInputWatermark(new Instant(50));

    // We should get the ON_TIME pane even though it is empty,
    // because we have an AfterWatermark.pastEndOfWindow() trigger.
    output = tester.extractOutput();
    assertThat(
        output,
        Matchers.contains(WindowMatchers.valueWithPaneInfo(
            PaneInfo.createPane(false, false, Timing.ON_TIME, 1, 0))));
    assertThat(
        output,
        Matchers.contains(
            WindowMatchers.isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 9, 0, 10)));

    // We should get the final pane even though it is empty.
    tester.advanceInputWatermark(new Instant(150));
    output = tester.extractOutput();
    assertThat(
        output,
        Matchers.contains(
            WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, true, Timing.LATE, 2, 1))));
    assertThat(
        output,
        Matchers.contains(
            WindowMatchers.isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 9, 0, 10)));
  }

  @Test
  public void testPaneInfoFinalAndOnTime() throws Exception {
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester = ReduceFnTester.nonCombining(
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)))
            .withTrigger(
                Repeatedly.<IntervalWindow>forever(AfterPane.<IntervalWindow>elementCountAtLeast(2))
                    .orFinally(AfterWatermark.<IntervalWindow>pastEndOfWindow()))
            .withMode(AccumulationMode.DISCARDING_FIRED_PANES)
            .withAllowedLateness(Duration.millis(100))
            .withClosingBehavior(ClosingBehavior.FIRE_ALWAYS));

    tester.advanceInputWatermark(new Instant(0));
    tester.injectElements(
        TimestampedValue.of(1, new Instant(1)), TimestampedValue.of(2, new Instant(2)));

    assertThat(
        tester.extractOutput(),
        Matchers.contains(WindowMatchers.valueWithPaneInfo(
            PaneInfo.createPane(true, false, Timing.EARLY, 0, -1))));

    tester.advanceInputWatermark(new Instant(150));
    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, true, Timing.ON_TIME, 1, 0))));
  }

  @Test
  public void testPaneInfoSkipToFinish() throws Exception {
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(FixedWindows.of(Duration.millis(10)), mockTrigger,
            AccumulationMode.DISCARDING_FIRED_PANES, Duration.millis(100));

    tester.advanceInputWatermark(new Instant(0));
    injectElement(tester, 1, TriggerResult.FIRE_AND_FINISH);
    assertThat(
        tester.extractOutput(),
        Matchers.contains(
            WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(true, true, Timing.EARLY))));
  }

  @Test
  public void testPaneInfoSkipToNonSpeculativeAndFinish() throws Exception {
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(FixedWindows.of(Duration.millis(10)), mockTrigger,
            AccumulationMode.DISCARDING_FIRED_PANES, Duration.millis(100));

    tester.advanceInputWatermark(new Instant(15));
    injectElement(tester, 1, TriggerResult.FIRE_AND_FINISH);
    assertThat(
        tester.extractOutput(),
        Matchers.contains(
            WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(true, true, Timing.LATE))));
  }

  @Test
  public void testMergeBeforeFinalizing() throws Exception {
    // Verify that we merge windows before producing output so users don't see undesired
    // unmerged windows.
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(Sessions.withGapDuration(Duration.millis(10)), mockTrigger,
            AccumulationMode.DISCARDING_FIRED_PANES, Duration.millis(0));

    // All on time data, verify watermark hold.
    when(mockTrigger.onMerge(Mockito.<Trigger<IntervalWindow>.OnMergeContext>any()))
        .thenReturn(MergeResult.CONTINUE);
    when(mockTrigger.onElement(Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
        .thenReturn(TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    tester.injectElements(
        TimestampedValue.of(1, new Instant(1)), TimestampedValue.of(10, new Instant(10)));

    when(mockTrigger.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
        .thenReturn(TriggerResult.CONTINUE);

    tester.advanceInputWatermark(new Instant(100));

    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output.size(), Matchers.equalTo(1));
    assertThat(output.get(0), isSingleWindowedValue(Matchers.containsInAnyOrder(1, 10), 1, 1, 20));
    assertThat(
        output.get(0).getPane(),
        Matchers.equalTo(PaneInfo.createPane(true, true, Timing.EARLY, 0, 0)));
  }

  @Test
  public void testDropDataMultipleWindows() throws Exception {
    ReduceFnTester<Integer, Integer, IntervalWindow> tester = ReduceFnTester.combining(
        SlidingWindows.of(Duration.millis(100)).every(Duration.millis(30)),
        AfterWatermark.<IntervalWindow>pastEndOfWindow(), AccumulationMode.ACCUMULATING_FIRED_PANES,
        new Sum.SumIntegerFn().<String>asKeyedFn(), VarIntCoder.of(), Duration.millis(20));

    tester.injectElements(
        TimestampedValue.of(10, new Instant(23)), // [-60, 40), [-30, 70), [0, 100)
        TimestampedValue.of(12, new Instant(40)));
        // [-30, 70), [0, 100), [30, 130)

    assertEquals(0, tester.getElementsDroppedDueToLateness());
    assertEquals(0, tester.getElementsDroppedDueToClosedWindow());

    tester.advanceInputWatermark(new Instant(70));
    tester.injectElements(
        TimestampedValue.of(14, new Instant(60))); // [-30, 70) = closed, [0, 100), [30, 130)

    assertEquals(0, tester.getElementsDroppedDueToLateness());
    assertEquals(1, tester.getElementsDroppedDueToClosedWindow());

    tester.injectElements(TimestampedValue.of(16, new Instant(40)));
        // dropped b/c lateness, assigned to 3 windows

    assertEquals(3, tester.getElementsDroppedDueToLateness());
    assertEquals(1, tester.getElementsDroppedDueToClosedWindow());
  }

  @Test
  public void testIdempotentEmptyPanes() throws Exception {
    // Test uninteresting (empty) panes don't increment the index or otherwise
    // modify PaneInfo.
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(FixedWindows.of(Duration.millis(10)), mockTrigger,
            AccumulationMode.DISCARDING_FIRED_PANES, Duration.millis(100));

    // Inject a couple of on-time elements and fire at the window end.
    injectElement(tester, 1, TriggerResult.CONTINUE);
    injectElement(tester, 2, TriggerResult.CONTINUE);
    when(mockTrigger.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
        .thenReturn(TriggerResult.CONTINUE);
    tester.advanceInputWatermark(new Instant(12));

    when(mockTrigger.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
        .thenReturn(TriggerResult.FIRE);
    tester.fireTimer(firstWindow, new Instant(9), TimeDomain.EVENT_TIME);

    // Fire another timer (with no data, so it's an uninteresting pane).
    when(mockTrigger.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
        .thenReturn(TriggerResult.FIRE);
    tester.fireTimer(firstWindow, new Instant(9), TimeDomain.EVENT_TIME);

    // Finish it off with another datum.
    injectElement(tester, 3, TriggerResult.FIRE_AND_FINISH);

    // The intermediate trigger firing shouldn't result in any output.
    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output.size(), equalTo(2));

    // The on-time pane is as expected.
    assertThat(output.get(0), isSingleWindowedValue(containsInAnyOrder(1, 2), 1, 0, 10));

    // The late pane has the correct indices.
    assertThat(output.get(1).getValue(), contains(3));
    assertThat(
        output.get(1).getPane(), equalTo(PaneInfo.createPane(false, true, Timing.LATE, 1, 1)));

    assertTrue(tester.isMarkedFinished(firstWindow));
    tester.assertHasOnlyGlobalAndFinishedSetsFor(firstWindow);

    assertEquals(0, tester.getElementsDroppedDueToClosedWindow());
    assertEquals(0, tester.getElementsDroppedDueToLateness());
  }

  @Test
  public void testIdempotentEmptyPanesAccumulating() throws Exception {
    // Test uninteresting (empty) panes don't increment the index or otherwise
    // modify PaneInfo.
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(FixedWindows.of(Duration.millis(10)), mockTrigger,
            AccumulationMode.ACCUMULATING_FIRED_PANES, Duration.millis(100));

    // Inject a couple of on-time elements and fire at the window end.
    injectElement(tester, 1, TriggerResult.CONTINUE);
    injectElement(tester, 2, TriggerResult.CONTINUE);
    when(mockTrigger.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
        .thenReturn(TriggerResult.CONTINUE);
    tester.advanceInputWatermark(new Instant(12));

    when(mockTrigger.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
        .thenReturn(TriggerResult.FIRE);
    tester.fireTimer(firstWindow, new Instant(9), TimeDomain.EVENT_TIME);

    // Fire another timer (with no data, so it's an uninteresting pane).
    when(mockTrigger.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
        .thenReturn(TriggerResult.FIRE);
    tester.fireTimer(firstWindow, new Instant(9), TimeDomain.EVENT_TIME);

    // Finish it off with another datum.
    injectElement(tester, 3, TriggerResult.FIRE_AND_FINISH);

    // The intermediate trigger firing shouldn't result in any output.
    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output.size(), equalTo(2));

    // The on-time pane is as expected.
    assertThat(output.get(0), isSingleWindowedValue(containsInAnyOrder(1, 2), 1, 0, 10));
    assertThat(
        output.get(0).getPane(), equalTo(PaneInfo.createPane(true, false, Timing.ON_TIME, 0, 0)));

    // The late pane has the correct indices.
    assertThat(output.get(1).getValue(), containsInAnyOrder(1, 2, 3));
    assertThat(
        output.get(1).getPane(), equalTo(PaneInfo.createPane(false, true, Timing.LATE, 1, 1)));

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

  /**
   * Test that we receive an empty on-time pane when an or-finally waiting for the watermark fires.
   * Specifically, verify the proper triggerings and pane-info of a typical speculative/on-time/late
   * when the on-time pane is empty.
   */
  @Test
  public void testEmptyOnTimeFromOrFinally() throws Exception {
    ReduceFnTester<Integer, Integer, IntervalWindow> tester =
        ReduceFnTester.combining(FixedWindows.of(Duration.millis(10)),
            AfterEach.<IntervalWindow>inOrder(
                Repeatedly
                    .<IntervalWindow>forever(
                        AfterProcessingTime.<IntervalWindow>pastFirstElementInPane().plusDelayOf(
                            new Duration(5)))
                    .orFinally(AfterWatermark.<IntervalWindow>pastEndOfWindow()),
                Repeatedly.<IntervalWindow>forever(
                    AfterProcessingTime.<IntervalWindow>pastFirstElementInPane().plusDelayOf(
                        new Duration(25)))),
            AccumulationMode.ACCUMULATING_FIRED_PANES, new Sum.SumIntegerFn().<String>asKeyedFn(),
            VarIntCoder.of(), Duration.millis(100));

    tester.advanceInputWatermark(new Instant(0));
    tester.advanceProcessingTime(new Instant(0));

    tester.injectElements(TimestampedValue.of(1, new Instant(1)),
        TimestampedValue.of(1, new Instant(3)), TimestampedValue.of(1, new Instant(7)),
        TimestampedValue.of(1, new Instant(5)));

    tester.advanceProcessingTime(new Instant(6));

    tester.advanceInputWatermark(new Instant(11));
    List<WindowedValue<Integer>> output = tester.extractOutput();
    assertEquals(2, output.size());

    assertThat(output.get(0), WindowMatchers.isSingleWindowedValue(4, 1, 0, 10));
    assertThat(output.get(1), WindowMatchers.isSingleWindowedValue(4, 9, 0, 10));

    assertThat(
        output.get(0),
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(true, false, Timing.EARLY, 0, -1)));
    assertThat(
        output.get(1),
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, false, Timing.ON_TIME, 1, 0)));
  }

  /**
   * Tests for processing time firings after the watermark passes the end of the window.
   * Specifically, verify the proper triggerings and pane-info of a typical speculative/on-time/late
   * when the on-time pane is non-empty.
   */
  @Test
  public void testProcessingTime() throws Exception {
    ReduceFnTester<Integer, Integer, IntervalWindow> tester =
        ReduceFnTester.combining(FixedWindows.of(Duration.millis(10)),
            AfterEach.<IntervalWindow>inOrder(
                Repeatedly
                    .<IntervalWindow>forever(
                        AfterProcessingTime.<IntervalWindow>pastFirstElementInPane().plusDelayOf(
                            new Duration(5)))
                    .orFinally(AfterWatermark.<IntervalWindow>pastEndOfWindow()),
                Repeatedly.<IntervalWindow>forever(
                    AfterProcessingTime.<IntervalWindow>pastFirstElementInPane().plusDelayOf(
                        new Duration(25)))),
            AccumulationMode.ACCUMULATING_FIRED_PANES, new Sum.SumIntegerFn().<String>asKeyedFn(),
            VarIntCoder.of(), Duration.millis(100));

    tester.advanceInputWatermark(new Instant(0));
    tester.advanceProcessingTime(new Instant(0));

    tester.injectElements(TimestampedValue.of(1, new Instant(1)),
        TimestampedValue.of(1, new Instant(3)), TimestampedValue.of(1, new Instant(7)),
        TimestampedValue.of(1, new Instant(5)));
    // 4 elements all at processing time 0

    tester.advanceProcessingTime(new Instant(6)); // fire [1,3,7,5] since 6 > 0 + 5
    tester.injectElements(
        TimestampedValue.of(1, new Instant(8)),
        TimestampedValue.of(1, new Instant(4)));
    // 6 elements

    tester.advanceInputWatermark(new Instant(11)); // fire [1,3,7,5,8,4] since 11 > 9
    tester.injectElements(
        TimestampedValue.of(1, new Instant(8)),
        TimestampedValue.of(1, new Instant(4)),
        TimestampedValue.of(1, new Instant(5)));
    // 9 elements

    tester.advanceInputWatermark(new Instant(12));
    tester.injectElements(
        TimestampedValue.of(1, new Instant(3)));
    // 10 elements

    tester.advanceProcessingTime(new Instant(15));
    tester.injectElements(
        TimestampedValue.of(1, new Instant(5)));
    // 11 elements
    tester.advanceProcessingTime(new Instant(32)); // fire since 32 > 6 + 25

    tester.injectElements(
        TimestampedValue.of(1, new Instant(3)));
    // 12 elements
    // fire [1,3,7,5,8,4,8,4,5,3,5,3] since 125 > 6 + 25
    tester.advanceInputWatermark(new Instant(125));

    List<WindowedValue<Integer>> output = tester.extractOutput();
    assertEquals(4, output.size());

    assertThat(output.get(0), WindowMatchers.isSingleWindowedValue(4, 1, 0, 10));
    assertThat(output.get(1), WindowMatchers.isSingleWindowedValue(6, 4, 0, 10));
    assertThat(output.get(2), WindowMatchers.isSingleWindowedValue(11, 9, 0, 10));
    assertThat(output.get(3), WindowMatchers.isSingleWindowedValue(12, 9, 0, 10));

    assertThat(
        output.get(0),
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(true, false, Timing.EARLY, 0, -1)));
    assertThat(
        output.get(1),
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, false, Timing.ON_TIME, 1, 0)));
    assertThat(
        output.get(2),
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, false, Timing.LATE, 2, 1)));
    assertThat(
        output.get(3),
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, true, Timing.LATE, 3, 2)));
  }

  @Test
  public void testMultipleTimerTypes() throws Exception {
    Trigger<IntervalWindow> trigger = spy(Repeatedly.forever(AfterFirst.of(
        AfterProcessingTime.<IntervalWindow>pastFirstElementInPane().plusDelayOf(
            Duration.millis(10)),
        AfterWatermark.<IntervalWindow>pastEndOfWindow())));

    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(FixedWindows.of(Duration.millis(10)), trigger,
            AccumulationMode.DISCARDING_FIRED_PANES, Duration.standardDays(1));

    tester.injectElements(TimestampedValue.of(1, new Instant(1)));

    ResultCaptor<TriggerResult> result = new ResultCaptor<>();
    doAnswer(result)
        .when(trigger)
        .onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any());
    tester.advanceInputWatermark(new Instant(1000));
    assertEquals(TriggerResult.FIRE, result.get());

    tester.advanceProcessingTime(Instant.now().plus(Duration.millis(10)));
    // Verify that the only onTimer call was the one from advancing the watermark.
    verify(trigger).onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any());
  }
}
