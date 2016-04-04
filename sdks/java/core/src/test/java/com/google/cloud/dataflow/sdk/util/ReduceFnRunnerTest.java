/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.dataflow.sdk.util;

import static com.google.cloud.dataflow.sdk.WindowMatchers.isSingleWindowedValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.cloud.dataflow.sdk.WindowMatchers;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.CombineWithContext.CombineFnWithContext;
import com.google.cloud.dataflow.sdk.transforms.CombineWithContext.Context;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterEach;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterFirst;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterPane;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterProcessingTime;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterWatermark;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo.Timing;
import com.google.cloud.dataflow.sdk.transforms.windowing.Repeatedly;
import com.google.cloud.dataflow.sdk.transforms.windowing.Sessions;
import com.google.cloud.dataflow.sdk.transforms.windowing.SlidingWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window.ClosingBehavior;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy.AccumulationMode;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TimestampedValue;
import com.google.common.base.Preconditions;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Iterator;
import java.util.List;

/**
 * Tests for {@link ReduceFnRunner}. These tests instantiate a full "stack" of
 * {@link ReduceFnRunner} with enclosed {@link ReduceFn}, down to the installed {@link Trigger}
 * (sometimes mocked). They proceed by injecting elements and advancing watermark and
 * processing time, then verifying produced panes and counters.
 */
@RunWith(JUnit4.class)
public class ReduceFnRunnerTest {
  @Mock private SideInputReader mockSideInputReader;
  private Trigger<IntervalWindow> mockTrigger;
  private PCollectionView<Integer> mockView;

  private IntervalWindow firstWindow;

  private static Trigger<IntervalWindow>.TriggerContext anyTriggerContext() {
    return Mockito.<Trigger<IntervalWindow>.TriggerContext>any();
  }
  private static Trigger<IntervalWindow>.OnElementContext anyElementContext() {
    return Mockito.<Trigger<IntervalWindow>.OnElementContext>any();
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    @SuppressWarnings("unchecked")
    Trigger<IntervalWindow> mockTriggerUnchecked =
        mock(Trigger.class, withSettings().serializable());
    mockTrigger = mockTriggerUnchecked;
    when(mockTrigger.buildTrigger()).thenReturn(mockTrigger);

    @SuppressWarnings("unchecked")
    PCollectionView<Integer> mockViewUnchecked =
        mock(PCollectionView.class, withSettings().serializable());
    mockView = mockViewUnchecked;
    firstWindow = new IntervalWindow(new Instant(0), new Instant(10));
  }

  private void injectElement(ReduceFnTester<Integer, ?, IntervalWindow> tester, int element)
      throws Exception {
    doNothing().when(mockTrigger).onElement(anyElementContext());
    tester.injectElements(TimestampedValue.of(element, new Instant(element)));
  }

  private void triggerShouldFinish(Trigger<IntervalWindow> mockTrigger) throws Exception {
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Exception {
        @SuppressWarnings("unchecked")
        Trigger<IntervalWindow>.TriggerContext context =
            (Trigger<IntervalWindow>.TriggerContext) invocation.getArguments()[0];
        context.trigger().setFinished(true);
        return null;
      }
    })
    .when(mockTrigger).onFire(anyTriggerContext());
 }

  @Test
  public void testOnElementBufferingDiscarding() throws Exception {
    // Test basic execution of a trigger using a non-combining window set and discarding mode.
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(FixedWindows.of(Duration.millis(10)), mockTrigger,
            AccumulationMode.DISCARDING_FIRED_PANES, Duration.millis(100),
            ClosingBehavior.FIRE_IF_NON_EMPTY);

    // Pane of {1, 2}
    injectElement(tester, 1);
    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    injectElement(tester, 2);
    assertThat(tester.extractOutput(),
        contains(isSingleWindowedValue(containsInAnyOrder(1, 2), 1, 0, 10)));

    // Pane of just 3, and finish
    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    triggerShouldFinish(mockTrigger);
    injectElement(tester, 3);
    assertThat(tester.extractOutput(),
            contains(isSingleWindowedValue(containsInAnyOrder(3), 3, 0, 10)));
    assertTrue(tester.isMarkedFinished(firstWindow));
    tester.assertHasOnlyGlobalAndFinishedSetsFor(firstWindow);

    // This element shouldn't be seen, because the trigger has finished
    injectElement(tester, 4);

    assertEquals(1, tester.getElementsDroppedDueToClosedWindow());
  }

  @Test
  public void testOnElementBufferingAccumulating() throws Exception {
    // Test basic execution of a trigger using a non-combining window set and accumulating mode.
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(FixedWindows.of(Duration.millis(10)), mockTrigger,
            AccumulationMode.ACCUMULATING_FIRED_PANES, Duration.millis(100),
            ClosingBehavior.FIRE_IF_NON_EMPTY);

    injectElement(tester, 1);

    // Fires {1, 2}
    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    injectElement(tester, 2);

    // Fires {1, 2, 3} because we are in accumulating mode
    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    triggerShouldFinish(mockTrigger);
    injectElement(tester, 3);

    // This element shouldn't be seen, because the trigger has finished
    injectElement(tester, 4);

    assertThat(
        tester.extractOutput(),
        contains(
            isSingleWindowedValue(containsInAnyOrder(1, 2), 1, 0, 10),
            isSingleWindowedValue(containsInAnyOrder(1, 2, 3), 3, 0, 10)));
    assertTrue(tester.isMarkedFinished(firstWindow));
    tester.assertHasOnlyGlobalAndFinishedSetsFor(firstWindow);
  }

  @Test
  public void testOnElementCombiningDiscarding() throws Exception {
    // Test basic execution of a trigger using a non-combining window set and discarding mode.
    ReduceFnTester<Integer, Integer, IntervalWindow> tester = ReduceFnTester.combining(
        FixedWindows.of(Duration.millis(10)), mockTrigger, AccumulationMode.DISCARDING_FIRED_PANES,
        new Sum.SumIntegerFn().<String>asKeyedFn(), VarIntCoder.of(), Duration.millis(100));

    injectElement(tester, 2);

    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    injectElement(tester, 3);

    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    triggerShouldFinish(mockTrigger);
    injectElement(tester, 4);

    // This element shouldn't be seen, because the trigger has finished
    injectElement(tester, 6);

    assertThat(
        tester.extractOutput(),
        contains(
            isSingleWindowedValue(equalTo(5), 2, 0, 10),
            isSingleWindowedValue(equalTo(4), 4, 0, 10)));
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

    injectElement(tester, 1);

    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    injectElement(tester, 2);

    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    triggerShouldFinish(mockTrigger);
    injectElement(tester, 3);

    // This element shouldn't be seen, because the trigger has finished
    injectElement(tester, 4);

    assertThat(
        tester.extractOutput(),
        contains(
            isSingleWindowedValue(equalTo(3), 1, 0, 10),
            isSingleWindowedValue(equalTo(6), 3, 0, 10)));
    assertTrue(tester.isMarkedFinished(firstWindow));
    tester.assertHasOnlyGlobalAndFinishedSetsFor(firstWindow);
  }

  @Test
  public void testOnElementCombiningWithContext() throws Exception {
    Integer expectedValue = 5;
    WindowingStrategy<?, IntervalWindow> windowingStrategy = WindowingStrategy
        .of(FixedWindows.of(Duration.millis(10)))
        .withTrigger(mockTrigger)
        .withMode(AccumulationMode.DISCARDING_FIRED_PANES)
        .withAllowedLateness(Duration.millis(100));

    TestOptions options = PipelineOptionsFactory.as(TestOptions.class);
    options.setValue(5);

    when(mockSideInputReader.contains(Matchers.<PCollectionView<Integer>>any())).thenReturn(true);
    when(mockSideInputReader.get(
        Matchers.<PCollectionView<Integer>>any(), any(BoundedWindow.class))).thenReturn(5);

    @SuppressWarnings({"rawtypes", "unchecked", "unused"})
    Object suppressWarningsVar = when(mockView.getWindowingStrategyInternal())
        .thenReturn((WindowingStrategy) windowingStrategy);

    SumAndVerifyContextFn combineFn = new SumAndVerifyContextFn(mockView, expectedValue);
    // Test basic execution of a trigger using a non-combining window set and discarding mode.
    ReduceFnTester<Integer, Integer, IntervalWindow> tester = ReduceFnTester.combining(
        windowingStrategy, combineFn.<String>asKeyedFn(),
        VarIntCoder.of(), options, mockSideInputReader);

    injectElement(tester, 2);

    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    injectElement(tester, 3);

    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    triggerShouldFinish(mockTrigger);
    injectElement(tester, 4);

    // This element shouldn't be seen, because the trigger has finished
    injectElement(tester, 6);

    assertThat(
        tester.extractOutput(),
        contains(
            isSingleWindowedValue(equalTo(5), 2, 0, 10),
            isSingleWindowedValue(equalTo(4), 4, 0, 10)));
    assertTrue(tester.isMarkedFinished(firstWindow));
    tester.assertHasOnlyGlobalAndFinishedSetsFor(firstWindow);
  }

  @Test
  public void testWatermarkHoldAndLateData() throws Exception {
    // Test handling of late data. Specifically, ensure the watermark hold is correct.
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(FixedWindows.of(Duration.millis(10)), mockTrigger,
            AccumulationMode.ACCUMULATING_FIRED_PANES, Duration.millis(10),
            ClosingBehavior.FIRE_IF_NON_EMPTY);

    // Input watermark -> null
    assertEquals(null, tester.getWatermarkHold());
    assertEquals(null, tester.getOutputWatermark());

    // All on time data, verify watermark hold.
    injectElement(tester, 1);
    injectElement(tester, 3);
    assertEquals(new Instant(1), tester.getWatermarkHold());
    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    injectElement(tester, 2);
    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output, contains(
        isSingleWindowedValue(containsInAnyOrder(1, 2, 3),
            1, // timestamp
            0, // window start
            10))); // window end
    assertThat(output.get(0).getPane(),
        equalTo(PaneInfo.createPane(true, false, Timing.EARLY, 0, -1)));

    // Holding for the end-of-window transition.
    assertEquals(new Instant(9), tester.getWatermarkHold());
    // Nothing dropped.
    assertEquals(0, tester.getElementsDroppedDueToClosedWindow());

    // Input watermark -> 4, output watermark should advance that far as well
    tester.advanceInputWatermark(new Instant(4));
    assertEquals(new Instant(4), tester.getOutputWatermark());

    // Some late, some on time. Verify that we only hold to the minimum of on-time.
    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(false);
    tester.advanceInputWatermark(new Instant(4));
    injectElement(tester, 2);
    injectElement(tester, 3);
    assertEquals(new Instant(9), tester.getWatermarkHold());
    injectElement(tester, 5);
    assertEquals(new Instant(5), tester.getWatermarkHold());
    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    injectElement(tester, 4);
    output = tester.extractOutput();
    assertThat(output,
        contains(
            isSingleWindowedValue(containsInAnyOrder(
                1, 2, 3, // earlier firing
                2, 3, 4, 5), // new elements
            4, // timestamp
            0, // window start
            10))); // window end
    assertThat(output.get(0).getPane(),
        equalTo(PaneInfo.createPane(false, false, Timing.EARLY, 1, -1)));

    // All late -- output at end of window timestamp.
    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(false);
    tester.advanceInputWatermark(new Instant(8));
    injectElement(tester, 6);
    injectElement(tester, 5);
    assertEquals(new Instant(9), tester.getWatermarkHold());
    injectElement(tester, 4);

    // Fire the ON_TIME pane
    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    tester.advanceInputWatermark(new Instant(10));

    // Output time is end of the window, because all the new data was late, but the pane
    // is the ON_TIME pane.
    output = tester.extractOutput();
    assertThat(output,
        contains(isSingleWindowedValue(
            containsInAnyOrder(1, 2, 3, // earlier firing
                2, 3, 4, 5, // earlier firing
                4, 5, 6), // new elements
            9, // timestamp
            0, // window start
            10))); // window end
    assertThat(output.get(0).getPane(),
        equalTo(PaneInfo.createPane(false, false, Timing.ON_TIME, 2, 0)));

    // This is "pending" at the time the watermark makes it way-late.
    // Because we're about to expire the window, we output it.
    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(false);
    injectElement(tester, 8);
    assertEquals(0, tester.getElementsDroppedDueToClosedWindow());

    // Exceed the GC limit, triggering the last pane to be fired
    tester.advanceInputWatermark(new Instant(50));
    output = tester.extractOutput();
    // Output time is still end of the window, because the new data (8) was behind
    // the output watermark.
    assertThat(output,
        contains(isSingleWindowedValue(
            containsInAnyOrder(1, 2, 3, // earlier firing
                2, 3, 4, 5, // earlier firing
                4, 5, 6, // earlier firing
                8), // new element prior to window becoming expired
            9, // timestamp
            0, // window start
            10))); // window end
    assertThat(
        output.get(0).getPane(),
        equalTo(PaneInfo.createPane(false, true, Timing.LATE, 3, 1)));
    assertEquals(new Instant(50), tester.getOutputWatermark());
    assertEquals(null, tester.getWatermarkHold());

    // Late timers are ignored
    tester.fireTimer(new IntervalWindow(new Instant(0), new Instant(10)), new Instant(12),
        TimeDomain.EVENT_TIME);

    // And because we're past the end of window + allowed lateness, everything should be cleaned up.
    assertFalse(tester.isMarkedFinished(firstWindow));
    tester.assertHasOnlyGlobalAndFinishedSetsFor();
  }

  @Test
  public void dontSetHoldIfTooLateForEndOfWindowTimer() throws Exception {
    // Make sure holds are only set if they are accompanied by an end-of-window timer.
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(FixedWindows.of(Duration.millis(10)), mockTrigger,
            AccumulationMode.ACCUMULATING_FIRED_PANES, Duration.millis(10),
            ClosingBehavior.FIRE_ALWAYS);
    tester.setAutoAdvanceOutputWatermark(false);

    // Case: Unobservably late
    tester.advanceInputWatermark(new Instant(15));
    tester.advanceOutputWatermark(new Instant(11));
    injectElement(tester, 14);
    // Hold was applied, waiting for end-of-window timer.
    assertEquals(new Instant(14), tester.getWatermarkHold());
    assertEquals(new Instant(19), tester.getNextTimer(TimeDomain.EVENT_TIME));

    // Trigger the end-of-window timer.
    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    tester.advanceInputWatermark(new Instant(20));
    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(false);
    // Hold has been replaced with garbage collection hold. Waiting for garbage collection.
    assertEquals(new Instant(29), tester.getWatermarkHold());
    assertEquals(new Instant(29), tester.getNextTimer(TimeDomain.EVENT_TIME));

    // Case: Maybe late 1
    injectElement(tester, 13);
    // No change to hold or timers.
    assertEquals(new Instant(29), tester.getWatermarkHold());
    assertEquals(new Instant(29), tester.getNextTimer(TimeDomain.EVENT_TIME));

    // Trigger the garbage collection timer.
    tester.advanceInputWatermark(new Instant(30));

    // Everything should be cleaned up.
    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(10), new Instant(20))));
    tester.assertHasOnlyGlobalAndFinishedSetsFor();
  }

  @Test
  public void testPaneInfoAllStates() throws Exception {
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(FixedWindows.of(Duration.millis(10)), mockTrigger,
            AccumulationMode.DISCARDING_FIRED_PANES, Duration.millis(100),
            ClosingBehavior.FIRE_IF_NON_EMPTY);

    tester.advanceInputWatermark(new Instant(0));
    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    injectElement(tester, 1);
    assertThat(tester.extractOutput(), contains(
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(true, false, Timing.EARLY))));

    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    injectElement(tester, 2);
    assertThat(tester.extractOutput(), contains(
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, false, Timing.EARLY, 1, -1))));

    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(false);
    tester.advanceInputWatermark(new Instant(15));
    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    injectElement(tester, 3);
    assertThat(tester.extractOutput(), contains(
        WindowMatchers.valueWithPaneInfo(
            PaneInfo.createPane(false, false, Timing.ON_TIME, 2, 0))));

    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    injectElement(tester, 4);
    assertThat(tester.extractOutput(), contains(
        WindowMatchers.valueWithPaneInfo(
            PaneInfo.createPane(false, false, Timing.LATE, 3, 1))));

    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    triggerShouldFinish(mockTrigger);
    injectElement(tester, 5);
    assertThat(tester.extractOutput(), contains(
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, true, Timing.LATE, 4, 2))));
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
        contains(WindowMatchers.valueWithPaneInfo(
            PaneInfo.createPane(true, false, Timing.EARLY, 0, -1))));
    assertThat(
        output,
        contains(
            WindowMatchers.isSingleWindowedValue(containsInAnyOrder(1, 2), 1, 0, 10)));

    tester.advanceInputWatermark(new Instant(50));

    // We should get the ON_TIME pane even though it is empty,
    // because we have an AfterWatermark.pastEndOfWindow() trigger.
    output = tester.extractOutput();
    assertThat(
        output,
        contains(WindowMatchers.valueWithPaneInfo(
            PaneInfo.createPane(false, false, Timing.ON_TIME, 1, 0))));
    assertThat(
        output,
        contains(
            WindowMatchers.isSingleWindowedValue(emptyIterable(), 9, 0, 10)));

    // We should get the final pane even though it is empty.
    tester.advanceInputWatermark(new Instant(150));
    output = tester.extractOutput();
    assertThat(
        output,
        contains(
            WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, true, Timing.LATE, 2, 1))));
    assertThat(
        output,
        contains(
            WindowMatchers.isSingleWindowedValue(emptyIterable(), 9, 0, 10)));
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
        contains(WindowMatchers.valueWithPaneInfo(
            PaneInfo.createPane(true, false, Timing.EARLY, 0, -1))));
    assertThat(
        output,
        contains(
            WindowMatchers.isSingleWindowedValue(containsInAnyOrder(1, 2), 1, 0, 10)));

    tester.advanceInputWatermark(new Instant(50));

    // We should get the ON_TIME pane even though it is empty,
    // because we have an AfterWatermark.pastEndOfWindow() trigger.
    output = tester.extractOutput();
    assertThat(
        output,
        contains(WindowMatchers.valueWithPaneInfo(
            PaneInfo.createPane(false, false, Timing.ON_TIME, 1, 0))));
    assertThat(
        output,
        contains(
            WindowMatchers.isSingleWindowedValue(containsInAnyOrder(1, 2), 9, 0, 10)));

    // We should get the final pane even though it is empty.
    tester.advanceInputWatermark(new Instant(150));
    output = tester.extractOutput();
    assertThat(
        output,
        contains(
            WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, true, Timing.LATE, 2, 1))));
    assertThat(
        output,
        contains(
            WindowMatchers.isSingleWindowedValue(containsInAnyOrder(1, 2), 9, 0, 10)));
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

    // Should trigger due to element count
    tester.injectElements(
        TimestampedValue.of(1, new Instant(1)), TimestampedValue.of(2, new Instant(2)));

    assertThat(
        tester.extractOutput(),
        contains(WindowMatchers.valueWithPaneInfo(
            PaneInfo.createPane(true, false, Timing.EARLY, 0, -1))));

    tester.advanceInputWatermark(new Instant(150));
    assertThat(tester.extractOutput(), contains(
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, true, Timing.ON_TIME, 1, 0))));
  }

  @Test
  public void testPaneInfoSkipToFinish() throws Exception {
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(FixedWindows.of(Duration.millis(10)), mockTrigger,
            AccumulationMode.DISCARDING_FIRED_PANES, Duration.millis(100),
            ClosingBehavior.FIRE_IF_NON_EMPTY);

    tester.advanceInputWatermark(new Instant(0));
    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    triggerShouldFinish(mockTrigger);
    injectElement(tester, 1);
    assertThat(tester.extractOutput(), contains(
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(true, true, Timing.EARLY))));
  }

  @Test
  public void testPaneInfoSkipToNonSpeculativeAndFinish() throws Exception {
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(FixedWindows.of(Duration.millis(10)), mockTrigger,
            AccumulationMode.DISCARDING_FIRED_PANES, Duration.millis(100),
            ClosingBehavior.FIRE_IF_NON_EMPTY);

    tester.advanceInputWatermark(new Instant(15));
    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    triggerShouldFinish(mockTrigger);
    injectElement(tester, 1);
    assertThat(tester.extractOutput(), contains(
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(true, true, Timing.LATE))));
  }

  @Test
  public void testMergeBeforeFinalizing() throws Exception {
    // Verify that we merge windows before producing output so users don't see undesired
    // unmerged windows.
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(Sessions.withGapDuration(Duration.millis(10)), mockTrigger,
            AccumulationMode.DISCARDING_FIRED_PANES, Duration.millis(0),
            ClosingBehavior.FIRE_IF_NON_EMPTY);

    // All on time data, verify watermark hold.
    // These two windows should pre-merge immediately to [1, 20)
    tester.injectElements(
        TimestampedValue.of(1, new Instant(1)), // in [1, 11)
        TimestampedValue.of(10, new Instant(10))); // in [10, 20)

    // And this should fire the end-of-window timer
    tester.advanceInputWatermark(new Instant(100));

    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output.size(), equalTo(1));
    assertThat(output.get(0),
        isSingleWindowedValue(containsInAnyOrder(1, 10),
            1, // timestamp
            1, // window start
            20)); // window end
    assertThat(
        output.get(0).getPane(),
        equalTo(PaneInfo.createPane(true, true, Timing.ON_TIME, 0, 0)));
  }

  /**
   * It is possible for a session window's trigger to be closed at the point at which
   * the (merged) session window is garbage collected. Make sure we don't accidentally
   * assume the window is still active.
   */
  @Test
  public void testMergingWithCloseBeforeGC() throws Exception {
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(Sessions.withGapDuration(Duration.millis(10)), mockTrigger,
            AccumulationMode.DISCARDING_FIRED_PANES, Duration.millis(50),
            ClosingBehavior.FIRE_IF_NON_EMPTY);

    // Two elements in two overlapping session windows.
    tester.injectElements(
        TimestampedValue.of(1, new Instant(1)), // in [1, 11)
        TimestampedValue.of(10, new Instant(10))); // in [10, 20)

    // Close the trigger, but the gargbage collection timer is still pending.
    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    triggerShouldFinish(mockTrigger);
    tester.advanceInputWatermark(new Instant(30));

    // Now the garbage collection timer will fire, finding the trigger already closed.
    tester.advanceInputWatermark(new Instant(100));

    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output.size(), equalTo(1));
    assertThat(output.get(0),
        isSingleWindowedValue(containsInAnyOrder(1, 10),
            1, // timestamp
            1, // window start
            20)); // window end
    assertThat(
        output.get(0).getPane(),
        equalTo(PaneInfo.createPane(true, true, Timing.ON_TIME, 0, 0)));
  }

  /**
   * If a later event tries to reuse an earlier session window which has been closed, we
   * should reject that element and not fail due to the window no longer having a representative.
   */
  @Test
  public void testMergingWithReusedWindow() throws Exception {
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(Sessions.withGapDuration(Duration.millis(10)), mockTrigger,
                                    AccumulationMode.DISCARDING_FIRED_PANES, Duration.millis(50),
                                    ClosingBehavior.FIRE_IF_NON_EMPTY);

    // One elements in one session window.
    tester.injectElements(TimestampedValue.of(1, new Instant(1))); // in [1, 11), gc at 21.

    // Close the trigger, but the gargbage collection timer is still pending.
    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    triggerShouldFinish(mockTrigger);
    tester.advanceInputWatermark(new Instant(15));

    // Another element in the same session window.
    // Should be discarded with 'window closed'.
    tester.injectElements(TimestampedValue.of(1, new Instant(1))); // in [1, 11), gc at 21.

    // Now the garbage collection timer will fire, finding the trigger already closed.
    tester.advanceInputWatermark(new Instant(100));

    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output.size(), equalTo(1));
    assertThat(output.get(0),
               isSingleWindowedValue(containsInAnyOrder(1),
                                     1, // timestamp
                                     1, // window start
                                     11)); // window end
    assertThat(
        output.get(0).getPane(),
        equalTo(PaneInfo.createPane(true, true, Timing.ON_TIME, 0, 0)));
  }

  /**
   * Tests that when data is assigned to multiple windows but some of those windows have
   * had their triggers finish, then the data is dropped and counted accurately.
   */
  @Test
  public void testDropDataMultipleWindowsFinishedTrigger() throws Exception {
    ReduceFnTester<Integer, Integer, IntervalWindow> tester = ReduceFnTester.combining(
        WindowingStrategy.of(
            SlidingWindows.of(Duration.millis(100)).every(Duration.millis(30)))
        .withTrigger(AfterWatermark.<IntervalWindow>pastEndOfWindow())
        .withAllowedLateness(Duration.millis(1000)),
        new Sum.SumIntegerFn().<String>asKeyedFn(), VarIntCoder.of());

    tester.injectElements(
        // assigned to [-60, 40), [-30, 70), [0, 100)
        TimestampedValue.of(10, new Instant(23)),
        // assigned to [-30, 70), [0, 100), [30, 130)
        TimestampedValue.of(12, new Instant(40)));

    assertEquals(0, tester.getElementsDroppedDueToClosedWindow());

    tester.advanceInputWatermark(new Instant(70));
    tester.injectElements(
        // assigned to [-30, 70), [0, 100), [30, 130)
        // but [-30, 70) is closed by the trigger
        TimestampedValue.of(14, new Instant(60)));

    assertEquals(1, tester.getElementsDroppedDueToClosedWindow());

    tester.advanceInputWatermark(new Instant(130));
    // assigned to [-30, 70), [0, 100), [30, 130)
    // but they are all closed
    tester.injectElements(TimestampedValue.of(16, new Instant(40)));

    assertEquals(4, tester.getElementsDroppedDueToClosedWindow());
  }

  @Test
  public void testIdempotentEmptyPanesDiscarding() throws Exception {
    // Test uninteresting (empty) panes don't increment the index or otherwise
    // modify PaneInfo.
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(FixedWindows.of(Duration.millis(10)), mockTrigger,
            AccumulationMode.DISCARDING_FIRED_PANES, Duration.millis(100),
            ClosingBehavior.FIRE_IF_NON_EMPTY);

    // Inject a couple of on-time elements and fire at the window end.
    injectElement(tester, 1);
    injectElement(tester, 2);
    tester.advanceInputWatermark(new Instant(12));

    // Fire the on-time pane
    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    tester.fireTimer(firstWindow, new Instant(9), TimeDomain.EVENT_TIME);

    // Fire another timer (with no data, so it's an uninteresting pane that should not be output).
    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    tester.fireTimer(firstWindow, new Instant(9), TimeDomain.EVENT_TIME);

    // Finish it off with another datum.
    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    triggerShouldFinish(mockTrigger);
    injectElement(tester, 3);

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
  }

  @Test
  public void testIdempotentEmptyPanesAccumulating() throws Exception {
    // Test uninteresting (empty) panes don't increment the index or otherwise
    // modify PaneInfo.
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(FixedWindows.of(Duration.millis(10)), mockTrigger,
            AccumulationMode.ACCUMULATING_FIRED_PANES, Duration.millis(100),
            ClosingBehavior.FIRE_IF_NON_EMPTY);

    // Inject a couple of on-time elements and fire at the window end.
    injectElement(tester, 1);
    injectElement(tester, 2);
    tester.advanceInputWatermark(new Instant(12));

    // Trigger the on-time pane
    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    tester.fireTimer(firstWindow, new Instant(9), TimeDomain.EVENT_TIME);
    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output.size(), equalTo(1));
    assertThat(output.get(0), isSingleWindowedValue(containsInAnyOrder(1, 2), 1, 0, 10));
    assertThat(output.get(0).getPane(),
        equalTo(PaneInfo.createPane(true, false, Timing.ON_TIME, 0, 0)));

    // Fire another timer with no data; the empty pane should not be output even though the
    // trigger is ready to fire
    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    tester.fireTimer(firstWindow, new Instant(9), TimeDomain.EVENT_TIME);
    assertThat(tester.extractOutput().size(), equalTo(0));

    // Finish it off with another datum, which is late
    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    triggerShouldFinish(mockTrigger);
    injectElement(tester, 3);
    output = tester.extractOutput();
    assertThat(output.size(), equalTo(1));

    // The late pane has the correct indices.
    assertThat(output.get(0).getValue(), containsInAnyOrder(1, 2, 3));
    assertThat(output.get(0).getPane(),
        equalTo(PaneInfo.createPane(false, true, Timing.LATE, 1, 1)));

    assertTrue(tester.isMarkedFinished(firstWindow));
    tester.assertHasOnlyGlobalAndFinishedSetsFor(firstWindow);

    assertEquals(0, tester.getElementsDroppedDueToClosedWindow());
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

    // Processing time timer for 5
    tester.injectElements(
        TimestampedValue.of(1, new Instant(1)),
        TimestampedValue.of(1, new Instant(3)),
        TimestampedValue.of(1, new Instant(7)),
        TimestampedValue.of(1, new Instant(5)));

    // Should fire early pane
    tester.advanceProcessingTime(new Instant(6));

    // Should fire empty on time pane
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

  private static class SumAndVerifyContextFn extends CombineFnWithContext<Integer, int[], Integer> {

    private final PCollectionView<Integer> view;
    private final int expectedValue;

    private SumAndVerifyContextFn(PCollectionView<Integer> view, int expectedValue) {
      this.view = view;
      this.expectedValue = expectedValue;
    }
    @Override
    public int[] createAccumulator(Context c) {
      Preconditions.checkArgument(
          c.getPipelineOptions().as(TestOptions.class).getValue() == expectedValue);
      Preconditions.checkArgument(c.sideInput(view) == expectedValue);
      return wrap(0);
    }

    @Override
    public int[] addInput(int[] accumulator, Integer input, Context c) {
      Preconditions.checkArgument(
          c.getPipelineOptions().as(TestOptions.class).getValue() == expectedValue);
      Preconditions.checkArgument(c.sideInput(view) == expectedValue);
      accumulator[0] += input.intValue();
      return accumulator;
    }

    @Override
    public int[] mergeAccumulators(Iterable<int[]> accumulators, Context c) {
      Preconditions.checkArgument(
          c.getPipelineOptions().as(TestOptions.class).getValue() == expectedValue);
      Preconditions.checkArgument(c.sideInput(view) == expectedValue);
      Iterator<int[]> iter = accumulators.iterator();
      if (!iter.hasNext()) {
        return createAccumulator(c);
      } else {
        int[] running = iter.next();
        while (iter.hasNext()) {
          running[0] += iter.next()[0];
        }
        return running;
      }
    }

    @Override
    public Integer extractOutput(int[] accumulator, Context c) {
      Preconditions.checkArgument(
          c.getPipelineOptions().as(TestOptions.class).getValue() == expectedValue);
      Preconditions.checkArgument(c.sideInput(view) == expectedValue);
      return accumulator[0];
    }

    private int[] wrap(int value) {
      return new int[] { value };
    }
  }

  /**
   * A {@link PipelineOptions} to test combining with context.
   */
  public interface TestOptions extends PipelineOptions {
    Integer getValue();
    void setValue(Integer value);
  }
}
