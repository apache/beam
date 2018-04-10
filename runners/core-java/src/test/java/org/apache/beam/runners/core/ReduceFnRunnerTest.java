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
package org.apache.beam.runners.core;

import static org.apache.beam.runners.core.WindowMatchers.isSingleWindowedValue;
import static org.apache.beam.runners.core.WindowMatchers.isWindowedValue;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.common.collect.Iterables;
import java.util.List;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.triggers.DefaultTriggerStateMachine;
import org.apache.beam.runners.core.triggers.TriggerStateMachine;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.CombineWithContext.Context;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.AfterEach;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Never;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
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
 * Tests for {@link ReduceFnRunner}. These tests instantiate a full "stack" of
 * {@link ReduceFnRunner} with enclosed {@link ReduceFn}, down to the installed {@link Trigger}
 * (sometimes mocked). They proceed by injecting elements and advancing watermark and
 * processing time, then verifying produced panes and counters.
 */
@RunWith(JUnit4.class)
public class ReduceFnRunnerTest {
  @Mock private SideInputReader mockSideInputReader;
  private TriggerStateMachine mockTriggerStateMachine;
  private PCollectionView<Integer> mockView;

  private IntervalWindow firstWindow;

  private static TriggerStateMachine.TriggerContext anyTriggerContext() {
    return Mockito.any();
  }
  private static TriggerStateMachine.OnElementContext anyElementContext() {
    return Mockito.any();
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    mockTriggerStateMachine = mock(TriggerStateMachine.class, withSettings().serializable());

    @SuppressWarnings("unchecked")
    PCollectionView<Integer> mockViewUnchecked =
        mock(PCollectionView.class, withSettings().serializable());
    mockView = mockViewUnchecked;
    firstWindow = new IntervalWindow(new Instant(0), new Instant(10));
  }

  private void injectElement(ReduceFnTester<Integer, ?, IntervalWindow> tester, int element)
      throws Exception {
    doNothing().when(mockTriggerStateMachine).onElement(anyElementContext());
    tester.injectElements(TimestampedValue.of(element, new Instant(element)));
  }

  private void triggerShouldFinish(TriggerStateMachine mockTrigger) throws Exception {
    doAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              TriggerStateMachine.TriggerContext context =
                  (TriggerStateMachine.TriggerContext) invocation.getArguments()[0];
              context.trigger().setFinished(true);
              return null;
            })
        .when(mockTrigger)
        .onFire(anyTriggerContext());
  }

  /**
   * Tests that a processing time timer does not cause window GC.
   */
  @Test
  public void testProcessingTimeTimerDoesNotGc() throws Exception {
    WindowingStrategy<?, IntervalWindow> strategy =
        WindowingStrategy.of((WindowFn<?, IntervalWindow>) FixedWindows.of(Duration.millis(100)))
            .withTimestampCombiner(TimestampCombiner.EARLIEST)
            .withMode(AccumulationMode.ACCUMULATING_FIRED_PANES)
            .withAllowedLateness(Duration.ZERO)
            .withTrigger(
                Repeatedly.forever(
                    AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.millis(10))));

    ReduceFnTester<Integer, Integer, IntervalWindow> tester =
        ReduceFnTester.combining(strategy, Sum.ofIntegers(), VarIntCoder.of());

    tester.advanceProcessingTime(new Instant(5000));
    injectElement(tester, 2); // processing timer @ 5000 + 10; EOW timer @ 100
    injectElement(tester, 5);

    tester.advanceProcessingTime(new Instant(10000));

    tester.assertHasOnlyGlobalAndStateFor(
        new IntervalWindow(new Instant(0), new Instant(100)));

    assertThat(
        tester.extractOutput(),
        contains(
            isSingleWindowedValue(
                equalTo(7), 2, 0, 100, PaneInfo.createPane(true, false, Timing.EARLY, 0, 0))));
  }

  @Test
  public void testOnElementBufferingDiscarding() throws Exception {
    // Test basic execution of a trigger using a non-combining window set and discarding mode.
    MetricsContainerImpl container = new MetricsContainerImpl("any");
    MetricsEnvironment.setCurrentContainer(container);
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(FixedWindows.of(Duration.millis(10)), mockTriggerStateMachine,
            AccumulationMode.DISCARDING_FIRED_PANES, Duration.millis(100),
            ClosingBehavior.FIRE_IF_NON_EMPTY);

    // Pane of {1, 2}
    injectElement(tester, 1);
    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    injectElement(tester, 2);
    assertThat(tester.extractOutput(),
        contains(isSingleWindowedValue(containsInAnyOrder(1, 2), 1, 0, 10)));

    // Pane of just 3, and finish
    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    triggerShouldFinish(mockTriggerStateMachine);
    injectElement(tester, 3);
    assertThat(tester.extractOutput(),
            contains(isSingleWindowedValue(containsInAnyOrder(3), 3, 0, 10)));
    assertTrue(tester.isMarkedFinished(firstWindow));
    tester.assertHasOnlyGlobalAndFinishedSetsFor(firstWindow);

    // This element shouldn't be seen, because the trigger has finished
    injectElement(tester, 4);

    long droppedElements =
        container
            .getCounter(
                MetricName.named(ReduceFnRunner.class, ReduceFnRunner.DROPPED_DUE_TO_CLOSED_WINDOW))
            .getCumulative();
    assertEquals(1, droppedElements);
  }

  @Test
  public void testOnElementBufferingAccumulating() throws Exception {
    // Test basic execution of a trigger using a non-combining window set and accumulating mode.
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(
            FixedWindows.of(Duration.millis(10)),
            mockTriggerStateMachine,
            AccumulationMode.ACCUMULATING_FIRED_PANES,
            Duration.millis(100),
            ClosingBehavior.FIRE_IF_NON_EMPTY);

    injectElement(tester, 1);

    // Fires {1, 2}
    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    injectElement(tester, 2);

    // Fires {1, 2, 3} because we are in accumulating mode
    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    triggerShouldFinish(mockTriggerStateMachine);
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

  /**
   * When the watermark passes the end-of-window and window expiration time
   * in a single update, this tests that it does not crash.
   */
  @Test
  public void testSessionEowAndGcTogether() throws Exception {
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(
            Sessions.withGapDuration(Duration.millis(10)),
            DefaultTriggerStateMachine.<IntervalWindow>of(),
            AccumulationMode.ACCUMULATING_FIRED_PANES,
            Duration.millis(50),
            ClosingBehavior.FIRE_ALWAYS);

    tester.setAutoAdvanceOutputWatermark(true);

    tester.advanceInputWatermark(new Instant(0));
    injectElement(tester, 1);
    tester.advanceInputWatermark(new Instant(100));

    assertThat(
        tester.extractOutput(),
        contains(
            isSingleWindowedValue(
                contains(1), 1, 1, 11, PaneInfo.createPane(true, true, Timing.ON_TIME))));
  }

  /**
   * When the watermark passes the end-of-window and window expiration time
   * in a single update, this tests that it does not crash.
   */
  @Test
  public void testFixedWindowsEowAndGcTogether() throws Exception {
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(
            FixedWindows.of(Duration.millis(10)),
            DefaultTriggerStateMachine.<IntervalWindow>of(),
            AccumulationMode.ACCUMULATING_FIRED_PANES,
            Duration.millis(50),
            ClosingBehavior.FIRE_ALWAYS);

    tester.setAutoAdvanceOutputWatermark(true);

    tester.advanceInputWatermark(new Instant(0));
    injectElement(tester, 1);
    tester.advanceInputWatermark(new Instant(100));

    assertThat(
        tester.extractOutput(),
        contains(
            isSingleWindowedValue(
                contains(1), 1, 0, 10, PaneInfo.createPane(true, true, Timing.ON_TIME))));
  }

  /**
   * When the watermark passes the end-of-window and window expiration time
   * in a single update, this tests that it does not crash.
   */
  @Test
  public void testFixedWindowsEowAndGcTogetherFireIfNonEmpty() throws Exception {
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(
            FixedWindows.of(Duration.millis(10)),
            DefaultTriggerStateMachine.<IntervalWindow>of(),
            AccumulationMode.ACCUMULATING_FIRED_PANES,
            Duration.millis(50),
            ClosingBehavior.FIRE_IF_NON_EMPTY);

    tester.setAutoAdvanceOutputWatermark(true);

    tester.advanceInputWatermark(new Instant(0));
    injectElement(tester, 1);
    tester.advanceInputWatermark(new Instant(100));

    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(
        output,
        contains(
            isSingleWindowedValue(
                contains(1), 1, 0, 10, PaneInfo.createPane(true, true, Timing.ON_TIME))));
  }

  /**
   * Tests that with the default trigger we will not produce two ON_TIME panes, even
   * if there are two outputs that are both candidates.
   */
  @Test
  public void testOnlyOneOnTimePane() throws Exception {
    WindowingStrategy<?, IntervalWindow> strategy =
        WindowingStrategy.of((WindowFn<?, IntervalWindow>) FixedWindows.of(Duration.millis(10)))
            .withTrigger(DefaultTrigger.of())
            .withMode(AccumulationMode.ACCUMULATING_FIRED_PANES)
            .withAllowedLateness(Duration.millis(100));

    ReduceFnTester<Integer, Integer, IntervalWindow> tester =
        ReduceFnTester.combining(strategy, Sum.ofIntegers(), VarIntCoder.of());

    tester.advanceInputWatermark(new Instant(0));

    int value1 = 1;
    int value2 = 3;

    // A single element that should be in the ON_TIME output
    tester.injectElements(
        TimestampedValue.of(value1, new Instant(1)));

    // Should fire ON_TIME
    tester.advanceInputWatermark(new Instant(10));

    // The DefaultTrigger should cause output labeled LATE, even though it does not have to be
    // labeled as such.
    tester.injectElements(
        TimestampedValue.of(value2, new Instant(3)));

    List<WindowedValue<Integer>> output = tester.extractOutput();
    assertEquals(2, output.size());

    assertThat(output.get(0), WindowMatchers.isWindowedValue(equalTo(value1)));
    assertThat(output.get(1), WindowMatchers.isWindowedValue(equalTo(value1 + value2)));

    assertThat(
        output.get(0),
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(true, false, Timing.ON_TIME, 0, 0)));
    assertThat(
        output.get(1),
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, false, Timing.LATE, 1, 1)));
  }

  @Test
  public void testOnElementCombiningDiscarding() throws Exception {
    // Test basic execution of a trigger using a non-combining window set and discarding mode.

    WindowingStrategy<?, IntervalWindow> strategy =
        WindowingStrategy.of((WindowFn<?, IntervalWindow>) FixedWindows.of(Duration.millis(10)))
            .withTimestampCombiner(TimestampCombiner.EARLIEST)
            .withMode(AccumulationMode.DISCARDING_FIRED_PANES)
            .withAllowedLateness(Duration.millis(100));

    ReduceFnTester<Integer, Integer, IntervalWindow> tester =
        ReduceFnTester.combining(
            strategy,
            mockTriggerStateMachine,
            Sum.ofIntegers(),
            VarIntCoder.of());

    injectElement(tester, 2);

    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    injectElement(tester, 3);

    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    triggerShouldFinish(mockTriggerStateMachine);
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

  /**
   * Tests that when a processing time timer comes in after a window is expired
   * it is just ignored.
   */
  @Test
  public void testLateProcessingTimeTimer() throws Exception {
    WindowingStrategy<?, IntervalWindow> strategy =
        WindowingStrategy.of((WindowFn<?, IntervalWindow>) FixedWindows.of(Duration.millis(100)))
            .withTimestampCombiner(TimestampCombiner.EARLIEST)
            .withMode(AccumulationMode.ACCUMULATING_FIRED_PANES)
            .withAllowedLateness(Duration.ZERO)
            .withTrigger(
                Repeatedly.forever(
                    AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.millis(10))));

    ReduceFnTester<Integer, Integer, IntervalWindow> tester =
        ReduceFnTester.combining(strategy, Sum.ofIntegers(), VarIntCoder.of());

    tester.advanceProcessingTime(new Instant(5000));
    injectElement(tester, 2); // processing timer @ 5000 + 10; EOW timer @ 100
    injectElement(tester, 5);

    // After this advancement, the window is expired and only the GC process
    // should be allowed to touch it
    tester.advanceInputWatermarkNoTimers(new Instant(100));

    // This should not output
    tester.advanceProcessingTime(new Instant(6000));

    assertThat(tester.extractOutput(), emptyIterable());
  }

  /**
   * Tests that when a processing time timer comes in after a window is expired
   * but in the same bundle it does not cause a spurious output.
   */
  @Test
  public void testCombiningAccumulatingProcessingTime() throws Exception {
    WindowingStrategy<?, IntervalWindow> strategy =
        WindowingStrategy.of((WindowFn<?, IntervalWindow>) FixedWindows.of(Duration.millis(100)))
            .withTimestampCombiner(TimestampCombiner.EARLIEST)
            .withMode(AccumulationMode.ACCUMULATING_FIRED_PANES)
            .withAllowedLateness(Duration.ZERO)
            .withTrigger(
                Repeatedly.forever(
                    AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.millis(10))));

    ReduceFnTester<Integer, Integer, IntervalWindow> tester =
        ReduceFnTester.combining(strategy, Sum.ofIntegers(), VarIntCoder.of());

    tester.advanceProcessingTime(new Instant(5000));
    injectElement(tester, 2); // processing timer @ 5000 + 10; EOW timer @ 100
    injectElement(tester, 5);

    tester.advanceInputWatermarkNoTimers(new Instant(100));
    tester.advanceProcessingTimeNoTimers(new Instant(5010));

    // Fires the GC/EOW timer at the same time as the processing time timer.
    tester.fireTimers(
        new IntervalWindow(new Instant(0), new Instant(100)),
        TimestampedValue.of(TimeDomain.EVENT_TIME, new Instant(100)),
        TimestampedValue.of(TimeDomain.PROCESSING_TIME, new Instant(5010)));

    assertThat(
        tester.extractOutput(),
        contains(
            isSingleWindowedValue(
                equalTo(7), 2, 0, 100, PaneInfo.createPane(true, true, Timing.ON_TIME, 0, 0))));
  }

  /**
   * Tests that the garbage collection time for a fixed window does not overflow the end of time.
   */
  @Test
  public void testFixedWindowEndOfTimeGarbageCollection() throws Exception {

    Duration allowedLateness = Duration.standardDays(365);
    Duration windowSize = Duration.millis(10);
    WindowFn<Object, IntervalWindow> windowFn = FixedWindows.of(windowSize);

    // This timestamp falls into a window where the end of the window is before the end of the
    // global window - the "end of time" - yet its expiration time is after.
    final Instant elementTimestamp =
        GlobalWindow.INSTANCE.maxTimestamp().minus(allowedLateness).plus(1);

    IntervalWindow window = Iterables.getOnlyElement(
        windowFn.assignWindows(
            windowFn.new AssignContext() {
              @Override
              public Object element() {
                throw new UnsupportedOperationException();
              }
              @Override
              public Instant timestamp() {
                return elementTimestamp;
              }

              @Override
              public BoundedWindow window() {
                throw new UnsupportedOperationException();
              }
            }));

    assertTrue(
        window.maxTimestamp().isBefore(GlobalWindow.INSTANCE.maxTimestamp()));
    assertTrue(
        window.maxTimestamp().plus(allowedLateness).isAfter(GlobalWindow.INSTANCE.maxTimestamp()));

    // Test basic execution of a trigger using a non-combining window set and accumulating mode.

    WindowingStrategy<?, IntervalWindow> strategy =
        WindowingStrategy.of((WindowFn<?, IntervalWindow>) windowFn)
            .withTimestampCombiner(TimestampCombiner.EARLIEST)
            .withTrigger(AfterWatermark.pastEndOfWindow().withLateFirings(Never.ever()))
            .withMode(AccumulationMode.DISCARDING_FIRED_PANES)
            .withAllowedLateness(allowedLateness);

    ReduceFnTester<Integer, Integer, IntervalWindow> tester =
        ReduceFnTester
            .combining(strategy, Sum.ofIntegers(), VarIntCoder.of());

    tester.injectElements(TimestampedValue.of(13, elementTimestamp));

    // Should fire ON_TIME pane and there will be a checkState that the cleanup time
    // is prior to timestamp max value
    tester.advanceInputWatermark(window.maxTimestamp());

    // Nothing in the ON_TIME pane (not governed by triggers, but by ReduceFnRunner)
    assertThat(tester.extractOutput(), emptyIterable());

    tester.injectElements(TimestampedValue.of(42, elementTimestamp));

    // Now the final pane should fire, demonstrating that the GC time was truncated
    tester.advanceInputWatermark(GlobalWindow.INSTANCE.maxTimestamp());
    assertThat(tester.extractOutput(), contains(isWindowedValue(equalTo(55))));
  }

  /**
   * Tests that when a processing time timers comes in after a window is expired
   * and GC'd it does not cause a spurious output.
   */
  @Test
  public void testCombiningAccumulatingProcessingTimeSeparateBundles() throws Exception {
    WindowingStrategy<?, IntervalWindow> strategy =
        WindowingStrategy.of((WindowFn<?, IntervalWindow>) FixedWindows.of(Duration.millis(100)))
            .withTimestampCombiner(TimestampCombiner.EARLIEST)
            .withMode(AccumulationMode.ACCUMULATING_FIRED_PANES)
            .withAllowedLateness(Duration.ZERO)
            .withTrigger(
                Repeatedly.forever(
                    AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.millis(10))));

    ReduceFnTester<Integer, Integer, IntervalWindow> tester =
        ReduceFnTester.combining(strategy, Sum.ofIntegers(), VarIntCoder.of());

    tester.advanceProcessingTime(new Instant(5000));
    injectElement(tester, 2); // processing timer @ 5000 + 10; EOW timer @ 100
    injectElement(tester, 5);

    tester.advanceInputWatermark(new Instant(100));
    tester.advanceProcessingTime(new Instant(5011));

    assertThat(
        tester.extractOutput(),
        contains(
            isSingleWindowedValue(
                equalTo(7), 2, 0, 100, PaneInfo.createPane(true, true, Timing.ON_TIME, 0, 0))));
  }

  /**
   * Tests that if end-of-window and GC timers come in together, that the pane is correctly
   * marked as final.
   */
  @Test
  public void testCombiningAccumulatingEventTime() throws Exception {
    WindowingStrategy<?, IntervalWindow> strategy =
        WindowingStrategy.of((WindowFn<?, IntervalWindow>) FixedWindows.of(Duration.millis(100)))
            .withTimestampCombiner(TimestampCombiner.EARLIEST)
            .withMode(AccumulationMode.ACCUMULATING_FIRED_PANES)
            .withAllowedLateness(Duration.millis(1))
            .withTrigger(Repeatedly.forever(AfterWatermark.pastEndOfWindow()));

    ReduceFnTester<Integer, Integer, IntervalWindow> tester =
        ReduceFnTester.combining(strategy, Sum.ofIntegers(), VarIntCoder.of());

    injectElement(tester, 2); // processing timer @ 5000 + 10; EOW timer @ 100
    injectElement(tester, 5);

    tester.advanceInputWatermark(new Instant(1000));

    assertThat(
        tester.extractOutput(),
        contains(
            isSingleWindowedValue(
                equalTo(7), 2, 0, 100, PaneInfo.createPane(true, true, Timing.ON_TIME, 0, 0))));
  }


  @Test
  public void testOnElementCombiningAccumulating() throws Exception {
    // Test basic execution of a trigger using a non-combining window set and accumulating mode.

    WindowingStrategy<?, IntervalWindow> strategy =
        WindowingStrategy.of((WindowFn<?, IntervalWindow>) FixedWindows.of(Duration.millis(10)))
            .withTimestampCombiner(TimestampCombiner.EARLIEST)
            .withMode(AccumulationMode.ACCUMULATING_FIRED_PANES)
            .withAllowedLateness(Duration.millis(100));

    ReduceFnTester<Integer, Integer, IntervalWindow> tester =
        ReduceFnTester.combining(
            strategy,
            mockTriggerStateMachine,
            Sum.ofIntegers(),
            VarIntCoder.of());

    injectElement(tester, 1);

    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    injectElement(tester, 2);

    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    triggerShouldFinish(mockTriggerStateMachine);
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
    // Create values at timestamps 0 .. 8, windowed into fixed windows of 2.
    // Side input windowed into fixed windows of 4:
    // main: [ 0 1 ] [ 2 3 ] [ 4 5 ] [ 6 7 ]
    // side: [     100     ] [     104     ]
    // Combine using a CombineFn "side input + sum(main inputs)".
    final int firstWindowSideInput = 100;
    final int secondWindowSideInput = 104;
    final Integer expectedValue = firstWindowSideInput;
    WindowingStrategy<?, IntervalWindow> mainInputWindowingStrategy =
        WindowingStrategy.of(FixedWindows.of(Duration.millis(2)))
            .withMode(AccumulationMode.ACCUMULATING_FIRED_PANES);

    WindowMappingFn<?> sideInputWindowMappingFn =
        FixedWindows.of(Duration.millis(4)).getDefaultWindowMappingFn();
    when(mockView.getWindowMappingFn()).thenReturn((WindowMappingFn) sideInputWindowMappingFn);

    TestOptions options = PipelineOptionsFactory.as(TestOptions.class);
    options.setValue(expectedValue);

    when(mockSideInputReader.contains(org.mockito.Matchers.any(PCollectionView.class)))
        .thenReturn(true);
    when(mockSideInputReader.get(any(PCollectionView.class), any(BoundedWindow.class)))
        .then(
            invocation -> {
              IntervalWindow sideInputWindow = (IntervalWindow) invocation.getArguments()[1];
              long startMs = sideInputWindow.start().getMillis();
              long endMs = sideInputWindow.end().getMillis();
              // Window should have been produced by sideInputWindowingStrategy.
              assertThat(startMs, anyOf(equalTo(0L), equalTo(4L)));
              assertThat(endMs - startMs, equalTo(4L));
              // If startMs == 4 (second window), equal to secondWindowSideInput.
              return firstWindowSideInput + (int) startMs;
            });

    SumAndVerifyContextFn combineFn = new SumAndVerifyContextFn(mockView, expectedValue);
    ReduceFnTester<Integer, Integer, IntervalWindow> tester =
        ReduceFnTester.combining(
            mainInputWindowingStrategy,
            mockTriggerStateMachine,
            combineFn,
            VarIntCoder.of(),
            options,
            mockSideInputReader);

    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    for (int i = 0; i < 8; ++i) {
      injectElement(tester, i);
    }

    assertThat(
        tester.extractOutput(),
        contains(
            isSingleWindowedValue(equalTo(0 + firstWindowSideInput), 1, 0, 2),
            isSingleWindowedValue(equalTo(0 + 1 + firstWindowSideInput), 1, 0, 2),
            isSingleWindowedValue(equalTo(2 + firstWindowSideInput), 3, 2, 4),
            isSingleWindowedValue(equalTo(2 + 3 + firstWindowSideInput), 3, 2, 4),
            isSingleWindowedValue(equalTo(4 + secondWindowSideInput), 5, 4, 6),
            isSingleWindowedValue(equalTo(4 + 5 + secondWindowSideInput), 5, 4, 6),
            isSingleWindowedValue(equalTo(6 + secondWindowSideInput), 7, 6, 8),
            isSingleWindowedValue(equalTo(6 + 7 + secondWindowSideInput), 7, 6, 8)));
  }

  @Test
  public void testWatermarkHoldAndLateData() throws Exception {
    MetricsContainerImpl container = new MetricsContainerImpl("any");
    MetricsEnvironment.setCurrentContainer(container);
    // Test handling of late data. Specifically, ensure the watermark hold is correct.
    Duration allowedLateness = Duration.millis(10);
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(
            FixedWindows.of(Duration.millis(10)),
            mockTriggerStateMachine,
            AccumulationMode.ACCUMULATING_FIRED_PANES,
            allowedLateness,
            ClosingBehavior.FIRE_IF_NON_EMPTY);

    // Input watermark -> null
    assertEquals(null, tester.getWatermarkHold());
    assertEquals(null, tester.getOutputWatermark());

    // All on time data, verify watermark hold.
    IntervalWindow expectedWindow = new IntervalWindow(new Instant(0), new Instant(10));
    injectElement(tester, 1);
    injectElement(tester, 3);
    assertEquals(new Instant(1), tester.getWatermarkHold());
    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    injectElement(tester, 2);
    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(
        output,
        contains(
            isSingleWindowedValue(
                containsInAnyOrder(1, 2, 3),
                equalTo(new Instant(1)),
                equalTo((BoundedWindow) expectedWindow))));
    assertThat(output.get(0).getPane(),
        equalTo(PaneInfo.createPane(true, false, Timing.EARLY, 0, -1)));

    // There is no end-of-window hold, but the timer set by the trigger holds the watermark
    assertThat(
        tester.getWatermarkHold(), nullValue());

    // Nothing dropped.
    long droppedElements =
        container
            .getCounter(
                MetricName.named(ReduceFnRunner.class, ReduceFnRunner.DROPPED_DUE_TO_CLOSED_WINDOW))
            .getCumulative();
    assertEquals(0, droppedElements);

    // Input watermark -> 4, output watermark should advance that far as well
    tester.advanceInputWatermark(new Instant(4));
    assertEquals(new Instant(4), tester.getOutputWatermark());

    // Some late, some on time. Verify that we only hold to the minimum of on-time.
    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(false);
    tester.advanceInputWatermark(new Instant(4));
    injectElement(tester, 2);
    injectElement(tester, 3);

    // Late data has arrived behind the _output_ watermark. The ReduceFnRunner sets a GC hold
    // since this data is not permitted to hold up the output watermark.
    assertThat(
        tester.getWatermarkHold(), equalTo(expectedWindow.maxTimestamp().plus(allowedLateness)));

    // Now data just ahead of the output watermark arrives and sets an earlier "element" hold
    injectElement(tester, 5);
    assertEquals(new Instant(5), tester.getWatermarkHold());

    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
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

    // Since the element hold is cleared, there is no hold remaining
    assertThat(tester.getWatermarkHold(), nullValue());

    // All behind the output watermark -- hold is at GC time (if we imagine the
    // trigger sets a timer for ON_TIME firing, that is actually when they'll be emitted)
    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(false);
    tester.advanceInputWatermark(new Instant(8));
    injectElement(tester, 6);
    injectElement(tester, 5);
    assertThat(
        tester.getWatermarkHold(), equalTo(expectedWindow.maxTimestamp().plus(allowedLateness)));

    injectElement(tester, 4);

    // Fire the ON_TIME pane
    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);

    // To get an ON_TIME pane, we need the output watermark to be held back a little; this would
    // be done by way of the timers set by the trigger, which are mocked here
    tester.setAutoAdvanceOutputWatermark(false);

    tester.advanceInputWatermark(expectedWindow.maxTimestamp().plus(1));
    tester.fireTimer(expectedWindow, expectedWindow.maxTimestamp(), TimeDomain.EVENT_TIME);

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

    tester.setAutoAdvanceOutputWatermark(true);

    // This is "pending" at the time the watermark makes it way-late.
    // Because we're about to expire the window, we output it.
    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(false);
    injectElement(tester, 8);
    droppedElements =
        container
            .getCounter(
                MetricName.named(ReduceFnRunner.class, ReduceFnRunner.DROPPED_DUE_TO_CLOSED_WINDOW))
            .getCumulative();
    assertEquals(0, droppedElements);

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

  /** Make sure that if data comes in too late to make it on time, the hold is the GC time. */
  @Test
  public void dontSetHoldIfTooLateForEndOfWindowTimer() throws Exception {
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(
            FixedWindows.of(Duration.millis(10)),
            mockTriggerStateMachine,
            AccumulationMode.ACCUMULATING_FIRED_PANES,
            Duration.millis(10),
            ClosingBehavior.FIRE_ALWAYS);
    tester.setAutoAdvanceOutputWatermark(false);

    // Case: Unobservably "late" relative to input watermark, but on time for output watermark
    tester.advanceInputWatermark(new Instant(15));
    tester.advanceOutputWatermark(new Instant(11));

    IntervalWindow expectedWindow = new IntervalWindow(new Instant(10), new Instant(20));
    injectElement(tester, 14);
    // Hold was applied, waiting for end-of-window timer.
    assertEquals(new Instant(14), tester.getWatermarkHold());

    // Trigger the end-of-window timer, fire a timer as though the mock trigger set it
    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    tester.advanceInputWatermark(new Instant(20));
    tester.fireTimer(expectedWindow, expectedWindow.maxTimestamp(), TimeDomain.EVENT_TIME);

    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(false);
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
        ReduceFnTester.nonCombining(
            FixedWindows.of(Duration.millis(10)),
            mockTriggerStateMachine,
            AccumulationMode.DISCARDING_FIRED_PANES,
            Duration.millis(100),
            ClosingBehavior.FIRE_IF_NON_EMPTY);

    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(10));

    tester.advanceInputWatermark(new Instant(0));
    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    injectElement(tester, 1);
    assertThat(tester.extractOutput(), contains(
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(true, false, Timing.EARLY))));

    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    injectElement(tester, 2);
    assertThat(tester.extractOutput(), contains(
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, false, Timing.EARLY, 1, -1))));

    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(false);
    tester.setAutoAdvanceOutputWatermark(false);
    tester.advanceInputWatermark(new Instant(15));

    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    injectElement(tester, 3);
    assertThat(tester.extractOutput(), contains(
        WindowMatchers.valueWithPaneInfo(
            PaneInfo.createPane(false, false, Timing.ON_TIME, 2, 0))));

    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    tester.setAutoAdvanceOutputWatermark(true);
    injectElement(tester, 4);
    assertThat(tester.extractOutput(), contains(
        WindowMatchers.valueWithPaneInfo(
            PaneInfo.createPane(false, false, Timing.LATE, 3, 1))));

    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    triggerShouldFinish(mockTriggerStateMachine);
    injectElement(tester, 5);
    assertThat(tester.extractOutput(), contains(
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, true, Timing.LATE, 4, 2))));
  }

  @Test
  public void testPaneInfoAllStatesAfterWatermark() throws Exception {
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester = ReduceFnTester.nonCombining(
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)))
            .withTrigger(Repeatedly.forever(AfterFirst.of(
                AfterPane.elementCountAtLeast(2),
                AfterWatermark.pastEndOfWindow())))
            .withMode(AccumulationMode.DISCARDING_FIRED_PANES)
            .withAllowedLateness(Duration.millis(100))
            .withTimestampCombiner(TimestampCombiner.EARLIEST)
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
  public void noEmptyPanesFinalIfNonEmpty() throws Exception {
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester = ReduceFnTester.nonCombining(
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)))
            .withTrigger(Repeatedly.<IntervalWindow>forever(AfterFirst.<IntervalWindow>of(
                AfterPane.elementCountAtLeast(2),
                AfterWatermark.pastEndOfWindow())))
            .withMode(AccumulationMode.ACCUMULATING_FIRED_PANES)
            .withAllowedLateness(Duration.millis(100))
            .withTimestampCombiner(TimestampCombiner.EARLIEST)
            .withClosingBehavior(ClosingBehavior.FIRE_IF_NON_EMPTY));

    tester.advanceInputWatermark(new Instant(0));
    tester.injectElements(
        TimestampedValue.of(1, new Instant(1)),
        TimestampedValue.of(2, new Instant(2)));
    tester.advanceInputWatermark(new Instant(20));
    tester.advanceInputWatermark(new Instant(250));

    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output, contains(
        // Trigger with 2 elements
        WindowMatchers.isSingleWindowedValue(containsInAnyOrder(1, 2), 1, 0, 10),
        // Trigger for the empty on time pane
        WindowMatchers.isSingleWindowedValue(containsInAnyOrder(1, 2), 9, 0, 10)));
  }

  @Test
  public void noEmptyPanesFinalAlways() throws Exception {
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester = ReduceFnTester.nonCombining(
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)))
            .withTrigger(Repeatedly.<IntervalWindow>forever(AfterFirst.<IntervalWindow>of(
                AfterPane.elementCountAtLeast(2),
                AfterWatermark.pastEndOfWindow())))
            .withMode(AccumulationMode.ACCUMULATING_FIRED_PANES)
            .withAllowedLateness(Duration.millis(100))
            .withTimestampCombiner(TimestampCombiner.EARLIEST)
            .withClosingBehavior(ClosingBehavior.FIRE_ALWAYS));

    tester.advanceInputWatermark(new Instant(0));
    tester.injectElements(
        TimestampedValue.of(1, new Instant(1)),
        TimestampedValue.of(2, new Instant(2)));
    tester.advanceInputWatermark(new Instant(20));
    tester.advanceInputWatermark(new Instant(250));

    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output, contains(
        // Trigger with 2 elements
        WindowMatchers.isSingleWindowedValue(containsInAnyOrder(1, 2), 1, 0, 10),
        // Trigger for the empty on time pane
        WindowMatchers.isSingleWindowedValue(containsInAnyOrder(1, 2), 9, 0, 10),
        // Trigger for the final pane
        WindowMatchers.isSingleWindowedValue(containsInAnyOrder(1, 2), 9, 0, 10)));
  }

  /**
   * If the trigger does not care about the watermark, the ReduceFnRunner should still emit an
   * element for the ON_TIME pane.
   */
  @Test
  public void testNoWatermarkTriggerNoHold() throws Exception {
    Duration allowedLateness = Duration.standardDays(1);
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(
            WindowingStrategy.of(FixedWindows.of(Duration.millis(10)))
                .withTrigger(
                    Repeatedly.forever(
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(Duration.standardSeconds(5))))
                .withAllowedLateness(allowedLateness));

    // First, an element comes in on time in [0, 10) but ReduceFnRunner should
    // not set a hold or timer for 9. That is the trigger's job.
    IntervalWindow expectedWindow = new IntervalWindow(new Instant(0), new Instant(10));
    tester.advanceInputWatermark(new Instant(0));
    tester.advanceProcessingTime(new Instant(0));

    tester.injectElements(TimestampedValue.of(1, new Instant(1)));

    // Since some data arrived, the element hold will be the end of the window.
    assertThat(tester.getWatermarkHold(), equalTo(expectedWindow.maxTimestamp()));

    tester.advanceProcessingTime(new Instant(6000));

    // Sanity check; we aren't trying to verify output in this test
    assertThat(tester.getOutputSize(), equalTo(1));

    // Since we did not request empty final panes, no hold
    assertThat(tester.getWatermarkHold(), nullValue());

    // So when the input watermark advanced, the output advances with it (automated by tester)
    tester.advanceInputWatermark(
        new Instant(expectedWindow.maxTimestamp().plus(Duration.standardHours(1))));

    // Now late data arrives
    tester.injectElements(TimestampedValue.of(3, new Instant(3)));

    // The ReduceFnRunner should set a GC hold since the element was too late and its timestamp
    // will be ignored for the purposes of the watermark hold
    assertThat(
        tester.getWatermarkHold(), equalTo(expectedWindow.maxTimestamp().plus(allowedLateness)));
  }

  @Test
  public void testPaneInfoAllStatesAfterWatermarkAccumulating() throws Exception {
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester = ReduceFnTester.nonCombining(
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)))
            .withTrigger(Repeatedly.forever(AfterFirst.of(
                AfterPane.elementCountAtLeast(2),
                AfterWatermark.pastEndOfWindow())))
            .withMode(AccumulationMode.ACCUMULATING_FIRED_PANES)
            .withAllowedLateness(Duration.millis(100))
            .withTimestampCombiner(TimestampCombiner.EARLIEST)
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
                Repeatedly.forever(AfterPane.elementCountAtLeast(2))
                    .orFinally(AfterWatermark.pastEndOfWindow()))
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
        ReduceFnTester.nonCombining(FixedWindows.of(Duration.millis(10)), mockTriggerStateMachine,
            AccumulationMode.DISCARDING_FIRED_PANES, Duration.millis(100),
            ClosingBehavior.FIRE_IF_NON_EMPTY);

    tester.advanceInputWatermark(new Instant(0));
    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    triggerShouldFinish(mockTriggerStateMachine);
    injectElement(tester, 1);
    assertThat(tester.extractOutput(), contains(
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(true, true, Timing.EARLY))));
  }

  @Test
  public void testPaneInfoSkipToNonSpeculativeAndFinish() throws Exception {
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(FixedWindows.of(Duration.millis(10)), mockTriggerStateMachine,
            AccumulationMode.DISCARDING_FIRED_PANES, Duration.millis(100),
            ClosingBehavior.FIRE_IF_NON_EMPTY);

    tester.advanceInputWatermark(new Instant(15));
    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    triggerShouldFinish(mockTriggerStateMachine);
    injectElement(tester, 1);
    assertThat(tester.extractOutput(), contains(
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(true, true, Timing.LATE))));
  }

  @Test
  public void testMergeBeforeFinalizing() throws Exception {
    // Verify that we merge windows before producing output so users don't see undesired
    // unmerged windows.
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(Sessions.withGapDuration(Duration.millis(10)),
            mockTriggerStateMachine,
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
        ReduceFnTester.nonCombining(Sessions.withGapDuration(Duration.millis(10)),
            mockTriggerStateMachine,
            AccumulationMode.DISCARDING_FIRED_PANES, Duration.millis(50),
            ClosingBehavior.FIRE_IF_NON_EMPTY);

    // Two elements in two overlapping session windows.
    tester.injectElements(
        TimestampedValue.of(1, new Instant(1)), // in [1, 11)
        TimestampedValue.of(10, new Instant(10))); // in [10, 20)

    // Close the trigger, but the gargbage collection timer is still pending.
    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    triggerShouldFinish(mockTriggerStateMachine);
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
   * Ensure a closed trigger has its state recorded in the merge result window.
   */
  @Test
  public void testMergingWithCloseTrigger() throws Exception {
    Duration allowedLateness = Duration.millis(50);
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(
            Sessions.withGapDuration(Duration.millis(10)),
            mockTriggerStateMachine,
            AccumulationMode.DISCARDING_FIRED_PANES,
            allowedLateness,
            ClosingBehavior.FIRE_IF_NON_EMPTY);

    // Create a new merged session window.
    IntervalWindow mergedWindow = new IntervalWindow(new Instant(1), new Instant(12));
    tester.injectElements(
        TimestampedValue.of(1, new Instant(1)), TimestampedValue.of(2, new Instant(2)));

    // Force the trigger to be closed for the merged window.
    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    triggerShouldFinish(mockTriggerStateMachine);

    // Fire and end-of-window timer as though the trigger set it
    tester.advanceInputWatermark(new Instant(13));
    tester.fireTimer(mergedWindow, mergedWindow.maxTimestamp(), TimeDomain.EVENT_TIME);

    // Trigger is now closed.
    assertTrue(tester.isMarkedFinished(mergedWindow));

    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(false);

    // Revisit the same session window.
    tester.injectElements(
        TimestampedValue.of(1, new Instant(1)), TimestampedValue.of(2, new Instant(2)));

    // Trigger is still closed.
    assertTrue(tester.isMarkedFinished(mergedWindow));
  }

  /**
   * If a later event tries to reuse an earlier session window which has been closed, we
   * should reject that element and not fail due to the window no longer being active.
   */
  @Test
  public void testMergingWithReusedWindow() throws Exception {
    Duration allowedLateness = Duration.millis(50);
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(
            Sessions.withGapDuration(Duration.millis(10)),
            mockTriggerStateMachine,
            AccumulationMode.DISCARDING_FIRED_PANES,
            allowedLateness,
            ClosingBehavior.FIRE_IF_NON_EMPTY);

    IntervalWindow mergedWindow = new IntervalWindow(new Instant(1), new Instant(11));

    // One elements in one session window.
    tester.injectElements(TimestampedValue.of(1, new Instant(1))); // in [1, 11), gc at 21.

    // Close the trigger, but the gargbage collection timer is still pending.
    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    triggerShouldFinish(mockTriggerStateMachine);
    tester.advanceInputWatermark(new Instant(15));
    tester.fireTimer(mergedWindow, mergedWindow.maxTimestamp(), TimeDomain.EVENT_TIME);

    // Another element in the same session window.
    // Should be discarded with 'window closed'.
    tester.injectElements(TimestampedValue.of(1, new Instant(1))); // in [1, 11), gc at 21.

    // And nothing should be left in the active window state.
    assertTrue(tester.hasNoActiveWindows());

    // Now the garbage collection timer will fire, finding the trigger already closed.
    tester.advanceInputWatermark(new Instant(100));

    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output.size(), equalTo(1));
    assertThat(
        output.get(0),
        isSingleWindowedValue(
            containsInAnyOrder(1),
            equalTo(new Instant(1)), // timestamp
            equalTo((BoundedWindow) mergedWindow)));

    assertThat(
        output.get(0).getPane(),
        equalTo(PaneInfo.createPane(true, true, Timing.ON_TIME, 0, 0)));
  }

  /**
   * When a merged window's trigger is closed we record that state using the merged window rather
   * than the original windows.
   */
  @Test
  public void testMergingWithClosedRepresentative() throws Exception {
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(Sessions.withGapDuration(Duration.millis(10)),
            mockTriggerStateMachine,
                                    AccumulationMode.DISCARDING_FIRED_PANES, Duration.millis(50),
                                    ClosingBehavior.FIRE_IF_NON_EMPTY);

    // 2 elements into merged session window.
    // Close the trigger, but the garbage collection timer is still pending.
    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    triggerShouldFinish(mockTriggerStateMachine);
    tester.injectElements(TimestampedValue.of(1, new Instant(1)),       // in [1, 11), gc at 21.
                          TimestampedValue.of(8, new Instant(8)));      // in [8, 18), gc at 28.

    // More elements into the same merged session window.
    // It has not yet been gced.
    // Should be discarded with 'window closed'.
    tester.injectElements(TimestampedValue.of(1, new Instant(1)),      // in [1, 11), gc at 21.
                          TimestampedValue.of(2, new Instant(2)),      // in [2, 12), gc at 22.
                          TimestampedValue.of(8, new Instant(8)));     // in [8, 18), gc at 28.

    // Now the garbage collection timer will fire, finding the trigger already closed.
    tester.advanceInputWatermark(new Instant(100));

    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();

    assertThat(output.size(), equalTo(1));
    assertThat(output.get(0),
               isSingleWindowedValue(containsInAnyOrder(1, 8),
                                     1, // timestamp
                                     1, // window start
                                     18)); // window end
    assertThat(
        output.get(0).getPane(),
        equalTo(PaneInfo.createPane(true, true, Timing.EARLY, 0, 0)));
  }

  /**
   * If an element for a closed session window ends up being merged into other still-open
   * session windows, the resulting session window is not 'poisoned'.
   */
  @Test
  public void testMergingWithClosedDoesNotPoison() throws Exception {
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(Sessions.withGapDuration(Duration.millis(10)),
            mockTriggerStateMachine,
            AccumulationMode.DISCARDING_FIRED_PANES, Duration.millis(50),
            ClosingBehavior.FIRE_IF_NON_EMPTY);

    // 1 element, force its trigger to close.
    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    triggerShouldFinish(mockTriggerStateMachine);
    tester.injectElements(TimestampedValue.of(2, new Instant(2)));

    // 3 elements, one already closed.
    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(false);
    tester.injectElements(TimestampedValue.of(1, new Instant(1)),
        TimestampedValue.of(2, new Instant(2)),
        TimestampedValue.of(3, new Instant(3)));

    tester.advanceInputWatermark(new Instant(100));

    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output.size(), equalTo(2));
    assertThat(output.get(0),
        isSingleWindowedValue(containsInAnyOrder(2),
            2, // timestamp
            2, // window start
            12)); // window end
    assertThat(
        output.get(0).getPane(),
        equalTo(PaneInfo.createPane(true, true, Timing.EARLY, 0, 0)));
    assertThat(output.get(1),
        isSingleWindowedValue(containsInAnyOrder(1, 2, 3),
            1, // timestamp
            1, // window start
            13)); // window end
    assertThat(
        output.get(1).getPane(),
        equalTo(PaneInfo.createPane(true, true, Timing.ON_TIME, 0, 0)));
  }

  /**
   * Tests that when data is assigned to multiple windows but some of those windows have
   * had their triggers finish, then the data is dropped and counted accurately.
   */
  @Test
  public void testDropDataMultipleWindowsFinishedTrigger() throws Exception {
    MetricsContainerImpl container = new MetricsContainerImpl("any");
    MetricsEnvironment.setCurrentContainer(container);
    ReduceFnTester<Integer, Integer, IntervalWindow> tester = ReduceFnTester.combining(
        WindowingStrategy.of(
            SlidingWindows.of(Duration.millis(100)).every(Duration.millis(30)))
        .withTrigger(AfterWatermark.pastEndOfWindow())
        .withAllowedLateness(Duration.millis(1000)),
        Sum.ofIntegers(), VarIntCoder.of());

    tester.injectElements(
        // assigned to [-60, 40), [-30, 70), [0, 100)
        TimestampedValue.of(10, new Instant(23)),
        // assigned to [-30, 70), [0, 100), [30, 130)
        TimestampedValue.of(12, new Instant(40)));

    long droppedElements =
        container
            .getCounter(
                MetricName.named(ReduceFnRunner.class, ReduceFnRunner.DROPPED_DUE_TO_CLOSED_WINDOW))
            .getCumulative();
    assertEquals(0, droppedElements);

    tester.advanceInputWatermark(new Instant(70));
    tester.injectElements(
        // assigned to [-30, 70), [0, 100), [30, 130)
        // but [-30, 70) is closed by the trigger
        TimestampedValue.of(14, new Instant(60)));

    droppedElements =
        container
            .getCounter(
                MetricName.named(ReduceFnRunner.class, ReduceFnRunner.DROPPED_DUE_TO_CLOSED_WINDOW))
            .getCumulative();
    assertEquals(1, droppedElements);

    tester.advanceInputWatermark(new Instant(130));
    // assigned to [-30, 70), [0, 100), [30, 130)
    // but they are all closed
    tester.injectElements(TimestampedValue.of(16, new Instant(40)));

    droppedElements =
        container
            .getCounter(
                MetricName.named(ReduceFnRunner.class, ReduceFnRunner.DROPPED_DUE_TO_CLOSED_WINDOW))
            .getCumulative();
    assertEquals(4, droppedElements);
  }

  @Test
  public void testIdempotentEmptyPanesDiscarding() throws Exception {
    MetricsContainerImpl container = new MetricsContainerImpl("any");
    MetricsEnvironment.setCurrentContainer(container);
    // Test uninteresting (empty) panes don't increment the index or otherwise
    // modify PaneInfo.
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(FixedWindows.of(Duration.millis(10)), mockTriggerStateMachine,
            AccumulationMode.DISCARDING_FIRED_PANES, Duration.millis(100),
            ClosingBehavior.FIRE_IF_NON_EMPTY);

    // Inject a couple of on-time elements and fire at the window end.
    injectElement(tester, 1);
    injectElement(tester, 2);
    tester.advanceInputWatermark(new Instant(12));

    // Fire the on-time pane
    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    tester.fireTimer(firstWindow, new Instant(9), TimeDomain.EVENT_TIME);

    // Fire another timer (with no data, so it's an uninteresting pane that should not be output).
    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    tester.fireTimer(firstWindow, new Instant(9), TimeDomain.EVENT_TIME);

    // Finish it off with another datum.
    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    triggerShouldFinish(mockTriggerStateMachine);
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

    long droppedElements =
        container
            .getCounter(
                MetricName.named(ReduceFnRunner.class, ReduceFnRunner.DROPPED_DUE_TO_CLOSED_WINDOW))
            .getCumulative();
    assertEquals(0, droppedElements);
  }

  @Test
  public void testIdempotentEmptyPanesAccumulating() throws Exception {
    MetricsContainerImpl container = new MetricsContainerImpl("any");
    MetricsEnvironment.setCurrentContainer(container);
    // Test uninteresting (empty) panes don't increment the index or otherwise
    // modify PaneInfo.
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(FixedWindows.of(Duration.millis(10)), mockTriggerStateMachine,
            AccumulationMode.ACCUMULATING_FIRED_PANES, Duration.millis(100),
            ClosingBehavior.FIRE_IF_NON_EMPTY);

    // Inject a couple of on-time elements and fire at the window end.
    injectElement(tester, 1);
    injectElement(tester, 2);
    tester.advanceInputWatermark(new Instant(12));

    // Trigger the on-time pane
    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    tester.fireTimer(firstWindow, new Instant(9), TimeDomain.EVENT_TIME);
    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertThat(output.size(), equalTo(1));
    assertThat(output.get(0), isSingleWindowedValue(containsInAnyOrder(1, 2), 1, 0, 10));
    assertThat(output.get(0).getPane(),
        equalTo(PaneInfo.createPane(true, false, Timing.ON_TIME, 0, 0)));

    // Fire another timer with no data; the empty pane should not be output even though the
    // trigger is ready to fire
    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    tester.fireTimer(firstWindow, new Instant(9), TimeDomain.EVENT_TIME);
    assertThat(tester.extractOutput().size(), equalTo(0));

    // Finish it off with another datum, which is late
    when(mockTriggerStateMachine.shouldFire(anyTriggerContext())).thenReturn(true);
    triggerShouldFinish(mockTriggerStateMachine);
    injectElement(tester, 3);
    output = tester.extractOutput();
    assertThat(output.size(), equalTo(1));

    // The late pane has the correct indices.
    assertThat(output.get(0).getValue(), containsInAnyOrder(1, 2, 3));
    assertThat(output.get(0).getPane(),
        equalTo(PaneInfo.createPane(false, true, Timing.LATE, 1, 1)));

    assertTrue(tester.isMarkedFinished(firstWindow));
    tester.assertHasOnlyGlobalAndFinishedSetsFor(firstWindow);

    long droppedElements =
        container
            .getCounter(
                MetricName.named(ReduceFnRunner.class, ReduceFnRunner.DROPPED_DUE_TO_CLOSED_WINDOW))
            .getCumulative();
    assertEquals(0, droppedElements);
  }

  /**
   * Test that we receive an empty on-time pane when an or-finally waiting for the watermark fires.
   * Specifically, verify the proper triggerings and pane-info of a typical speculative/on-time/late
   * when the on-time pane is empty.
   */
  @Test
  public void testEmptyOnTimeFromOrFinally() throws Exception {

    WindowingStrategy<?, IntervalWindow> strategy =
        WindowingStrategy.of((WindowFn<?, IntervalWindow>) FixedWindows.of(Duration.millis(10)))
            .withTimestampCombiner(TimestampCombiner.EARLIEST)
            .withTrigger(
                AfterEach.<IntervalWindow>inOrder(
                    Repeatedly.forever(
                            AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(new Duration(5)))
                        .orFinally(AfterWatermark.pastEndOfWindow()),
                    Repeatedly.forever(
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(new Duration(25)))))
            .withMode(AccumulationMode.ACCUMULATING_FIRED_PANES)
            .withAllowedLateness(Duration.millis(100));

    ReduceFnTester<Integer, Integer, IntervalWindow> tester =
        ReduceFnTester.combining(strategy, Sum.ofIntegers(), VarIntCoder.of());

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
   * Test that it won't fire an empty on-time pane when OnTimeBehavior is FIRE_IF_NON_EMPTY.
   */
  @Test
  public void testEmptyOnTimeWithOnTimeBehaviorFireIfNonEmpty() throws Exception {

    WindowingStrategy<?, IntervalWindow> strategy =
        WindowingStrategy.of((WindowFn<?, IntervalWindow>) FixedWindows.of(Duration.millis(10)))
            .withTimestampCombiner(TimestampCombiner.EARLIEST)
            .withTrigger(
                AfterEach.<IntervalWindow>inOrder(
                    Repeatedly.forever(
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(new Duration(5)))
                        .orFinally(AfterWatermark.pastEndOfWindow()),
                    Repeatedly.forever(
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(new Duration(25)))))
            .withMode(AccumulationMode.ACCUMULATING_FIRED_PANES)
            .withAllowedLateness(Duration.millis(100))
            .withClosingBehavior(ClosingBehavior.FIRE_ALWAYS)
            .withOnTimeBehavior(Window.OnTimeBehavior.FIRE_IF_NON_EMPTY);

    ReduceFnTester<Integer, Integer, IntervalWindow> tester =
        ReduceFnTester.combining(strategy, Sum.ofIntegers(), VarIntCoder.of());

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

    // Should not fire empty on time pane
    tester.advanceInputWatermark(new Instant(11));

    // Should fire final GC pane
    tester.advanceInputWatermark(new Instant(10 + 100));
    List<WindowedValue<Integer>> output = tester.extractOutput();
    assertEquals(2, output.size());

    assertThat(output.get(0), WindowMatchers.isSingleWindowedValue(4, 1, 0, 10));
    assertThat(output.get(1), WindowMatchers.isSingleWindowedValue(4, 9, 0, 10));

    assertThat(
        output.get(0),
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(true, false, Timing.EARLY, 0, -1)));
    assertThat(
        output.get(1),
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, true, Timing.LATE, 1, 0)));
  }

  /**
   * Test that it fires an empty on-time isFinished pane when OnTimeBehavior is FIRE_ALWAYS
   * and ClosingBehavior is FIRE_IF_NON_EMPTY.
   *
   * <p>This is a test just for backward compatibility.
   */
  @Test
  public void testEmptyOnTimeWithOnTimeBehaviorBackwardCompatibility() throws Exception {

    WindowingStrategy<?, IntervalWindow> strategy =
        WindowingStrategy.of((WindowFn<?, IntervalWindow>) FixedWindows.of(Duration.millis(10)))
            .withTimestampCombiner(TimestampCombiner.EARLIEST)
            .withTrigger(AfterWatermark.pastEndOfWindow()
                .withEarlyFirings(AfterPane.elementCountAtLeast(1)))
            .withMode(AccumulationMode.ACCUMULATING_FIRED_PANES)
            .withAllowedLateness(Duration.millis(0))
            .withClosingBehavior(ClosingBehavior.FIRE_IF_NON_EMPTY);

    ReduceFnTester<Integer, Integer, IntervalWindow> tester =
        ReduceFnTester.combining(strategy, Sum.ofIntegers(), VarIntCoder.of());

    tester.advanceInputWatermark(new Instant(0));
    tester.advanceProcessingTime(new Instant(0));

    tester.injectElements(
        TimestampedValue.of(1, new Instant(1)));

    // Should fire empty on time isFinished pane
    tester.advanceInputWatermark(new Instant(11));

    List<WindowedValue<Integer>> output = tester.extractOutput();
    assertEquals(2, output.size());

    assertThat(
        output.get(0),
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(true, false, Timing.EARLY, 0, -1)));
    assertThat(
        output.get(1),
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, true, Timing.ON_TIME, 1, 0)));
  }

  /**
   * Test that it won't fire an empty on-time pane when OnTimeBehavior is FIRE_IF_NON_EMPTY
   * and when receiving late data.
   */
  @Test
  public void testEmptyOnTimeWithOnTimeBehaviorFireIfNonEmptyAndLateData() throws Exception {

    WindowingStrategy<?, IntervalWindow> strategy =
        WindowingStrategy.of((WindowFn<?, IntervalWindow>) FixedWindows.of(Duration.millis(10)))
            .withTimestampCombiner(TimestampCombiner.EARLIEST)
            .withTrigger(
                AfterEach.<IntervalWindow>inOrder(
                    Repeatedly.forever(
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(new Duration(5)))
                        .orFinally(AfterWatermark.pastEndOfWindow()),
                    Repeatedly.forever(
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(new Duration(25)))))
            .withMode(AccumulationMode.ACCUMULATING_FIRED_PANES)
            .withAllowedLateness(Duration.millis(100))
            .withOnTimeBehavior(Window.OnTimeBehavior.FIRE_IF_NON_EMPTY);

    ReduceFnTester<Integer, Integer, IntervalWindow> tester =
        ReduceFnTester.combining(strategy, Sum.ofIntegers(), VarIntCoder.of());

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

    // Should not fire empty on time pane
    tester.advanceInputWatermark(new Instant(11));

    // Processing late data, and should fire late pane
    tester.injectElements(
        TimestampedValue.of(1, new Instant(9)));
    tester.advanceProcessingTime(new Instant(6 + 25 + 1));

    List<WindowedValue<Integer>> output = tester.extractOutput();
    assertEquals(2, output.size());

    assertThat(output.get(0), WindowMatchers.isSingleWindowedValue(4, 1, 0, 10));
    assertThat(output.get(1), WindowMatchers.isSingleWindowedValue(5, 9, 0, 10));

    assertThat(
        output.get(0),
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(true, false, Timing.EARLY, 0, -1)));
    assertThat(
        output.get(1),
        WindowMatchers.valueWithPaneInfo(PaneInfo.createPane(false, false, Timing.LATE, 1, 0)));
  }

  /**
   * Tests for processing time firings after the watermark passes the end of the window.
   * Specifically, verify the proper triggerings and pane-info of a typical speculative/on-time/late
   * when the on-time pane is non-empty.
   */
  @Test
  public void testProcessingTime() throws Exception {

    WindowingStrategy<?, IntervalWindow> strategy =
        WindowingStrategy.of((WindowFn<?, IntervalWindow>) FixedWindows.of(Duration.millis(10)))
            .withTimestampCombiner(TimestampCombiner.EARLIEST)
            .withTrigger(
                AfterEach.<IntervalWindow>inOrder(
                    Repeatedly.forever(
                            AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(new Duration(5)))
                        .orFinally(AfterWatermark.pastEndOfWindow()),
                    Repeatedly.forever(
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(new Duration(25)))))
            .withMode(AccumulationMode.ACCUMULATING_FIRED_PANES)
            .withAllowedLateness(Duration.millis(100));

    ReduceFnTester<Integer, Integer, IntervalWindow> tester =
        ReduceFnTester.combining(strategy, Sum.ofIntegers(), VarIntCoder.of());

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

  /**
   * We should fire a non-empty ON_TIME pane in the GlobalWindow when the watermark moves to
   * end-of-time.
   */
  @Test
  public void fireNonEmptyOnDrainInGlobalWindow() throws Exception {
    ReduceFnTester<Integer, Iterable<Integer>, GlobalWindow> tester =
        ReduceFnTester.nonCombining(
            WindowingStrategy.of(new GlobalWindows())
                             .withTrigger(Repeatedly.<GlobalWindow>forever(
                                 AfterPane.elementCountAtLeast(3)))
                             .withMode(AccumulationMode.DISCARDING_FIRED_PANES));

    tester.advanceInputWatermark(new Instant(0));

    final int n = 20;
    for (int i = 0; i < n; i++) {
      tester.injectElements(TimestampedValue.of(i, new Instant(i)));
    }

    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertEquals(n / 3, output.size());
    for (int i = 0; i < output.size(); i++) {
      assertEquals(Timing.EARLY, output.get(i).getPane().getTiming());
      assertEquals(i, output.get(i).getPane().getIndex());
      assertEquals(3, Iterables.size(output.get(i).getValue()));
    }

    tester.advanceInputWatermark(BoundedWindow.TIMESTAMP_MAX_VALUE);

    output = tester.extractOutput();
    assertEquals(1, output.size());
    assertEquals(Timing.ON_TIME, output.get(0).getPane().getTiming());
    assertEquals(n / 3, output.get(0).getPane().getIndex());
    assertEquals(n - ((n / 3) * 3), Iterables.size(output.get(0).getValue()));
  }

  /**
   * We should fire an empty ON_TIME pane in the GlobalWindow when the watermark moves to
   * end-of-time.
   */
  @Test
  public void fireEmptyOnDrainInGlobalWindowIfRequested() throws Exception {
    ReduceFnTester<Integer, Iterable<Integer>, GlobalWindow> tester =
        ReduceFnTester.nonCombining(
            WindowingStrategy.of(new GlobalWindows())
                             .withTrigger(Repeatedly.<GlobalWindow>forever(
                                 AfterProcessingTime.pastFirstElementInPane().plusDelayOf(
                                     new Duration(3))))
                             .withMode(AccumulationMode.DISCARDING_FIRED_PANES));

    final int n = 20;
    for (int i = 0; i < n; i++) {
      tester.advanceProcessingTime(new Instant(i));
      tester.injectElements(TimestampedValue.of(i, new Instant(i)));
    }
    tester.advanceProcessingTime(new Instant(n + 4));
    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertEquals((n + 3) / 4, output.size());
    for (int i = 0; i < output.size(); i++) {
      assertEquals(Timing.EARLY, output.get(i).getPane().getTiming());
      assertEquals(i, output.get(i).getPane().getIndex());
      assertEquals(4, Iterables.size(output.get(i).getValue()));
    }

    tester.advanceInputWatermark(BoundedWindow.TIMESTAMP_MAX_VALUE);

    output = tester.extractOutput();
    assertEquals(1, output.size());
    assertEquals(Timing.ON_TIME, output.get(0).getPane().getTiming());
    assertEquals((n + 3) / 4, output.get(0).getPane().getIndex());
    assertEquals(0, Iterables.size(output.get(0).getValue()));
  }

  /**
   * Late elements should still have a garbage collection hold set so that they
   * can make a late pane rather than be dropped due to lateness.
   */
  @Test
  public void setGarbageCollectionHoldOnLateElements() throws Exception {
    ReduceFnTester<Integer, Iterable<Integer>, IntervalWindow> tester =
        ReduceFnTester.nonCombining(
            WindowingStrategy.of(FixedWindows.of(Duration.millis(10)))
                .withTrigger(
                    AfterWatermark.pastEndOfWindow()
                        .withLateFirings(AfterPane.elementCountAtLeast(2)))
                .withMode(AccumulationMode.DISCARDING_FIRED_PANES)
                .withAllowedLateness(Duration.millis(100))
                .withClosingBehavior(ClosingBehavior.FIRE_IF_NON_EMPTY));

    tester.advanceInputWatermark(new Instant(0));
    tester.advanceOutputWatermark(new Instant(0));
    tester.injectElements(TimestampedValue.of(1,  new Instant(1)));

    // Fire ON_TIME pane @ 9 with 1

    tester.advanceInputWatermark(new Instant(109));
    tester.advanceOutputWatermark(new Instant(109));
    tester.injectElements(TimestampedValue.of(2,  new Instant(2)));
    // We should have set a garbage collection hold for the final pane.
    Instant hold = tester.getWatermarkHold();
    assertEquals(new Instant(109), hold);

    tester.advanceInputWatermark(new Instant(110));
    tester.advanceOutputWatermark(new Instant(110));

    // Fire final LATE pane @ 9 with 2

    List<WindowedValue<Iterable<Integer>>> output = tester.extractOutput();
    assertEquals(2, output.size());
  }

  private static class SumAndVerifyContextFn
      extends CombineFnWithContext<Integer, Integer, Integer> {

    private final PCollectionView<Integer> view;
    private final int expectedValue;

    private SumAndVerifyContextFn(PCollectionView<Integer> view, int expectedValue) {
      this.view = view;
      this.expectedValue = expectedValue;
    }

    private void verifyContext(Context c) {
      assertThat(expectedValue, equalTo(c.getPipelineOptions().as(TestOptions.class).getValue()));
      assertThat(c.sideInput(view), greaterThanOrEqualTo(100));
    }

    @Override
    public Integer createAccumulator(Context c) {
      verifyContext(c);
      return 0;
    }

    @Override
    public Integer addInput(Integer accumulator, Integer input, Context c) {
      verifyContext(c);
      return accumulator + input;
    }

    @Override
    public Integer mergeAccumulators(Iterable<Integer> accumulators, Context c) {
      verifyContext(c);
      int res = 0;
      for (Integer accum : accumulators) {
        res += accum;
      }
      return res;
    }

    @Override
    public Integer extractOutput(Integer accumulator, Context c) {
      verifyContext(c);
      return accumulator + c.sideInput(view);
    }
  }

  /**
   * A {@link PipelineOptions} to test combining with context.
   */
  public interface TestOptions extends PipelineOptions {
    int getValue();
    void setValue(int value);
  }
}
