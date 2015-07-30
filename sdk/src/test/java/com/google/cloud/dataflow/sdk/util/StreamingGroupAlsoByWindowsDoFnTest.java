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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.coders.BigEndianLongCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.transforms.windowing.Sessions;
import com.google.cloud.dataflow.sdk.transforms.windowing.SlidingWindows;
import com.google.cloud.dataflow.sdk.util.TimerInternals.TimerData;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.state.StateNamespace;
import com.google.cloud.dataflow.sdk.util.state.StateNamespaces;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TupleTag;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Unit tests for {@link StreamingGroupAlsoByWindowsDoFn}. */
@RunWith(JUnit4.class)
public class StreamingGroupAlsoByWindowsDoFnTest {
  private ExecutionContext execContext;
  private CounterSet counters;

  @Mock
  private TimerInternals mockTimerInternals;

  @Before public void setUp() {
    MockitoAnnotations.initMocks(this);
    execContext = new DirectModeExecutionContext() {
      // Normally timerInternals doesn't come from the execution context, but
      // StreamingGroupAlsoByWindows expects it to. So, hook that up.

      @Override
      public ExecutionContext.StepContext createStepContext(String stepName, String transformName) {
        ExecutionContext.StepContext context =
            Mockito.spy(super.createStepContext(stepName, transformName));
        Mockito.doReturn(mockTimerInternals).when(context).timerInternals();
        return context;
      }
    };
    counters = new CounterSet();
  }

  @Test public void testEmpty() throws Exception {
    TupleTag<KV<String, Iterable<String>>> outputTag = new TupleTag<>();
    DoFnRunner.ListOutputManager outputManager = new DoFnRunner.ListOutputManager();
    DoFnRunner<TimerOrElement<KV<String, String>>, KV<String, Iterable<String>>> runner =
        makeRunner(
            outputTag, outputManager, WindowingStrategy.of(FixedWindows.of(Duration.millis(10))));

    runner.startBundle();

    runner.finishBundle();

    List<?> result = outputManager.getOutput(outputTag);

    assertEquals(0, result.size());
  }

  private <W extends BoundedWindow, V> TimerOrElement<KV<String, V>> timer(
      Coder<W> windowCoder, W window, Instant timestamp, TimeDomain domain) {
    StateNamespace namespace = StateNamespaces.window(windowCoder, window);
    return TimerOrElement.<KV<String, V>>timer("k", TimerData.of(namespace, timestamp, domain));
  }

  @Test public void testFixedWindows() throws Exception {
    TupleTag<KV<String, Iterable<String>>> outputTag = new TupleTag<>();
    DoFnRunner.ListOutputManager outputManager = new DoFnRunner.ListOutputManager();
    DoFnRunner<TimerOrElement<KV<String, String>>, KV<String, Iterable<String>>> runner =
        makeRunner(
            outputTag, outputManager, WindowingStrategy.of(FixedWindows.of(Duration.millis(10))));

    Coder<IntervalWindow> windowCoder = FixedWindows.of(Duration.millis(10)).windowCoder();

    runner.startBundle();
    when(mockTimerInternals.currentWatermarkTime()).thenReturn(new Instant(0));

    runner.processElement(WindowedValue.of(
        TimerOrElement.element(KV.of("k", "v1")),
        new Instant(1),
        Arrays.asList(window(0, 10)),
        PaneInfo.NO_FIRING));

    runner.processElement(WindowedValue.of(
        TimerOrElement.element(KV.of("k", "v2")),
        new Instant(2),
        Arrays.asList(window(0, 10)),
        PaneInfo.NO_FIRING));

    runner.processElement(WindowedValue.of(
        TimerOrElement.element(KV.of("k", "v0")),
        new Instant(0),
        Arrays.asList(window(0, 10)),
        PaneInfo.NO_FIRING));

    runner.processElement(WindowedValue.of(
        TimerOrElement.element(KV.of("k", "v3")),
        new Instant(13),
        Arrays.asList(window(10, 20)),
        PaneInfo.NO_FIRING));

    runner.processElement(WindowedValue.valueInEmptyWindows(this.<IntervalWindow, String>timer(
            windowCoder, window(0, 10), new Instant(9), TimeDomain.EVENT_TIME)));

    runner.processElement(WindowedValue.valueInEmptyWindows(this.<IntervalWindow, String>timer(
        windowCoder, window(10, 20), new Instant(19), TimeDomain.EVENT_TIME)));

    runner.finishBundle();

    List<WindowedValue<KV<String, Iterable<String>>>> result = outputManager.getOutput(outputTag);

    assertEquals(2, result.size());

    WindowedValue<KV<String, Iterable<String>>> item0 = result.get(0);
    assertEquals("k", item0.getValue().getKey());
    assertThat(item0.getValue().getValue(), Matchers.containsInAnyOrder("v0", "v1", "v2"));
    assertEquals(new Instant(0), item0.getTimestamp());
    assertThat(item0.getWindows(), Matchers.<BoundedWindow>contains(window(0, 10)));

    WindowedValue<KV<String, Iterable<String>>> item1 = result.get(1);
    assertEquals("k", item1.getValue().getKey());
    assertThat(item1.getValue().getValue(), Matchers.containsInAnyOrder("v3"));
    assertEquals(new Instant(13), item1.getTimestamp());
    assertThat(item1.getWindows(), Matchers.<BoundedWindow>contains(window(10, 20)));
  }

  @Test public void testSlidingWindows() throws Exception {
    TupleTag<KV<String, Iterable<String>>> outputTag = new TupleTag<>();
    DoFnRunner.ListOutputManager outputManager = new DoFnRunner.ListOutputManager();
    DoFnRunner<TimerOrElement<KV<String, String>>, KV<String, Iterable<String>>> runner =
        makeRunner(
            outputTag,
            outputManager,
            WindowingStrategy.of(
            SlidingWindows.of(Duration.millis(20)).every(Duration.millis(10))));

    Coder<IntervalWindow> windowCoder =
        SlidingWindows.of(Duration.millis(10)).every(Duration.millis(10)).windowCoder();

    runner.startBundle();
    when(mockTimerInternals.currentWatermarkTime()).thenReturn(new Instant(0));

    runner.processElement(WindowedValue.of(
        TimerOrElement.element(KV.of("k", "v1")),
        new Instant(5),
        Arrays.asList(window(-10, 10), window(0, 20)),
        PaneInfo.NO_FIRING));

    runner.processElement(WindowedValue.of(
        TimerOrElement.element(KV.of("k", "v0")),
        new Instant(2),
        Arrays.asList(window(-10, 10), window(0, 20)),
        PaneInfo.NO_FIRING));

    runner.processElement(WindowedValue.valueInEmptyWindows(this.<IntervalWindow, String>timer(
        windowCoder, window(-10, 10), new Instant(9), TimeDomain.EVENT_TIME)));

    runner.processElement(WindowedValue.of(
        TimerOrElement.element(KV.of("k", "v2")),
        new Instant(5),
        Arrays.asList(window(0, 20), window(10, 30)),
        PaneInfo.NO_FIRING));

    runner.processElement(WindowedValue.valueInEmptyWindows(this.<IntervalWindow, String>timer(
        windowCoder, window(0, 20), new Instant(19), TimeDomain.EVENT_TIME)));
    runner.processElement(WindowedValue.valueInEmptyWindows(this.<IntervalWindow, String>timer(
        windowCoder, window(10, 30), new Instant(29), TimeDomain.EVENT_TIME)));

    runner.finishBundle();

    List<WindowedValue<KV<String, Iterable<String>>>> result = outputManager.getOutput(outputTag);

    assertEquals(3, result.size());

    WindowedValue<KV<String, Iterable<String>>> item0 = result.get(0);
    assertEquals("k", item0.getValue().getKey());
    assertThat(item0.getValue().getValue(), Matchers.containsInAnyOrder("v0", "v1"));
    assertEquals(new Instant(2), item0.getTimestamp());
    assertThat(item0.getWindows(), Matchers.<BoundedWindow>contains(window(-10, 10)));

    WindowedValue<KV<String, Iterable<String>>> item1 = result.get(1);
    assertEquals("k", item1.getValue().getKey());
    assertThat(item1.getValue().getValue(), Matchers.containsInAnyOrder("v0", "v1", "v2"));
    // For this sliding window, the minimum output timestmap was 10, since we didn't want to overlap
    // with the previous window that was [-10, 10).
    assertEquals(new Instant(10), item1.getTimestamp());
    assertThat(item1.getWindows(), Matchers.<BoundedWindow>contains(window(0, 20)));

    WindowedValue<KV<String, Iterable<String>>> item2 = result.get(2);
    assertEquals("k", item2.getValue().getKey());
    assertThat(item2.getValue().getValue(), Matchers.containsInAnyOrder("v2"));
    assertEquals(new Instant(20), item2.getTimestamp());
    assertThat(item2.getWindows(), Matchers.<BoundedWindow>contains(window(10, 30)));
  }

  @Test public void testSessions() throws Exception {
    TupleTag<KV<String, Iterable<String>>> outputTag = new TupleTag<>();
    DoFnRunner.ListOutputManager outputManager = new DoFnRunner.ListOutputManager();
    DoFnRunner<TimerOrElement<KV<String, String>>, KV<String, Iterable<String>>> runner =
        makeRunner(
            outputTag,
            outputManager,
            WindowingStrategy.of(Sessions.withGapDuration(Duration.millis(10))));

    Coder<IntervalWindow> windowCoder =
        Sessions.withGapDuration(Duration.millis(10)).windowCoder();
    runner.startBundle();
    when(mockTimerInternals.currentWatermarkTime()).thenReturn(new Instant(0));

    runner.processElement(WindowedValue.of(
        TimerOrElement.element(KV.of("k", "v1")),
        new Instant(0),
        Arrays.asList(window(0, 10)),
        PaneInfo.NO_FIRING));

    runner.processElement(WindowedValue.of(
        TimerOrElement.element(KV.of("k", "v2")),
        new Instant(5),
        Arrays.asList(window(5, 15)),
        PaneInfo.NO_FIRING));

    runner.processElement(WindowedValue.of(
        TimerOrElement.element(KV.of("k", "v3")),
        new Instant(15),
        Arrays.asList(window(15, 25)),
        PaneInfo.NO_FIRING));

    runner.processElement(WindowedValue.of(
        TimerOrElement.element(KV.of("k", "v0")),
        new Instant(3),
        Arrays.asList(window(3, 13)),
        PaneInfo.NO_FIRING));

    runner.processElement(WindowedValue.valueInEmptyWindows(this.<IntervalWindow, String>timer(
        windowCoder, window(0, 10), new Instant(9), TimeDomain.EVENT_TIME)));
    runner.processElement(WindowedValue.valueInEmptyWindows(this.<IntervalWindow, String>timer(
        windowCoder, window(0, 15), new Instant(14), TimeDomain.EVENT_TIME)));
    runner.processElement(WindowedValue.valueInEmptyWindows(this.<IntervalWindow, String>timer(
        windowCoder, window(15, 25), new Instant(24), TimeDomain.EVENT_TIME)));

    runner.finishBundle();

    List<WindowedValue<KV<String, Iterable<String>>>> result = outputManager.getOutput(outputTag);

    assertEquals(2, result.size());

    WindowedValue<KV<String, Iterable<String>>> item0 = result.get(0);
    assertEquals("k", item0.getValue().getKey());
    assertThat(item0.getValue().getValue(), Matchers.containsInAnyOrder("v0", "v1", "v2"));
    assertEquals(new Instant(0), item0.getTimestamp());
    assertThat(item0.getWindows(), Matchers.<BoundedWindow>contains(window(0, 15)));

    WindowedValue<KV<String, Iterable<String>>> item1 = result.get(1);
    assertEquals("k", item1.getValue().getKey());
    assertThat(item1.getValue().getValue(), Matchers.containsInAnyOrder("v3"));
    assertEquals(new Instant(15), item1.getTimestamp());
    assertThat(item1.getWindows(), Matchers.<BoundedWindow>contains(window(15, 25)));
  }

  /**
   * A custom combine fn that doesn't take any performace shortcuts
   * to ensure that we are using the CombineFn API properly.
   */
  private static class SumLongs extends CombineFn<Long, Long, Long> {
    private static final long serialVersionUID = 0L;

    @Override
    public Long createAccumulator() {
      return 0L;
    }

    @Override
    public Long addInput(Long accumulator, Long input) {
      return accumulator + input;
    }

    @Override
    public Long mergeAccumulators(Iterable<Long> accumulators) {
      Long sum = 0L;
      for (Long value : accumulators) {
        sum += value;
      }
      return sum;
    }

    @Override
    public Long extractOutput(Long accumulator) {
      return new Long(accumulator);
    }
  }

  @Test public void testSessionsCombine() throws Exception {
    TupleTag<KV<String, Long>> outputTag = new TupleTag<>();
    CombineFn<Long, ?, Long> combineFn = new SumLongs();
    CoderRegistry registry = new CoderRegistry();
    registry.registerStandardCoders();

    AppliedCombineFn<String, Long, ?, Long> appliedCombineFn = AppliedCombineFn.withInputCoder(
        combineFn.asKeyedFn(), registry, KvCoder.of(StringUtf8Coder.of(), BigEndianLongCoder.of()));

    DoFnRunner.ListOutputManager outputManager = new DoFnRunner.ListOutputManager();
    DoFnRunner<TimerOrElement<KV<String, Long>>, KV<String, Long>> runner =
        makeRunner(
            outputTag,
            outputManager,
            WindowingStrategy.of(Sessions.withGapDuration(Duration.millis(10))),
            appliedCombineFn);

    Coder<IntervalWindow> windowCoder =
        Sessions.withGapDuration(Duration.millis(10)).windowCoder();

    runner.startBundle();
    when(mockTimerInternals.currentWatermarkTime()).thenReturn(new Instant(0));

    runner.processElement(WindowedValue.of(
        TimerOrElement.element(KV.of("k", 1L)),
        new Instant(0),
        Arrays.asList(window(0, 10)),
        PaneInfo.NO_FIRING));

    runner.processElement(WindowedValue.of(
        TimerOrElement.element(KV.of("k", 2L)),
        new Instant(5),
        Arrays.asList(window(5, 15)),
        PaneInfo.NO_FIRING));

    runner.processElement(WindowedValue.of(
        TimerOrElement.element(KV.of("k", 3L)),
        new Instant(15),
        Arrays.asList(window(15, 25)),
        PaneInfo.NO_FIRING));

    runner.processElement(WindowedValue.of(
        TimerOrElement.element(KV.of("k", 4L)),
        new Instant(3),
        Arrays.asList(window(3, 13)),
        PaneInfo.NO_FIRING));

    runner.processElement(WindowedValue.valueInEmptyWindows(this.<IntervalWindow, Long>timer(
        windowCoder, window(0, 10), new Instant(9), TimeDomain.EVENT_TIME)));
    runner.processElement(WindowedValue.valueInEmptyWindows(this.<IntervalWindow, Long>timer(
        windowCoder, window(0, 15), new Instant(14), TimeDomain.EVENT_TIME)));
    runner.processElement(WindowedValue.valueInEmptyWindows(this.<IntervalWindow, Long>timer(
        windowCoder, window(15, 25), new Instant(24), TimeDomain.EVENT_TIME)));

    runner.finishBundle();

    List<WindowedValue<KV<String, Long>>> result = outputManager.getOutput(outputTag);

    assertEquals(2, result.size());

    WindowedValue<KV<String, Long>> item0 = result.get(0);
    assertEquals("k", item0.getValue().getKey());
    assertEquals((Long) 7L, item0.getValue().getValue());
    assertEquals(new Instant(0), item0.getTimestamp());
    assertThat(item0.getWindows(), Matchers.<BoundedWindow>contains(window(0, 15)));

    WindowedValue<KV<String, Long>> item1 = result.get(1);
    assertEquals("k", item1.getValue().getKey());
    assertEquals((Long) 3L, item1.getValue().getValue());
    assertEquals(new Instant(15), item1.getTimestamp());
    assertThat(item1.getWindows(), Matchers.<BoundedWindow>contains(window(15, 25)));
  }

  private DoFnRunner<TimerOrElement<KV<String, String>>, KV<String, Iterable<String>>> makeRunner(
          TupleTag<KV<String, Iterable<String>>> outputTag,
          DoFnRunner.OutputManager outputManager,
          WindowingStrategy<? super String, IntervalWindow> windowingStrategy) {

    StreamingGroupAlsoByWindowsDoFn<String, String, Iterable<String>, IntervalWindow> fn =
        StreamingGroupAlsoByWindowsDoFn.createForIterable(windowingStrategy, StringUtf8Coder.of());

    return makeRunner(outputTag, outputManager, windowingStrategy, fn);
  }

  private DoFnRunner<TimerOrElement<KV<String, Long>>, KV<String, Long>> makeRunner(
          TupleTag<KV<String, Long>> outputTag,
          DoFnRunner.OutputManager outputManager,
          WindowingStrategy<? super String, IntervalWindow> windowingStrategy,
          AppliedCombineFn<String, Long, ?, Long> combineFn) {

    StreamingGroupAlsoByWindowsDoFn<String, Long, Long, IntervalWindow> fn =
        StreamingGroupAlsoByWindowsDoFn.create(windowingStrategy, combineFn, StringUtf8Coder.of());

    return makeRunner(outputTag, outputManager, windowingStrategy, fn);
  }

  private <InputT, OutputT>
      DoFnRunner<TimerOrElement<KV<String, InputT>>, KV<String, OutputT>> makeRunner(
          TupleTag<KV<String, OutputT>> outputTag,
          DoFnRunner.OutputManager outputManager,
          WindowingStrategy<? super String, IntervalWindow> windowingStrategy,
          StreamingGroupAlsoByWindowsDoFn<String, InputT, OutputT, IntervalWindow> fn) {
    return
        DoFnRunner.create(
            PipelineOptionsFactory.create(),
            fn,
            NullSideInputReader.empty(),
            outputManager,
            outputTag,
            new ArrayList<TupleTag<?>>(),
            execContext.getStepContext("merge", "merge"),
            counters.getAddCounterMutator(),
            windowingStrategy);
  }

  private IntervalWindow window(long start, long end) {
    return new IntervalWindow(new Instant(start), new Instant(end));
  }
}
