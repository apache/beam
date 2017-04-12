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

import static com.google.common.base.Preconditions.checkArgument;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.common.base.MoreObjects;
import java.util.Collections;
import org.apache.beam.runners.core.BaseExecutionContext.StepContext;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.NullSideInputReader;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.state.StateSpec;
import org.apache.beam.sdk.util.state.StateSpecs;
import org.apache.beam.sdk.util.state.ValueState;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link StatefulDoFnRunnerTest}. */
@RunWith(JUnit4.class)
public class StatefulDoFnRunnerTest {

  private static final long WINDOW_SIZE = 10;
  private static final long ALLOWED_LATENESS = 1;

  private static final WindowingStrategy<?, ?> WINDOWING_STRATEGY =
      WindowingStrategy
          .of(FixedWindows.of(Duration.millis(WINDOW_SIZE)))
          .withAllowedLateness(Duration.millis(ALLOWED_LATENESS));

  private static final IntervalWindow WINDOW_1 =
      new IntervalWindow(new Instant(0), new Instant(10));

  private static final IntervalWindow WINDOW_2 =
      new IntervalWindow(new Instant(10), new Instant(20));

  @Mock StepContext mockStepContext;

  private InMemoryLongSumAggregator droppedDueToLateness;
  private AggregatorFactory aggregatorFactory;
  private InMemoryStateInternals<String> stateInternals;
  private InMemoryTimerInternals timerInternals;

  private static StateNamespace windowNamespace(IntervalWindow window) {
    return StateNamespaces.<IntervalWindow>window(
        (Coder) WINDOWING_STRATEGY.getWindowFn().windowCoder(), window);
  }

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(mockStepContext.timerInternals()).thenReturn(timerInternals);
    droppedDueToLateness = new InMemoryLongSumAggregator("droppedDueToLateness");

    aggregatorFactory = new AggregatorFactory() {
      @Override
      public <InputT, AccumT, OutputT> Aggregator<InputT, OutputT> createAggregatorForDoFn(
          Class<?> fnClass, ExecutionContext.StepContext stepContext, String aggregatorName,
          Combine.CombineFn<InputT, AccumT, OutputT> combine) {
        return (Aggregator<InputT, OutputT>) droppedDueToLateness;
      }
    };

    stateInternals = new InMemoryStateInternals<>("hello");
    timerInternals = new InMemoryTimerInternals();

    when(mockStepContext.stateInternals()).thenReturn((StateInternals) stateInternals);
    when(mockStepContext.timerInternals()).thenReturn(timerInternals);
  }

  @Test
  public void testLateDropping() throws Exception {

    timerInternals.advanceInputWatermark(new Instant(BoundedWindow.TIMESTAMP_MAX_VALUE));
    timerInternals.advanceOutputWatermark(new Instant(BoundedWindow.TIMESTAMP_MAX_VALUE));

    DoFn<KV<String, Integer>, Integer> fn = new MyDoFn();

    DoFnRunner<KV<String, Integer>, Integer> runner = DoFnRunners.defaultStatefulDoFnRunner(
        fn,
        getDoFnRunner(fn),
        mockStepContext,
        aggregatorFactory,
        WINDOWING_STRATEGY,
        new StatefulDoFnRunner.TimeInternalsCleanupTimer(timerInternals, WINDOWING_STRATEGY),
        new StatefulDoFnRunner.StateInternalsStateCleaner<>(
            fn, stateInternals, (Coder) WINDOWING_STRATEGY.getWindowFn().windowCoder()));

    runner.startBundle();

    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(0L + WINDOW_SIZE));
    Instant timestamp = new Instant(0);

    runner.processElement(
        WindowedValue.of(KV.of("hello", 1), timestamp, window, PaneInfo.NO_FIRING));
    assertEquals(1L, droppedDueToLateness.sum);

    runner.finishBundle();
  }

  @Test
  public void testGarbageCollect() throws Exception {
    timerInternals.advanceInputWatermark(new Instant(1L));

    MyDoFn fn = new MyDoFn();
    StateTag<Object, ValueState<Integer>> stateTag = StateTags.tagForSpec(fn.stateId, fn.intState);

    DoFnRunner<KV<String, Integer>, Integer> runner = DoFnRunners.defaultStatefulDoFnRunner(
        fn,
        getDoFnRunner(fn),
        mockStepContext,
        aggregatorFactory,
        WINDOWING_STRATEGY,
        new StatefulDoFnRunner.TimeInternalsCleanupTimer(timerInternals, WINDOWING_STRATEGY),
        new StatefulDoFnRunner.StateInternalsStateCleaner<>(
            fn, stateInternals, (Coder) WINDOWING_STRATEGY.getWindowFn().windowCoder()));

    Instant elementTime = new Instant(1);

    // first element, key is hello, WINDOW_1
    runner.processElement(
        WindowedValue.of(KV.of("hello", 1), elementTime, WINDOW_1, PaneInfo.NO_FIRING));

    assertEquals(
        1, (int) stateInternals.state(windowNamespace(WINDOW_1), stateTag).read());

    // second element, key is hello, WINDOW_2
    runner.processElement(
        WindowedValue.of(
            KV.of("hello", 1), elementTime.plus(WINDOW_SIZE), WINDOW_2, PaneInfo.NO_FIRING));

    runner.processElement(
        WindowedValue.of(
            KV.of("hello", 1), elementTime.plus(WINDOW_SIZE), WINDOW_2, PaneInfo.NO_FIRING));

    assertEquals(
        2, (int) stateInternals.state(windowNamespace(WINDOW_2), stateTag).read());

    // advance watermark past end of WINDOW_1 + allowed lateness
    // the cleanup timer is set to window.maxTimestamp() + allowed lateness + 1
    // to ensure that state is still available when a user timer for window.maxTimestamp() fires
    advanceInputWatermark(
        timerInternals,
        WINDOW_1.maxTimestamp()
            .plus(ALLOWED_LATENESS)
            .plus(StatefulDoFnRunner.TimeInternalsCleanupTimer.GC_DELAY_MS)
            .plus(1), // so the watermark is past the GC horizon, not on it
        runner);

    assertTrue(
        stateInternals.isEmptyForTesting(
            stateInternals.state(windowNamespace(WINDOW_1), stateTag)));

    assertEquals(
        2, (int) stateInternals.state(windowNamespace(WINDOW_2), stateTag).read());

    // advance watermark past end of WINDOW_2 + allowed lateness
    advanceInputWatermark(
        timerInternals,
        WINDOW_2.maxTimestamp()
            .plus(ALLOWED_LATENESS)
            .plus(StatefulDoFnRunner.TimeInternalsCleanupTimer.GC_DELAY_MS)
            .plus(1), // so the watermark is past the GC horizon, not on it
        runner);

    assertTrue(
        stateInternals.isEmptyForTesting(
            stateInternals.state(windowNamespace(WINDOW_2), stateTag)));
  }

  private DoFnRunner<KV<String, Integer>, Integer> getDoFnRunner(
      DoFn<KV<String, Integer>, Integer> fn) {
    return new SimpleDoFnRunner<>(
        null,
        fn,
        NullSideInputReader.empty(),
        null,
        null,
        Collections.<TupleTag<?>>emptyList(),
        mockStepContext,
        null,
        WINDOWING_STRATEGY);
  }

  private static void advanceInputWatermark(
      InMemoryTimerInternals timerInternals,
      Instant newInputWatermark,
      DoFnRunner<?, ?> toTrigger) throws Exception {
    timerInternals.advanceInputWatermark(newInputWatermark);
    TimerInternals.TimerData timer;
    while ((timer = timerInternals.removeNextEventTimer()) != null) {
      StateNamespace namespace = timer.getNamespace();
      checkArgument(namespace instanceof StateNamespaces.WindowNamespace);
      BoundedWindow window = ((StateNamespaces.WindowNamespace) namespace).getWindow();
      toTrigger.onTimer(timer.getTimerId(), window, timer.getTimestamp(), timer.getDomain());
    }
  }

  private static class MyDoFn extends DoFn<KV<String, Integer>, Integer> {

    public final String stateId = "foo";

    @StateId(stateId)
    public final StateSpec<Object, ValueState<Integer>> intState =
        StateSpecs.value(VarIntCoder.of());

    @ProcessElement
    public void processElement(
        ProcessContext c, @StateId(stateId) ValueState<Integer> state) {
      Integer currentValue = MoreObjects.firstNonNull(state.read(), 0);
      state.write(currentValue + 1);
    }
  };

  private static class InMemoryLongSumAggregator implements Aggregator<Long, Long> {
    private final String name;
    private long sum = 0;

    public InMemoryLongSumAggregator(String name) {
      this.name = name;
    }

    @Override
    public void addValue(Long value) {
      sum += value;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public Combine.CombineFn<Long, ?, Long> getCombineFn() {
      return Sum.ofLongs();
    }
  }
}
