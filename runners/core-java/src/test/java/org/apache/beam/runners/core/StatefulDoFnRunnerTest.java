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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.Nullable;
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
      WindowingStrategy.of(FixedWindows.of(Duration.millis(WINDOW_SIZE)))
          .withAllowedLateness(Duration.millis(ALLOWED_LATENESS));

  private static final IntervalWindow WINDOW_1 =
      new IntervalWindow(new Instant(0), new Instant(10));

  private static final IntervalWindow WINDOW_2 =
      new IntervalWindow(new Instant(10), new Instant(20));

  private final TupleTag<Integer> outputTag = new TupleTag<>();

  @Mock StepContext mockStepContext;

  private InMemoryStateInternals<String> stateInternals;
  private InMemoryTimerInternals timerInternals;

  private static StateNamespace windowNamespace(IntervalWindow window) {
    return StateNamespaces.window((Coder) WINDOWING_STRATEGY.getWindowFn().windowCoder(), window);
  }

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    stateInternals = new InMemoryStateInternals<>("hello");
    timerInternals = new InMemoryTimerInternals();

    when(mockStepContext.stateInternals()).thenReturn((StateInternals) stateInternals);
    when(mockStepContext.timerInternals()).thenReturn(timerInternals);
  }

  @Test
  public void testLateDroppingUnordered() throws Exception {
    testLateDropping(false);
  }

  @Test
  public void testLateDroppingOrdered() throws Exception {
    testLateDropping(true);
  }

  @Test
  public void testGargageCollectUnordered() throws Exception {
    testGarbageCollect(false);
  }

  @Test
  public void testGargageCollectOrdered() throws Exception {
    testGarbageCollect(true);
  }

  @Test
  public void testOutputUnordered() throws Exception {
    testOutput(false);
  }

  @Test
  public void testOutputOrdered() throws Exception {
    testOutput(true);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testOutputOrderedUnsupported() throws Exception {
    testOutput(true, (fn, output) -> createStatefulDoFnRunner(fn, output, false));
  }

  @Test
  public void testDataDroppedBasedOnInputWatermarkWhenOrdered() throws Exception {
    MetricsContainerImpl container = new MetricsContainerImpl("any");
    MetricsEnvironment.setCurrentContainer(container);
    Instant timestamp = new Instant(0);

    MyDoFn fn = MyDoFn.create(true);

    DoFnRunner<KV<String, Integer>, Integer> runner = createStatefulDoFnRunner(fn);

    runner.startBundle();

    IntervalWindow window = new IntervalWindow(timestamp, timestamp.plus(WINDOW_SIZE));

    runner.processElement(
        WindowedValue.of(KV.of("hello", 1), timestamp, window, PaneInfo.NO_FIRING));

    long droppedValues =
        container
            .getCounter(
                MetricName.named(
                    StatefulDoFnRunner.class, StatefulDoFnRunner.DROPPED_DUE_TO_LATENESS_COUNTER))
            .getCumulative();
    assertEquals(0L, droppedValues);

    timerInternals.advanceInputWatermark(timestamp.plus(ALLOWED_LATENESS + 1));

    runner.processElement(
        WindowedValue.of(KV.of("hello", 1), timestamp, window, PaneInfo.NO_FIRING));

    droppedValues =
        container
            .getCounter(
                MetricName.named(
                    StatefulDoFnRunner.class, StatefulDoFnRunner.DROPPED_DUE_TO_LATENESS_COUNTER))
            .getCumulative();
    assertEquals(1L, droppedValues);

    runner.finishBundle();
  }

  private void testLateDropping(boolean ordered) throws Exception {
    MetricsContainerImpl container = new MetricsContainerImpl("any");
    MetricsEnvironment.setCurrentContainer(container);

    timerInternals.advanceInputWatermark(new Instant(BoundedWindow.TIMESTAMP_MAX_VALUE));
    timerInternals.advanceOutputWatermark(new Instant(BoundedWindow.TIMESTAMP_MAX_VALUE));

    MyDoFn fn = MyDoFn.create(ordered);

    DoFnRunner<KV<String, Integer>, Integer> runner = createStatefulDoFnRunner(fn);

    runner.startBundle();

    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(0L + WINDOW_SIZE));
    Instant timestamp = new Instant(0);

    runner.processElement(
        WindowedValue.of(KV.of("hello", 1), timestamp, window, PaneInfo.NO_FIRING));

    long droppedValues =
        container
            .getCounter(
                MetricName.named(
                    StatefulDoFnRunner.class, StatefulDoFnRunner.DROPPED_DUE_TO_LATENESS_COUNTER))
            .getCumulative();
    assertEquals(1L, droppedValues);

    runner.finishBundle();
  }

  private void testGarbageCollect(boolean ordered) throws Exception {
    timerInternals.advanceInputWatermark(new Instant(1L));

    MyDoFn fn = MyDoFn.create(ordered);
    StateTag<ValueState<Integer>> stateTag = StateTags.tagForSpec(MyDoFn.STATE_ID, fn.intState());

    DoFnRunner<KV<String, Integer>, Integer> runner = createStatefulDoFnRunner(fn);

    Instant elementTime = new Instant(1);

    // first element, key is hello, WINDOW_1
    runner.processElement(
        WindowedValue.of(KV.of("hello", 1), elementTime, WINDOW_1, PaneInfo.NO_FIRING));

    if (ordered) {
      // move forward in time so that the input might get flushed
      advanceInputWatermark(timerInternals, elementTime.plus(ALLOWED_LATENESS + 1), runner);
    }

    assertEquals(1, (int) stateInternals.state(windowNamespace(WINDOW_1), stateTag).read());

    // second element, key is hello, WINDOW_2
    runner.processElement(
        WindowedValue.of(
            KV.of("hello", 1), elementTime.plus(WINDOW_SIZE), WINDOW_2, PaneInfo.NO_FIRING));

    runner.processElement(
        WindowedValue.of(
            KV.of("hello", 1), elementTime.plus(WINDOW_SIZE), WINDOW_2, PaneInfo.NO_FIRING));

    if (ordered) {
      // move forward in time so that the input might get flushed
      advanceInputWatermark(
          timerInternals, elementTime.plus(ALLOWED_LATENESS + 1 + WINDOW_SIZE), runner);
    }

    assertEquals(2, (int) stateInternals.state(windowNamespace(WINDOW_2), stateTag).read());

    // advance watermark past end of WINDOW_1 + allowed lateness
    // the cleanup timer is set to window.maxTimestamp() + allowed lateness + 1
    // to ensure that state is still available when a user timer for window.maxTimestamp() fires
    advanceInputWatermark(
        timerInternals, elementTime.plus(ALLOWED_LATENESS + 1 + WINDOW_SIZE), runner);

    assertTrue(
        stateInternals.isEmptyForTesting(
            stateInternals.state(windowNamespace(WINDOW_1), stateTag)));

    assertEquals(2, (int) stateInternals.state(windowNamespace(WINDOW_2), stateTag).read());

    // advance watermark past end of WINDOW_2 + allowed lateness
    advanceInputWatermark(
        timerInternals,
        WINDOW_2
            .maxTimestamp()
            .plus(ALLOWED_LATENESS)
            .plus(StatefulDoFnRunner.TimeInternalsCleanupTimer.GC_DELAY_MS)
            .plus(1), // so the watermark is past the GC horizon, not on it
        runner);

    assertTrue(
        stateInternals.isEmptyForTesting(
            stateInternals.state(windowNamespace(WINDOW_2), stateTag)));
  }

  private void testOutput(boolean ordered) throws Exception {
    testOutput(ordered, this::createStatefulDoFnRunner);
  }

  private void testOutput(
      boolean ordered,
      BiFunction<MyDoFn, OutputManager, DoFnRunner<KV<String, Integer>, Integer>> runnerFactory)
      throws Exception {

    timerInternals.advanceInputWatermark(new Instant(1L));

    MyDoFn fn = MyDoFn.create(ordered);
    StateTag<ValueState<Integer>> stateTag = StateTags.tagForSpec(MyDoFn.STATE_ID, fn.intState());

    List<KV<TupleTag<?>, WindowedValue<?>>> outputs = new ArrayList<>();
    OutputManager output = asOutputManager(outputs);
    DoFnRunner<KV<String, Integer>, Integer> runner = runnerFactory.apply(fn, output);

    Instant elementTime = new Instant(5);

    // write two elements, with descending timestamps
    runner.processElement(
        WindowedValue.of(KV.of("hello", 1), elementTime, WINDOW_1, PaneInfo.NO_FIRING));
    runner.processElement(
        WindowedValue.of(KV.of("hello", 2), elementTime.minus(1), WINDOW_1, PaneInfo.NO_FIRING));

    if (ordered) {
      // move forward in time so that the input might get flushed
      advanceInputWatermark(timerInternals, elementTime.plus(ALLOWED_LATENESS + 1), runner);
    }

    assertEquals(3, (int) stateInternals.state(windowNamespace(WINDOW_1), stateTag).read());
    assertEquals(2, outputs.size());
    if (ordered) {
      assertEquals(
          Arrays.asList(
              KV.of(
                  outputTag,
                  WindowedValue.of(2, elementTime.minus(1), WINDOW_1, PaneInfo.NO_FIRING)),
              KV.of(outputTag, WindowedValue.of(3, elementTime, WINDOW_1, PaneInfo.NO_FIRING))),
          outputs);
    } else {
      assertEquals(
          Arrays.asList(
              KV.of(outputTag, WindowedValue.of(1, elementTime, WINDOW_1, PaneInfo.NO_FIRING)),
              KV.of(
                  outputTag,
                  WindowedValue.of(3, elementTime.minus(1), WINDOW_1, PaneInfo.NO_FIRING))),
          outputs);
    }
    outputs.clear();

    // another window
    elementTime = elementTime.plus(WINDOW_SIZE);
    runner.processElement(
        WindowedValue.of(KV.of("hello", 1), elementTime, WINDOW_2, PaneInfo.NO_FIRING));

    runner.processElement(
        WindowedValue.of(KV.of("hello", 2), elementTime.minus(1), WINDOW_2, PaneInfo.NO_FIRING));

    runner.processElement(
        WindowedValue.of(KV.of("hello", 3), elementTime.minus(2), WINDOW_2, PaneInfo.NO_FIRING));

    if (ordered) {
      // move forward in time so that the input might get flushed
      advanceInputWatermark(timerInternals, elementTime.plus(ALLOWED_LATENESS + 1), runner);
    }

    assertEquals(6, (int) stateInternals.state(windowNamespace(WINDOW_2), stateTag).read());
    assertEquals(3, outputs.size());
    if (ordered) {
      assertEquals(
          Arrays.asList(
              KV.of(
                  outputTag,
                  WindowedValue.of(3, elementTime.minus(2), WINDOW_2, PaneInfo.NO_FIRING)),
              KV.of(
                  outputTag,
                  WindowedValue.of(5, elementTime.minus(1), WINDOW_2, PaneInfo.NO_FIRING)),
              KV.of(outputTag, WindowedValue.of(6, elementTime, WINDOW_2, PaneInfo.NO_FIRING))),
          outputs);
    } else {
      assertEquals(
          Arrays.asList(
              KV.of(outputTag, WindowedValue.of(1, elementTime, WINDOW_2, PaneInfo.NO_FIRING)),
              KV.of(
                  outputTag,
                  WindowedValue.of(3, elementTime.minus(1), WINDOW_2, PaneInfo.NO_FIRING)),
              KV.of(
                  outputTag,
                  WindowedValue.of(6, elementTime.minus(2), WINDOW_2, PaneInfo.NO_FIRING))),
          outputs);
    }
  }

  private DoFnRunner createStatefulDoFnRunner(DoFn<KV<String, Integer>, Integer> fn) {
    return createStatefulDoFnRunner(fn, null);
  }

  private DoFnRunner createStatefulDoFnRunner(
      DoFn<KV<String, Integer>, Integer> fn, OutputManager outputManager) {
    return createStatefulDoFnRunner(fn, outputManager, true);
  }

  private DoFnRunner createStatefulDoFnRunner(
      DoFn<KV<String, Integer>, Integer> fn,
      OutputManager outputManager,
      boolean supportTimeSortedInput) {
    return DoFnRunners.defaultStatefulDoFnRunner(
        fn,
        KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()),
        getDoFnRunner(fn, outputManager),
        mockStepContext,
        WINDOWING_STRATEGY,
        new StatefulDoFnRunner.TimeInternalsCleanupTimer(timerInternals, WINDOWING_STRATEGY),
        new StatefulDoFnRunner.StateInternalsStateCleaner<>(
            fn, stateInternals, (Coder) WINDOWING_STRATEGY.getWindowFn().windowCoder()),
        supportTimeSortedInput);
  }

  private DoFnRunner<KV<String, Integer>, Integer> getDoFnRunner(
      DoFn<KV<String, Integer>, Integer> fn) {
    return getDoFnRunner(fn, null);
  }

  private DoFnRunner<KV<String, Integer>, Integer> getDoFnRunner(
      DoFn<KV<String, Integer>, Integer> fn, @Nullable OutputManager outputManager) {
    return new SimpleDoFnRunner<>(
        null,
        fn,
        NullSideInputReader.empty(),
        MoreObjects.firstNonNull(outputManager, discardingOutputManager()),
        outputTag,
        Collections.emptyList(),
        mockStepContext,
        null,
        Collections.emptyMap(),
        WINDOWING_STRATEGY,
        DoFnSchemaInformation.create(),
        Collections.emptyMap());
  }

  private OutputManager discardingOutputManager() {
    return new OutputManager() {
      @Override
      public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
        // discard
      }
    };
  }

  private static void advanceInputWatermark(
      InMemoryTimerInternals timerInternals, Instant newInputWatermark, DoFnRunner<?, ?> toTrigger)
      throws Exception {
    timerInternals.advanceInputWatermark(newInputWatermark);
    TimerInternals.TimerData timer;
    while ((timer = timerInternals.removeNextEventTimer()) != null) {
      StateNamespace namespace = timer.getNamespace();
      checkArgument(namespace instanceof StateNamespaces.WindowNamespace);
      BoundedWindow window = ((StateNamespaces.WindowNamespace) namespace).getWindow();
      toTrigger.onTimer(
          timer.getTimerId(),
          timer.getTimerFamilyId(),
          null,
          window,
          timer.getTimestamp(),
          timer.getOutputTimestamp(),
          timer.getDomain());
    }
  }

  private static OutputManager asOutputManager(List<KV<TupleTag<?>, WindowedValue<?>>> outputs) {
    return new OutputManager() {
      @Override
      public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
        outputs.add(KV.of(tag, output));
      }
    };
  }

  private abstract static class MyDoFn extends DoFn<KV<String, Integer>, Integer> {

    static final String STATE_ID = "foo";

    static MyDoFn create(boolean sorted) {
      return sorted ? new MyDoFnSorted() : new MyDoFnUnsorted();
    }

    abstract StateSpec<ValueState<Integer>> intState();

    public void processElement(ProcessContext c, ValueState<Integer> state) {
      KV<String, Integer> elem = c.element();
      Integer currentValue = MoreObjects.firstNonNull(state.read(), 0);
      int updated = currentValue + elem.getValue();
      state.write(updated);
      c.output(updated);
    }
  }

  private static class MyDoFnUnsorted extends MyDoFn {

    @StateId(STATE_ID)
    public final StateSpec<ValueState<Integer>> intState = StateSpecs.value(VarIntCoder.of());

    @ProcessElement
    @Override
    public void processElement(ProcessContext c, @StateId(STATE_ID) ValueState<Integer> state) {
      super.processElement(c, state);
    }

    @Override
    StateSpec<ValueState<Integer>> intState() {
      return intState;
    }
  }

  private static class MyDoFnSorted extends MyDoFn {

    @StateId(STATE_ID)
    public final StateSpec<ValueState<Integer>> intState = StateSpecs.value(VarIntCoder.of());

    @RequiresTimeSortedInput
    @ProcessElement
    @Override
    public void processElement(
        ProcessContext c, @StateId(MyDoFn.STATE_ID) ValueState<Integer> state) {
      super.processElement(c, state);
    }

    @Override
    StateSpec<ValueState<Integer>> intState() {
      return intState;
    }
  }
}
