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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.IdentitySideInputWindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Tests for {@link SimplePushbackSideInputDoFnRunner}. */
@RunWith(JUnit4.class)
public class SimplePushbackSideInputDoFnRunnerTest {
  @Mock StepContext mockStepContext;
  @Mock private ReadyCheckingSideInputReader reader;
  private TestDoFnRunner<Integer, Integer> underlying;
  private PCollectionView<Integer> singletonView;
  private DoFnRunner<KV<String, Integer>, Integer> statefulRunner;

  private static final long WINDOW_SIZE = 10;
  private static final long ALLOWED_LATENESS = 1;

  private static final IntervalWindow WINDOW_1 =
      new IntervalWindow(new Instant(0), new Instant(10));

  private static final IntervalWindow WINDOW_2 =
      new IntervalWindow(new Instant(10), new Instant(20));

  private static final WindowingStrategy<?, ?> WINDOWING_STRATEGY =
      WindowingStrategy.of(FixedWindows.of(Duration.millis(WINDOW_SIZE)))
          .withAllowedLateness(Duration.millis(ALLOWED_LATENESS));

  private InMemoryStateInternals<String> stateInternals;
  private InMemoryTimerInternals timerInternals;

  @Rule public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    PCollection<Integer> created = p.apply(Create.of(1, 2, 3));
    singletonView =
        created
            .apply(Window.into(new IdentitySideInputWindowFn()))
            .apply(Sum.integersGlobally().asSingletonView());

    underlying = new TestDoFnRunner<>(VarIntCoder.of());

    DoFn<KV<String, Integer>, Integer> fn = new MyDoFn();

    MockitoAnnotations.initMocks(this);
    when(mockStepContext.timerInternals()).thenReturn(timerInternals);

    stateInternals = new InMemoryStateInternals<>("hello");
    timerInternals = new InMemoryTimerInternals();

    when(mockStepContext.stateInternals()).thenReturn((StateInternals) stateInternals);
    when(mockStepContext.timerInternals()).thenReturn(timerInternals);

    statefulRunner =
        DoFnRunners.defaultStatefulDoFnRunner(
            fn,
            KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()),
            getDoFnRunner(fn),
            asStepContext(stateInternals, timerInternals),
            WINDOWING_STRATEGY,
            new StatefulDoFnRunner.TimeInternalsCleanupTimer(timerInternals, WINDOWING_STRATEGY),
            new StatefulDoFnRunner.StateInternalsStateCleaner<>(
                fn, stateInternals, (Coder) WINDOWING_STRATEGY.getWindowFn().windowCoder()));
  }

  private StepContext asStepContext(StateInternals stateInternals, TimerInternals timerInternals) {
    return new StepContext() {

      @Override
      public StateInternals stateInternals() {
        return stateInternals;
      }

      @Override
      public TimerInternals timerInternals() {
        return timerInternals;
      }
    };
  }

  private SimplePushbackSideInputDoFnRunner<Integer, Integer> createRunner(
      ImmutableList<PCollectionView<?>> views) {
    SimplePushbackSideInputDoFnRunner<Integer, Integer> runner =
        SimplePushbackSideInputDoFnRunner.create(underlying, views, reader);
    runner.startBundle();
    return runner;
  }

  @Test
  public void startFinishBundleDelegates() {
    PushbackSideInputDoFnRunner runner = createRunner(ImmutableList.of(singletonView));

    assertThat(underlying.started, is(true));
    assertThat(underlying.finished, is(false));
    runner.finishBundle();
    assertThat(underlying.finished, is(true));
  }

  @Test
  public void processElementSideInputNotReady() {
    when(reader.isReady(Mockito.eq(singletonView), Mockito.any(BoundedWindow.class)))
        .thenReturn(false);

    SimplePushbackSideInputDoFnRunner<Integer, Integer> runner =
        createRunner(ImmutableList.of(singletonView));

    WindowedValue<Integer> oneWindow =
        WindowedValue.of(
            2,
            new Instant(-2),
            new IntervalWindow(new Instant(-500L), new Instant(0L)),
            PaneInfo.ON_TIME_AND_ONLY_FIRING);
    Iterable<WindowedValue<Integer>> oneWindowPushback =
        runner.processElementInReadyWindows(oneWindow);
    assertThat(oneWindowPushback, containsInAnyOrder(oneWindow));
    assertThat(underlying.inputElems, emptyIterable());
  }

  @Test
  public void processElementSideInputNotReadyMultipleWindows() {
    when(reader.isReady(Mockito.eq(singletonView), Mockito.any(BoundedWindow.class)))
        .thenReturn(false);

    SimplePushbackSideInputDoFnRunner<Integer, Integer> runner =
        createRunner(ImmutableList.of(singletonView));

    WindowedValue<Integer> multiWindow =
        WindowedValue.of(
            2,
            new Instant(-2),
            ImmutableList.of(
                new IntervalWindow(new Instant(-500L), new Instant(0L)),
                new IntervalWindow(BoundedWindow.TIMESTAMP_MIN_VALUE, new Instant(250L)),
                GlobalWindow.INSTANCE),
            PaneInfo.ON_TIME_AND_ONLY_FIRING);
    Iterable<WindowedValue<Integer>> multiWindowPushback =
        runner.processElementInReadyWindows(multiWindow);
    assertThat(multiWindowPushback, equalTo(multiWindow.explodeWindows()));
    assertThat(underlying.inputElems, emptyIterable());
  }

  @Test
  public void processElementSideInputNotReadySomeWindows() {
    when(reader.isReady(Mockito.eq(singletonView), Mockito.eq(GlobalWindow.INSTANCE)))
        .thenReturn(false);
    when(reader.isReady(
            Mockito.eq(singletonView),
            org.mockito.AdditionalMatchers.not(Mockito.eq(GlobalWindow.INSTANCE))))
        .thenReturn(true);

    SimplePushbackSideInputDoFnRunner<Integer, Integer> runner =
        createRunner(ImmutableList.of(singletonView));

    IntervalWindow littleWindow = new IntervalWindow(new Instant(-500L), new Instant(0L));
    IntervalWindow bigWindow =
        new IntervalWindow(BoundedWindow.TIMESTAMP_MIN_VALUE, new Instant(250L));
    WindowedValue<Integer> multiWindow =
        WindowedValue.of(
            2,
            new Instant(-2),
            ImmutableList.of(littleWindow, bigWindow, GlobalWindow.INSTANCE),
            PaneInfo.NO_FIRING);
    Iterable<WindowedValue<Integer>> multiWindowPushback =
        runner.processElementInReadyWindows(multiWindow);
    assertThat(
        multiWindowPushback,
        containsInAnyOrder(WindowedValue.timestampedValueInGlobalWindow(2, new Instant(-2L))));
    assertThat(
        underlying.inputElems,
        containsInAnyOrder(
            WindowedValue.of(
                2, new Instant(-2), ImmutableList.of(littleWindow), PaneInfo.NO_FIRING),
            WindowedValue.of(2, new Instant(-2), ImmutableList.of(bigWindow), PaneInfo.NO_FIRING)));
  }

  @Test
  public void processElementSideInputReadyAllWindows() {
    when(reader.isReady(Mockito.eq(singletonView), Mockito.any(BoundedWindow.class)))
        .thenReturn(true);

    ImmutableList<PCollectionView<?>> views = ImmutableList.of(singletonView);
    SimplePushbackSideInputDoFnRunner<Integer, Integer> runner = createRunner(views);

    WindowedValue<Integer> multiWindow =
        WindowedValue.of(
            2,
            new Instant(-2),
            ImmutableList.of(
                new IntervalWindow(new Instant(-500L), new Instant(0L)),
                new IntervalWindow(BoundedWindow.TIMESTAMP_MIN_VALUE, new Instant(250L)),
                GlobalWindow.INSTANCE),
            PaneInfo.ON_TIME_AND_ONLY_FIRING);
    Iterable<WindowedValue<Integer>> multiWindowPushback =
        runner.processElementInReadyWindows(multiWindow);
    assertThat(multiWindowPushback, emptyIterable());
    assertThat(
        underlying.inputElems,
        containsInAnyOrder(ImmutableList.copyOf(multiWindow.explodeWindows()).toArray()));
  }

  @Test
  public void processElementNoSideInputs() {
    SimplePushbackSideInputDoFnRunner<Integer, Integer> runner = createRunner(ImmutableList.of());

    WindowedValue<Integer> multiWindow =
        WindowedValue.of(
            2,
            new Instant(-2),
            ImmutableList.of(
                new IntervalWindow(new Instant(-500L), new Instant(0L)),
                new IntervalWindow(BoundedWindow.TIMESTAMP_MIN_VALUE, new Instant(250L)),
                GlobalWindow.INSTANCE),
            PaneInfo.ON_TIME_AND_ONLY_FIRING);
    Iterable<WindowedValue<Integer>> multiWindowPushback =
        runner.processElementInReadyWindows(multiWindow);
    assertThat(multiWindowPushback, emptyIterable());
    // Should preserve the compressed representation when there's no side inputs.
    assertThat(underlying.inputElems, containsInAnyOrder(multiWindow));
  }

  /** Tests that a call to onTimer gets delegated. */
  @Test
  public void testOnTimerCalled() {
    PushbackSideInputDoFnRunner<Integer, Integer> runner = createRunner(ImmutableList.of());

    String timerId = "fooTimer";
    IntervalWindow window = new IntervalWindow(new Instant(4), new Instant(16));
    Instant timestamp = new Instant(72);

    // Mocking is not easily compatible with annotation analysis, so we manually record
    // the method call.
    runner.onTimer(
        timerId,
        "",
        null,
        window,
        new Instant(timestamp),
        new Instant(timestamp),
        TimeDomain.EVENT_TIME);

    assertThat(
        underlying.firedTimers,
        contains(
            TimerData.of(
                timerId,
                StateNamespaces.window(IntervalWindow.getCoder(), window),
                timestamp,
                timestamp,
                TimeDomain.EVENT_TIME)));
  }

  private static class TestDoFnRunner<InputT, OutputT> implements DoFnRunner<InputT, OutputT> {
    private final Coder<InputT> inputCoder;
    List<WindowedValue<InputT>> inputElems;
    List<TimerData> firedTimers;
    private boolean started = false;
    private boolean finished = false;

    TestDoFnRunner(Coder<InputT> inputCoder) {
      this.inputCoder = inputCoder;
    }

    @Override
    public DoFn<InputT, OutputT> getFn() {
      return null;
    }

    @Override
    public void startBundle() {
      started = true;
      inputElems = new ArrayList<>();
      firedTimers = new ArrayList<>();
    }

    @Override
    public void processElement(WindowedValue<InputT> elem) {
      inputElems.add(elem);
    }

    @Override
    public <KeyT> void onTimer(
        String timerId,
        String timerFamilyId,
        KeyT key,
        BoundedWindow window,
        Instant timestamp,
        Instant outputTimestamp,
        TimeDomain timeDomain) {
      firedTimers.add(
          TimerData.of(
              timerId,
              timerFamilyId,
              StateNamespaces.window(IntervalWindow.getCoder(), (IntervalWindow) window),
              timestamp,
              outputTimestamp,
              timeDomain));
    }

    @Override
    public void finishBundle() {
      finished = true;
    }

    @Override
    public <KeyT> void onWindowExpiration(BoundedWindow window, Instant timestamp, KeyT key) {}
  }

  private SimplePushbackSideInputDoFnRunner<KV<String, Integer>, Integer> createRunner(
      DoFnRunner<KV<String, Integer>, Integer> doFnRunner,
      ImmutableList<PCollectionView<?>> views) {
    SimplePushbackSideInputDoFnRunner<KV<String, Integer>, Integer> runner =
        SimplePushbackSideInputDoFnRunner.create(doFnRunner, views, reader);
    runner.startBundle();
    return runner;
  }

  @Test
  @Category({ValidatesRunner.class})
  public void testLateDroppingForStatefulDoFnRunner() throws Exception {
    MetricsContainerImpl container = new MetricsContainerImpl("any");
    MetricsEnvironment.setCurrentContainer(container);

    timerInternals.advanceInputWatermark(new Instant(BoundedWindow.TIMESTAMP_MAX_VALUE));
    timerInternals.advanceOutputWatermark(new Instant(BoundedWindow.TIMESTAMP_MAX_VALUE));

    PushbackSideInputDoFnRunner runner =
        createRunner(statefulRunner, ImmutableList.of(singletonView));

    runner.startBundle();

    when(reader.isReady(Mockito.eq(singletonView), Mockito.any(BoundedWindow.class)))
        .thenReturn(true);

    WindowedValue<Integer> multiWindow =
        WindowedValue.of(
            1,
            new Instant(0),
            ImmutableList.of(new IntervalWindow(new Instant(0), new Instant(0L + WINDOW_SIZE))),
            PaneInfo.ON_TIME_AND_ONLY_FIRING);

    runner.processElementInReadyWindows(multiWindow);

    long droppedValues =
        container
            .getCounter(
                MetricName.named(
                    StatefulDoFnRunner.class, StatefulDoFnRunner.DROPPED_DUE_TO_LATENESS_COUNTER))
            .getCumulative();
    assertEquals(1L, droppedValues);

    runner.finishBundle();
  }

  @Test
  @Category({ValidatesRunner.class})
  public void testGarbageCollectForStatefulDoFnRunner() throws Exception {
    timerInternals.advanceInputWatermark(new Instant(1L));

    MyDoFn fn = new MyDoFn();
    StateTag<ValueState<Integer>> stateTag = StateTags.tagForSpec(fn.stateId, fn.intState);

    PushbackSideInputDoFnRunner runner =
        createRunner(statefulRunner, ImmutableList.of(singletonView));

    Instant elementTime = new Instant(1);

    when(reader.isReady(Mockito.eq(singletonView), Mockito.any(BoundedWindow.class)))
        .thenReturn(true);

    // first element, key is hello, WINDOW_1
    runner.processElementInReadyWindows(
        WindowedValue.of(KV.of("hello", 1), elementTime, WINDOW_1, PaneInfo.NO_FIRING));

    assertEquals(1, (int) stateInternals.state(windowNamespace(WINDOW_1), stateTag).read());

    // second element, key is hello, WINDOW_2
    runner.processElementInReadyWindows(
        WindowedValue.of(
            KV.of("hello", 1), elementTime.plus(WINDOW_SIZE), WINDOW_2, PaneInfo.NO_FIRING));

    runner.processElementInReadyWindows(
        WindowedValue.of(
            KV.of("hello", 1), elementTime.plus(WINDOW_SIZE), WINDOW_2, PaneInfo.NO_FIRING));

    assertEquals(2, (int) stateInternals.state(windowNamespace(WINDOW_2), stateTag).read());

    // advance watermark past end of WINDOW_1 + allowed lateness
    // the cleanup timer is set to window.maxTimestamp() + allowed lateness + 1
    // to ensure that state is still available when a user timer for window.maxTimestamp() fires
    advanceInputWatermark(
        timerInternals,
        WINDOW_1
            .maxTimestamp()
            .plus(ALLOWED_LATENESS)
            .plus(StatefulDoFnRunner.TimeInternalsCleanupTimer.GC_DELAY_MS)
            .plus(1), // so the watermark is past the GC horizon, not on it
        runner);

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

  private static void advanceInputWatermark(
      InMemoryTimerInternals timerInternals,
      Instant newInputWatermark,
      PushbackSideInputDoFnRunner<?, ?> toTrigger)
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

  private static StateNamespace windowNamespace(IntervalWindow window) {
    return StateNamespaces.window((Coder) WINDOWING_STRATEGY.getWindowFn().windowCoder(), window);
  }

  private static class MyDoFn extends DoFn<KV<String, Integer>, Integer> {

    public final String stateId = "foo";

    @StateId(stateId)
    public final StateSpec<ValueState<Integer>> intState = StateSpecs.value(VarIntCoder.of());

    @ProcessElement
    public void processElement(ProcessContext c, @StateId(stateId) ValueState<Integer> state) {
      Integer currentValue = MoreObjects.firstNonNull(state.read(), 0);
      state.write(currentValue + 1);
    }
  }

  private DoFnRunner<KV<String, Integer>, Integer> getDoFnRunner(
      DoFn<KV<String, Integer>, Integer> fn) {
    return new SimpleDoFnRunner<>(
        null,
        fn,
        NullSideInputReader.empty(),
        null,
        null,
        Collections.emptyList(),
        mockStepContext,
        null,
        Collections.emptyMap(),
        WINDOWING_STRATEGY,
        DoFnSchemaInformation.create(),
        Collections.emptyMap());
  }
}
