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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.IdentitySideInputWindowFn;
import org.apache.beam.sdk.util.ReadyCheckingSideInputReader;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link PushbackSideInputDoFnRunner}.
 */
@RunWith(JUnit4.class)
public class PushbackSideInputDoFnRunnerTest {
  @Mock private ReadyCheckingSideInputReader reader;
  private TestDoFnRunner<Integer, Integer> underlying;
  private PCollectionView<Integer> singletonView;

  @Rule
  public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    PCollection<Integer> created = p.apply(Create.of(1, 2, 3));
    singletonView =
        created
            .apply(Window.into(new IdentitySideInputWindowFn()))
            .apply(Sum.integersGlobally().asSingletonView());

    underlying = new TestDoFnRunner<>();
  }

  private PushbackSideInputDoFnRunner<Integer, Integer> createRunner(
      ImmutableList<PCollectionView<?>> views) {
    PushbackSideInputDoFnRunner<Integer, Integer> runner =
        PushbackSideInputDoFnRunner.create(underlying, views, reader);
    runner.startBundle();
    return runner;
  }

  @Test
  public void startFinishBundleDelegates() {
    PushbackSideInputDoFnRunner runner =
        createRunner(ImmutableList.<PCollectionView<?>>of(singletonView));

    assertThat(underlying.started, is(true));
    assertThat(underlying.finished, is(false));
    runner.finishBundle();
    assertThat(underlying.finished, is(true));
  }

  @Test
  public void processElementSideInputNotReady() {
    when(reader.isReady(Mockito.eq(singletonView), Mockito.any(BoundedWindow.class)))
        .thenReturn(false);

    PushbackSideInputDoFnRunner<Integer, Integer> runner =
        createRunner(ImmutableList.<PCollectionView<?>>of(singletonView));

    WindowedValue<Integer> oneWindow =
        WindowedValue.of(
            2,
            new Instant(-2),
            new IntervalWindow(new Instant(-500L), new Instant(0L)),
            PaneInfo.ON_TIME_AND_ONLY_FIRING);
    Iterable<WindowedValue<Integer>> oneWindowPushback =
        runner.processElementInReadyWindows(oneWindow);
    assertThat(oneWindowPushback, containsInAnyOrder(oneWindow));
    assertThat(underlying.inputElems, Matchers.<WindowedValue<Integer>>emptyIterable());
  }

  @Test
  public void processElementSideInputNotReadyMultipleWindows() {
    when(reader.isReady(Mockito.eq(singletonView), Mockito.any(BoundedWindow.class)))
        .thenReturn(false);

    PushbackSideInputDoFnRunner<Integer, Integer> runner =
        createRunner(ImmutableList.<PCollectionView<?>>of(singletonView));

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
    assertThat(underlying.inputElems, Matchers.<WindowedValue<Integer>>emptyIterable());
  }

  @Test
  public void processElementSideInputNotReadySomeWindows() {
    when(reader.isReady(Mockito.eq(singletonView), Mockito.eq(GlobalWindow.INSTANCE)))
        .thenReturn(false);
    when(
            reader.isReady(
                Mockito.eq(singletonView),
                org.mockito.AdditionalMatchers.not(Mockito.eq(GlobalWindow.INSTANCE))))
        .thenReturn(true);

    PushbackSideInputDoFnRunner<Integer, Integer> runner =
        createRunner(ImmutableList.<PCollectionView<?>>of(singletonView));

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

    ImmutableList<PCollectionView<?>> views = ImmutableList.<PCollectionView<?>>of(singletonView);
    PushbackSideInputDoFnRunner<Integer, Integer> runner = createRunner(views);

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
    PushbackSideInputDoFnRunner<Integer, Integer> runner =
        createRunner(ImmutableList.<PCollectionView<?>>of());

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
    PushbackSideInputDoFnRunner<Integer, Integer> runner =
        createRunner(ImmutableList.<PCollectionView<?>>of());

    String timerId = "fooTimer";
    IntervalWindow window = new IntervalWindow(new Instant(4), new Instant(16));
    Instant timestamp = new Instant(72);

    // Mocking is not easily compatible with annotation analysis, so we manually record
    // the method call.
    runner.onTimer(timerId, window, new Instant(timestamp), TimeDomain.EVENT_TIME);

    assertThat(
        underlying.firedTimers,
        contains(
            TimerData.of(
                timerId,
                StateNamespaces.window(IntervalWindow.getCoder(), window),
                timestamp,
                TimeDomain.EVENT_TIME)));
  }

  private static class TestDoFnRunner<InputT, OutputT> implements DoFnRunner<InputT, OutputT> {
    List<WindowedValue<InputT>> inputElems;
    List<TimerData> firedTimers;
    private boolean started = false;
    private boolean finished = false;

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
    public void onTimer(String timerId, BoundedWindow window, Instant timestamp,
        TimeDomain timeDomain) {
      firedTimers.add(
          TimerData.of(
              timerId,
              StateNamespaces.window(IntervalWindow.getCoder(), (IntervalWindow) window),
              timestamp,
              timeDomain));
    }

    @Override
    public void finishBundle() {
      finished = true;
    }
  }
}
