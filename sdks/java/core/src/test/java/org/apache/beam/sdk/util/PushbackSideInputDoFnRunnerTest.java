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
package org.apache.beam.sdk.util;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.common.collect.ImmutableList;

import org.joda.time.Instant;
import org.junit.Before;
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
  @Mock private DoFnRunner<Integer, Integer> underlying;
  private PCollectionView<Integer> singletonView;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    TestPipeline p = TestPipeline.create();
    PCollection<Integer> created = p.apply(Create.of(1, 2, 3));
    singletonView =
        created
            .apply(Window.into(new IdentitySideInputWindowFn()))
            .apply(Sum.integersGlobally().asSingletonView());
  }

  @Test
  public void processElementSideInputNotReady() {
    when(reader.isReady(Mockito.eq(singletonView), Mockito.any(BoundedWindow.class)))
        .thenReturn(false);

    PushbackSideInputDoFnRunner<Integer, Integer> runner =
        PushbackSideInputDoFnRunner.create(
            underlying, ImmutableList.<PCollectionView<?>>of(singletonView), reader);
    runner.startBundle();

    WindowedValue<Integer> oneWindow =
        WindowedValue.of(
            2,
            new Instant(-2),
            new IntervalWindow(new Instant(-500L), new Instant(0L)),
            PaneInfo.ON_TIME_AND_ONLY_FIRING);
    Iterable<WindowedValue<Integer>> oneWindowPushback =
        runner.processElementInReadyWindows(oneWindow);
    assertThat(oneWindowPushback, containsInAnyOrder(oneWindow));
    verify(underlying, never()).processElement(Mockito.any(WindowedValue.class));
  }

  @Test
  public void processElementSideInputNotReadyMultipleWindows() {
    when(reader.isReady(Mockito.eq(singletonView), Mockito.any(BoundedWindow.class)))
        .thenReturn(false);

    PushbackSideInputDoFnRunner<Integer, Integer> runner =
        PushbackSideInputDoFnRunner.create(
            underlying, ImmutableList.<PCollectionView<?>>of(singletonView), reader);

    runner.startBundle();
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
    verify(underlying, never()).processElement(Mockito.any(WindowedValue.class));
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
        PushbackSideInputDoFnRunner.create(
            underlying, ImmutableList.<PCollectionView<?>>of(singletonView), reader);
    runner.startBundle();

    WindowedValue<Integer> multiWindow =
        WindowedValue.of(
            2,
            new Instant(-2),
            ImmutableList.of(
                new IntervalWindow(new Instant(-500L), new Instant(0L)),
                new IntervalWindow(BoundedWindow.TIMESTAMP_MIN_VALUE, new Instant(250L)),
                GlobalWindow.INSTANCE),
            PaneInfo.NO_FIRING);
    Iterable<WindowedValue<Integer>> multiWindowPushback =
        runner.processElementInReadyWindows(multiWindow);
    assertThat(
        multiWindowPushback,
        containsInAnyOrder(WindowedValue.timestampedValueInGlobalWindow(2, new Instant(-2L))));
    verify(underlying, times(2)).processElement(Mockito.any(WindowedValue.class));
  }

  @Test
  public void processElementSideInputReadyAllWindows() {
    when(reader.isReady(Mockito.eq(singletonView), Mockito.any(BoundedWindow.class)))
        .thenReturn(true);

    PushbackSideInputDoFnRunner<Integer, Integer> runner =
        PushbackSideInputDoFnRunner.create(
            underlying, ImmutableList.<PCollectionView<?>>of(singletonView), reader);
    runner.startBundle();

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
    for (WindowedValue<Integer> explodedValue : multiWindow.explodeWindows()) {
      verify(underlying).processElement(explodedValue);
    }
  }

  @Test
  public void processElementNoSideInputs() {
    PushbackSideInputDoFnRunner<Integer, Integer> runner = PushbackSideInputDoFnRunner.create(
        underlying,
        ImmutableList.<PCollectionView<?>>of(),
        reader);

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
    verify(underlying).processElement(multiWindow);
  }
}
