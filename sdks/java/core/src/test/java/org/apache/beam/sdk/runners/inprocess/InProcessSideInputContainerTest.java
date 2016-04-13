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
package com.google.cloud.dataflow.sdk.runners.inprocess;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doAnswer;

import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.Mean;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.transforms.WithKeys;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo.Timing;
import com.google.cloud.dataflow.sdk.util.PCollectionViews;
import com.google.cloud.dataflow.sdk.util.SideInputReader;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.common.collect.ImmutableList;

import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Tests for {@link InProcessSideInputContainer}.
 */
@RunWith(JUnit4.class)
public class InProcessSideInputContainerTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Mock
  private InProcessEvaluationContext context;

  private TestPipeline pipeline;

  private InProcessSideInputContainer container;

  private PCollectionView<Map<String, Integer>> mapView;
  private PCollectionView<Double> singletonView;

  // Not present in container.
  private PCollectionView<Iterable<Integer>> iterableView;

  private BoundedWindow firstWindow = new BoundedWindow() {
    @Override
    public Instant maxTimestamp() {
      return new Instant(789541L);
    }

    @Override
    public String toString() {
      return "firstWindow";
    }
  };

  private BoundedWindow secondWindow = new BoundedWindow() {
    @Override
    public Instant maxTimestamp() {
      return new Instant(14564786L);
    }

    @Override
    public String toString() {
      return "secondWindow";
    }
  };

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    pipeline = TestPipeline.create();

    PCollection<Integer> create =
        pipeline.apply("forBaseCollection", Create.<Integer>of(1, 2, 3, 4));

    mapView =
        create.apply("forKeyTypes", WithKeys.<String, Integer>of("foo"))
            .apply("asMapView", View.<String, Integer>asMap());

    singletonView =
        create.apply("forCombinedTypes", Mean.<Integer>globally())
            .apply("asDoubleView", View.<Double>asSingleton());

    iterableView = create.apply("asIterableView", View.<Integer>asIterable());

    container = InProcessSideInputContainer.create(
        context, ImmutableList.of(iterableView, mapView, singletonView));
  }

  @Test
  public void getAfterWriteReturnsPaneInWindow() throws Exception {
    WindowedValue<KV<String, Integer>> one = WindowedValue.of(
        KV.of("one", 1), new Instant(1L), firstWindow, PaneInfo.ON_TIME_AND_ONLY_FIRING);
    WindowedValue<KV<String, Integer>> two = WindowedValue.of(
        KV.of("two", 2), new Instant(20L), firstWindow, PaneInfo.ON_TIME_AND_ONLY_FIRING);
    container.write(mapView, ImmutableList.<WindowedValue<?>>of(one, two));

    Map<String, Integer> viewContents =
        container.createReaderForViews(ImmutableList.<PCollectionView<?>>of(mapView))
            .get(mapView, firstWindow);
    assertThat(viewContents, hasEntry("one", 1));
    assertThat(viewContents, hasEntry("two", 2));
    assertThat(viewContents.size(), is(2));
  }

  @Test
  public void getReturnsLatestPaneInWindow() throws Exception {
    WindowedValue<KV<String, Integer>> one = WindowedValue.of(KV.of("one", 1), new Instant(1L),
        secondWindow, PaneInfo.createPane(true, false, Timing.EARLY));
    WindowedValue<KV<String, Integer>> two = WindowedValue.of(KV.of("two", 2), new Instant(20L),
        secondWindow, PaneInfo.createPane(true, false, Timing.EARLY));
    container.write(mapView, ImmutableList.<WindowedValue<?>>of(one, two));

    Map<String, Integer> viewContents =
        container.createReaderForViews(ImmutableList.<PCollectionView<?>>of(mapView))
            .get(mapView, secondWindow);
    assertThat(viewContents, hasEntry("one", 1));
    assertThat(viewContents, hasEntry("two", 2));
    assertThat(viewContents.size(), is(2));

    WindowedValue<KV<String, Integer>> three = WindowedValue.of(KV.of("three", 3),
        new Instant(300L), secondWindow, PaneInfo.createPane(false, false, Timing.EARLY, 1, -1));
    container.write(mapView, ImmutableList.<WindowedValue<?>>of(three));

    Map<String, Integer> overwrittenViewContents =
        container.createReaderForViews(ImmutableList.<PCollectionView<?>>of(mapView))
            .get(mapView, secondWindow);
    assertThat(overwrittenViewContents, hasEntry("three", 3));
    assertThat(overwrittenViewContents.size(), is(1));
  }

  /**
   * Demonstrates that calling get() on a window that currently has no data does not return until
   * there is data in the pane.
   */
  @Test
  public void getBlocksUntilPaneAvailable() throws Exception {
    BoundedWindow window =
        new BoundedWindow() {
          @Override
          public Instant maxTimestamp() {
            return new Instant(1024L);
          }
        };
    Future<Double> singletonFuture =
        getFutureOfView(
            container.createReaderForViews(ImmutableList.<PCollectionView<?>>of(singletonView)),
            singletonView,
            window);

    WindowedValue<Double> singletonValue =
        WindowedValue.of(4.75, new Instant(475L), window, PaneInfo.ON_TIME_AND_ONLY_FIRING);

    assertThat(singletonFuture.isDone(), is(false));
    container.write(singletonView, ImmutableList.<WindowedValue<?>>of(singletonValue));
    assertThat(singletonFuture.get(), equalTo(4.75));
  }

  @Test
  public void withPCollectionViewsWithPutInOriginalReturnsContents() throws Exception {
    BoundedWindow window = new BoundedWindow() {
      @Override
      public Instant maxTimestamp() {
        return new Instant(1024L);
      }
    };
    SideInputReader newReader =
        container.createReaderForViews(ImmutableList.<PCollectionView<?>>of(singletonView));
    Future<Double> singletonFuture = getFutureOfView(newReader, singletonView, window);

    WindowedValue<Double> singletonValue =
        WindowedValue.of(24.125, new Instant(475L), window, PaneInfo.ON_TIME_AND_ONLY_FIRING);

    assertThat(singletonFuture.isDone(), is(false));
    container.write(singletonView, ImmutableList.<WindowedValue<?>>of(singletonValue));
    assertThat(singletonFuture.get(), equalTo(24.125));
  }

  @Test
  public void withPCollectionViewsErrorsForContainsNotInViews() {
    PCollectionView<Map<String, Iterable<String>>> newView =
        PCollectionViews.multimapView(
            pipeline,
            WindowingStrategy.globalDefault(),
            KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("with unknown views " + ImmutableList.of(newView).toString());

    container.createReaderForViews(ImmutableList.<PCollectionView<?>>of(newView));
  }

  @Test
  public void withViewsForViewNotInContainerFails() {
    PCollectionView<Map<String, Iterable<String>>> newView =
        PCollectionViews.multimapView(
            pipeline,
            WindowingStrategy.globalDefault(),
            KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("unknown views");
    thrown.expectMessage(newView.toString());

    container.createReaderForViews(ImmutableList.<PCollectionView<?>>of(newView));
  }

  @Test
  public void getOnReaderForViewNotInReaderFails() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("unknown view: " + iterableView.toString());

    container.createReaderForViews(ImmutableList.<PCollectionView<?>>of(mapView))
        .get(iterableView, GlobalWindow.INSTANCE);
  }

  @Test
  public void writeForMultipleElementsInDifferentWindowsSucceeds() throws Exception {
    WindowedValue<Double> firstWindowedValue = WindowedValue.of(2.875,
        firstWindow.maxTimestamp().minus(200L), firstWindow, PaneInfo.ON_TIME_AND_ONLY_FIRING);
    WindowedValue<Double> secondWindowedValue =
        WindowedValue.of(4.125, secondWindow.maxTimestamp().minus(2_000_000L), secondWindow,
            PaneInfo.ON_TIME_AND_ONLY_FIRING);
    container.write(singletonView, ImmutableList.of(firstWindowedValue, secondWindowedValue));
    assertThat(
        container.createReaderForViews(ImmutableList.<PCollectionView<?>>of(singletonView))
            .get(singletonView, firstWindow),
        equalTo(2.875));
    assertThat(
        container.createReaderForViews(ImmutableList.<PCollectionView<?>>of(singletonView))
            .get(singletonView, secondWindow),
        equalTo(4.125));
  }

  @Test
  public void writeForMultipleIdenticalElementsInSameWindowSucceeds() throws Exception {
    WindowedValue<Integer> firstValue = WindowedValue.of(
        44, firstWindow.maxTimestamp().minus(200L), firstWindow, PaneInfo.ON_TIME_AND_ONLY_FIRING);
    WindowedValue<Integer> secondValue = WindowedValue.of(
        44, firstWindow.maxTimestamp().minus(200L), firstWindow, PaneInfo.ON_TIME_AND_ONLY_FIRING);

    container.write(iterableView, ImmutableList.of(firstValue, secondValue));

    assertThat(
        container.createReaderForViews(ImmutableList.<PCollectionView<?>>of(iterableView))
            .get(iterableView, firstWindow),
        contains(44, 44));
  }

  @Test
  public void writeForElementInMultipleWindowsSucceeds() throws Exception {
    WindowedValue<Double> multiWindowedValue =
        WindowedValue.of(2.875, firstWindow.maxTimestamp().minus(200L),
            ImmutableList.of(firstWindow, secondWindow), PaneInfo.ON_TIME_AND_ONLY_FIRING);
    container.write(singletonView, ImmutableList.of(multiWindowedValue));
    assertThat(
        container.createReaderForViews(ImmutableList.<PCollectionView<?>>of(singletonView))
            .get(singletonView, firstWindow),
        equalTo(2.875));
    assertThat(
        container.createReaderForViews(ImmutableList.<PCollectionView<?>>of(singletonView))
            .get(singletonView, secondWindow),
        equalTo(2.875));
  }

  @Test
  public void finishDoesNotOverwriteWrittenElements() throws Exception {
    WindowedValue<KV<String, Integer>> one = WindowedValue.of(KV.of("one", 1), new Instant(1L),
        secondWindow, PaneInfo.createPane(true, false, Timing.EARLY));
    WindowedValue<KV<String, Integer>> two = WindowedValue.of(KV.of("two", 2), new Instant(20L),
        secondWindow, PaneInfo.createPane(true, false, Timing.EARLY));
    container.write(mapView, ImmutableList.<WindowedValue<?>>of(one, two));

    immediatelyInvokeCallback(mapView, secondWindow);

    Map<String, Integer> viewContents =
        container.createReaderForViews(ImmutableList.<PCollectionView<?>>of(mapView))
            .get(mapView, secondWindow);

    assertThat(viewContents, hasEntry("one", 1));
    assertThat(viewContents, hasEntry("two", 2));
    assertThat(viewContents.size(), is(2));
  }

  @Test
  public void finishOnPendingViewsSetsEmptyElements() throws Exception {
    immediatelyInvokeCallback(mapView, secondWindow);
    Future<Map<String, Integer>> mapFuture =
        getFutureOfView(
            container.createReaderForViews(ImmutableList.<PCollectionView<?>>of(mapView)),
            mapView,
            secondWindow);

    assertThat(mapFuture.get().isEmpty(), is(true));
  }

  /**
   * When a callAfterWindowCloses with the specified view's producing transform, window, and
   * windowing strategy is invoked, immediately execute the callback.
   */
  private void immediatelyInvokeCallback(PCollectionView<?> view, BoundedWindow window) {
    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocation) throws Throwable {
                Object callback = invocation.getArguments()[3];
                Runnable callbackRunnable = (Runnable) callback;
                callbackRunnable.run();
                return null;
              }
            })
        .when(context)
        .scheduleAfterOutputWouldBeProduced(
            Mockito.eq(view),
            Mockito.eq(window),
            Mockito.eq(view.getWindowingStrategyInternal()),
            Mockito.any(Runnable.class));
  }

  private <ValueT> Future<ValueT> getFutureOfView(final SideInputReader myReader,
      final PCollectionView<ValueT> view, final BoundedWindow window) {
    Callable<ValueT> callable = new Callable<ValueT>() {
      @Override
      public ValueT call() throws Exception {
        return myReader.get(view, window);
      }
    };
    return Executors.newSingleThreadExecutor().submit(callable);
  }
}
