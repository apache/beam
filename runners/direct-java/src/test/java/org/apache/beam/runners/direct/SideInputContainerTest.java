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
package org.apache.beam.runners.direct;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;

import com.google.common.collect.ImmutableList;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing;
import org.apache.beam.sdk.util.PCollectionViews;
import org.apache.beam.sdk.util.ReadyCheckingSideInputReader;
import org.apache.beam.sdk.util.SideInputReader;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
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

/**
 * Tests for {@link SideInputContainer}.
 */
@RunWith(JUnit4.class)
public class SideInputContainerTest {
  private static final BoundedWindow FIRST_WINDOW =
      new BoundedWindow() {
        @Override
        public Instant maxTimestamp() {
          return new Instant(789541L);
        }

        @Override
        public String toString() {
          return "firstWindow";
        }
      };

  private static final BoundedWindow SECOND_WINDOW =
      new BoundedWindow() {
        @Override
        public Instant maxTimestamp() {
          return new Instant(14564786L);
        }

        @Override
        public String toString() {
          return "secondWindow";
        }
      };

  @Rule
  public TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Mock
  private EvaluationContext context;


  private SideInputContainer container;

  private PCollectionView<Map<String, Integer>> mapView;
  private PCollectionView<Double> singletonView;

  // Not present in container.
  private PCollectionView<Iterable<Integer>> iterableView;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    PCollection<Integer> create =
        pipeline.apply("forBaseCollection", Create.<Integer>of(1, 2, 3, 4));

    mapView =
        create.apply("forKeyTypes", WithKeys.<String, Integer>of("foo"))
            .apply("asMapView", View.<String, Integer>asMap());

    singletonView = create.apply("forCombinedTypes", Mean.<Integer>globally().asSingletonView());
    iterableView = create.apply("asIterableView", View.<Integer>asIterable());

    container = SideInputContainer.create(
        context, ImmutableList.of(iterableView, mapView, singletonView));
  }

  @Test
  public void getAfterWriteReturnsPaneInWindow() throws Exception {
    WindowedValue<KV<String, Integer>> one =
        WindowedValue.of(
            KV.of("one", 1), new Instant(1L), FIRST_WINDOW, PaneInfo.ON_TIME_AND_ONLY_FIRING);
    WindowedValue<KV<String, Integer>> two =
        WindowedValue.of(
            KV.of("two", 2), new Instant(20L), FIRST_WINDOW, PaneInfo.ON_TIME_AND_ONLY_FIRING);
    container.write(mapView, ImmutableList.<WindowedValue<?>>of(one, two));

    Map<String, Integer> viewContents =
        container
            .createReaderForViews(ImmutableList.<PCollectionView<?>>of(mapView))
            .get(mapView, FIRST_WINDOW);
    assertThat(viewContents, hasEntry("one", 1));
    assertThat(viewContents, hasEntry("two", 2));
    assertThat(viewContents.size(), is(2));
  }

  @Test
  public void getReturnsLatestPaneInWindow() throws Exception {
    WindowedValue<KV<String, Integer>> one =
        WindowedValue.of(
            KV.of("one", 1),
            new Instant(1L),
            SECOND_WINDOW,
            PaneInfo.createPane(true, false, Timing.EARLY));
    WindowedValue<KV<String, Integer>> two =
        WindowedValue.of(
            KV.of("two", 2),
            new Instant(20L),
            SECOND_WINDOW,
            PaneInfo.createPane(true, false, Timing.EARLY));
    container.write(mapView, ImmutableList.<WindowedValue<?>>of(one, two));

    Map<String, Integer> viewContents =
        container
            .createReaderForViews(ImmutableList.<PCollectionView<?>>of(mapView))
            .get(mapView, SECOND_WINDOW);
    assertThat(viewContents, hasEntry("one", 1));
    assertThat(viewContents, hasEntry("two", 2));
    assertThat(viewContents.size(), is(2));

    WindowedValue<KV<String, Integer>> three =
        WindowedValue.of(
            KV.of("three", 3),
            new Instant(300L),
            SECOND_WINDOW,
            PaneInfo.createPane(false, false, Timing.EARLY, 1, -1));
    container.write(mapView, ImmutableList.<WindowedValue<?>>of(three));

    Map<String, Integer> overwrittenViewContents =
        container
            .createReaderForViews(ImmutableList.<PCollectionView<?>>of(mapView))
            .get(mapView, SECOND_WINDOW);
    assertThat(overwrittenViewContents, hasEntry("three", 3));
    assertThat(overwrittenViewContents.size(), is(1));
  }

  /**
   * Demonstrates that calling get() on a window that currently has no data does not return until
   * there is data in the pane.
   */
  @Test
  public void getNotReadyThrows() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("not ready");

    container.createReaderForViews(ImmutableList.<PCollectionView<?>>of(mapView))
        .get(mapView, GlobalWindow.INSTANCE);
  }

  @Test
  public void withViewsForViewNotInContainerFails() {
    PCollection<KV<String, String>> input =
        pipeline.apply(Create.empty(new TypeDescriptor<KV<String, String>>() {}));
    PCollectionView<Map<String, Iterable<String>>> newView =
        PCollectionViews.multimapView(
            input,
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
    WindowedValue<Double> firstWindowedValue =
        WindowedValue.of(
            2.875,
            FIRST_WINDOW.maxTimestamp().minus(200L),
            FIRST_WINDOW,
            PaneInfo.ON_TIME_AND_ONLY_FIRING);
    WindowedValue<Double> secondWindowedValue =
        WindowedValue.of(
            4.125,
            SECOND_WINDOW.maxTimestamp().minus(2_000_000L),
            SECOND_WINDOW,
            PaneInfo.ON_TIME_AND_ONLY_FIRING);
    container.write(singletonView, ImmutableList.of(firstWindowedValue, secondWindowedValue));
    assertThat(
        container
            .createReaderForViews(ImmutableList.<PCollectionView<?>>of(singletonView))
            .get(singletonView, FIRST_WINDOW),
        equalTo(2.875));
    assertThat(
        container
            .createReaderForViews(ImmutableList.<PCollectionView<?>>of(singletonView))
            .get(singletonView, SECOND_WINDOW),
        equalTo(4.125));
  }

  @Test
  public void writeForMultipleIdenticalElementsInSameWindowSucceeds() throws Exception {
    WindowedValue<Integer> firstValue =
        WindowedValue.of(
            44,
            FIRST_WINDOW.maxTimestamp().minus(200L),
            FIRST_WINDOW,
            PaneInfo.ON_TIME_AND_ONLY_FIRING);
    WindowedValue<Integer> secondValue =
        WindowedValue.of(
            44,
            FIRST_WINDOW.maxTimestamp().minus(200L),
            FIRST_WINDOW,
            PaneInfo.ON_TIME_AND_ONLY_FIRING);

    container.write(iterableView, ImmutableList.of(firstValue, secondValue));

    assertThat(
        container
            .createReaderForViews(ImmutableList.<PCollectionView<?>>of(iterableView))
            .get(iterableView, FIRST_WINDOW),
        contains(44, 44));
  }

  @Test
  public void writeForElementInMultipleWindowsSucceeds() throws Exception {
    WindowedValue<Double> multiWindowedValue =
        WindowedValue.of(
            2.875,
            FIRST_WINDOW.maxTimestamp().minus(200L),
            ImmutableList.of(FIRST_WINDOW, SECOND_WINDOW),
            PaneInfo.ON_TIME_AND_ONLY_FIRING);
    container.write(singletonView, ImmutableList.of(multiWindowedValue));
    assertThat(
        container
            .createReaderForViews(ImmutableList.<PCollectionView<?>>of(singletonView))
            .get(singletonView, FIRST_WINDOW),
        equalTo(2.875));
    assertThat(
        container
            .createReaderForViews(ImmutableList.<PCollectionView<?>>of(singletonView))
            .get(singletonView, SECOND_WINDOW),
        equalTo(2.875));
  }

  @Test
  public void finishDoesNotOverwriteWrittenElements() throws Exception {
    WindowedValue<KV<String, Integer>> one =
        WindowedValue.of(
            KV.of("one", 1),
            new Instant(1L),
            SECOND_WINDOW,
            PaneInfo.createPane(true, false, Timing.EARLY));
    WindowedValue<KV<String, Integer>> two =
        WindowedValue.of(
            KV.of("two", 2),
            new Instant(20L),
            SECOND_WINDOW,
            PaneInfo.createPane(true, false, Timing.EARLY));
    container.write(mapView, ImmutableList.<WindowedValue<?>>of(one, two));

    immediatelyInvokeCallback(mapView, SECOND_WINDOW);

    Map<String, Integer> viewContents =
        container
            .createReaderForViews(ImmutableList.<PCollectionView<?>>of(mapView))
            .get(mapView, SECOND_WINDOW);

    assertThat(viewContents, hasEntry("one", 1));
    assertThat(viewContents, hasEntry("two", 2));
    assertThat(viewContents.size(), is(2));
  }

  @Test
  public void finishOnPendingViewsSetsEmptyElements() throws Exception {
    immediatelyInvokeCallback(mapView, SECOND_WINDOW);
    Future<Map<String, Integer>> mapFuture =
        getFutureOfView(
            container.createReaderForViews(ImmutableList.<PCollectionView<?>>of(mapView)),
            mapView,
            SECOND_WINDOW);

    assertThat(mapFuture.get().isEmpty(), is(true));
  }

  /**
   * Demonstrates that calling isReady on an empty container throws an
   * {@link IllegalArgumentException}.
   */
  @Test
  public void isReadyInEmptyReaderThrows() {
    ReadyCheckingSideInputReader reader =
        container.createReaderForViews(ImmutableList.<PCollectionView<?>>of());
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("does not contain");
    thrown.expectMessage(ImmutableList.of().toString());
    reader.isReady(mapView, GlobalWindow.INSTANCE);
  }

  /**
   * Demonstrates that calling isReady returns false until elements are written to the
   * {@link PCollectionView}, {@link BoundedWindow} pair, at which point it returns true.
   */
  @Test
  public void isReadyForSomeNotReadyViewsFalseUntilElements() {
    container.write(
        mapView,
        ImmutableList.of(
            WindowedValue.of(
                KV.of("one", 1),
                SECOND_WINDOW.maxTimestamp().minus(100L),
                SECOND_WINDOW,
                PaneInfo.ON_TIME_AND_ONLY_FIRING)));

    ReadyCheckingSideInputReader reader =
        container.createReaderForViews(ImmutableList.of(mapView, singletonView));
    assertThat(reader.isReady(mapView, FIRST_WINDOW), is(false));
    assertThat(reader.isReady(mapView, SECOND_WINDOW), is(true));

    assertThat(reader.isReady(singletonView, SECOND_WINDOW), is(false));

    container.write(
        mapView,
        ImmutableList.of(
            WindowedValue.of(
                KV.of("too", 2),
                FIRST_WINDOW.maxTimestamp().minus(100L),
                FIRST_WINDOW,
                PaneInfo.ON_TIME_AND_ONLY_FIRING)));
    // Cached value is false
    assertThat(reader.isReady(mapView, FIRST_WINDOW), is(false));

    container.write(
        singletonView,
        ImmutableList.of(
            WindowedValue.of(
                1.25,
                SECOND_WINDOW.maxTimestamp().minus(100L),
                SECOND_WINDOW,
                PaneInfo.ON_TIME_AND_ONLY_FIRING)));
    assertThat(reader.isReady(mapView, SECOND_WINDOW), is(true));
    assertThat(reader.isReady(singletonView, SECOND_WINDOW), is(false));

    assertThat(reader.isReady(mapView, GlobalWindow.INSTANCE), is(false));
    assertThat(reader.isReady(singletonView, GlobalWindow.INSTANCE), is(false));

    reader = container.createReaderForViews(ImmutableList.of(mapView, singletonView));
    assertThat(reader.isReady(mapView, SECOND_WINDOW), is(true));
    assertThat(reader.isReady(singletonView, SECOND_WINDOW), is(true));
    assertThat(reader.isReady(mapView, FIRST_WINDOW), is(true));
  }

  @Test
  public void isReadyForEmptyWindowTrue() throws Exception {
    CountDownLatch onComplete = new CountDownLatch(1);
    immediatelyInvokeCallback(mapView, GlobalWindow.INSTANCE);
    CountDownLatch latch = invokeLatchedCallback(singletonView, GlobalWindow.INSTANCE, onComplete);

    ReadyCheckingSideInputReader reader =
        container.createReaderForViews(ImmutableList.of(mapView, singletonView));
    assertThat(reader.isReady(mapView, GlobalWindow.INSTANCE), is(true));
    assertThat(reader.isReady(singletonView, GlobalWindow.INSTANCE), is(false));

    latch.countDown();
    if (!onComplete.await(1500L, TimeUnit.MILLISECONDS)) {
      fail("Callback to set empty values did not complete!");
    }
    // The cached value was false, so it continues to be true
    assertThat(reader.isReady(singletonView, GlobalWindow.INSTANCE), is(false));

    // A new reader for the same container gets a fresh look
    reader = container.createReaderForViews(ImmutableList.of(mapView, singletonView));
    assertThat(reader.isReady(singletonView, GlobalWindow.INSTANCE), is(true));
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

  /**
   * When a callAfterWindowCloses with the specified view's producing transform, window, and
   * windowing strategy is invoked, start a thread that will invoke the callback after the returned
   * {@link CountDownLatch} is counted down once.
   */
  private CountDownLatch invokeLatchedCallback(
      PCollectionView<?> view, BoundedWindow window, final CountDownLatch onComplete) {
    final CountDownLatch runLatch = new CountDownLatch(1);
    doAnswer(
        new Answer<Void>() {
          @Override
          public Void answer(InvocationOnMock invocation) throws Throwable {
            Object callback = invocation.getArguments()[3];
            final Runnable callbackRunnable = (Runnable) callback;
            Executors.newSingleThreadExecutor().submit(new Runnable() {
              @Override
              public void run() {
                try {
                  if (!runLatch.await(1500L, TimeUnit.MILLISECONDS)) {
                    fail("Run latch didn't count down within timeout");
                  }
                  callbackRunnable.run();
                  onComplete.countDown();
                } catch (InterruptedException e) {
                  fail("Unexpectedly interrupted while waiting for latch to be counted down");
                }
              }
            });
            return null;
          }
        })
        .when(context)
        .scheduleAfterOutputWouldBeProduced(
            Mockito.eq(view),
            Mockito.eq(window),
            Mockito.eq(view.getWindowingStrategyInternal()),
            Mockito.any(Runnable.class));
    return runLatch;
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
