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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.flink.api.common.JobID;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;

/** Tests for cached materialization of Flink side-input views. */
public class FlinkCachedSideInputReaderTest {

  private static final int INITIAL_ATTEMPT = 0;
  private static final int RETRY_ATTEMPT = 1;

  @Test
  public void repeatedGetMaterializesOnce() {
    JobID jobId = new JobID();
    PCollectionView<String> view = view();
    CountingSideInputReader delegate = new CountingSideInputReader("value");
    SideInputReader reader =
        CachedSideInputReader.of(jobId, INITIAL_ATTEMPT, delegate, Collections.singleton(view));

    assertThat(reader.get(view, GlobalWindow.INSTANCE), is("value"));
    assertThat(reader.get(view, GlobalWindow.INSTANCE), is("value"));
    assertThat(delegate.getCount(), is(1));
  }

  @Test
  public void readerInstancesForSameJobShareMaterialization() {
    JobID jobId = new JobID();
    PCollectionView<String> view = view();
    CountingSideInputReader delegate = new CountingSideInputReader("value");
    Collection<PCollectionView<?>> views = Collections.singleton(view);

    CachedSideInputReader.of(jobId, INITIAL_ATTEMPT, delegate, views)
        .get(view, GlobalWindow.INSTANCE);
    CachedSideInputReader.of(jobId, INITIAL_ATTEMPT, delegate, views)
        .get(view, GlobalWindow.INSTANCE);

    assertThat(delegate.getCount(), is(1));
  }

  @Test
  public void retryAttemptRematerializesValue() {
    JobID jobId = new JobID();
    PCollectionView<String> view = view();
    CountingSideInputReader delegate = new CountingSideInputReader("value");
    Collection<PCollectionView<?>> views = Collections.singleton(view);

    CachedSideInputReader.of(jobId, INITIAL_ATTEMPT, delegate, views)
        .get(view, GlobalWindow.INSTANCE);
    CachedSideInputReader.of(jobId, RETRY_ATTEMPT, delegate, views)
        .get(view, GlobalWindow.INSTANCE);

    assertThat(delegate.getCount(), is(2));
  }

  @Test
  public void keyIncludesViewWindowAndJob() {
    PCollectionView<String> firstView = view();
    PCollectionView<String> secondView = view();
    IntervalWindow firstWindow = new IntervalWindow(Instant.EPOCH, Instant.ofEpochMilli(10));
    IntervalWindow secondWindow =
        new IntervalWindow(Instant.ofEpochMilli(10), Instant.ofEpochMilli(20));
    CountingSideInputReader delegate = new CountingSideInputReader("value");
    JobID firstJob = new JobID();

    Collection<PCollectionView<?>> views = Arrays.asList(firstView, secondView);
    CachedSideInputReader.of(firstJob, INITIAL_ATTEMPT, delegate, views)
        .get(firstView, firstWindow);
    CachedSideInputReader.of(firstJob, INITIAL_ATTEMPT, delegate, views)
        .get(secondView, firstWindow);
    CachedSideInputReader.of(firstJob, INITIAL_ATTEMPT, delegate, views)
        .get(firstView, secondWindow);
    CachedSideInputReader.of(new JobID(), INITIAL_ATTEMPT, delegate, views)
        .get(firstView, firstWindow);

    assertThat(delegate.getCount(), is(4));
  }

  @Test
  public void invalidateRematerializesValue() {
    JobID jobId = new JobID();
    PCollectionView<String> view = view();
    CountingSideInputReader delegate = new CountingSideInputReader("value");
    SideInputReader reader =
        CachedSideInputReader.of(jobId, INITIAL_ATTEMPT, delegate, Collections.singleton(view));

    reader.get(view, GlobalWindow.INSTANCE);
    SideInputCache.invalidate(jobId, INITIAL_ATTEMPT, view, GlobalWindow.INSTANCE);
    reader.get(view, GlobalWindow.INSTANCE);

    assertThat(delegate.getCount(), is(2));
  }

  @Test
  public void cachesNull() {
    JobID jobId = new JobID();
    PCollectionView<String> view = view();
    CountingSideInputReader delegate = new CountingSideInputReader(null);
    SideInputReader reader =
        CachedSideInputReader.of(jobId, INITIAL_ATTEMPT, delegate, Collections.singleton(view));

    assertThat(reader.get(view, GlobalWindow.INSTANCE), nullValue());
    assertThat(reader.get(view, GlobalWindow.INSTANCE), nullValue());
    assertThat(delegate.getCount(), is(1));
  }

  @Test
  public void automaticallyWrapsReaderWithCacheableViews() {
    JobID jobId = new JobID();
    SideInputReader delegate = new CountingSideInputReader("value");
    PCollectionView<String> view = cacheableView();
    Collection<PCollectionView<?>> cacheableViews =
        CachedSideInputReader.cacheableViews(Collections.singleton(view));

    assertThat(
        DoFnOperator.createSideInputReader(
            Collections.emptyList(), jobId, INITIAL_ATTEMPT, delegate),
        is(delegate));

    assertThat(
        DoFnOperator.createSideInputReader(cacheableViews, jobId, INITIAL_ATTEMPT, delegate),
        instanceOf(CachedSideInputReader.class));
  }

  @Test
  public void selectsOnlyBoundedDefaultTriggerViewsWithoutLateness() {
    PCollectionView<String> cacheableView = cacheableView();
    PCollectionView<String> unboundedView =
        view(PCollection.IsBounded.UNBOUNDED, DefaultTrigger.of(), Duration.ZERO);
    PCollectionView<String> customTriggerView =
        view(PCollection.IsBounded.BOUNDED, mock(Trigger.class), Duration.ZERO);
    PCollectionView<String> lateDataView =
        view(PCollection.IsBounded.BOUNDED, DefaultTrigger.of(), Duration.standardMinutes(1));

    Collection<PCollectionView<?>> cacheableViews =
        CachedSideInputReader.cacheableViews(
            Arrays.asList(cacheableView, unboundedView, customTriggerView, lateDataView));

    assertThat(cacheableViews.size(), is(1));
    assertThat(cacheableViews.contains(cacheableView), is(true));
  }

  @Test
  public void nonCacheableViewAlwaysUsesDelegate() {
    PCollectionView<String> cacheableView = view();
    PCollectionView<String> nonCacheableView = view();
    CountingSideInputReader delegate = new CountingSideInputReader("value");
    SideInputReader reader =
        CachedSideInputReader.of(
            new JobID(), INITIAL_ATTEMPT, delegate, Collections.singleton(cacheableView));

    reader.get(cacheableView, GlobalWindow.INSTANCE);
    reader.get(cacheableView, GlobalWindow.INSTANCE);
    reader.get(nonCacheableView, GlobalWindow.INSTANCE);
    reader.get(nonCacheableView, GlobalWindow.INSTANCE);

    assertThat(delegate.getCount(), is(3));
  }

  @Test
  public void materializationExceptionPropagatesUnwrapped() {
    PCollectionView<String> view = view();
    SideInputReader reader =
        CachedSideInputReader.of(
            new JobID(),
            INITIAL_ATTEMPT,
            new SideInputReader() {
              @Override
              public <T> @Nullable T get(PCollectionView<T> view, BoundedWindow window) {
                throw new IllegalStateException("materialization failed");
              }

              @Override
              public <T> boolean contains(PCollectionView<T> view) {
                return true;
              }

              @Override
              public boolean isEmpty() {
                return false;
              }
            },
            Collections.singleton(view));

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> reader.get(view, GlobalWindow.INSTANCE));
    assertThat(exception.getMessage(), is("materialization failed"));
  }

  private static <T> PCollectionView<T> cacheableView() {
    return view(PCollection.IsBounded.BOUNDED, DefaultTrigger.of(), Duration.ZERO);
  }

  @SuppressWarnings("unchecked")
  private static <T> PCollectionView<T> view(
      PCollection.IsBounded bounded, Trigger trigger, Duration allowedLateness) {
    PCollectionView<T> view = mock(PCollectionView.class);
    PCollection<T> pCollection = mock(PCollection.class);
    WindowingStrategy<?, ?> strategy = mock(WindowingStrategy.class);
    doReturn(pCollection).when(view).getPCollection();
    when(pCollection.isBounded()).thenReturn(bounded);
    doReturn(strategy).when(view).getWindowingStrategyInternal();
    when(strategy.getTrigger()).thenReturn(trigger);
    when(strategy.getAllowedLateness()).thenReturn(allowedLateness);
    return view;
  }

  @SuppressWarnings("unchecked")
  private static <T> PCollectionView<T> view() {
    return mock(PCollectionView.class);
  }

  private static final class CountingSideInputReader implements SideInputReader {
    private final AtomicInteger getCount = new AtomicInteger();
    private final @Nullable Object value;

    private CountingSideInputReader(@Nullable Object value) {
      this.value = value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> @Nullable T get(PCollectionView<T> view, BoundedWindow window) {
      getCount.incrementAndGet();
      return (T) value;
    }

    @Override
    public <T> boolean contains(PCollectionView<T> view) {
      return true;
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    private int getCount() {
      return getCount.get();
    }
  }
}
