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
import static org.mockito.Mockito.mock;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.flink.api.common.JobID;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.junit.Test;

/** Tests for cached materialization of Flink side-input views. */
public class FlinkCachedSideInputReaderTest {

  @Test
  public void repeatedGetMaterializesOnce() {
    JobID jobId = new JobID();
    PCollectionView<String> view = view();
    CountingSideInputReader delegate = new CountingSideInputReader("value");
    SideInputReader reader = CachedSideInputReader.of(jobId, delegate);

    assertThat(reader.get(view, GlobalWindow.INSTANCE), is("value"));
    assertThat(reader.get(view, GlobalWindow.INSTANCE), is("value"));
    assertThat(delegate.getCount(), is(1));
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

    CachedSideInputReader.of(firstJob, delegate).get(firstView, firstWindow);
    CachedSideInputReader.of(firstJob, delegate).get(secondView, firstWindow);
    CachedSideInputReader.of(firstJob, delegate).get(firstView, secondWindow);
    CachedSideInputReader.of(new JobID(), delegate).get(firstView, firstWindow);

    assertThat(delegate.getCount(), is(4));
  }

  @Test
  public void invalidateRematerializesValue() {
    JobID jobId = new JobID();
    PCollectionView<String> view = view();
    CountingSideInputReader delegate = new CountingSideInputReader("value");
    SideInputReader reader = CachedSideInputReader.of(jobId, delegate);

    reader.get(view, GlobalWindow.INSTANCE);
    SideInputCache.invalidate(jobId, view, GlobalWindow.INSTANCE);
    reader.get(view, GlobalWindow.INSTANCE);

    assertThat(delegate.getCount(), is(2));
  }

  @Test
  public void cachesNull() {
    JobID jobId = new JobID();
    PCollectionView<String> view = view();
    CountingSideInputReader delegate = new CountingSideInputReader(null);
    SideInputReader reader = CachedSideInputReader.of(jobId, delegate);

    assertThat(reader.get(view, GlobalWindow.INSTANCE), nullValue());
    assertThat(reader.get(view, GlobalWindow.INSTANCE), nullValue());
    assertThat(delegate.getCount(), is(1));
  }

  @Test
  public void optionWrapsOnlyBatchReaderWhenEnabled() {
    JobID jobId = new JobID();
    SideInputReader delegate = new CountingSideInputReader("value");
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);

    assertThat(DoFnOperator.createSideInputReader(false, options, jobId, delegate), is(delegate));

    options.setCacheSideInputMaterialization(true);
    assertThat(
        DoFnOperator.createSideInputReader(false, options, jobId, delegate),
        instanceOf(CachedSideInputReader.class));
    assertThat(DoFnOperator.createSideInputReader(true, options, jobId, delegate), is(delegate));
  }

  @Test
  public void invalidateAllRemovesOnlyEntriesOfJob() {
    JobID firstJob = new JobID();
    JobID secondJob = new JobID();
    PCollectionView<String> view = view();
    CountingSideInputReader delegate = new CountingSideInputReader("value");
    CachedSideInputReader.of(firstJob, delegate).get(view, GlobalWindow.INSTANCE);
    CachedSideInputReader.of(secondJob, delegate).get(view, GlobalWindow.INSTANCE);

    SideInputCache.invalidateAll(firstJob);

    CachedSideInputReader.of(secondJob, delegate).get(view, GlobalWindow.INSTANCE);
    assertThat(delegate.getCount(), is(2));
    CachedSideInputReader.of(firstJob, delegate).get(view, GlobalWindow.INSTANCE);
    assertThat(delegate.getCount(), is(3));
  }

  @Test
  public void materializationExceptionPropagatesUnwrapped() {
    SideInputReader reader =
        CachedSideInputReader.of(
            new JobID(),
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
            });

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> reader.get(view(), GlobalWindow.INSTANCE));
    assertThat(exception.getMessage(), is("materialization failed"));
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
