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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.flink.api.common.JobID;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/** {@link SideInputReader} that caches single-pane materialized views within a TaskManager JVM. */
public final class CachedSideInputReader implements SideInputReader {

  public static CachedSideInputReader of(
      JobID jobId,
      int attemptNumber,
      SideInputReader delegate,
      Collection<PCollectionView<?>> cacheableViews) {
    return new CachedSideInputReader(jobId, attemptNumber, delegate, cacheableViews);
  }

  static Collection<PCollectionView<?>> cacheableViews(Collection<PCollectionView<?>> sideInputs) {
    Collection<PCollectionView<?>> cacheableViews = new ArrayList<>();
    for (PCollectionView<?> view : sideInputs) {
      PCollection<?> pCollection = view.getPCollection();
      WindowingStrategy<?, ?> strategy = view.getWindowingStrategyInternal();
      if (pCollection != null
          && pCollection.isBounded() == PCollection.IsBounded.BOUNDED
          && strategy.getTrigger() instanceof DefaultTrigger
          && Duration.ZERO.equals(strategy.getAllowedLateness())) {
        cacheableViews.add(view);
      }
    }
    return Collections.unmodifiableCollection(cacheableViews);
  }

  private final JobID jobId;
  private final int attemptNumber;
  private final SideInputReader delegate;
  private final Collection<PCollectionView<?>> cacheableViews;

  private CachedSideInputReader(
      JobID jobId,
      int attemptNumber,
      SideInputReader delegate,
      Collection<PCollectionView<?>> cacheableViews) {
    this.jobId = jobId;
    this.attemptNumber = attemptNumber;
    this.delegate = delegate;
    this.cacheableViews = cacheableViews;
  }

  @Override
  public <T> @Nullable T get(PCollectionView<T> view, BoundedWindow window) {
    if (!cacheableViews.contains(view)) {
      return delegate.get(view, window);
    }
    return SideInputCache.getOrMaterialize(
        jobId, attemptNumber, view, window, () -> delegate.get(view, window));
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return delegate.contains(view);
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }
}
