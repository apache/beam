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

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.flink.api.common.JobID;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Process-wide cache of materialized side-input views. */
final class SideInputCache {

  // Materialized view sizes are unknown to the runner, so the cache cannot be bounded by weight;
  // soft values let the JVM reclaim entries under memory pressure instead of failing with OOM.
  private static final Cache<Key<?>, Value<?>> MATERIALIZED_SIDE_INPUTS =
      CacheBuilder.newBuilder().expireAfterAccess(5, TimeUnit.MINUTES).softValues().build();

  private SideInputCache() {}

  static <T> @Nullable T getOrMaterialize(
      JobID jobId,
      PCollectionView<T> view,
      BoundedWindow window,
      Supplier<@Nullable T> materializer) {
    @SuppressWarnings("unchecked")
    Cache<Key<T>, Value<T>> cache =
        (Cache<Key<T>, Value<T>>) (Cache<?, ?>) MATERIALIZED_SIDE_INPUTS;
    try {
      return cache
          .get(new Key<>(jobId, view, window), () -> new Value<>(materializer.get()))
          .getValue();
    } catch (ExecutionException | UncheckedExecutionException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      Throwables.throwIfUnchecked(cause);
      throw new RuntimeException(cause);
    }
  }

  static void invalidate(JobID jobId, PCollectionView<?> view, BoundedWindow window) {
    MATERIALIZED_SIDE_INPUTS.invalidate(new Key<>(jobId, view, window));
  }

  static void invalidateAll(JobID jobId) {
    MATERIALIZED_SIDE_INPUTS.asMap().keySet().removeIf(key -> jobId.equals(key.jobId));
  }

  private static final class Key<T> {
    private final JobID jobId;
    private final PCollectionView<T> view;
    private final BoundedWindow window;

    private Key(JobID jobId, PCollectionView<T> view, BoundedWindow window) {
      this.jobId = jobId;
      this.view = view;
      this.window = window;
    }

    @Override
    public boolean equals(@Nullable Object object) {
      if (this == object) {
        return true;
      }
      if (!(object instanceof Key)) {
        return false;
      }
      Key<?> other = (Key<?>) object;
      return Objects.equals(jobId, other.jobId)
          && Objects.equals(view, other.view)
          && Objects.equals(window, other.window);
    }

    @Override
    public int hashCode() {
      return Objects.hash(jobId, view, window);
    }
  }

  /** Guava caches reject null values, but null is valid for a side-input reader. */
  private static final class Value<T> {
    private final @Nullable T value;

    private Value(@Nullable T value) {
      this.value = value;
    }

    private @Nullable T getValue() {
      return value;
    }
  }
}
