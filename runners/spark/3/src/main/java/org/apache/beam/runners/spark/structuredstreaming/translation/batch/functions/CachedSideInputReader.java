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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions;

import static org.apache.beam.sdk.transforms.Materializations.MULTIMAP_MATERIALIZATION_URN;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.Materialization;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheStats;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * SideInputReader that caches results for costly {@link Materialization Materializations}.
 * Concurrent access is not expected, but it won't impact correctness.
 */
@Internal
public class CachedSideInputReader implements SideInputReader {
  private final SideInputReader reader;
  private final Map<PCollectionView<?>, Cache<BoundedWindow, Object>> caches;

  /**
   * Creates a SideInputReader that caches results for costly {@link Materialization
   * Materializations} if present, otherwise the SideInputReader is returned as is. Concurrent
   * access is not expected, but it won't impact correctness.
   */
  public static SideInputReader of(SideInputReader reader, Collection<PCollectionView<?>> views) {
    Map<PCollectionView<?>, Cache<BoundedWindow, Object>> caches = initCaches(views, 1000);
    return caches.isEmpty() ? reader : new CachedSideInputReader(reader, caches);
  }

  private CachedSideInputReader(
      SideInputReader reader, Map<PCollectionView<?>, Cache<BoundedWindow, Object>> caches) {
    this.reader = reader;
    this.caches = caches;
  }

  /**
   * Init caches based on {@link #shouldCache} using {@link SingletonCache} if using global window,
   * and otherwise a Guava LRU cache.
   */
  private static Map<PCollectionView<?>, Cache<BoundedWindow, Object>> initCaches(
      Iterable<PCollectionView<?>> views, int maxSize) {
    ImmutableMap.Builder<PCollectionView<?>, Cache<BoundedWindow, Object>> builder =
        ImmutableMap.builder();
    for (PCollectionView<?> view : views) {
      if (shouldCache(view)) {
        boolean isGlobal =
            view.getWindowingStrategyInternal().getWindowFn() instanceof GlobalWindows;
        builder.put(view, isGlobal ? new SingletonCache<>() : lruCache(maxSize));
      }
    }
    return builder.build();
  }

  private static boolean shouldCache(PCollectionView<?> view) {
    // only cache expensive multimap views
    return MULTIMAP_MATERIALIZATION_URN.equals(view.getViewFn().getMaterialization().getUrn());
  }

  private static Cache<BoundedWindow, Object> lruCache(int maxSize) {
    // no concurrent access expected, using separate instance per partition iterator
    return CacheBuilder.newBuilder().concurrencyLevel(1).maximumSize(maxSize).build();
  }

  @Override
  public <T> @Nullable T get(PCollectionView<T> view, BoundedWindow window) {
    Cache<BoundedWindow, Object> cache = caches.get(view);
    if (cache != null) {
      Object result = cache.getIfPresent(window);
      if (result == null) {
        result = reader.get(view, window);
        if (result != null) {
          cache.put(window, result);
        }
        return (T) result;
      }
    }
    return reader.get(view, window);
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return reader.contains(view);
  }

  @Override
  public boolean isEmpty() {
    return reader.isEmpty();
  }

  /** Caching a singleton value, ignoring any key. */
  private static class SingletonCache<K extends @NonNull Object, V extends @NonNull Object>
      implements Cache<K, V> {
    private @Nullable V value;

    @Override
    public @Nullable V getIfPresent(Object o) {
      return value;
    }

    @Override
    public void put(K k, V v) {
      value = v;
    }

    @Override
    public long size() {
      return value != null ? 1 : 0;
    }

    @Override
    public V get(K k, Callable<? extends V> callable) throws ExecutionException {
      throw new UnsupportedOperationException();
    }

    @Override
    public ImmutableMap<K, V> getAllPresent(Iterable<?> iterable) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void invalidate(Object o) {}

    @Override
    public void invalidateAll(Iterable<?> iterable) {}

    @Override
    public void invalidateAll() {}

    @Override
    public CacheStats stats() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ConcurrentMap<K, V> asMap() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void cleanUp() {
      value = null;
    }
  }
}
