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
package org.apache.beam.runners.dataflow.worker.streaming.sideinput;

import com.google.auto.value.AutoValue;
import com.google.errorprone.annotations.CheckReturnValue;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Weigher;

/**
 * Wrapper around {@code Cache<SideInputId, SideInput>} that mostly delegates to the underlying
 * cache, but adds threadsafe functionality to invalidate and load entries that are not ready.
 *
 * @implNote Returned values are explicitly cast, because the {@link #sideInputCache} holds wildcard
 *     types of all objects.
 */
@CheckReturnValue
@SuppressWarnings("unchecked")
final class SideInputCache {

  private static final long MAXIMUM_CACHE_WEIGHT = 100000000; /* 100 MB */
  private static final long CACHE_ENTRY_EXPIRY_MINUTES = 1L;

  private final Cache<Key, SideInput<?>> sideInputCache;

  SideInputCache(Cache<Key, SideInput<?>> sideInputCache) {
    this.sideInputCache = sideInputCache;
  }

  static SideInputCache create() {
    return new SideInputCache(
        CacheBuilder.newBuilder()
            .maximumWeight(MAXIMUM_CACHE_WEIGHT)
            .expireAfterWrite(CACHE_ENTRY_EXPIRY_MINUTES, TimeUnit.MINUTES)
            .weigher((Weigher<Key, SideInput<?>>) (id, entry) -> entry.size())
            .build());
  }

  synchronized <T> SideInput<T> invalidateThenLoadNewEntry(
      Key key, Callable<SideInput<T>> cacheLoaderFn) throws ExecutionException {
    // Invalidate the existing not-ready entry.  This must be done atomically
    // so that another thread doesn't replace the entry with a ready entry, which
    // would then be deleted here.
    SideInput<?> newEntry = sideInputCache.getIfPresent(key);
    if (newEntry != null && !newEntry.isReady()) {
      sideInputCache.invalidate(key);
    }

    return (SideInput<T>) sideInputCache.get(key, cacheLoaderFn);
  }

  <T> Optional<SideInput<T>> get(Key key) {
    return Optional.ofNullable((SideInput<T>) sideInputCache.getIfPresent(key));
  }

  <T> SideInput<T> getOrLoad(Key key, Callable<SideInput<T>> cacheLoaderFn)
      throws ExecutionException {
    return (SideInput<T>) sideInputCache.get(key, cacheLoaderFn);
  }

  @AutoValue
  abstract static class Key {
    abstract TupleTag<?> tag();

    abstract BoundedWindow window();

    static Key create(TupleTag<?> tag, BoundedWindow window) {
      return new AutoValue_SideInputCache_Key(tag, window);
    }
  }
}
