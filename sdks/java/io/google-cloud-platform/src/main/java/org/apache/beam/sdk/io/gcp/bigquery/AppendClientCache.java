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
package org.apache.beam.sdk.io.gcp.bigquery;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalNotification;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the cache of {@link AppendClientInfo} objects and the synchronization protocol
 * required to use them safely. The Guava cache object is thread-safe. However our protocol requires
 * that client pin the StreamAppendClient after looking up the cache, and we must ensure that the
 * cache is not accessed in between the lookup and the pin (any access of the cache could trigger
 * element expiration).
 */
class AppendClientCache<KeyT extends @NonNull Object> {
  private static final Logger LOG = LoggerFactory.getLogger(AppendClientCache.class);
  private final ExecutorService closeWriterExecutor = Executors.newCachedThreadPool();

  private final Cache<KeyT, AppendClientInfo> appendCache;

  @SuppressWarnings({"FutureReturnValueIgnored"})
  AppendClientCache(Duration expireAfterAccess) {
    this.appendCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(expireAfterAccess.getMillis(), TimeUnit.MILLISECONDS)
            .removalListener(
                (RemovalNotification<KeyT, AppendClientInfo> removal) -> {
                  LOG.info("Expiring append client for {}", removal.getKey());
                  final @Nullable AppendClientInfo appendClientInfo = removal.getValue();
                  if (appendClientInfo != null) {
                    // Remove the pin owned by the cache itself. Since the client has not been
                    // marked as closed, we
                    // can call unpin in this thread without worrying about blocking the thread.
                    appendClientInfo.unpinAppendClient(null);
                    // Close the client in another thread to avoid blocking the main thread.
                    closeWriterExecutor.submit(appendClientInfo::close);
                  }
                })
            .build();
  }

  // The cache itself always own one pin on the object. This Callable is always used to ensure that
  // the cache
  // adds a pin before loading a value.
  private static Callable<AppendClientInfo> wrapWithPin(Callable<AppendClientInfo> loader) {
    return () -> {
      AppendClientInfo client = loader.call();
      client.pinAppendClient();
      return client;
    };
  }

  /**
   * Atomically get an append client from the cache and add a pin. This pin is owned by the client,
   * which has the responsibility of removing it. If the client is not in the cache, loader will be
   * used to load the client; in this case an additional pin will be added owned by the cache,
   * removed when the item is evicted.
   */
  public AppendClientInfo getAndPin(KeyT key, Callable<AppendClientInfo> loader) throws Exception {
    synchronized (this) {
      AppendClientInfo info = appendCache.get(key, wrapWithPin(loader));
      info.pinAppendClient();
      return info;
    }
  }

  /** "Refresh" an object by invalidating the old cache entry. */
  public AppendClientInfo refreshObjectAndAndPin(KeyT key, Callable<AppendClientInfo> loader)
      throws Exception {
    synchronized (this) {
      appendCache.invalidate(key);
      return getAndPin(key, loader);
    }
  }

  public void invalidate(KeyT key, AppendClientInfo expectedClient) {
    // The default stream is cached across multiple different DoFns. If they all try
    // and
    // invalidate, then we can get races between threads invalidating and recreating
    // streams. For this reason,
    // we check to see that the cache still contains the object we created before
    // invalidating (in case another
    // thread has already invalidated and recreated the stream).
    synchronized (this) {
      AppendClientInfo cachedAppendClient = appendCache.getIfPresent(key);
      if (cachedAppendClient != null
          && System.identityHashCode(cachedAppendClient)
              == System.identityHashCode(expectedClient)) {
        appendCache.invalidate(key);
      }
    }
  }

  public void invalidate(KeyT key) {
    synchronized (this) {
      appendCache.invalidate(key);
    }
  }

  public void tickle(KeyT key) {
    synchronized (this) {
      appendCache.getIfPresent(key);
    }
  }

  public void clear() {
    synchronized (this) {
      appendCache.invalidateAll();
    }
  }

  public void unpinAsync(AppendClientInfo appendClientInfo) {
    appendClientInfo.unpinAppendClient(closeWriterExecutor);
  }
}
