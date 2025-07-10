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
package org.apache.beam.runners.dataflow.worker;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalCause;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalNotification;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cache of active readers keyed by computationId and the split ID. The entries have a (default) 1
 * minute expiration timeout and the reader will be closed if it is not used within this period.
 */
@ThreadSafe
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@Internal
public class ReaderCache {

  private static final Logger LOG = LoggerFactory.getLogger(ReaderCache.class);
  private final Executor invalidationExecutor;

  // Note on thread safety. This class is thread safe because:
  //   - Guava Cache is thread safe.
  //   - There is no state other than Cache.
  //   - API is strictly a 1:1 wrapper over Cache API (not counting cache.cleanUp() calls).
  //       - i.e. it does not invoke more than one call, which could make it inconsistent.
  // If any of these conditions changes, please test ensure and test thread safety.

  private static class CacheEntry {

    final UnboundedSource.UnboundedReader<?> reader;
    final long cacheToken;
    final long workToken;

    CacheEntry(UnboundedSource.UnboundedReader<?> reader, long cacheToken, long workToken) {
      this.reader = reader;
      this.cacheToken = cacheToken;
      this.workToken = workToken;
    }
  }

  private final Cache<WindmillComputationKey, CacheEntry> cache;

  /** Cache reader for {@code cacheDuration}. Readers will be closed on {@code executor}. */
  ReaderCache(Duration cacheDuration, Executor invalidationExecutor) {
    this.invalidationExecutor = invalidationExecutor;
    this.cache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(cacheDuration.getMillis(), TimeUnit.MILLISECONDS)
            .removalListener(
                (RemovalNotification<WindmillComputationKey, CacheEntry> notification) -> {
                  if (notification.getCause() != RemovalCause.EXPLICIT) {
                    LOG.info(
                        "Asynchronously closing reader for {} as it has been idle for over {}",
                        notification.getKey(),
                        cacheDuration);
                    asyncCloseReader(notification.getKey(), notification.getValue());
                  }
                })
            .build();
  }

  /** Close the reader and log a warning if close fails. */
  private void asyncCloseReader(WindmillComputationKey key, CacheEntry entry) {
    invalidationExecutor.execute(
        () -> {
          try {
            entry.reader.close();
            LOG.info("Finished closing reader for {}", key);
          } catch (IOException e) {
            LOG.warn("Failed to close UnboundedReader for {}", key, e);
          }
        });
  }

  /**
   * If there is a cached reader for this computationKey and the cache token matches, the reader is
   * <i>removed</i> from the cache and returned. Cache the reader using cacheReader() as required.
   * Note that cache will expire in one minute. If cacheToken does not match the token already
   * cached, it is assumed that the cached reader (if any) is no longer relevant and will be closed.
   * Return null in case of a cache miss.
   */
  UnboundedSource.UnboundedReader<?> acquireReader(
      WindmillComputationKey computationKey, long cacheToken, long workToken) {
    CacheEntry entry = cache.asMap().remove(computationKey);

    cache.cleanUp();

    if (entry != null) {
      if (entry.cacheToken == cacheToken && workToken > entry.workToken) {
        return entry.reader;
      } else {
        // new cacheToken invalidates old one or this is a retried or stale request,
        // close the reader.
        LOG.info("Asynchronously closing reader for {} as it is no longer valid", computationKey);
        asyncCloseReader(computationKey, entry);
      }
    }
    return null;
  }

  /** Cache the reader for a minute. It will be closed if it is not acquired with in a minute. */
  void cacheReader(
      WindmillComputationKey computationKey,
      long cacheToken,
      long workToken,
      UnboundedSource.UnboundedReader<?> reader) {
    CacheEntry existing =
        cache.asMap().putIfAbsent(computationKey, new CacheEntry(reader, cacheToken, workToken));
    Preconditions.checkState(existing == null, "Overwriting existing readers is not allowed");
    cache.cleanUp();
  }

  /** If a reader is cached for this key, remove and close it. */
  void invalidateReader(WindmillComputationKey computationKey) {
    // use an invalid cache token that will trigger close.
    acquireReader(computationKey, -1L, -1);
  }
}
