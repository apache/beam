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

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.RemovalCause;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.RemovalNotification;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cache of active readers keyed by computationId and the split ID. The entries have a (default) 1
 * minute expiration timeout and the reader will be closed if it is not used within this period.
 */
@ThreadSafe
class ReaderCache {

  private static final Logger LOG = LoggerFactory.getLogger(ReaderCache.class);

  // Note on thread safety. This class is thread safe because:
  //   - Guava Cache is thread safe.
  //   - There is no state other than Cache.
  //   - API is strictly a 1:1 wrapper over Cache API (not counting cache.cleanUp() calls).
  //       - i.e. it does not invoke more than one call, which could make it inconsistent.
  // If any of these conditions changes, please test ensure and test thread safety.

  private static class CacheEntry {

    final UnboundedSource.UnboundedReader<?> reader;
    final long token;

    CacheEntry(UnboundedSource.UnboundedReader<?> reader, long token) {
      this.reader = reader;
      this.token = token;
    }
  }

  @AutoValue
  abstract static class CacheKey {

    public static CacheKey create(String computationId, ByteString splitId, long shardId) {
      return new AutoValue_ReaderCache_CacheKey(computationId, splitId, shardId);
    }

    public abstract String computationId();

    public abstract ByteString splitId();

    public abstract long shardId();

    @Override
    public String toString() {
      return String.format("%s-%s-%d", computationId(), splitId().toStringUtf8(), shardId());
    }
  }

  private final Cache<CacheKey, CacheEntry> cache;

  /** ReaderCache with default 1 minute expiration for readers. */
  ReaderCache() {
    this(Duration.standardMinutes(1));
  }

  /** Cache reader for {@code cacheDuration}. */
  ReaderCache(Duration cacheDuration) {
    this.cache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(cacheDuration.getMillis(), TimeUnit.MILLISECONDS)
            .removalListener(
                (RemovalNotification<CacheKey, CacheEntry> notification) -> {
                  if (notification.getCause() != RemovalCause.EXPLICIT) {
                    LOG.info("Closing idle reader for {}", notification.getKey());
                    closeReader(notification.getKey(), notification.getValue());
                  }
                })
            .build();
  }

  /** Close the reader and log a warning if close fails. */
  private void closeReader(CacheKey key, CacheEntry entry) {
    try {
      entry.reader.close();
    } catch (IOException e) {
      LOG.warn("Failed to close UnboundedReader for {}", key, e);
    }
  }

  /**
   * If there is a cached reader for this split and the cache token matches, the reader is
   * <i>removed</i> from the cache and returned. Cache the reader using cacheReader() as required.
   * Note that cache will expire in one minute. If cacheToken does not match the token already
   * cached, it is assumed that the cached reader (if any) is no longer relevant and will be closed.
   * Return null in case of a cache miss.
   */
  UnboundedSource.UnboundedReader<?> acquireReader(
      String computationId, ByteString splitId, long shardId, long cacheToken) {
    CacheKey key = CacheKey.create(computationId, splitId, shardId);
    CacheEntry entry = cache.asMap().remove(key);

    cache.cleanUp();

    if (entry != null) {
      if (entry.token == cacheToken) {
        return entry.reader;
      } else { // new cacheToken invalidates old one. close the reader.
        closeReader(key, entry);
      }
    }
    return null;
  }

  /** Cache the reader for a minute. It will be closed if it is not acquired with in a minute. */
  void cacheReader(
      String computationId,
      ByteString splitId,
      long shardId,
      long cacheToken,
      UnboundedSource.UnboundedReader<?> reader) {
    CacheEntry existing =
        cache
            .asMap()
            .putIfAbsent(
                CacheKey.create(computationId, splitId, shardId),
                new CacheEntry(reader, cacheToken));
    Preconditions.checkState(existing == null, "Overwriting existing readers is not allowed");
    cache.cleanUp();
  }

  /** If a reader is cached for this key, remove and close it. */
  void invalidateReader(String computationId, ByteString splitId, long shardId) {
    // use an invalid cache token that will trigger close.
    acquireReader(computationId, splitId, shardId, -1L);
  }
}
