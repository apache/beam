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

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalNotification;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.ByteString;
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

  private final Cache<KV<String, ByteString>, CacheEntry> cache;

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
                (RemovalNotification<KV<String, ByteString>, CacheEntry> notification) -> {
                  if (notification.getCause() != RemovalCause.EXPLICIT) {
                    LOG.info("Closing idle reader for {}", keyToString(notification.getKey()));
                    closeReader(notification.getKey(), notification.getValue());
                  }
                })
            .build();
  }

  private static String keyToString(KV<String, ByteString> key) {
    return key.getKey() + "-" + key.getValue().toStringUtf8();
  }

  /** Close the reader and log a warning if close fails. */
  private void closeReader(KV<String, ByteString> key, CacheEntry entry) {
    try {
      entry.reader.close();
    } catch (IOException e) {
      LOG.warn("Failed to close UnboundedReader for {}", keyToString(key), e);
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
      String computationId, ByteString splitId, long cacheToken) {
    KV<String, ByteString> key = KV.of(computationId, splitId);
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
      long cacheToken,
      UnboundedSource.UnboundedReader<?> reader) {
    CacheEntry existing =
        cache
            .asMap()
            .putIfAbsent(KV.of(computationId, splitId), new CacheEntry(reader, cacheToken));
    Preconditions.checkState(existing == null, "Overwriting existing readers is not allowed");
    cache.cleanUp();
  }

  /** If a reader is cached for this key, remove and close it. */
  void invalidateReader(String computationId, ByteString splitId) {
    // use an invalid cache token that will trigger close.
    acquireReader(computationId, splitId, -1L);
  }
}
