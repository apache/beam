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
package org.apache.beam.runners.dataflow.worker.windmill.work.processing;

import java.time.Duration;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
@Internal
final class StreamingCommitFinalizer {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingCommitFinalizer.class);
  private static final Duration DEFAULT_CACHE_ENTRY_EXPIRY = Duration.ofMinutes(5L);
  private final Cache<Long, Runnable> commitFinalizerCache;
  private final BoundedQueueExecutor finalizationExecutor;

  private StreamingCommitFinalizer(
      Cache<Long, Runnable> commitFinalizerCache, BoundedQueueExecutor finalizationExecutor) {
    this.commitFinalizerCache = commitFinalizerCache;
    this.finalizationExecutor = finalizationExecutor;
  }

  static StreamingCommitFinalizer create(BoundedQueueExecutor workExecutor) {
    return new StreamingCommitFinalizer(
        CacheBuilder.newBuilder().expireAfterWrite(DEFAULT_CACHE_ENTRY_EXPIRY).build(),
        workExecutor);
  }

  /**
   * Stores a map of user worker generated finalization ids and callbacks to execute once a commit
   * has been successfully committed to the backing state store.
   */
  void cacheCommitFinalizers(Map<Long, Runnable> commitCallbacks) {
    commitFinalizerCache.putAll(commitCallbacks);
  }

  /**
   * When this method is called, the commits associated with the provided finalizeIds have been
   * successfully persisted in the backing state store. If the commitCallback for the finalizationId
   * is still cached it is invoked.
   */
  void finalizeCommits(Iterable<Long> finalizeIds) {
    for (long finalizeId : finalizeIds) {
      @Nullable Runnable finalizeCommit = commitFinalizerCache.getIfPresent(finalizeId);
      // NOTE: It is possible the same callback id may be removed twice if
      // windmill restarts.
      // TODO: It is also possible for an earlier finalized id to be lost.
      // We should automatically discard all older callbacks for the same computation and key.
      if (finalizeCommit != null) {
        commitFinalizerCache.invalidate(finalizeId);
        finalizationExecutor.forceExecute(
            () -> {
              try {
                finalizeCommit.run();
              } catch (Throwable t) {
                LOG.error("Source checkpoint finalization failed:", t);
              }
            },
            0);
      }
    }
  }
}
