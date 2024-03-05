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
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache of callbackId to {@link Runnable}(s) callbacks that are meant to be called once processed
 * {@link WorkItem}(s) have been successfully committed to the backing persistent store.
 */
@ThreadSafe
@Internal
public final class CommitCallbackCache {
  private static final Logger LOG = LoggerFactory.getLogger(CommitCallbackCache.class);

  private final Cache<Long, Runnable> commitCallbacks;
  private final BoundedQueueExecutor workUnitExecutor;

  public CommitCallbackCache(BoundedQueueExecutor workUnitExecutor, Duration cacheEntryTtl) {
    this.workUnitExecutor = workUnitExecutor;
    this.commitCallbacks = CacheBuilder.newBuilder().expireAfterWrite(cacheEntryTtl).build();
  }

  /**
   * Executes commit callbacks for {@link WorkItem}(s) that have been successfully persisted to the
   * backing persistent store.
   */
  void executeCommitCallbacksFor(WorkItem work) {
    for (Long callbackId : work.getSourceState().getFinalizeIdsList()) {
      @Nullable Runnable callback = commitCallbacks.getIfPresent(callbackId);
      // NOTE: It is possible the same callbackId may be removed twice if
      // windmill restarts.
      // TODO: It is also possible for an earlier finalized id to be lost.
      // We should automatically discard all older callbacks for the same computation and key.
      if (callback != null) {
        commitCallbacks.invalidate(callbackId);
        workUnitExecutor.forceExecute(
            () -> {
              try {
                callback.run();
              } catch (Throwable t) {
                LOG.error("Source checkpoint finalization failed:", t);
              }
            },
            0);
      }
    }
  }

  void cacheCallbacksForProcessedWork(Map<Long, Runnable> workState) {
    commitCallbacks.putAll(workState);
  }
}
