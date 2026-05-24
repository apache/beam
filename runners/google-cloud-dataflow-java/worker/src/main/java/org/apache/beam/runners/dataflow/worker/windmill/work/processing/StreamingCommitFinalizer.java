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

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
@Internal
final class StreamingCommitFinalizer {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingCommitFinalizer.class);

  /** A {@link Runnable} and expiry time pair. */
  @AutoValue
  public abstract static class FinalizationInfo {
    public abstract long getId();

    public abstract Instant getExpiryTime();

    public abstract Runnable getCallback();

    public abstract ScheduledFuture<?> getCleanupFuture();

    public static FinalizationInfo create(
        Long id, Instant expiryTime, Runnable callback, ScheduledFuture<?> cleanupFuture) {
      return new AutoValue_StreamingCommitFinalizer_FinalizationInfo(
          id, expiryTime, callback, cleanupFuture);
    }
  }

  private final ReentrantLock lock = new ReentrantLock();

  @GuardedBy("lock")
  private final HashMap<Long, FinalizationInfo> commitFinalizationCallbacks = new HashMap<>();

  private final BoundedQueueExecutor finalizationExecutor;

  // The cleanup threads run in their own Executor, so they don't block processing.
  private final ScheduledExecutorService cleanupExecutor;

  private StreamingCommitFinalizer(
      BoundedQueueExecutor finalizationExecutor, ScheduledExecutorService cleanupExecutor) {
    this.finalizationExecutor = finalizationExecutor;
    this.cleanupExecutor = cleanupExecutor;
  }

  static StreamingCommitFinalizer create(
      BoundedQueueExecutor workExecutor, ScheduledExecutorService cleanupExecutor) {
    return new StreamingCommitFinalizer(workExecutor, cleanupExecutor);
  }

  /**
   * Stores a map of user worker generated finalization ids and callbacks to execute once a commit
   * has been successfully committed to the backing state store.
   */
  public void cacheCommitFinalizers(Map<Long, Pair<Instant, Runnable>> callbacks) {
    if (callbacks.isEmpty()) {
      return;
    }
    Instant now = Instant.now();
    lock.lock();
    try {
      for (Map.Entry<Long, Pair<Instant, Runnable>> entry : callbacks.entrySet()) {
        Instant cleanupTime = entry.getValue().getLeft();
        // Ignore finalizers that have already expired.
        if (cleanupTime.isBefore(now)) {
          continue;
        }
        ScheduledFuture<?> cleanupFuture =
            cleanupExecutor.schedule(
                () -> {
                  lock.lock();
                  try {
                    commitFinalizationCallbacks.remove(entry.getKey());
                  } finally {
                    lock.unlock();
                  }
                },
                new Duration(now, cleanupTime).getMillis(),
                TimeUnit.MILLISECONDS);
        FinalizationInfo info =
            FinalizationInfo.create(
                entry.getKey(),
                entry.getValue().getLeft(),
                entry.getValue().getRight(),
                cleanupFuture);
        FinalizationInfo existingInfo = commitFinalizationCallbacks.put(info.getId(), info);
        if (existingInfo != null) {
          throw new IllegalStateException(
              "Expected to not have any past callbacks for bundle "
                  + info.getId()
                  + " but had "
                  + existingInfo);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * When this method is called, the commits associated with the provided finalizeIds have been
   * successfully persisted in the backing state store. If the commitCallback for the finalizationId
   * is still cached it is invoked.
   */
  public void finalizeCommits(Iterable<Long> finalizeIds) {
    if (Iterables.isEmpty(finalizeIds)) {
      return;
    }
    List<Runnable> callbacksToExecute = new ArrayList<>();
    lock.lock();
    try {
      for (long finalizeId : finalizeIds) {
        @Nullable FinalizationInfo info = commitFinalizationCallbacks.remove(finalizeId);
        if (info != null) {
          callbacksToExecute.add(info.getCallback());
          info.getCleanupFuture().cancel(true);
        }
      }
    } finally {
      lock.unlock();
    }
    for (Runnable callback : callbacksToExecute) {
      try {
        finalizationExecutor.forceExecute(callback, 0);
      } catch (OutOfMemoryError oom) {
        throw oom;
      } catch (Throwable t) {
        LOG.error("Commit finalization failed:", t);
      }
    }
  }

  @VisibleForTesting
  int pendingCallbacksSize() {
    lock.lock();
    try {
      return commitFinalizationCallbacks.size();
    } finally {
      lock.unlock();
    }
  }
}
