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

import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.commons.lang3.tuple.Pair;
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
    public abstract Long getId();

    public abstract Instant getExpiryTime();

    public abstract Runnable getCallback();

    public static FinalizationInfo create(Long id, Instant expiryTime, Runnable callback) {
      return new AutoValue_StreamingCommitFinalizer_FinalizationInfo(id, expiryTime, callback);
    }
  }

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition queueMinChanged = lock.newCondition();

  @GuardedBy("lock")
  private final HashMap<Long, FinalizationInfo> commitFinalizationCallbacks = new HashMap<>();

  @GuardedBy("lock")
  private final PriorityQueue<FinalizationInfo> cleanUpQueue =
      new PriorityQueue<>(11, Comparator.comparing(FinalizationInfo::getExpiryTime));

  private StreamingCommitFinalizer(BoundedQueueExecutor finalizationCleanupExecutor) {
    finalizationCleanupExecutor.execute(this::cleanupThreadBody, 0);
  }

  private void cleanupThreadBody() {
    lock.lock();
    try {
      while (true) {
        final @Nullable FinalizationInfo minValue = cleanUpQueue.peek();
        if (minValue == null) {
          // Wait for an element to be added and loop to re-examine the min.
          queueMinChanged.await();
          continue;
        }

        Instant now = Instant.now();
        Duration timeDifference = new Duration(now, minValue.getExpiryTime());
        if (timeDifference.getMillis() < 0
            || (queueMinChanged.await(timeDifference.getMillis(), TimeUnit.MILLISECONDS)
                && cleanUpQueue.peek() == minValue)) {
          // The minimum element has an expiry time before now, either because it had elapsed when
          // we pulled it or because we awaited it, and it is still the minimum.
          checkState(minValue == cleanUpQueue.poll());
          checkState(commitFinalizationCallbacks.remove(minValue.getId()) == minValue);
        }
      }
    } catch (InterruptedException e) {
      // We're being shutdown.
    } finally {
      lock.unlock();
    }
  }

  static StreamingCommitFinalizer create(BoundedQueueExecutor workExecutor) {
    return new StreamingCommitFinalizer(workExecutor);
  }

  /**
   * Stores a map of user worker generated finalization ids and callbacks to execute once a commit
   * has been successfully committed to the backing state store.
   */
  public void cacheCommitFinalizers(Map<Long, Pair<Instant, Runnable>> callbacks) {
    for (Map.Entry<Long, Pair<Instant, Runnable>> entry : callbacks.entrySet()) {
      Long finalizeId = entry.getKey();
      final FinalizationInfo info =
          FinalizationInfo.create(
              finalizeId, entry.getValue().getLeft(), entry.getValue().getRight());

      lock.lock();
      try {
        FinalizationInfo existingInfo = commitFinalizationCallbacks.put(finalizeId, info);
        if (existingInfo != null) {
          throw new IllegalStateException(
              "Expected to not have any past callbacks for bundle "
                  + finalizeId
                  + " but had "
                  + existingInfo);
        }
        cleanUpQueue.add(info);
        @SuppressWarnings("ReferenceEquality")
        boolean newMin = cleanUpQueue.peek() == info;
        if (newMin) {
          queueMinChanged.signal();
        }
      } finally {
        lock.unlock();
      }
    }
  }

  /**
   * When this method is called, the commits associated with the provided finalizeIds have been
   * successfully persisted in the backing state store. If the commitCallback for the finalizationId
   * is still cached it is invoked.
   */
  public void finalizeCommits(Iterable<Long> finalizeIds) {
    for (long finalizeId : finalizeIds) {
      @Nullable FinalizationInfo info;
      lock.lock();
      try {
        info = commitFinalizationCallbacks.remove(finalizeId);
        if (info != null) {
          checkState(cleanUpQueue.remove(info));
        }
      } finally {
        lock.unlock();
      }
      if (info != null) {
        try {
          info.getCallback().run();
        } catch (Throwable t) {
          LOG.error("Commit finalization failed:", t);
        }
      }
    }
  }

  // Only exposed for tests.
  public int cleanupQueueSize() {
    lock.lock();
    try {
      return cleanUpQueue.size();
    } finally {
      lock.unlock();
    }
  }
}
