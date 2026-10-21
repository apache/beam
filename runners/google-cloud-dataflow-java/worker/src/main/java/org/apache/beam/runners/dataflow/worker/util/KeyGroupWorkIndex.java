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
package org.apache.beam.runners.dataflow.worker.util;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor.QueuedWork;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Sharded FIFO index of {@link QueuedWork} by computation and {@link Work.KeyGroup} for work
 * stealing by multi-key bundles.
 */
@ThreadSafe
final class KeyGroupWorkIndex {

  private static final int NUM_SHARDS = 64;
  private static final int INITIAL_QUEUE_CAPACITY = 4;

  private final ImmutableList<Shard> shards;

  KeyGroupWorkIndex() {
    ImmutableList.Builder<Shard> builder = ImmutableList.builderWithExpectedSize(NUM_SHARDS);
    for (int i = 0; i < NUM_SHARDS; i++) {
      builder.add(new Shard());
    }
    this.shards = builder.build();
  }

  private Shard shardFor(QueueKey queueKey) {
    return shards.get(Math.floorMod(queueKey.hashCode(), NUM_SHARDS));
  }

  /** Adds {@code work} to its key-group queue if it has not been claimed yet. */
  void add(QueuedWork work) {
    if (work.isClaimed()) {
      return;
    }
    QueueKey queueKey = work.getQueueKey();
    shardFor(queueKey).add(queueKey, work);
  }

  /**
   * Drains claimed work from the head of {@code work}'s key-group queue. If an earlier item is
   * still unclaimed, {@code work} remains in the queue with its references cleared and is drained
   * in amortized O(1) time once the earlier item is claimed.
   */
  void drainClaimed(QueuedWork completedWork) {
    checkArgument(completedWork.isClaimed());
    QueueKey queueKey = completedWork.getQueueKey();
    shardFor(queueKey).drainClaimed(queueKey);
  }

  /** Claims and returns the oldest unclaimed work for the computation and key group, or null. */
  @Nullable QueuedWork pollAndClaim(String computationId, Work.KeyGroup keyGroup) {
    QueueKey queueKey = new QueueKey(computationId, keyGroup);
    return shardFor(queueKey).pollAndClaim(queueKey);
  }

  @VisibleForTesting
  boolean isEmpty() {
    for (Shard shard : shards) {
      if (!shard.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  /** A shard of key-group queues; the map and its queues are guarded by the shard's monitor. */
  private static final class Shard {

    @GuardedBy("this")
    private final Map<QueueKey, ArrayDeque<QueuedWork>> queuesByKeyGroup = new HashMap<>();

    private synchronized void add(QueueKey queueKey, QueuedWork work) {
      if (work.isClaimed()) {
        return;
      }
      ArrayDeque<QueuedWork> queue =
          queuesByKeyGroup.computeIfAbsent(
              queueKey, unused -> new ArrayDeque<>(INITIAL_QUEUE_CAPACITY));
      drainClaimedFromHead(queue);
      queue.addLast(work);
    }

    private synchronized void drainClaimed(QueueKey queueKey) {
      @Nullable ArrayDeque<QueuedWork> queue = queuesByKeyGroup.get(queueKey);
      if (queue == null) {
        return;
      }
      drainClaimedFromHead(queue);
      if (queue.isEmpty()) {
        queuesByKeyGroup.remove(queueKey);
      }
    }

    private synchronized @Nullable QueuedWork pollAndClaim(QueueKey queueKey) {
      @Nullable ArrayDeque<QueuedWork> queue = queuesByKeyGroup.get(queueKey);
      if (queue == null) {
        return null;
      }
      @Nullable QueuedWork claimed = null;
      while (claimed == null && !queue.isEmpty()) {
        QueuedWork candidate = queue.removeFirst();
        if (candidate.tryClaimForSteal()) {
          claimed = candidate;
        }
      }
      drainClaimedFromHead(queue);
      if (queue.isEmpty()) {
        queuesByKeyGroup.remove(queueKey);
      }
      return claimed;
    }

    private synchronized boolean isEmpty() {
      return queuesByKeyGroup.isEmpty();
    }

    private static void drainClaimedFromHead(ArrayDeque<QueuedWork> queue) {
      while (!queue.isEmpty() && queue.peekFirst().isClaimed()) {
        queue.removeFirst();
      }
    }
  }

  static final class QueueKey {
    private final String computationId;
    private final Work.KeyGroup keyGroup;
    private final int hashCode;

    QueueKey(String computationId, Work.KeyGroup keyGroup) {
      this.computationId = computationId;
      this.keyGroup = keyGroup;
      this.hashCode = 31 * computationId.hashCode() + keyGroup.hashCode();
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof QueueKey)) {
        return false;
      }
      QueueKey other = (QueueKey) o;
      return computationId.equals(other.computationId) && keyGroup.equals(other.keyGroup);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public String toString() {
      return "QueueKey{"
          + "computationId='"
          + computationId
          + '\''
          + ", keyGroup="
          + keyGroup
          + '}';
    }
  }
}
