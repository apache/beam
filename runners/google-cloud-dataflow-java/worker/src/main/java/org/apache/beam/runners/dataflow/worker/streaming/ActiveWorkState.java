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
package org.apache.beam.runners.dataflow.worker.streaming;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList.toImmutableList;

import com.google.auto.value.AutoValue;
import java.io.PrintWriter;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.HeartbeatRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the active {@link Work} queues for their {@link ShardedKey}(s). Gives an interface to
 * activate, queue, and complete {@link Work} (including invalidating stuck {@link Work}).
 */
@ThreadSafe
@Internal
public final class ActiveWorkState {
  private static final Logger LOG = LoggerFactory.getLogger(ActiveWorkState.class);

  /* The max number of keys in COMMITTING or COMMIT_QUEUED status to be shown.*/
  private static final int MAX_PRINTABLE_COMMIT_PENDING_KEYS = 50;

  /**
   * Map from {@link ShardedKey} to {@link Work} for the key. The first item in the {@link
   * Queue<Work>} is actively processing.
   */
  @GuardedBy("this")
  private final Map<ShardedKey, Deque<Work>> activeWork;

  @GuardedBy("this")
  private final WindmillStateCache.ForComputation computationStateCache;

  /**
   * Current budget that is being processed or queued on the user worker. Incremented when work is
   * activated in {@link #activateWorkForKey(ShardedKey, Work)}, and decremented when work is
   * completed in {@link #completeWorkAndGetNextWorkForKey(ShardedKey, long)}.
   */
  private final AtomicReference<GetWorkBudget> activeGetWorkBudget;

  private ActiveWorkState(
      Map<ShardedKey, Deque<Work>> activeWork,
      WindmillStateCache.ForComputation computationStateCache) {
    this.activeWork = activeWork;
    this.computationStateCache = computationStateCache;
    this.activeGetWorkBudget = new AtomicReference<>(GetWorkBudget.noBudget());
  }

  static ActiveWorkState create(WindmillStateCache.ForComputation computationStateCache) {
    return new ActiveWorkState(new HashMap<>(), computationStateCache);
  }

  @VisibleForTesting
  static ActiveWorkState forTesting(
      Map<ShardedKey, Deque<Work>> activeWork,
      WindmillStateCache.ForComputation computationStateCache) {
    return new ActiveWorkState(activeWork, computationStateCache);
  }

  private static String elapsedString(Instant start, Instant end) {
    Duration activeFor = new Duration(start, end);
    // Duration's toString always starts with "PT"; remove that here.
    return activeFor.toString().substring(2);
  }

  /**
   * Activates {@link Work} for the {@link ShardedKey}. Outcome can be 1 of 3 {@link
   * ActivateWorkResult}
   *
   * <p>1. EXECUTE: The {@link ShardedKey} has not been seen before, create a {@link Queue<Work>}
   * for the key. The caller should execute the work.
   *
   * <p>2. DUPLICATE: A work queue for the {@link ShardedKey} exists, and the work already exists in
   * the {@link ShardedKey}'s work queue, mark the {@link Work} as a duplicate.
   *
   * <p>3. QUEUED: A work queue for the {@link ShardedKey} exists, and the work is not in the key's
   * work queue, queue the work for later processing.
   */
  synchronized ActivateWorkResult activateWorkForKey(ShardedKey shardedKey, Work work) {
    Deque<Work> workQueue = activeWork.getOrDefault(shardedKey, new ArrayDeque<>());
    // This key does not have any work queued up on it. Create one, insert Work, and mark the work
    // to be executed.
    if (!activeWork.containsKey(shardedKey) || workQueue.isEmpty()) {
      workQueue.addLast(work);
      activeWork.put(shardedKey, workQueue);
      incrementActiveWorkBudget(work);
      return ActivateWorkResult.EXECUTE;
    }

    // Ensure we don't already have this work token queued.
    for (Work queuedWork : workQueue) {
      if (queuedWork.getWorkItem().getWorkToken() == work.getWorkItem().getWorkToken()) {
        return ActivateWorkResult.DUPLICATE;
      }
    }

    // Queue the work for later processing.
    workQueue.addLast(work);
    incrementActiveWorkBudget(work);
    return ActivateWorkResult.QUEUED;
  }

  @AutoValue
  public abstract static class FailedTokens {
    public static Builder newBuilder() {
      return new AutoValue_ActiveWorkState_FailedTokens.Builder();
    }

    public abstract long workToken();

    public abstract long cacheToken();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setWorkToken(long value);

      public abstract Builder setCacheToken(long value);

      public abstract FailedTokens build();
    }
  }

  /**
   * Fails any active work matching an element of the input Map.
   *
   * @param failedWork a map from sharding_key to tokens for the corresponding work.
   */
  synchronized void failWorkForKey(Map<Long, List<FailedTokens>> failedWork) {
    // Note we can't construct a ShardedKey and look it up in activeWork directly since
    // HeartbeatResponse doesn't include the user key.
    for (Entry<ShardedKey, Deque<Work>> entry : activeWork.entrySet()) {
      List<FailedTokens> failedTokens = failedWork.get(entry.getKey().shardingKey());
      if (failedTokens == null) continue;
      for (FailedTokens failedToken : failedTokens) {
        for (Work queuedWork : entry.getValue()) {
          WorkItem workItem = queuedWork.getWorkItem();
          if (workItem.getWorkToken() == failedToken.workToken()
              && workItem.getCacheToken() == failedToken.cacheToken()) {
            LOG.debug(
                "Failing work "
                    + computationStateCache.getComputation()
                    + " "
                    + entry.getKey().shardingKey()
                    + " "
                    + failedToken.workToken()
                    + " "
                    + failedToken.cacheToken()
                    + ". The work will be retried and is not lost.");
            queuedWork.setFailed();
            break;
          }
        }
      }
    }
  }

  private void incrementActiveWorkBudget(Work work) {
    activeGetWorkBudget.updateAndGet(
        getWorkBudget -> getWorkBudget.apply(1, work.getWorkItem().getSerializedSize()));
  }

  private void decrementActiveWorkBudget(Work work) {
    activeGetWorkBudget.updateAndGet(
        getWorkBudget -> getWorkBudget.subtract(1, work.getWorkItem().getSerializedSize()));
  }

  /**
   * Removes the complete work from the {@link Queue<Work>}. The {@link Work} is marked as completed
   * if its workToken matches the one that is passed in. Returns the next {@link Work} in the {@link
   * ShardedKey}'s work queue, if one exists else removes the {@link ShardedKey} from {@link
   * #activeWork}.
   */
  synchronized Optional<Work> completeWorkAndGetNextWorkForKey(
      ShardedKey shardedKey, long workToken) {
    @Nullable Queue<Work> workQueue = activeWork.get(shardedKey);
    if (workQueue == null) {
      // Work may have been completed due to clearing of stuck commits.
      LOG.warn("Unable to complete inactive work for key {} and token {}.", shardedKey, workToken);
      return Optional.empty();
    }
    removeCompletedWorkFromQueue(workQueue, shardedKey, workToken);
    return getNextWork(workQueue, shardedKey);
  }

  private synchronized void removeCompletedWorkFromQueue(
      Queue<Work> workQueue, ShardedKey shardedKey, long workToken) {
    Work completedWork = workQueue.peek();
    if (completedWork == null) {
      // Work may have been completed due to clearing of stuck commits.
      LOG.warn(
          String.format("Active key %s without work, expected token %d", shardedKey, workToken));
      return;
    }

    if (completedWork.getWorkItem().getWorkToken() != workToken) {
      // Work may have been completed due to clearing of stuck commits.
      LOG.warn(
          "Unable to complete due to token mismatch for key {} and token {}, actual token was {}.",
          shardedKey,
          workToken,
          completedWork.getWorkItem().getWorkToken());
      return;
    }

    // We consumed the matching work item.
    workQueue.remove();
    decrementActiveWorkBudget(completedWork);
  }

  private synchronized Optional<Work> getNextWork(Queue<Work> workQueue, ShardedKey shardedKey) {
    Optional<Work> nextWork = Optional.ofNullable(workQueue.peek());
    if (!nextWork.isPresent()) {
      Preconditions.checkState(workQueue == activeWork.remove(shardedKey));
    }

    return nextWork;
  }

  /**
   * Invalidates all {@link Work} that is in the {@link Work.State#COMMITTING} state which started
   * before the stuckCommitDeadline.
   */
  synchronized void invalidateStuckCommits(
      Instant stuckCommitDeadline, BiConsumer<ShardedKey, Long> shardedKeyAndWorkTokenConsumer) {
    for (Entry<ShardedKey, Long> shardedKeyAndWorkToken :
        getStuckCommitsAt(stuckCommitDeadline).entrySet()) {
      ShardedKey shardedKey = shardedKeyAndWorkToken.getKey();
      long workToken = shardedKeyAndWorkToken.getValue();
      computationStateCache.invalidate(shardedKey.key(), shardedKey.shardingKey());
      shardedKeyAndWorkTokenConsumer.accept(shardedKey, workToken);
    }
  }

  private synchronized ImmutableMap<ShardedKey, Long> getStuckCommitsAt(
      Instant stuckCommitDeadline) {
    // Determine the stuck commit keys but complete them outside the loop iterating over
    // activeWork as completeWork may delete the entry from activeWork.
    ImmutableMap.Builder<ShardedKey, Long> stuckCommits = ImmutableMap.builder();
    for (Entry<ShardedKey, Deque<Work>> entry : activeWork.entrySet()) {
      ShardedKey shardedKey = entry.getKey();
      @Nullable Work work = entry.getValue().peek();
      if (work != null) {
        if (work.isStuckCommittingAt(stuckCommitDeadline)) {
          LOG.error(
              "Detected key {} stuck in COMMITTING state since {}, completing it with error.",
              shardedKey,
              work.getStateStartTime());
          stuckCommits.put(shardedKey, work.getWorkItem().getWorkToken());
        }
      }
    }

    return stuckCommits.build();
  }

  synchronized ImmutableList<HeartbeatRequest> getKeyHeartbeats(
      Instant refreshDeadline, DataflowExecutionStateSampler sampler) {
    return activeWork.entrySet().stream()
        .flatMap(entry -> toHeartbeatRequestStream(entry, refreshDeadline, sampler))
        .collect(toImmutableList());
  }

  private static Stream<HeartbeatRequest> toHeartbeatRequestStream(
      Entry<ShardedKey, Deque<Work>> shardedKeyAndWorkQueue,
      Instant refreshDeadline,
      DataflowExecutionStateSampler sampler) {
    ShardedKey shardedKey = shardedKeyAndWorkQueue.getKey();
    Deque<Work> workQueue = shardedKeyAndWorkQueue.getValue();

    return workQueue.stream()
        .filter(work -> work.getStartTime().isBefore(refreshDeadline))
        // Don't send heartbeats for queued work we already know is failed.
        .filter(work -> !work.isFailed())
        .map(
            work ->
                Windmill.HeartbeatRequest.newBuilder()
                    .setShardingKey(shardedKey.shardingKey())
                    .setWorkToken(work.getWorkItem().getWorkToken())
                    .setCacheToken(work.getWorkItem().getCacheToken())
                    .addAllLatencyAttribution(
                        work.getLatencyAttributions(true, work.getLatencyTrackingId(), sampler))
                    .build());
  }

  /**
   * Returns the current aggregate {@link GetWorkBudget} that is active on the user worker. Active
   * means that the work is received from Windmill, being processed or queued to be processed in
   * {@link ActiveWorkState}, and not committed back to Windmill.
   */
  GetWorkBudget currentActiveWorkBudget() {
    return activeGetWorkBudget.get();
  }

  synchronized void printActiveWork(PrintWriter writer, Instant now) {
    writer.println(
        "<table border=\"1\" "
            + "style=\"border-collapse:collapse;padding:5px;border-spacing:5px;border:1px\">");
    writer.println(
        "<tr><th>Key</th><th>Token</th><th>Queued</th><th>Active For</th><th>State</th><th>State Active For</th></tr>");
    // Use StringBuilder because we are appending in loop.
    StringBuilder activeWorkStatus = new StringBuilder();
    int commitsPendingCount = 0;
    for (Map.Entry<ShardedKey, Deque<Work>> entry : activeWork.entrySet()) {
      Queue<Work> workQueue = Preconditions.checkNotNull(entry.getValue());
      Work activeWork = Preconditions.checkNotNull(workQueue.peek());
      WorkItem workItem = activeWork.getWorkItem();
      if (activeWork.isCommitPending()) {
        if (++commitsPendingCount >= MAX_PRINTABLE_COMMIT_PENDING_KEYS) {
          continue;
        }
      }
      activeWorkStatus.append("<tr>");
      activeWorkStatus.append("<td>");
      activeWorkStatus.append(String.format("%016x", workItem.getShardingKey()));
      activeWorkStatus.append("</td><td>");
      activeWorkStatus.append(String.format("%016x", workItem.getWorkToken()));
      activeWorkStatus.append("</td><td>");
      activeWorkStatus.append(workQueue.size() - 1);
      activeWorkStatus.append("</td><td>");
      activeWorkStatus.append(elapsedString(activeWork.getStartTime(), now));
      activeWorkStatus.append("</td><td>");
      activeWorkStatus.append(activeWork.getState());
      activeWorkStatus.append("</td><td>");
      activeWorkStatus.append(elapsedString(activeWork.getStateStartTime(), now));
      activeWorkStatus.append("</td></tr>\n");
    }

    writer.print(activeWorkStatus);
    writer.println("</table>");

    if (commitsPendingCount >= MAX_PRINTABLE_COMMIT_PENDING_KEYS) {
      writer.println("<br>");
      writer.print("Skipped keys in COMMITTING/COMMIT_QUEUED: ");
      writer.println(commitsPendingCount - MAX_PRINTABLE_COMMIT_PENDING_KEYS);
      writer.println("<br>");
    }

    writer.println("<br>");
    writer.println("Current Active Work Budget: ");
    writer.println(currentActiveWorkBudget());
    writer.println("<br>");
  }

  enum ActivateWorkResult {
    QUEUED,
    EXECUTE,
    DUPLICATE
  }
}
