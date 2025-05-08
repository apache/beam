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

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache.ForComputation;
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

  /* The max number of keys in COMMITTING or COMMIT_QUEUED status to be shown for observability.*/
  private static final int MAX_PRINTABLE_COMMIT_PENDING_KEYS = 50;

  /**
   * Map from shardingKey to {@link Work} for the key. The first item in the {@link LinkedHashMap}
   * is actively processing.
   */
  @GuardedBy("this")
  private final Map<Long /*shardingKey*/, LinkedHashMap<WorkId, ExecutableWork>> activeWork;

  @GuardedBy("this")
  private final WindmillStateCache.ForComputation computationStateCache;

  /**
   * Current budget that is being processed or queued on the user worker. Incremented when work is
   * activated in {@link #activateWorkForKey(ExecutableWork)}, and decremented when work is
   * completed in {@link #completeWorkAndGetNextWorkForKey(ShardedKey, WorkId)}.
   */
  @GuardedBy("this")
  private GetWorkBudget activeGetWorkBudget;

  private ActiveWorkState(
      Map<Long, LinkedHashMap<WorkId, ExecutableWork>> activeWork,
      ForComputation computationStateCache) {
    this.activeWork = activeWork;
    this.computationStateCache = computationStateCache;
    this.activeGetWorkBudget = GetWorkBudget.noBudget();
  }

  static ActiveWorkState create(WindmillStateCache.ForComputation computationStateCache) {
    return new ActiveWorkState(new HashMap<>(), computationStateCache);
  }

  @VisibleForTesting
  static ActiveWorkState forTesting(
      Map<Long, LinkedHashMap<WorkId, ExecutableWork>> activeWork,
      WindmillStateCache.ForComputation computationStateCache) {
    return new ActiveWorkState(activeWork, computationStateCache);
  }

  private static String elapsedString(Instant start, Instant end) {
    Duration activeFor = new Duration(start, end);
    // Duration's toString always starts with "PT"; remove that here.
    return activeFor.toString().substring(2);
  }

  /**
   * Activates {@link Work} for the {@link ShardedKey}. Outcome can be 1 of 4 {@link
   * ActivateWorkResult}
   *
   * <p>1. EXECUTE: The {@link ShardedKey} has not been seen before, create a {@link Queue<Work>}
   * for the key. The caller should execute the work.
   *
   * <p>2. DUPLICATE: A work queue for the {@link ShardedKey} exists, and the work already exists in
   * the {@link ShardedKey}'s work queue, mark the {@link Work} as a duplicate.
   *
   * <p>3. QUEUED: A work queue for the {@link ShardedKey} exists, and the work is not in the key's
   * work queue, OR the work in the work queue is stale, OR the work in the queue has a matching
   * work token but different cache token, queue the work for later processing.
   *
   * <p>4. STALE: A work queue for the {@link ShardedKey} exists, and there is a queued {@link Work}
   * with a greater workToken than the passed in {@link Work}.
   */
  synchronized ActivateWorkResult activateWorkForKey(ExecutableWork executableWork) {
    ShardedKey shardedKey = executableWork.work().getShardedKey();
    long shardingKey = shardedKey.shardingKey();
    LinkedHashMap<WorkId, ExecutableWork> workQueue =
        activeWork.computeIfAbsent(shardingKey, (unused) -> new LinkedHashMap<>());
    // This key does not have any work queued up on it. Create one, insert Work, and mark the work
    // to be executed.
    if (workQueue.isEmpty()) {
      workQueue.put(executableWork.id(), executableWork);
      incrementActiveWorkBudget(executableWork.work());
      return ActivateWorkResult.EXECUTE;
    }

    // Check to see if we have this work token queued.
    Iterator<Entry<WorkId, ExecutableWork>> workIterator = workQueue.entrySet().iterator();
    while (workIterator.hasNext()) {
      ExecutableWork queuedWork = workIterator.next().getValue();
      if (queuedWork.id().equals(executableWork.id())) {
        return ActivateWorkResult.DUPLICATE;
      }
      if (queuedWork.id().cacheToken() == executableWork.id().cacheToken()
          && queuedWork.work().getShardedKey().equals(executableWork.work().getShardedKey())) {
        if (executableWork.id().workToken() > queuedWork.id().workToken()) {
          // Check to see if the queuedWork is active. We only want to remove it if it is NOT
          // currently active.
          if (!queuedWork.equals(Preconditions.checkNotNull(firstValue(workQueue)))) {
            workIterator.remove();
            decrementActiveWorkBudget(queuedWork.work());
          }
          // Continue here to possibly remove more non-active stale work that is queued.
        } else {
          return ActivateWorkResult.STALE;
        }
      }
    }

    // Queue the work for later processing.
    workQueue.put(executableWork.id(), executableWork);
    incrementActiveWorkBudget(executableWork.work());
    return ActivateWorkResult.QUEUED;
  }

  /**
   * Fails any active work matching an element of the input Map.
   *
   * @param failedWork a map from sharding_key to tokens for the corresponding work.
   */
  synchronized void failWorkForKey(ImmutableList<WorkIdWithShardingKey> failedWork) {
    for (WorkIdWithShardingKey failedId : failedWork) {
      @Nullable
      LinkedHashMap<WorkId, ExecutableWork> workQueue = activeWork.get(failedId.shardingKey());
      if (workQueue == null) {
        // Work could complete/fail before heartbeat response arrives
        continue;
      }
      @Nullable ExecutableWork executableWork = workQueue.get(failedId.workId());
      if (executableWork == null) {
        continue;
      }
      executableWork.work().setFailed();
      LOG.debug(
          "Failing work {} {}. The work will be retried and is not lost.",
          computationStateCache.getComputation(),
          failedId);
    }
  }

  synchronized ImmutableList<RefreshableWork> getRefreshableWork(Instant refreshDeadline) {
    return activeWork.values().stream()
        .flatMap(workMap -> workMap.values().stream())
        .map(ExecutableWork::work)
        .filter(work -> !work.isFailed() && work.getStartTime().isBefore(refreshDeadline))
        .collect(toImmutableList());
  }

  private synchronized void incrementActiveWorkBudget(Work work) {
    activeGetWorkBudget = activeGetWorkBudget.apply(1, work.getSerializedWorkItemSize());
  }

  private synchronized void decrementActiveWorkBudget(Work work) {
    activeGetWorkBudget = activeGetWorkBudget.subtract(1, work.getSerializedWorkItemSize());
  }

  /**
   * Removes the complete work from the {@link Queue<Work>}. The {@link Work} is marked as completed
   * if its workToken matches the one that is passed in. Returns the next {@link Work} in the {@link
   * ShardedKey}'s work queue, if one exists else removes the {@link ShardedKey} from {@link
   * #activeWork}.
   */
  synchronized Optional<ExecutableWork> completeWorkAndGetNextWorkForKey(
      ShardedKey shardedKey, WorkId workId) {
    @Nullable
    LinkedHashMap<WorkId, ExecutableWork> workQueue = activeWork.get(shardedKey.shardingKey());
    if (workQueue == null) {
      // Work may have been completed due to clearing of stuck commits.
      LOG.warn(
          "Unable to complete inactive work for key={} and token={}.  Work queue for key does not exist.",
          shardedKey,
          workId);
      return Optional.empty();
    }

    removeCompletedWorkFromQueue(workQueue, shardedKey, workId);
    return getNextWork(workQueue, shardedKey);
  }

  private synchronized void removeCompletedWorkFromQueue(
      LinkedHashMap<WorkId, ExecutableWork> workQueue, ShardedKey shardedKey, WorkId workId) {
    Iterator<Entry<WorkId, ExecutableWork>> completedWorkIterator = workQueue.entrySet().iterator();
    if (!completedWorkIterator.hasNext()) {
      // Work may have been completed due to clearing of stuck commits.
      LOG.warn("Active key {} without work, expected token {}", shardedKey, workId);
      return;
    }

    ExecutableWork completedWork = completedWorkIterator.next().getValue();
    if (!completedWork.id().equals(workId)) {
      // Work may have been completed due to clearing of stuck commits.
      LOG.warn(
          "Unable to complete due to token mismatch for "
              + "key {},"
              + "expected work_id {}, "
              + "actual work_id was {}",
          shardedKey,
          workId,
          completedWork.id());
      return;
    }
    // We consumed the matching work item.
    completedWorkIterator.remove();
    decrementActiveWorkBudget(completedWork.work());
  }

  @SuppressWarnings("ReferenceEquality")
  private synchronized Optional<ExecutableWork> getNextWork(
      LinkedHashMap<WorkId, ExecutableWork> workQueue, ShardedKey shardedKey) {
    Optional<ExecutableWork> nextWork = Optional.ofNullable(firstValue(workQueue));
    if (!nextWork.isPresent()) {
      Preconditions.checkState(workQueue == activeWork.remove(shardedKey.shardingKey()));
    }
    return nextWork;
  }

  /**
   * Invalidates all {@link Work} that is in the {@link Work.State#COMMITTING} state which started
   * before the stuckCommitDeadline.
   */
  synchronized void invalidateStuckCommits(
      Instant stuckCommitDeadline, BiConsumer<ShardedKey, WorkId> shardedKeyAndWorkTokenConsumer) {
    for (Entry<ShardedKey, WorkId> shardedKeyAndWorkId :
        getStuckCommitsAt(stuckCommitDeadline).entrySet()) {
      ShardedKey shardedKey = shardedKeyAndWorkId.getKey();
      WorkId workId = shardedKeyAndWorkId.getValue();
      computationStateCache.invalidate(shardedKey.key(), shardedKey.shardingKey());
      shardedKeyAndWorkTokenConsumer.accept(shardedKey, workId);
    }
  }

  private static @Nullable ExecutableWork firstValue(Map<WorkId, ExecutableWork> map) {
    Iterator<Entry<WorkId, ExecutableWork>> iterator = map.entrySet().iterator();
    return iterator.hasNext() ? iterator.next().getValue() : null;
  }

  private synchronized ImmutableMap<ShardedKey, WorkId> getStuckCommitsAt(
      Instant stuckCommitDeadline) {
    // Determine the stuck commit keys but complete them outside the loop iterating over
    // activeWork as completeWork may delete the entry from activeWork.
    ImmutableMap.Builder<ShardedKey, WorkId> stuckCommits = ImmutableMap.builder();
    for (Entry<Long, LinkedHashMap<WorkId, ExecutableWork>> entry : activeWork.entrySet()) {
      @Nullable ExecutableWork executableWork = firstValue(entry.getValue());
      if (executableWork != null) {
        Work work = executableWork.work();
        if (work.isStuckCommittingAt(stuckCommitDeadline)) {
          LOG.error(
              "Detected key {} stuck in COMMITTING state since {}, completing it with error.",
              work.getShardedKey(),
              work.getStateStartTime());
          stuckCommits.put(work.getShardedKey(), work.id());
        }
      }
    }

    return stuckCommits.build();
  }

  /**
   * Returns the current aggregate {@link GetWorkBudget} that is active on the user worker. Active
   * means that the work is received from Windmill, being processed or queued to be processed in
   * {@link ActiveWorkState}, and not committed back to Windmill.
   */
  synchronized GetWorkBudget currentActiveWorkBudget() {
    return activeGetWorkBudget;
  }

  synchronized void printActiveWork(PrintWriter writer, Instant now) {
    writer.println(
        "<table border=\"1\" "
            + "style=\"border-collapse:collapse;padding:5px;border-spacing:5px;border:1px\">");
    // Columns.
    writer.println(
        "<tr>"
            + "<th>Key</th>"
            + "<th>Token</th>"
            + "<th>Queued</th>"
            + "<th>Active For</th>"
            + "<th>State</th>"
            + "<th>State Active For</th>"
            + "<th>Processing Thread</th>"
            + "<th>Backend</th>"
            + "</tr>");
    // Use StringBuilder because we are appending in loop.
    StringBuilder activeWorkStatus = new StringBuilder();
    int commitsPendingCount = 0;
    for (Entry<Long, LinkedHashMap<WorkId, ExecutableWork>> entry : activeWork.entrySet()) {
      LinkedHashMap<WorkId, ExecutableWork> workQueue =
          Preconditions.checkNotNull(entry.getValue());
      Work activeWork = Preconditions.checkNotNull(firstValue(workQueue)).work();
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
      activeWorkStatus.append("</td><td>");
      activeWorkStatus.append(activeWork.getProcessingThreadName());
      activeWorkStatus.append("</td><td>");
      activeWorkStatus.append(activeWork.backendWorkerToken());
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
    DUPLICATE,
    STALE
  }
}
