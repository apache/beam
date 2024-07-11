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

import com.google.api.services.dataflow.model.MapTask;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimap;
import org.joda.time.Instant;

/**
 * Class representing the state of a computation.
 *
 * <p>This class is synchronized, but only used from the dispatch and commit threads, so should not
 * be heavily contended. Still, blocking work should not be done by it.
 */
public class ComputationState {
  private final String computationId;
  private final MapTask mapTask;
  private final ImmutableMap<String, String> transformUserNameToStateFamily;
  private final ActiveWorkState activeWorkState;
  private final BoundedQueueExecutor executor;
  private final ConcurrentLinkedQueue<ComputationWorkExecutor> computationWorkExecutors;
  private final String sourceBytesProcessCounterName;

  public ComputationState(
      String computationId,
      MapTask mapTask,
      BoundedQueueExecutor executor,
      Map<String, String> transformUserNameToStateFamily,
      WindmillStateCache.ForComputation computationStateCache) {
    Preconditions.checkNotNull(mapTask.getStageName());
    Preconditions.checkNotNull(mapTask.getSystemName());
    this.computationId = computationId;
    this.mapTask = mapTask;
    this.executor = executor;
    this.transformUserNameToStateFamily = ImmutableMap.copyOf(transformUserNameToStateFamily);
    this.computationWorkExecutors = new ConcurrentLinkedQueue<>();
    this.activeWorkState = ActiveWorkState.create(computationStateCache);
    this.sourceBytesProcessCounterName =
        "dataflow_source_bytes_processed-" + mapTask.getSystemName();
  }

  public String getComputationId() {
    return computationId;
  }

  public MapTask getMapTask() {
    return mapTask;
  }

  public ImmutableMap<String, String> getTransformUserNameToStateFamily() {
    return transformUserNameToStateFamily;
  }

  /**
   * Cache the {@link ComputationWorkExecutor} so that it can be re-used in future {@link
   * #acquireComputationWorkExecutor()} calls.
   */
  public void releaseComputationWorkExecutor(ComputationWorkExecutor computationWorkExecutor) {
    computationWorkExecutors.offer(computationWorkExecutor);
  }

  /**
   * Returns {@link ComputationWorkExecutor} that was previously offered in {@link
   * #releaseComputationWorkExecutor(ComputationWorkExecutor)} or {@link Optional#empty()} if one
   * does not exist.
   */
  public Optional<ComputationWorkExecutor> acquireComputationWorkExecutor() {
    return Optional.ofNullable(computationWorkExecutors.poll());
  }

  /**
   * Mark the given {@link ShardedKey} and {@link Work} as active, and schedules execution of {@link
   * Work} if there is no active {@link Work} for the {@link ShardedKey} already processing. Returns
   * whether the {@link Work} will be activated, either immediately or sometime in the future.
   */
  public boolean activateWork(ExecutableWork executableWork) {
    switch (activeWorkState.activateWorkForKey(executableWork)) {
      case DUPLICATE:
        // Fall through intentionally. Work was not and will not be activated in these cases.
      case STALE:
        return false;
      case QUEUED:
        return true;
      case EXECUTE:
        {
          execute(executableWork);
          return true;
        }
      default:
        // This will never happen, the switch is exhaustive.
        throw new IllegalStateException("Unrecognized ActivateWorkResult");
    }
  }

  public void failWork(Multimap<Long, WorkId> failedWork) {
    activeWorkState.failWorkForKey(failedWork);
  }

  /**
   * Marks the work for the given shardedKey as complete. Schedules queued work for the key if any.
   */
  public void completeWorkAndScheduleNextWorkForKey(ShardedKey shardedKey, WorkId workId) {
    activeWorkState
        .completeWorkAndGetNextWorkForKey(shardedKey, workId)
        .ifPresent(this::forceExecute);
  }

  public void invalidateStuckCommits(Instant stuckCommitDeadline) {
    activeWorkState.invalidateStuckCommits(
        stuckCommitDeadline, this::completeWorkAndScheduleNextWorkForKey);
  }

  private void execute(ExecutableWork executableWork) {
    executor.execute(executableWork, executableWork.work().getWorkItem().getSerializedSize());
  }

  private void forceExecute(ExecutableWork executableWork) {
    executor.forceExecute(executableWork, executableWork.work().getWorkItem().getSerializedSize());
  }

  public ImmutableListMultimap<ShardedKey, RefreshableWork> currentActiveWorkReadOnly() {
    return activeWorkState.getReadOnlyActiveWork();
  }

  public ImmutableList<RefreshableWork> getRefreshableWork(Instant refreshDeadline) {
    return activeWorkState.getRefreshableWork(refreshDeadline);
  }

  public GetWorkBudget getActiveWorkBudget() {
    return activeWorkState.currentActiveWorkBudget();
  }

  public void printActiveWork(PrintWriter writer) {
    activeWorkState.printActiveWork(writer, Instant.now());
  }

  public String sourceBytesProcessCounterName() {
    return sourceBytesProcessCounterName;
  }

  @VisibleForTesting
  public final void close() {
    @Nullable ComputationWorkExecutor computationWorkExecutor;
    while ((computationWorkExecutor = computationWorkExecutors.poll()) != null) {
      computationWorkExecutor.invalidate();
    }
    computationWorkExecutors.clear();
  }
}
