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
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimap;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class representing the state of a computation.
 *
 * <p>This class is synchronized, but only used from the dispatch and commit threads, so should not
 * be heavily contended. Still, blocking work should not be done by it.
 */
@Internal
public class ComputationState {
  private static final Logger LOG = LoggerFactory.getLogger(ComputationState.class);
  private final String computationId;
  private final MapTask mapTask;
  private final ImmutableMap<String, String> transformUserNameToStateFamily;
  private final ActiveWorkState activeWorkState;
  private final BoundedQueueExecutor executor;
  private final ConcurrentLinkedQueue<ExecutionState> executionStateQueue;

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
    this.executionStateQueue = new ConcurrentLinkedQueue<>();
    this.activeWorkState = ActiveWorkState.create(computationStateCache);
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
   * Adds the {@link ExecutionState} to the internal {@link #executionStateQueue} so that it can be
   * re-used in future processing.
   */
  public void releaseExecutionState(ExecutionState executionState) {
    executionStateQueue.offer(executionState);
  }

  /**
   * Removes an {@link ExecutionState} instance from {@link #executionStateQueue} if one exists, and
   * returns it. Calls to this method must be followed by a call to {@link
   * #releaseExecutionState(ExecutionState)} to return the {@link ExecutionState} if it is to be
   * reused.
   */
  public Optional<ExecutionState> acquireExecutionState() {
    return Optional.ofNullable(executionStateQueue.poll());
  }

  /**
   * Mark the given {@link ShardedKey} and {@link Work} as active, and schedules execution of {@link
   * Work} if there is no active {@link Work} for the {@link ShardedKey} already processing. Returns
   * whether the {@link Work} will be activated, either immediately or sometime in the future.
   */
  public boolean activateWork(ShardedKey shardedKey, Work work) {
    switch (activeWorkState.activateWorkForKey(shardedKey, work)) {
      case DUPLICATE:
        // Fall through intentionally. Work was not and will not be activated in these cases.
      case STALE:
        return false;
      case QUEUED:
        return true;
      case EXECUTE:
        {
          execute(work);
          return true;
        }
      default:
        // This will never happen, the switch is exhaustive.
        throw new IllegalStateException("Unrecognized ActivateWorkResult");
    }
  }

  public boolean activateWork(Work work) {
    return activateWork(
        ShardedKey.create(work.getWorkItem().getKey(), work.getWorkItem().getShardingKey()), work);
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

  private void execute(Work work) {
    executor.execute(work, work.getWorkItem().getSerializedSize());
  }

  private void forceExecute(Work work) {
    executor.forceExecute(work, work.getWorkItem().getSerializedSize());
  }

  public ImmutableMap<ShardedKey, Deque<Work>> currentActiveWorkReadOnly() {
    return activeWorkState.getReadOnlyActiveWork();
  }

  public void printActiveWork(PrintWriter writer) {
    activeWorkState.printActiveWork(writer, Instant.now());
  }

  public GetWorkBudget getActiveWorkBudget() {
    return activeWorkState.currentActiveWorkBudget();
  }

  public final void close() {
    @Nullable ExecutionState executionState;
    while ((executionState = executionStateQueue.poll()) != null) {
      try {
        executionState.workExecutor().close();
      } catch (Exception e) {
        LOG.warn("Failed to close workExecutor {}.", executionState.workExecutor(), e);
      }
    }
    executionStateQueue.clear();
  }
}
