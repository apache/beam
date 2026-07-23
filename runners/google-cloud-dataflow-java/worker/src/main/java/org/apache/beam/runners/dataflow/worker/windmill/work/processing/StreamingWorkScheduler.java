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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.services.dataflow.model.MapTask;
import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.DataflowMapTaskExecutorFactory;
import org.apache.beam.runners.dataflow.worker.HotKeyLogger;
import org.apache.beam.runners.dataflow.worker.ReaderCache;
import org.apache.beam.runners.dataflow.worker.StreamingModeExecutionContext;
import org.apache.beam.runners.dataflow.worker.StreamingModeExecutionContext.KeyTransitionListener;
import org.apache.beam.runners.dataflow.worker.WorkItemCancelledException;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingMDC;
import org.apache.beam.runners.dataflow.worker.streaming.BoundedQueueExecutorWorkHandle;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationWorkExecutor;
import org.apache.beam.runners.dataflow.worker.streaming.ExecutableWork;
import org.apache.beam.runners.dataflow.worker.streaming.StageInfo;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingGlobalConfigHandle;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingCounters;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputStateFetcherFactory;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.util.ExceptionUtils;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.LatencyAttribution;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.Commit;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.FailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.WorkFailureProcessor;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Schedules execution of user code to process a {@link
 * org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem} then commits the work item
 * back to streaming execution backend.
 */
@Internal
@ThreadSafe
public class StreamingWorkScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingWorkScheduler.class);

  private final Supplier<Instant> clock;
  private final ComputationWorkExecutorFactory computationWorkExecutorFactory;
  private final WorkFailureProcessor workFailureProcessor;
  private final StreamingCommitFinalizer commitFinalizer;
  private final StreamingCounters streamingCounters;
  private final ConcurrentMap<String, StageInfo> stageInfoMap;
  private final DataflowExecutionStateSampler sampler;
  private final BoundedQueueExecutor workExecutor;
  private final boolean multiKeyExperimentEnabled;

  public StreamingWorkScheduler(
      Supplier<Instant> clock,
      BoundedQueueExecutor workExecutor,
      ComputationWorkExecutorFactory computationWorkExecutorFactory,
      WorkFailureProcessor workFailureProcessor,
      StreamingCommitFinalizer commitFinalizer,
      StreamingCounters streamingCounters,
      ConcurrentMap<String, StageInfo> stageInfoMap,
      DataflowExecutionStateSampler sampler,
      boolean multiKeyExperimentEnabled) {
    this.clock = clock;
    this.workExecutor = workExecutor;
    this.computationWorkExecutorFactory = computationWorkExecutorFactory;
    this.workFailureProcessor = workFailureProcessor;
    this.commitFinalizer = commitFinalizer;
    this.streamingCounters = streamingCounters;
    this.stageInfoMap = stageInfoMap;
    this.sampler = sampler;
    this.multiKeyExperimentEnabled = multiKeyExperimentEnabled;
  }

  public static StreamingWorkScheduler create(
      DataflowWorkerHarnessOptions options,
      boolean multiKeyExperimentEnabled,
      Supplier<Instant> clock,
      ReaderCache readerCache,
      DataflowMapTaskExecutorFactory mapTaskExecutorFactory,
      BoundedQueueExecutor workExecutor,
      ScheduledExecutorService commitFinalizerCleanupExecutor,
      Function<String, WindmillStateCache.ForComputation> stateCacheFactory,
      FailureTracker failureTracker,
      WorkFailureProcessor workFailureProcessor,
      StreamingCounters streamingCounters,
      HotKeyLogger hotKeyLogger,
      DataflowExecutionStateSampler sampler,
      IdGenerator idGenerator,
      StreamingGlobalConfigHandle globalConfigHandle,
      ConcurrentMap<String, StageInfo> stageInfoMap) {
    SideInputStateFetcherFactory sideInputStateFetcherFactory =
        SideInputStateFetcherFactory.fromOptions(options);

    ComputationWorkExecutorFactory computationWorkExecutorFactory =
        new ComputationWorkExecutorFactory(
            options,
            mapTaskExecutorFactory,
            readerCache,
            stateCacheFactory,
            sampler,
            streamingCounters,
            failureTracker,
            idGenerator,
            globalConfigHandle,
            hotKeyLogger,
            sideInputStateFetcherFactory);

    return new StreamingWorkScheduler(
        clock,
        workExecutor,
        computationWorkExecutorFactory,
        workFailureProcessor,
        StreamingCommitFinalizer.create(workExecutor, commitFinalizerCleanupExecutor),
        streamingCounters,
        stageInfoMap,
        sampler,
        multiKeyExperimentEnabled);
  }

  private static long computeShuffleBytesRead(Windmill.WorkItem workItem) {
    return workItem.getMessageBundlesList().stream()
        .flatMap(bundle -> bundle.getMessagesList().stream())
        .map(Windmill.Message::getSerializedSize)
        .map(size -> (long) size)
        .reduce(0L, Long::sum);
  }

  private static Windmill.WorkItemCommitRequest.Builder initializeOutputBuilder(
      ByteString key, Windmill.WorkItem workItem) {
    return Windmill.WorkItemCommitRequest.newBuilder()
        .setKey(key)
        .setShardingKey(workItem.getShardingKey())
        .setWorkToken(workItem.getWorkToken())
        .setCacheToken(workItem.getCacheToken());
  }

  private static void setLoggingContextComputation(@Nullable String computationId) {
    DataflowWorkerLoggingMDC.setStageName(computationId);
  }

  private static void setLoggingContextWorkId(@Nullable String workLatencyTrackingId) {
    DataflowWorkerLoggingMDC.setWorkId(workLatencyTrackingId);
  }

  /**
   * Schedule work for execution. Work may be executed immediately, or queued and executed in the
   * future. Only one work may be "active" (currently executing) per key at a time.
   */
  public void scheduleWork(
      ComputationState computationState,
      Windmill.WorkItem workItem,
      long serializedWorkItemSize,
      Watermarks watermarks,
      Work.ProcessingContext processingContext,
      boolean drainMode,
      ImmutableList<LatencyAttribution> getWorkStreamLatencies) {
    // Before any processing starts, call any pending OnCommit callbacks
    commitFinalizer.finalizeCommits(workItem.getSourceState().getFinalizeIdsList());
    computationState.activateWork(
        ExecutableWork.create(
            Work.create(
                workItem,
                serializedWorkItemSize,
                watermarks,
                processingContext,
                drainMode,
                clock,
                getWorkStreamLatencies),
            (work, handle) -> processWork(computationState, work, handle)));
  }

  /** Adds any applied finalize ids to the commit finalizer to have their callbacks executed. */
  public void queueAppliedFinalizeIds(ImmutableList<Long> appliedFinalizeIds) {
    commitFinalizer.finalizeCommits(appliedFinalizeIds);
  }

  /**
   * Executes the user DoFns processing {@link Work} then queues the {@link Commit}(s) to be sent to
   * backing persistent store to mark that the {@link Work} has finished processing. May retry
   * internally if processing fails due to uncaught {@link Exception}(s).
   *
   * @implNote This will block the calling thread during execution of user DoFns.
   * @param handle handled to pass to BoundedQueueExecutor.pollWork
   */
  private void processWork(
      ComputationState computationState, Work work, BoundedQueueExecutorWorkHandle handle) {
    Windmill.WorkItem workItem = work.getWorkItem();
    String computationId = computationState.getComputationId();
    LOG.debug("Starting processing for {}:\n{}", computationId, work);
    setLoggingContextComputation(computationId);
    KeyTransitionListener keyTransitionListener = createKeyTransitionListener();
    keyTransitionListener.onKeyTransition(null, work);

    // Before any processing starts, call any pending OnCommit callbacks.  Nothing that requires
    // cleanup should be done before this, since we might exit early here.
    commitFinalizer.finalizeCommits(workItem.getSourceState().getFinalizeIdsList());

    if (workItem.getSourceState().getOnlyFinalize()) {
      handleOnlyFinalize(computationState, work, workItem);
      return;
    }

    long processingStartTimeNanos = System.nanoTime();
    StageInfo stageInfo = getStageInfo(computationState);

    @Nullable List<Work> workBatch = null;
    try {
      if (work.isFailed()) {
        throw new WorkItemCancelledException(workItem.getShardingKey());
      }

      // Execute the user code for the Work batch.
      ExecuteWorkResult executeWorkResult =
          executeWork(work, stageInfo, computationState, handle, keyTransitionListener);
      workBatch = executeWorkResult.workBatch();
      List<Windmill.WorkItemCommitRequest> workItemCommits = executeWorkResult.workItemCommits();

      commitFinalizer.cacheCommitFinalizers(executeWorkResult.finalizationCallbacks());

      commitWorkBatch(computationState, workBatch, workItemCommits);

      recordProcessingStats(workBatch, workItemCommits, executeWorkResult.stateBytesRead());
      LOG.debug("Processing done for work batch size: {}", workBatch.size());
    } catch (Throwable t) {
      handleProcessWorkFailure(computationState, handle.getWorkBatch(), computationId, work, t);
    } finally {
      List<Work> processedWorkBatch = workBatch != null ? workBatch : ImmutableList.of(work);
      // Update total processing time counters. Updating in finally clause ensures that
      // work items causing exceptions are also accounted in time spent.
      recordProcessingTime(stageInfo, processedWorkBatch, processingStartTimeNanos);

      setLoggingContextWorkId(null);
      setLoggingContextComputation(null);
      sampler.resetForWorkId(work.getLatencyTrackingId());
      for (Work w : processedWorkBatch) {
        w.setProcessingThreadName("");
      }
    }
  }

  private void recordProcessingStats(
      List<Work> workBatch,
      List<Windmill.WorkItemCommitRequest> workItemCommits,
      long totalStateBytesRead) {
    long totalStateBytesWritten = 0;
    long totalShuffleBytesRead = 0;
    checkState(workBatch.size() == workItemCommits.size());
    for (int i = 0; i < workBatch.size(); i++) {
      Windmill.WorkItem workItem = workBatch.get(i).getWorkItem();
      Windmill.WorkItemCommitRequest commit = workItemCommits.get(i);
      // Compute shuffle and state byte statistics these will be flushed asynchronously.
      long stateBytesWritten =
          commit
              .toBuilder()
              .clearOutputMessages()
              .clearPerWorkItemLatencyAttributions()
              .build()
              .getSerializedSize();
      totalStateBytesWritten += stateBytesWritten;
      totalShuffleBytesRead += computeShuffleBytesRead(workItem);
    }
    streamingCounters.windmillShuffleBytesRead().addValue(totalShuffleBytesRead);
    streamingCounters.windmillStateBytesRead().addValue(totalStateBytesRead);
    streamingCounters.windmillStateBytesWritten().addValue(totalStateBytesWritten);
  }

  private ExecuteWorkResult executeWork(
      Work work,
      StageInfo stageInfo,
      ComputationState computationState,
      BoundedQueueExecutorWorkHandle handle,
      KeyTransitionListener keyTransitionListener)
      throws Exception {
    ComputationWorkExecutor computationWorkExecutor =
        computationState
            .acquireComputationWorkExecutor()
            .orElseGet(
                () ->
                    computationWorkExecutorFactory.createComputationWorkExecutor(
                        stageInfo, computationState, work.getLatencyTrackingId()));

    try {
      StreamingModeExecutionContext context = computationWorkExecutor.context();

      // Blocks while executing work.
      computationWorkExecutor.executeWork(work, workExecutor, handle, keyTransitionListener);

      List<Work> workBatch;
      List<Windmill.WorkItemCommitRequest> workItemCommits;
      Map<Long, Pair<Instant, Runnable>> finalizationCallbacks;
      long stateBytesRead;
      {
        if (context.workIsFailed()) {
          throw new WorkItemCancelledException(work.getWorkItem().getShardingKey());
        }
        context.flushState();

        // Retrieve executed works, work item commits, and accumulated callbacks from execution
        // context
        workBatch = context.getExecutedWorks();
        workItemCommits = context.getWorkItemCommits();
        finalizationCallbacks = context.getFinalizationCallbacks();
        stateBytesRead = context.getStateBytesRead();

        context.reset(); // Don't use context after this.
      }
      // Release the execution state for another thread to use.
      computationState.releaseComputationWorkExecutor(computationWorkExecutor);
      computationWorkExecutor = null;

      return ExecuteWorkResult.create(
          workBatch, workItemCommits, finalizationCallbacks, stateBytesRead);
    } catch (Throwable t) {
      if (computationWorkExecutor != null) {
        // If processing failed due to a thrown exception, close the executionState. Do not
        // return/release the executionState back to computationState as that will lead to this
        // executionState instance being reused.
        LOG.debug(
            "Invalidating executor after work item {} failed",
            work.getWorkItem().getWorkToken(),
            t);
        computationWorkExecutor.invalidate();
      }
      // Re-throw the exception, it will be caught and handled by workFailureProcessor downstream.
      throw t;
    }
  }

  private void handleOnlyFinalize(
      ComputationState computationState, Work work, Windmill.WorkItem workItem) {
    Windmill.WorkItemCommitRequest.Builder outputBuilder =
        initializeOutputBuilder(workItem.getKey(), workItem);
    outputBuilder.setSourceStateUpdates(Windmill.SourceState.newBuilder().setOnlyFinalize(true));
    work.setState(Work.State.COMMIT_QUEUED);
    work.queueCommit(outputBuilder.build(), computationState);
  }

  private StageInfo getStageInfo(ComputationState computationState) {
    MapTask mapTask = computationState.getMapTask();
    return stageInfoMap.computeIfAbsent(
        mapTask.getStageName(), s -> StageInfo.create(s, mapTask.getSystemName()));
  }

  private void commitWorkBatch(
      ComputationState computationState,
      List<Work> workBatch,
      List<Windmill.WorkItemCommitRequest> workItemCommits) {
    if (workBatch.isEmpty()) {
      return;
    }
    if (workBatch.size() > 1 || multiKeyExperimentEnabled) {
      commitMultiKeyWorkBatch(computationState, workBatch, workItemCommits);
    } else {
      commitSingleKeyWork(computationState, workBatch.get(0), workItemCommits.get(0));
    }
  }

  private void commitMultiKeyWorkBatch(
      ComputationState computationState,
      List<Work> workBatch,
      List<Windmill.WorkItemCommitRequest> workItemCommits) {
    Windmill.MultiKeyWorkItemCommitRequest.Builder multiKeyBuilder =
        Windmill.MultiKeyWorkItemCommitRequest.newBuilder();

    Work primaryWork = workBatch.get(0);
    Work.KeyGroup keyGroup = primaryWork.getKeyGroup();
    multiKeyBuilder.setKeyGroup(
        Windmill.Uint128Proto.newBuilder().setHigh(keyGroup.high()).setLow(keyGroup.low()).build());

    for (int i = 0; i < workBatch.size(); i++) {
      // TODO: Retry on commit truncations
      Windmill.WorkItemCommitRequest commit = workItemCommits.get(i);
      Work w = workBatch.get(i);
      multiKeyBuilder.addRequests(
          commit
              .toBuilder()
              .addAllPerWorkItemLatencyAttributions(w.getLatencyAttributions(sampler))
              .build());
    }

    // Transition states of all completed works in the batch to COMMIT_QUEUED and submit
    for (Work w : workBatch) {
      w.setState(Work.State.COMMIT_QUEUED);
    }

    // Package and submit the commit batch transactionally
    primaryWork
        .workCommitter()
        .accept(
            Commit.createMultiKey(
                multiKeyBuilder.build(), computationState, ImmutableList.copyOf(workBatch)));
  }

  private void commitSingleKeyWork(
      ComputationState computationState, Work work, Windmill.WorkItemCommitRequest commitRequest) {
    work.setState(Work.State.COMMIT_QUEUED);
    Windmill.WorkItemCommitRequest commitRequestWithAttributions =
        commitRequest
            .toBuilder()
            .addAllPerWorkItemLatencyAttributions(work.getLatencyAttributions(sampler))
            .build();
    work.queueCommit(commitRequestWithAttributions, computationState);
  }

  private void handleProcessWorkFailure(
      ComputationState computationState,
      List<Work> failedBatch,
      String computationId,
      Work primaryWork,
      Throwable t) {
    try {
      List<ExecutableWork> executableWorks = new ArrayList<>();
      for (Work w : failedBatch) {
        executableWorks.add(
            ExecutableWork.create(w, (retry, h) -> processWork(computationState, retry, h)));
      }

      workFailureProcessor.logAndProcessFailureBatch(
          computationId,
          executableWorks,
          t,
          invalidWork ->
              computationState.completeWorkAndScheduleNextWorkForKey(
                  invalidWork.getShardedKey(), invalidWork.id()));
    } catch (OutOfMemoryError oom) {
      throw oom;
    } catch (Throwable t2) {
      LOG.warn("Failed to process work failure safely for work {}", primaryWork.id(), t2);
      throw ExceptionUtils.safeWrapThrowableAsException(t2);
    }
  }

  private void recordProcessingTime(
      StageInfo stageInfo, List<Work> workBatch, long processingStartTimeNanos) {
    long processingTimeMsecs =
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - processingStartTimeNanos);
    stageInfo.totalProcessingMsecs().addValue(processingTimeMsecs);
    if (anyWorkHasTimers(workBatch)) {
      // Attribute all the processing to timers if the work item contains any timers.
      // Tests show that work items rarely contain both timers and message bundles. It should
      // be a fairly close approximation.
      // Another option: Derive time split between messages and timers based on recent totals.
      // either here or in DFE.
      stageInfo.timerProcessingMsecs().addValue(processingTimeMsecs);
    }
  }

  private static boolean anyWorkHasTimers(List<Work> works) {
    return works.stream().anyMatch(w -> w.getWorkItem().hasTimers());
  }

  private KeyTransitionListener createKeyTransitionListener() {
    return (oldWork, newWork) -> {
      newWork.recordGetWorkStreamLatencies();
      newWork.setState(Work.State.PROCESSING);
      setLoggingContextWorkId(newWork.getLatencyTrackingId());
      if (oldWork != null) {
        newWork.setProcessingThreadName(oldWork.getProcessingThreadName());
        oldWork.setProcessingThreadName("");
      } else {
        newWork.setProcessingThreadName(Thread.currentThread().getName());
      }
    };
  }

  @AutoValue
  abstract static class ExecuteWorkResult {
    static ExecuteWorkResult create(
        List<Work> workBatch,
        List<Windmill.WorkItemCommitRequest> workItemCommits,
        Map<Long, Pair<Instant, Runnable>> finalizationCallbacks,
        long stateBytesRead) {
      return new AutoValue_StreamingWorkScheduler_ExecuteWorkResult(
          workBatch, workItemCommits, finalizationCallbacks, stateBytesRead);
    }

    abstract List<Work> workBatch();

    abstract List<Windmill.WorkItemCommitRequest> workItemCommits();

    // Map<finalizerId, Pair<callbackExpiration, callback>>
    abstract Map<Long, Pair<Instant, Runnable>> finalizationCallbacks();

    abstract long stateBytesRead();
  }
}
