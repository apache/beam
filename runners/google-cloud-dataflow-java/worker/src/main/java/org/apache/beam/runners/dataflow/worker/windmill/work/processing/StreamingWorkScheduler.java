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

import com.google.api.services.dataflow.model.MapTask;
import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.HashMap;
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
import org.apache.beam.runners.dataflow.worker.WorkItemCancelledException;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingMDC;
import org.apache.beam.runners.dataflow.worker.streaming.BoundedQueueExecutorWorkHandle;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationWorkExecutor;
import org.apache.beam.runners.dataflow.worker.streaming.ExecutableWork;
import org.apache.beam.runners.dataflow.worker.streaming.KeyCommitTooLargeException;
import org.apache.beam.runners.dataflow.worker.streaming.StageInfo;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingGlobalConfigHandle;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingCounters;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputStateFetcher;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputStateFetcherFactory;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.util.ExceptionUtils;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.LatencyAttribution;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.Commit;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateReader;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.FailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.WorkFailureProcessor;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
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

  private static class BatchExecutionException extends RuntimeException {
    private final List<Work> executedWorks;

    BatchExecutionException(Throwable cause, List<Work> executedWorks) {
      super(cause);
      this.executedWorks = executedWorks;
    }

    List<Work> getExecutedWorks() {
      return executedWorks;
    }
  }

  private final DataflowWorkerHarnessOptions options;
  private final Supplier<Instant> clock;
  private final ComputationWorkExecutorFactory computationWorkExecutorFactory;
  private final SideInputStateFetcherFactory sideInputStateFetcherFactory;
  private final FailureTracker failureTracker;
  private final WorkFailureProcessor workFailureProcessor;
  private final StreamingCommitFinalizer commitFinalizer;
  private final StreamingCounters streamingCounters;
  private final ConcurrentMap<String, StageInfo> stageInfoMap;
  private final DataflowExecutionStateSampler sampler;
  private final StreamingGlobalConfigHandle globalConfigHandle;
  private final BoundedQueueExecutor workExecutor;

  public StreamingWorkScheduler(
      DataflowWorkerHarnessOptions options,
      Supplier<Instant> clock,
      BoundedQueueExecutor workExecutor,
      ComputationWorkExecutorFactory computationWorkExecutorFactory,
      SideInputStateFetcherFactory sideInputStateFetcherFactory,
      FailureTracker failureTracker,
      WorkFailureProcessor workFailureProcessor,
      StreamingCommitFinalizer commitFinalizer,
      StreamingCounters streamingCounters,
      ConcurrentMap<String, StageInfo> stageInfoMap,
      DataflowExecutionStateSampler sampler,
      StreamingGlobalConfigHandle globalConfigHandle) {
    this.options = options;
    this.clock = clock;
    this.workExecutor = workExecutor;
    this.computationWorkExecutorFactory = computationWorkExecutorFactory;
    this.sideInputStateFetcherFactory = sideInputStateFetcherFactory;
    this.failureTracker = failureTracker;
    this.workFailureProcessor = workFailureProcessor;
    this.commitFinalizer = commitFinalizer;
    this.streamingCounters = streamingCounters;
    this.stageInfoMap = stageInfoMap;
    this.sampler = sampler;
    this.globalConfigHandle = globalConfigHandle;
  }

  public static StreamingWorkScheduler create(
      DataflowWorkerHarnessOptions options,
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
    ComputationWorkExecutorFactory computationWorkExecutorFactory =
        new ComputationWorkExecutorFactory(
            options,
            mapTaskExecutorFactory,
            readerCache,
            stateCacheFactory,
            sampler,
            streamingCounters.pendingDeltaCounters(),
            idGenerator,
            globalConfigHandle,
            hotKeyLogger);

    return new StreamingWorkScheduler(
        options,
        clock,
        workExecutor,
        computationWorkExecutorFactory,
        SideInputStateFetcherFactory.fromOptions(options),
        failureTracker,
        workFailureProcessor,
        StreamingCommitFinalizer.create(workExecutor, commitFinalizerCleanupExecutor),
        streamingCounters,
        stageInfoMap,
        sampler,
        globalConfigHandle);
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

  private static Windmill.WorkItemCommitRequest buildWorkItemTruncationRequest(
      ByteString key, Windmill.WorkItem workItem, int estimatedCommitSize) {
    Windmill.WorkItemCommitRequest.Builder outputBuilder = initializeOutputBuilder(key, workItem);
    outputBuilder.setExceedsMaxWorkItemCommitBytes(true);
    outputBuilder.setEstimatedWorkItemCommitBytes(estimatedCommitSize);
    return outputBuilder.build();
  }

  /** Sets the stage name and workId of the Thread executing the {@link Work} for logging. */
  private static void setUpWorkLoggingContext(String workLatencyTrackingId, String computationId) {
    DataflowWorkerLoggingMDC.setWorkId(workLatencyTrackingId);
    DataflowWorkerLoggingMDC.setStageName(computationId);
  }

  private static String getShuffleTaskStepName(MapTask mapTask) {
    // The MapTask instruction is ordered by dependencies, such that the first element is
    // always going to be the shuffle task.
    return mapTask.getInstructions().get(0).getName();
  }

  /** Resets logging context of the Thread executing the {@link Work} for logging. */
  private void resetWorkLoggingContext() {
    DataflowWorkerLoggingMDC.setWorkId(null);
    DataflowWorkerLoggingMDC.setStageName(null);
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
    computationState.activateWork(
        ExecutableWork.create(
            Work.create(
                workItem, serializedWorkItemSize, watermarks, processingContext, drainMode, clock),
            (work, handle) -> processWork(computationState, work, getWorkStreamLatencies, handle)));
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
   * @param handle budget handle associated with the executed work
   */
  private void processWork(
      ComputationState computationState,
      Work work,
      ImmutableList<LatencyAttribution> getWorkStreamLatencies,
      BoundedQueueExecutorWorkHandle handle) {
    work.recordGetWorkStreamLatencies(getWorkStreamLatencies);
    processWork(computationState, work, handle);
  }

  private void processWork(
      ComputationState computationState, Work work, BoundedQueueExecutorWorkHandle handle) {
    Windmill.WorkItem workItem = work.getWorkItem();
    String computationId = computationState.getComputationId();
    ByteString key = workItem.getKey();
    work.setProcessingThreadName(Thread.currentThread().getName());
    work.setState(Work.State.PROCESSING);
    setUpWorkLoggingContext(work.getLatencyTrackingId(), computationId);
    LOG.debug("Starting processing for {}:\n{}", computationId, work);

    // Before any processing starts, call any pending OnCommit callbacks.  Nothing that requires
    // cleanup should be done before this, since we might exit early here.
    commitFinalizer.finalizeCommits(workItem.getSourceState().getFinalizeIdsList());
    if (workItem.getSourceState().getOnlyFinalize()) {
      Windmill.WorkItemCommitRequest.Builder outputBuilder = initializeOutputBuilder(key, workItem);
      outputBuilder.setSourceStateUpdates(Windmill.SourceState.newBuilder().setOnlyFinalize(true));
      work.setState(Work.State.COMMIT_QUEUED);
      work.queueCommit(outputBuilder.build(), computationState);
      return;
    }

    long processingStartTimeNanos = System.nanoTime();
    MapTask mapTask = computationState.getMapTask();
    StageInfo stageInfo =
        stageInfoMap.computeIfAbsent(
            mapTask.getStageName(), s -> StageInfo.create(s, mapTask.getSystemName()));

    List<Work> worksToCleanup = null;
    try {
      if (work.isFailed()) {
        throw new WorkItemCancelledException(workItem.getShardingKey());
      }

      // Execute the user code for the Work batch.
      ExecuteWorkResult executeWorkResult = executeWork(work, stageInfo, computationState, handle);
      List<Work> workBatch = executeWorkResult.workBatch();
      worksToCleanup = workBatch;
      List<Windmill.WorkItemCommitRequest.Builder> outputBuilders =
          executeWorkResult.outputBuilders();
      Map<Long, Pair<Instant, Runnable>> accumulatedCallbacks =
          executeWorkResult.accumulatedCallbacks();

      // Cache accumulated flat-map of all callbacks
      commitFinalizer.cacheCommitFinalizers(accumulatedCallbacks);

      if (workBatch.size() > 1) {
        // Multi-key commit packing
        Windmill.MultiKeyWorkItemCommitRequest.Builder multiKeyBuilder =
            Windmill.MultiKeyWorkItemCommitRequest.newBuilder();

        // Group by the KeyGroup (from primary work item)
        Work primaryWork = workBatch.get(0);
        if (primaryWork.getKeyGroup().isPresent()) {
          Work.KeyGroup keyGroup = primaryWork.getKeyGroup().get();
          multiKeyBuilder.setKeyGroup(
              Windmill.Uint128Proto.newBuilder()
                  .setHigh(keyGroup.high())
                  .setLow(keyGroup.low())
                  .build());
        }

        for (int i = 0; i < workBatch.size(); i++) {
          Windmill.WorkItemCommitRequest.Builder builder = outputBuilders.get(i);
          Work w = workBatch.get(i);
          if (i == 0) {
            builder.addAllPerWorkItemLatencyAttributions(w.getLatencyAttributions(sampler));
          }

          // Aggregate ONLY finalize IDs to the top level
          multiKeyBuilder.addAllFinalizeIds(builder.getFinalizeIdsList());
          // Clear only finalize IDs from individual request
          builder.clearFinalizeIds();
          // Keep output_messages and pubsub_messages scoped inside builder
          multiKeyBuilder.addRequests(builder.build());
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
      } else {
        // Standard single-key commit path (fully backward-compatible with Appliance path)
        Windmill.WorkItemCommitRequest.Builder commitRequest = outputBuilders.get(0);
        Windmill.WorkItemCommitRequest validatedCommitRequest =
            validateCommitRequestSize(
                commitRequest.build(), computationId, workBatch.get(0).getWorkItem());
        workBatch.get(0).setState(Work.State.COMMIT_QUEUED);
        validatedCommitRequest =
            validatedCommitRequest
                .toBuilder()
                .addAllPerWorkItemLatencyAttributions(
                    workBatch.get(0).getLatencyAttributions(sampler))
                .build();
        workBatch.get(0).queueCommit(validatedCommitRequest, computationState);
      }

      recordProcessingStats(workBatch, outputBuilders, executeWorkResult.stateBytesRead());
      LOG.debug("Processing done for work batch size: {}", workBatch.size());
    } catch (Throwable t) {
      // Handle batch failure rollback and rescheduling
      try {
        List<Work> failedBatch = ImmutableList.of(work);
        Throwable errorToProcess = t;
        if (t instanceof BatchExecutionException) {
          failedBatch = ((BatchExecutionException) t).getExecutedWorks();
          Throwable cause = t.getCause();
          if (cause != null) {
            errorToProcess = cause;
          }
        }
        worksToCleanup = failedBatch;

        List<ExecutableWork> executableWorks = new ArrayList<>();
        for (Work w : failedBatch) {
          executableWorks.add(
              ExecutableWork.create(w, (retry, h) -> processWork(computationState, retry, h)));
        }

        workFailureProcessor.logAndProcessFailureBatch(
            computationId,
            executableWorks,
            errorToProcess,
            invalidWork ->
                computationState.completeWorkAndScheduleNextWorkForKey(
                    invalidWork.getShardedKey(), invalidWork.id()));
      } catch (OutOfMemoryError oom) {
        throw oom;
      } catch (Throwable t2) {
        LOG.warn("Failed to process work failure safely for work batch", t2);
        throw ExceptionUtils.propagate(t2);
      }
    } finally {
      long processingTimeMsecs =
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - processingStartTimeNanos);
      stageInfo.totalProcessingMsecs().addValue(processingTimeMsecs);

      if (work.getWorkItem().hasTimers()) {
        stageInfo.timerProcessingMsecs().addValue(processingTimeMsecs);
      }

      if (worksToCleanup != null) {
        for (Work w : worksToCleanup) {
          w.setOnFailureListener(null);
        }
      } else {
        work.setOnFailureListener(null);
      }

      resetWorkLoggingContext();
      sampler.resetForWorkId(work.getLatencyTrackingId());
      work.setProcessingThreadName("");
    }
  }

  private Windmill.WorkItemCommitRequest validateCommitRequestSize(
      Windmill.WorkItemCommitRequest commitRequest,
      String computationId,
      Windmill.WorkItem workItem) {
    long byteLimit = globalConfigHandle.getConfig().operationalLimits().getMaxWorkItemCommitBytes();
    int commitSize = commitRequest.getSerializedSize();
    int estimatedCommitSize = commitSize < 0 ? Integer.MAX_VALUE : commitSize;

    // Detect overflow of integer serialized size or if the byte limit was exceeded.
    // Commit is too large if overflow has occurred or the commitSize has exceeded the allowed
    // commit byte limit.
    streamingCounters.windmillMaxObservedWorkItemCommitBytes().addValue(estimatedCommitSize);
    if (commitSize >= 0 && commitSize < byteLimit) {
      return commitRequest;
    }

    KeyCommitTooLargeException e =
        KeyCommitTooLargeException.causedBy(computationId, byteLimit, commitRequest);
    failureTracker.trackFailure(computationId, workItem, e);
    LOG.error("{}", e.toString());

    // Drop the current request in favor of a new, minimal one requesting truncation.
    // Messages, timers, counters, and other commit content will not be used by the service
    // so, we're purposefully dropping them here
    return buildWorkItemTruncationRequest(workItem.getKey(), workItem, estimatedCommitSize);
  }

  private void recordProcessingStats(
      List<Work> workBatch,
      List<Windmill.WorkItemCommitRequest.Builder> outputBuilders,
      long totalStateBytesRead) {
    long totalStateBytesWritten = 0;
    long totalShuffleBytesRead = 0;
    for (int i = 0; i < workBatch.size(); i++) {
      Windmill.WorkItem workItem = workBatch.get(i).getWorkItem();
      Windmill.WorkItemCommitRequest.Builder outputBuilder = outputBuilders.get(i);
      long stateBytesWritten =
          outputBuilder
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
      BoundedQueueExecutorWorkHandle handle)
      throws Exception {
    ComputationWorkExecutor computationWorkExecutor =
        computationState
            .acquireComputationWorkExecutor()
            .orElseGet(
                () ->
                    computationWorkExecutorFactory.createComputationWorkExecutor(
                        stageInfo, computationState, work.getLatencyTrackingId()));

    try {
      WindmillStateReader stateReader = work.createWindmillStateReader();
      SideInputStateFetcher localSideInputStateFetcher =
          sideInputStateFetcherFactory.createSideInputStateFetcher(work::fetchSideInput);

      // Parse limits from experiments
      String batchSizeStr =
          org.apache.beam.sdk.options.ExperimentalOptions.getExperimentValue(
              options, "max_key_group_batch_size");
      int maxKeyGroupBatchSize = batchSizeStr != null ? Integer.parseInt(batchSizeStr) : 100;

      String batchTimeStr =
          org.apache.beam.sdk.options.ExperimentalOptions.getExperimentValue(
              options, "max_key_group_batch_time_ms");
      long maxKeyGroupBatchTimeNanos =
          TimeUnit.MILLISECONDS.toNanos(batchTimeStr != null ? Long.parseLong(batchTimeStr) : 100);

      String batchBytesStr =
          org.apache.beam.sdk.options.ExperimentalOptions.getExperimentValue(
              options, "max_key_group_batch_bytes");
      long maxKeyGroupBatchBytes =
          batchBytesStr != null ? Long.parseLong(batchBytesStr) : 10L * 1024 * 1024;

      // MDC and samplers aligner callback
      StreamingModeExecutionContext.KeySwitchListener keySwitchListener =
          (oldWork, newWork) -> {
            resetWorkLoggingContext();
            setUpWorkLoggingContext(
                newWork.getLatencyTrackingId(), computationState.getComputationId());
            newWork.setProcessingThreadName(Thread.currentThread().getName());
            oldWork.setProcessingThreadName("");
          };

      // Blocks while executing work.
      computationWorkExecutor.executeWork(
          work,
          stateReader,
          localSideInputStateFetcher,
          workExecutor,
          handle,
          maxKeyGroupBatchSize,
          maxKeyGroupBatchTimeNanos,
          maxKeyGroupBatchBytes,
          keySwitchListener);

      StreamingModeExecutionContext context = computationWorkExecutor.context();
      if (context.workIsFailed()) {
        throw new WorkItemCancelledException(
            Preconditions.checkNotNull(context.getFailedWork()).getWorkItem().getShardingKey());
      }

      // Retrieve executed works, output builders, and accumulated callbacks from execution context
      ImmutableList<Work> workBatch = ImmutableList.copyOf(context.getExecutedWorks());
      ImmutableList<Windmill.WorkItemCommitRequest.Builder> outputBuilders =
          ImmutableList.copyOf(context.getOutputBuilders());
      Map<Long, Pair<Instant, Runnable>> accumulatedCallbacks =
          new HashMap<>(context.getAccumulatedCallbacks());

      // Release the execution state for another thread to use.
      computationState.releaseComputationWorkExecutor(computationWorkExecutor);
      computationWorkExecutor = null;

      return ExecuteWorkResult.create(
          workBatch,
          outputBuilders,
          accumulatedCallbacks,
          context.getStateBytesRead() + localSideInputStateFetcher.getBytesRead());
    } catch (Throwable t) {
      List<Work> executedWorks = ImmutableList.of(work);
      if (computationWorkExecutor != null) {
        executedWorks = ImmutableList.copyOf(computationWorkExecutor.context().getExecutedWorks());
        if (executedWorks.isEmpty()) {
          executedWorks = ImmutableList.of(work);
        }
        LOG.debug("Invalidating executor after work failure", t);
        computationWorkExecutor.invalidate();
      }
      throw new BatchExecutionException(t, executedWorks);
    }
  }

  @AutoValue
  abstract static class ExecuteWorkResult {
    static ExecuteWorkResult create(
        ImmutableList<Work> workBatch,
        ImmutableList<Windmill.WorkItemCommitRequest.Builder> outputBuilders,
        Map<Long, Pair<Instant, Runnable>> accumulatedCallbacks,
        long stateBytesRead) {
      return new AutoValue_StreamingWorkScheduler_ExecuteWorkResult(
          workBatch, outputBuilders, accumulatedCallbacks, stateBytesRead);
    }

    abstract ImmutableList<Work> workBatch();

    abstract ImmutableList<Windmill.WorkItemCommitRequest.Builder> outputBuilders();

    abstract Map<Long, Pair<Instant, Runnable>> accumulatedCallbacks();

    abstract long stateBytesRead();
  }
}
