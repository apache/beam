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

import static org.apache.beam.sdk.options.ExperimentalOptions.hasExperiment;

import com.google.api.services.dataflow.model.MapTask;
import com.google.auto.value.AutoValue;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.DataflowMapTaskExecutorFactory;
import org.apache.beam.runners.dataflow.worker.HotKeyLogger;
import org.apache.beam.runners.dataflow.worker.ReaderCache;
import org.apache.beam.runners.dataflow.worker.WorkItemCancelledException;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingMDC;
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
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.Commit;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateReader;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.FailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.WorkFailureProcessor;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
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
public final class StreamingWorkScheduler {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingWorkScheduler.class);

  private final DataflowWorkerHarnessOptions options;
  private final Supplier<Instant> clock;
  private final ComputationWorkExecutorFactory computationWorkExecutorFactory;
  private final SideInputStateFetcherFactory sideInputStateFetcherFactory;
  private final FailureTracker failureTracker;
  private final WorkFailureProcessor workFailureProcessor;
  private final StreamingCommitFinalizer commitFinalizer;
  private final StreamingCounters streamingCounters;
  private final HotKeyLogger hotKeyLogger;
  private final ConcurrentMap<String, StageInfo> stageInfoMap;
  private final DataflowExecutionStateSampler sampler;
  private final StreamingGlobalConfigHandle globalConfigHandle;

  public StreamingWorkScheduler(
      DataflowWorkerHarnessOptions options,
      Supplier<Instant> clock,
      ComputationWorkExecutorFactory computationWorkExecutorFactory,
      SideInputStateFetcherFactory sideInputStateFetcherFactory,
      FailureTracker failureTracker,
      WorkFailureProcessor workFailureProcessor,
      StreamingCommitFinalizer commitFinalizer,
      StreamingCounters streamingCounters,
      HotKeyLogger hotKeyLogger,
      ConcurrentMap<String, StageInfo> stageInfoMap,
      DataflowExecutionStateSampler sampler,
      StreamingGlobalConfigHandle globalConfigHandle) {
    this.options = options;
    this.clock = clock;
    this.computationWorkExecutorFactory = computationWorkExecutorFactory;
    this.sideInputStateFetcherFactory = sideInputStateFetcherFactory;
    this.failureTracker = failureTracker;
    this.workFailureProcessor = workFailureProcessor;
    this.commitFinalizer = commitFinalizer;
    this.streamingCounters = streamingCounters;
    this.hotKeyLogger = hotKeyLogger;
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
            globalConfigHandle);

    return new StreamingWorkScheduler(
        options,
        clock,
        computationWorkExecutorFactory,
        SideInputStateFetcherFactory.fromOptions(options),
        failureTracker,
        workFailureProcessor,
        StreamingCommitFinalizer.create(workExecutor),
        streamingCounters,
        hotKeyLogger,
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
  private void resetWorkLoggingContext(String workLatencyTrackingId) {
    sampler.resetForWorkId(workLatencyTrackingId);
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
      Watermarks watermarks,
      Work.ProcessingContext processingContext,
      Collection<Windmill.LatencyAttribution> getWorkStreamLatencies) {
    computationState.activateWork(
        ExecutableWork.create(
            Work.create(workItem, watermarks, processingContext, clock, getWorkStreamLatencies),
            work -> processWork(computationState, work)));
  }

  /**
   * Executes the user DoFns processing {@link Work} then queues the {@link Commit}(s) to be sent to
   * backing persistent store to mark that the {@link Work} has finished processing. May retry
   * internally if processing fails due to uncaught {@link Exception}(s).
   *
   * @implNote This will block the calling thread during execution of user DoFns.
   */
  private void processWork(ComputationState computationState, Work work) {
    Windmill.WorkItem workItem = work.getWorkItem();
    String computationId = computationState.getComputationId();
    ByteString key = workItem.getKey();
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

    try {
      if (work.isFailed()) {
        throw new WorkItemCancelledException(workItem.getShardingKey());
      }

      // Execute the user code for the Work.
      ExecuteWorkResult executeWorkResult = executeWork(work, stageInfo, computationState);
      Windmill.WorkItemCommitRequest.Builder commitRequest = executeWorkResult.commitWorkRequest();

      // Validate the commit request, possibly requesting truncation if the commitSize is too large.
      Windmill.WorkItemCommitRequest validatedCommitRequest =
          validateCommitRequestSize(commitRequest.build(), computationId, workItem);

      // Queue the commit.
      work.queueCommit(validatedCommitRequest, computationState);
      recordProcessingStats(commitRequest, workItem, executeWorkResult);
      LOG.debug("Processing done for work token: {}", workItem.getWorkToken());
    } catch (Throwable t) {
      workFailureProcessor.logAndProcessFailure(
          computationId,
          ExecutableWork.create(work, retry -> processWork(computationState, retry)),
          t,
          invalidWork ->
              computationState.completeWorkAndScheduleNextWorkForKey(
                  invalidWork.getShardedKey(), invalidWork.id()));
    } finally {
      // Update total processing time counters. Updating in finally clause ensures that
      // work items causing exceptions are also accounted in time spent.
      long processingTimeMsecs =
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - processingStartTimeNanos);
      stageInfo.totalProcessingMsecs().addValue(processingTimeMsecs);

      // Attribute all the processing to timers if the work item contains any timers.
      // Tests show that work items rarely contain both timers and message bundles. It should
      // be a fairly close approximation.
      // Another option: Derive time split between messages and timers based on recent totals.
      // either here or in DFE.
      if (work.getWorkItem().hasTimers()) {
        stageInfo.timerProcessingMsecs().addValue(processingTimeMsecs);
      }

      resetWorkLoggingContext(work.getLatencyTrackingId());
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
    LOG.error(e.toString());

    // Drop the current request in favor of a new, minimal one requesting truncation.
    // Messages, timers, counters, and other commit content will not be used by the service
    // so, we're purposefully dropping them here
    return buildWorkItemTruncationRequest(workItem.getKey(), workItem, estimatedCommitSize);
  }

  private void recordProcessingStats(
      Windmill.WorkItemCommitRequest.Builder outputBuilder,
      Windmill.WorkItem workItem,
      ExecuteWorkResult executeWorkResult) {
    // Compute shuffle and state byte statistics these will be flushed asynchronously.
    long stateBytesWritten =
        outputBuilder
            .clearOutputMessages()
            .clearPerWorkItemLatencyAttributions()
            .build()
            .getSerializedSize();

    streamingCounters.windmillShuffleBytesRead().addValue(computeShuffleBytesRead(workItem));
    streamingCounters.windmillStateBytesRead().addValue(executeWorkResult.stateBytesRead());
    streamingCounters.windmillStateBytesWritten().addValue(stateBytesWritten);
  }

  private ExecuteWorkResult executeWork(
      Work work, StageInfo stageInfo, ComputationState computationState) throws Exception {
    Windmill.WorkItem workItem = work.getWorkItem();
    ByteString key = workItem.getKey();
    Windmill.WorkItemCommitRequest.Builder outputBuilder = initializeOutputBuilder(key, workItem);
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

      // If the read output KVs, then we can decode Windmill's byte key into userland
      // key object and provide it to the execution context for use with per-key state.
      // Otherwise, we pass null.
      //
      // The coder type that will be present is:
      //     WindowedValueCoder(TimerOrElementCoder(KvCoder))
      Optional<Coder<?>> keyCoder = computationWorkExecutor.keyCoder();
      @SuppressWarnings("deprecation")
      @Nullable
      Object executionKey =
          !keyCoder.isPresent() ? null : keyCoder.get().decode(key.newInput(), Coder.Context.OUTER);

      if (workItem.hasHotKeyInfo()) {
        Windmill.HotKeyInfo hotKeyInfo = workItem.getHotKeyInfo();
        Duration hotKeyAge = Duration.millis(hotKeyInfo.getHotKeyAgeUsec() / 1000);

        String stepName = getShuffleTaskStepName(computationState.getMapTask());
        if ((options.isHotKeyLoggingEnabled() || hasExperiment(options, "enable_hot_key_logging"))
            && keyCoder.isPresent()) {
          hotKeyLogger.logHotKeyDetection(stepName, hotKeyAge, executionKey);
        } else {
          hotKeyLogger.logHotKeyDetection(stepName, hotKeyAge);
        }
      }

      // Blocks while executing work.
      computationWorkExecutor.executeWork(
          executionKey, work, stateReader, localSideInputStateFetcher, outputBuilder);

      if (work.isFailed()) {
        throw new WorkItemCancelledException(workItem.getShardingKey());
      }

      // Reports source bytes processed to WorkItemCommitRequest if available.
      try {
        long sourceBytesProcessed =
            computationWorkExecutor.computeSourceBytesProcessed(
                computationState.sourceBytesProcessCounterName());
        outputBuilder.setSourceBytesProcessed(sourceBytesProcessed);
      } catch (Exception e) {
        LOG.error(e.toString());
      }

      commitFinalizer.cacheCommitFinalizers(computationWorkExecutor.context().flushState());

      // Release the execution state for another thread to use.
      computationState.releaseComputationWorkExecutor(computationWorkExecutor);

      work.setState(Work.State.COMMIT_QUEUED);
      outputBuilder.addAllPerWorkItemLatencyAttributions(work.getLatencyAttributions(sampler));

      return ExecuteWorkResult.create(
          outputBuilder, stateReader.getBytesRead() + localSideInputStateFetcher.getBytesRead());
    } catch (Throwable t) {
      // If processing failed due to a thrown exception, close the executionState. Do not
      // return/release the executionState back to computationState as that will lead to this
      // executionState instance being reused.
      computationWorkExecutor.invalidate();

      // Re-throw the exception, it will be caught and handled by workFailureProcessor downstream.
      throw t;
    }
  }

  @AutoValue
  abstract static class ExecuteWorkResult {
    private static ExecuteWorkResult create(
        Windmill.WorkItemCommitRequest.Builder commitWorkRequest, long stateBytesRead) {
      return new AutoValue_StreamingWorkScheduler_ExecuteWorkResult(
          commitWorkRequest, stateBytesRead);
    }

    abstract Windmill.WorkItemCommitRequest.Builder commitWorkRequest();

    abstract long stateBytesRead();
  }
}
