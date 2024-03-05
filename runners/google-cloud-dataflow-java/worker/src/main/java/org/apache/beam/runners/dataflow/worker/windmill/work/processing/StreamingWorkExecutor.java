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
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.DataflowMapTaskExecutor;
import org.apache.beam.runners.dataflow.worker.HotKeyLogger;
import org.apache.beam.runners.dataflow.worker.WorkItemCancelledException;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingMDC;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.ExecutionState;
import org.apache.beam.runners.dataflow.worker.streaming.KeyCommitTooLargeException;
import org.apache.beam.runners.dataflow.worker.streaming.ShardedKey;
import org.apache.beam.runners.dataflow.worker.streaming.StageInfo;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputStateFetcher;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ElementCounter;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputObjectAndByteCounter;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItemCommitRequest;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.Commit;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateReader;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.FailureReporter;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.WorkFailureProcessor;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
@Internal
public class StreamingWorkExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingWorkExecutor.class);
  private static final String SOURCE_BYTES_PROCESSED_COUNTER_NAME_PREFIX =
      "dataflow_source_bytes_processed";

  private final WorkCommitter workCommitter;
  private final Function<String, StageInfo> stageInfoFetcher;
  private final DataflowExecutionStateSampler sampler;
  private final DataflowWorkerHarnessOptions options;
  private final BiFunction<String, KeyedGetDataRequest, Optional<KeyedGetDataResponse>>
      fetchStateFromWindmillFn;
  private final Supplier<SideInputStateFetcher> sideInputStateFetcherFactory;
  private final CommitCallbackCache commitCallbacks;
  private final FailureReporter failureReporter;
  private final WorkFailureProcessor workFailureProcessor;
  private final HotKeyLogger hotKeyLogger;
  private final int maxWorkItemCommitBytes;
  private final Counter<Long, Long> windmillShuffleBytesRead;
  private final Counter<Long, Long> windmillStateBytesRead;
  private final Counter<Long, Long> windmillStateBytesWritten;
  private final Counter<Integer, Integer> windmillMaxObservedWorkItemCommitBytes;
  private final ExecutionStateFactory executionStateFactory;

  public StreamingWorkExecutor(
      WorkCommitter workCommitter,
      Function<String, StageInfo> stageInfoFetcher,
      DataflowExecutionStateSampler sampler,
      DataflowWorkerHarnessOptions options,
      BiFunction<String, KeyedGetDataRequest, Optional<KeyedGetDataResponse>>
          fetchStateFromWindmillFn,
      Supplier<SideInputStateFetcher> sideInputStateFetcherFactory,
      CommitCallbackCache commitCallbacks,
      FailureReporter failureReporter,
      WorkFailureProcessor workFailureProcessor,
      HotKeyLogger hotKeyLogger,
      int maxWorkItemCommitBytes,
      Counter<Long, Long> windmillShuffleBytesRead,
      Counter<Long, Long> windmillStateBytesRead,
      Counter<Long, Long> windmillStateBytesWritten,
      Counter<Integer, Integer> windmillMaxObservedWorkItemCommitBytes,
      ExecutionStateFactory executionStateFactory) {
    this.workCommitter = workCommitter;
    this.stageInfoFetcher = stageInfoFetcher;
    this.sampler = sampler;
    this.options = options;
    this.fetchStateFromWindmillFn = fetchStateFromWindmillFn;
    this.sideInputStateFetcherFactory = sideInputStateFetcherFactory;
    this.commitCallbacks = commitCallbacks;
    this.failureReporter = failureReporter;
    this.workFailureProcessor = workFailureProcessor;
    this.hotKeyLogger = hotKeyLogger;
    this.maxWorkItemCommitBytes = maxWorkItemCommitBytes;
    this.windmillShuffleBytesRead = windmillShuffleBytesRead;
    this.windmillStateBytesRead = windmillStateBytesRead;
    this.windmillStateBytesWritten = windmillStateBytesWritten;
    this.windmillMaxObservedWorkItemCommitBytes = windmillMaxObservedWorkItemCommitBytes;
    this.executionStateFactory = executionStateFactory;
  }

  private static WorkItemCommitRequest.Builder commitWorkRequestBuilder(
      Windmill.WorkItem workItem) {
    return WorkItemCommitRequest.newBuilder()
        .setKey(workItem.getKey())
        .setShardingKey(workItem.getShardingKey())
        .setWorkToken(workItem.getWorkToken())
        .setCacheToken(workItem.getCacheToken());
  }

  /** Sets the stage name and workId of the current Thread for logging. */
  private static void setUpWorkLoggingContext(Work work, String computationId) {
    work.setState(Work.State.PROCESSING);
    DataflowWorkerLoggingMDC.setWorkId(work.getLatencyTrackingId());
    DataflowWorkerLoggingMDC.setStageName(computationId);
    LOG.debug("Starting processing for {}:\n{}", computationId, work);
  }

  /** Resets the stage name and workId of the current Thread for logging. */
  private static void resetWorkLoggingContext() {
    DataflowWorkerLoggingMDC.setWorkId(null);
    DataflowWorkerLoggingMDC.setStageName(null);
  }

  private static String getShuffleTaskStepName(MapTask mapTask) {
    // The MapTask instruction is ordered by dependencies, such that the first element is
    // always going to be the shuffle task.
    return mapTask.getInstructions().get(0).getName();
  }

  static String getSourceBytesProcessedCounterName(MapTask mapTask) {
    return SOURCE_BYTES_PROCESSED_COUNTER_NAME_PREFIX + mapTask.getSystemName();
  }

  /** Reports source bytes processed to WorkItemCommitRequest if available. */
  private static void trySetSourceBytesProcessed(
      WorkItemCommitRequest.Builder commitRequest, ExecutionState executionState, MapTask mapTask) {
    try {
      HashMap<String, ElementCounter> counters =
          ((DataflowMapTaskExecutor) executionState.workExecutor())
              .getReadOperation()
              .receivers[0]
              .getOutputCounters();
      String sourceBytesProcessCounterName = getSourceBytesProcessedCounterName(mapTask);

      long sourceBytesProcessed =
          counters.containsKey(sourceBytesProcessCounterName)
              ? ((OutputObjectAndByteCounter) counters.get(sourceBytesProcessCounterName))
                  .getByteCount()
                  .getAndReset()
              : 0;
      commitRequest.setSourceBytesProcessed(sourceBytesProcessed);
    } catch (Exception e) {
      LOG.error(e.toString());
    }
  }

  private static ShardedKey createShardedKey(Work work) {
    return ShardedKey.create(work.getWorkItem().getKey(), work.getWorkItem().getShardingKey());
  }

  /**
   * Executes the {@link Work}. Runs the user processing, commits the work, and records processing
   * stats. Uses {@link WorkFailureProcessor} to handle exceptions thrown while processing.
   */
  public void execute(
      ComputationState computationState,
      Instant inputDataWatermark,
      Instant outputDataWatermark,
      @Nullable Instant synchronizedProcessingTime,
      Work work) {
    setUpWorkLoggingContext(work, computationState.getComputationId());
    boolean isPipelineInDrain = processSuccessfulCommits(work, computationState);
    if (isPipelineInDrain) {
      return;
    }

    long processingStartTimeNanos = System.nanoTime();
    MapTask mapTask = computationState.getMapTask();
    StageInfo stageInfo = stageInfoFetcher.apply(mapTask.getStageName());

    try {
      CommitWorkRequestAndStats commitWorkRequestAndStats =
          processInternal(
              work,
              computationState,
              inputDataWatermark,
              outputDataWatermark,
              synchronizedProcessingTime,
              mapTask,
              stageInfo);
      commitWork(computationState, work, commitWorkRequestAndStats.commitWorkRequest());
      recordProcessingStats(commitWorkRequestAndStats, work.getWorkItem().getMessageBundlesList());
      LOG.debug("Processing done for work token: {}", work.getWorkItem().getWorkToken());
    } catch (Throwable t) {
      workFailureProcessor.logAndProcessFailure(
          computationState.getComputationId(),
          work,
          t,
          invalidWork ->
              computationState.completeWorkAndScheduleNextWorkForKey(
                  createShardedKey(invalidWork), invalidWork.id()));
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

      sampler.resetForWorkId(work.getLatencyTrackingId());
      resetWorkLoggingContext();
    }
  }

  private CommitWorkRequestAndStats processInternal(
      Work work,
      ComputationState computationState,
      Instant inputDataWatermark,
      @Nullable Instant outputDataWatermark,
      @Nullable Instant synchronizedProcessingTime,
      MapTask mapTask,
      StageInfo stageInfo)
      throws Exception {
    if (work.isFailed()) {
      throw new WorkItemCancelledException(work.getWorkItem().getShardingKey());
    }

    // Get existing or create new ExecutionState.
    try (ExecutionState executionState =
        Optional.ofNullable(computationState.getExecutionStateQueue().poll())
            .orElseGet(
                () ->
                    executionStateFactory.createExecutionState(
                        computationState.getComputationId(),
                        mapTask,
                        stageInfo,
                        work.getLatencyTrackingId(),
                        computationState.getTransformUserNameToStateFamily()))) {

      WindmillStateReader stateReader =
          createStateReaderFor(computationState.getComputationId(), work);
      SideInputStateFetcher localSideInputStateFetcher = sideInputStateFetcherFactory.get();

      @Nullable
      Object executionKey =
          getExecutionAndLogHotKeys(
                  work, getShuffleTaskStepName(computationState.getMapTask()), executionState)
              .orElse(null);

      WorkItemCommitRequest.Builder commitWorkRequest =
          commitWorkRequestBuilder(work.getWorkItem());

      executionState
          .context()
          .start(
              executionKey,
              work.getWorkItem(),
              inputDataWatermark,
              outputDataWatermark,
              synchronizedProcessingTime,
              stateReader,
              localSideInputStateFetcher,
              commitWorkRequest,
              work::isFailed);

      // Blocks while executing work.
      executionState.workExecutor().execute();

      if (work.isFailed()) {
        throw new WorkItemCancelledException(work.getWorkItem().getShardingKey());
      }

      trySetSourceBytesProcessed(commitWorkRequest, executionState, mapTask);
      commitCallbacks.cacheCallbacksForProcessedWork(executionState.context().flushState());

      // Release the execution state for another thread to use.
      computationState.getExecutionStateQueue().offer(executionState);
      return CommitWorkRequestAndStats.create(
          commitWorkRequest,
          () -> stateReader.getBytesRead() + localSideInputStateFetcher.getBytesRead());
    }
  }

  private void commitWork(
      ComputationState computationState,
      Work work,
      WorkItemCommitRequest.Builder commitRequestBuilder) {
    // Add the output to the commit queue.
    work.setState(Work.State.COMMIT_QUEUED);
    commitRequestBuilder.addAllPerWorkItemLatencyAttributions(
        work.getLatencyAttributions(false, work.getLatencyTrackingId(), sampler));
    WorkItemCommitRequest commitRequest =
        truncateCommitIfNecessary(
            computationState.getComputationId(), work.getWorkItem(), commitRequestBuilder.build());

    workCommitter.commit(Commit.create(commitRequest, computationState, work));
  }

  private void recordProcessingStats(
      CommitWorkRequestAndStats commitWorkRequestAndStats,
      List<Windmill.InputMessageBundle> messageBundles) {
    // Compute shuffle and state byte statistics. These will be flushed asynchronously.
    long stateBytesWritten =
        commitWorkRequestAndStats
            .commitWorkRequest()
            .clearOutputMessages()
            .clearPerWorkItemLatencyAttributions()
            .build()
            .getSerializedSize();

    long shuffleBytesRead = 0;
    for (Windmill.InputMessageBundle bundle : messageBundles) {
      for (Windmill.Message message : bundle.getMessagesList()) {
        shuffleBytesRead += message.getSerializedSize();
      }
    }

    windmillShuffleBytesRead.addValue(shuffleBytesRead);
    windmillStateBytesRead.addValue(commitWorkRequestAndStats.currentStateBytesRead().get());
    windmillStateBytesWritten.addValue(stateBytesWritten);
  }

  /**
   * Processes commits that have been successfully persisted to streaming backend. Returns whether
   * the pipeline is in drain (specified by {@link Windmill.SourceState#getOnlyFinalize()}).
   */
  private boolean processSuccessfulCommits(Work work, ComputationState computationState) {
    // Before any processing starts, call any pending OnCommit callbacks.  Nothing that requires
    // cleanup should be done before this, since we might exit early here.
    commitCallbacks.executeCommitCallbacksFor(work.getWorkItem());
    if (work.getWorkItem().getSourceState().getOnlyFinalize()) {
      work.setState(Work.State.COMMIT_QUEUED);
      workCommitter.commit(
          Commit.create(
              commitWorkRequestBuilder(work.getWorkItem())
                  .setSourceStateUpdates(Windmill.SourceState.newBuilder().setOnlyFinalize(true))
                  .build(),
              computationState,
              work));
      return true;
    }

    return false;
  }

  private WindmillStateReader createStateReaderFor(String computationId, Work work) {
    return new WindmillStateReader(
        request -> fetchStateFromWindmillFn.apply(computationId, request),
        createShardedKey(work),
        work.getWorkItem().getWorkToken(),
        () -> {
          work.setState(Work.State.READING);
          return () -> work.setState(Work.State.PROCESSING);
        },
        work::isFailed);
  }

  private Optional<Object> getExecutionAndLogHotKeys(
      Work work, String stepName, ExecutionState executionState) throws IOException {
    // If the read output KVs, then we can decode Windmill's byte key into a userland
    // key object and provide it to the execution context for use with per-key state.
    // Otherwise, we pass null.
    //
    // The coder type that will be present is:
    //     WindowedValueCoder(TimerOrElementCoder(KvCoder))
    Optional<Coder<?>> keyCoder = executionState.keyCoder();

    @SuppressWarnings("deprecation")
    @Nullable
    Object executionKey =
        !keyCoder.isPresent()
            ? null
            : keyCoder.get().decode(work.getWorkItem().getKey().newInput(), Coder.Context.OUTER);

    if (work.getWorkItem().hasHotKeyInfo()) {
      Windmill.HotKeyInfo hotKeyInfo = work.getWorkItem().getHotKeyInfo();
      Duration hotKeyAge = Duration.millis(hotKeyInfo.getHotKeyAgeUsec() / 1000);

      if (options.isHotKeyLoggingEnabled() && keyCoder.isPresent()) {
        hotKeyLogger.logHotKeyDetection(stepName, hotKeyAge, executionKey);
      } else {
        hotKeyLogger.logHotKeyDetection(stepName, hotKeyAge);
      }
    }

    return Optional.ofNullable(executionKey);
  }

  private WorkItemCommitRequest buildWorkItemTruncationRequest(
      Windmill.WorkItem workItem, int estimatedCommitSize) {
    Windmill.WorkItemCommitRequest.Builder outputBuilder = commitWorkRequestBuilder(workItem);
    outputBuilder.setExceedsMaxWorkItemCommitBytes(true);
    outputBuilder.setEstimatedWorkItemCommitBytes(estimatedCommitSize);
    return outputBuilder.build();
  }

  private WorkItemCommitRequest truncateCommitIfNecessary(
      String computationId, Windmill.WorkItem workItem, WorkItemCommitRequest commitRequest) {
    int commitSize = commitRequest.getSerializedSize();
    int estimatedCommitSize = commitSize < 0 ? Integer.MAX_VALUE : commitSize;

    // Detect overflow of integer serialized size or if the byte limit was exceeded.
    windmillMaxObservedWorkItemCommitBytes.addValue(estimatedCommitSize);
    if (commitSize < 0 || commitSize > maxWorkItemCommitBytes) {
      KeyCommitTooLargeException e =
          KeyCommitTooLargeException.causedBy(computationId, maxWorkItemCommitBytes, commitRequest);
      failureReporter.reportFailure(computationId, workItem, e);
      LOG.error(e.toString());

      // Drop the current request in favor of a new, minimal one requesting truncation.
      // Messages, timers, counters, and other commit content will not be used by the service,
      // so we're purposefully dropping them here
      return buildWorkItemTruncationRequest(workItem, estimatedCommitSize);
    }

    return commitRequest;
  }

  @AutoValue
  abstract static class CommitWorkRequestAndStats {
    private static CommitWorkRequestAndStats create(
        WorkItemCommitRequest.Builder commitWorkRequest, Supplier<Long> currentStateBytesRead) {
      return new AutoValue_StreamingWorkExecutor_CommitWorkRequestAndStats(
          commitWorkRequest, currentStateBytesRead);
    }

    abstract WorkItemCommitRequest.Builder commitWorkRequest();

    abstract Supplier<Long> currentStateBytesRead();
  }
}
