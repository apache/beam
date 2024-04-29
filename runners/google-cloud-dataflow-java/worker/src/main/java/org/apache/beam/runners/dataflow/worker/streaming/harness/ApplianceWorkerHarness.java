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
package org.apache.beam.runners.dataflow.worker.streaming.harness;

import com.google.api.services.dataflow.model.MapTask;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.DataflowMapTaskExecutorFactory;
import org.apache.beam.runners.dataflow.worker.DataflowWorkUnitClient;
import org.apache.beam.runners.dataflow.worker.HotKeyLogger;
import org.apache.beam.runners.dataflow.worker.IntrinsicMapTaskExecutorFactory;
import org.apache.beam.runners.dataflow.worker.MetricTrackingWindmillServerStub;
import org.apache.beam.runners.dataflow.worker.ReaderCache;
import org.apache.beam.runners.dataflow.worker.WindmillComputationKey;
import org.apache.beam.runners.dataflow.worker.WindmillTimeUtils;
import org.apache.beam.runners.dataflow.worker.WorkUnitClient;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationStateCache;
import org.apache.beam.runners.dataflow.worker.streaming.StageInfo;
import org.apache.beam.runners.dataflow.worker.streaming.StreamingApplianceComputationStateCacheLoader;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingApplianceConfigLoader;
import org.apache.beam.runners.dataflow.worker.streaming.processing.ExecutionStateFactory;
import org.apache.beam.runners.dataflow.worker.streaming.processing.StreamingCommitFinalizer;
import org.apache.beam.runners.dataflow.worker.streaming.processing.StreamingWorkScheduler;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputStateFetcher;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationWorkItems;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkResponse;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.runners.dataflow.worker.windmill.appliance.JniWindmillApplianceServer;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.CompleteCommit;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.StreamingApplianceWorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkProcessingContext;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.FailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.StreamingApplianceFailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.WorkFailureProcessor;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.ActiveWorkRefresher;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.DispatchedActiveWorkRefresher;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Streaming Appliance worker harness implementation. */
@Internal
@ThreadSafe
public final class ApplianceWorkerHarness implements StreamingWorkerHarness {
  private static final Logger LOG = LoggerFactory.getLogger(ApplianceWorkerHarness.class);
  private static final String MEMORY_MONITOR_EXECUTOR = "MemoryMonitor";
  private static final String GET_WORK_EXECUTOR = "GetWorkDispatcher";
  private static final String REFRESH_WORK_EXECUTOR = "RefreshWork";

  private final DataflowWorkerHarnessOptions options;
  private final long clientId;
  private final WindmillServerStub windmillServer;
  private final MetricTrackingWindmillServerStub getDataClient;
  private final MemoryMonitor memoryMonitor;
  private final ComputationStateCache computationCache;
  private final StreamingWorkScheduler streamingWorkScheduler;
  private final WorkCommitter workCommitter;
  private final StreamingWorkerStatusReporter workerStatusReporter;
  private final StreamingStatusPages statusPages;
  private final DataflowExecutionStateSampler sampler;
  private final ActiveWorkRefresher activeWorkRefresher;
  private final BoundedQueueExecutor workExecutor;
  private final ExecutorService memoryMonitoryExecutor;
  private final ExecutorService getWorkExecutor;
  private final AtomicBoolean isRunning;

  private ApplianceWorkerHarness(
      DataflowWorkerHarnessOptions options,
      long clientId,
      WindmillServerStub windmillServer,
      MetricTrackingWindmillServerStub getDataClient,
      MemoryMonitor memoryMonitor,
      ComputationStateCache computationCache,
      StreamingWorkScheduler streamingWorkScheduler,
      WorkCommitter workCommitter,
      StreamingWorkerStatusReporter workerStatusReporter,
      StreamingStatusPages statusPages,
      DataflowExecutionStateSampler sampler,
      ActiveWorkRefresher activeWorkRefresher,
      ExecutorService memoryMonitoryExecutor,
      ExecutorService getWorkExecutor,
      AtomicBoolean isRunning,
      BoundedQueueExecutor workExecutor) {
    this.options = options;
    this.clientId = clientId;
    this.windmillServer = windmillServer;
    this.getDataClient = getDataClient;
    this.memoryMonitor = memoryMonitor;
    this.computationCache = computationCache;
    this.streamingWorkScheduler = streamingWorkScheduler;
    this.workCommitter = workCommitter;
    this.workerStatusReporter = workerStatusReporter;
    this.statusPages = statusPages;
    this.sampler = sampler;
    this.activeWorkRefresher = activeWorkRefresher;
    this.memoryMonitoryExecutor = memoryMonitoryExecutor;
    this.getWorkExecutor = getWorkExecutor;
    this.isRunning = isRunning;
    this.workExecutor = workExecutor;
    // Register standard file systems.
    FileSystems.setDefaultPipelineOptions(options);
  }

  public static ApplianceWorkerHarness fromOptions(DataflowWorkerHarnessOptions options) {
    long clientId = StreamingWorkerEnvironment.newClientId();
    MemoryMonitor memoryMonitor = MemoryMonitor.fromOptions(options);
    Supplier<Instant> clock = Instant::now;
    WindmillServerStub windmillServer =
        new JniWindmillApplianceServer(options.getLocalWindmillHostport());
    MetricTrackingWindmillServerStub getDataClient =
        MetricTrackingWindmillServerStub.builder(windmillServer, memoryMonitor)
            .setUseStreamingRequests(false)
            .build();
    WindmillStateCache stateCache = WindmillStateCache.ofSizeMbs(options.getWorkerCacheMb());
    BoundedQueueExecutor workExecutor = StreamingWorkerEnvironment.createWorkUnitExecutor(options);
    ConcurrentMap<String, String> stateNameMap = new ConcurrentHashMap<>();
    ComputationStateCache computationStateCache =
        ComputationStateCache.create(
            new StreamingApplianceComputationStateCacheLoader(
                new StreamingApplianceConfigLoader(
                    windmillServer, response -> onConfigResponse(response, stateNameMap)),
                workExecutor,
                stateCache::forComputation));
    StreamingCounters streamingCounters = StreamingCounters.create();
    ReaderCache readerCache =
        new ReaderCache(
            Duration.standardSeconds(options.getReaderCacheTimeoutSec()),
            Executors.newCachedThreadPool());
    WorkCommitter applianceWorkCommitter =
        StreamingApplianceWorkCommitter.create(
            windmillServer::commitWork,
            completeCommit ->
                onCompleteCommit(completeCommit, readerCache, stateCache, computationStateCache));
    AtomicInteger maxWorkItemCommitBytes = new AtomicInteger(Integer.MAX_VALUE);
    DataflowExecutionStateSampler sampler = DataflowExecutionStateSampler.instance();
    FailureTracker failureTracker =
        StreamingApplianceFailureTracker.create(
            StreamingWorkerEnvironment.getMaxFailuresToReportInUpdate(),
            options.getMaxStackTraceDepthToReport(),
            windmillServer::reportStats);
    ConcurrentMap<String, StageInfo> stageInfo = new ConcurrentHashMap<>();
    WorkUnitClient dataflowServiceClient = new DataflowWorkUnitClient(options, LOG);
    StreamingWorkerStatusReporter workerStatusReporter =
        StreamingWorkerStatusReporter.create(
            dataflowServiceClient,
            windmillServer::getAndResetThrottleTime,
            stageInfo::values,
            failureTracker,
            streamingCounters,
            memoryMonitor,
            workExecutor,
            options.getWindmillHarnessUpdateReportingPeriod().getMillis(),
            options.getPerWorkerMetricsUpdateReportingPeriodMillis());
    StreamingWorkScheduler streamingWorkScheduler =
        new StreamingWorkScheduler(
            options,
            clock,
            new ExecutionStateFactory(
                options,
                IntrinsicMapTaskExecutorFactory.defaultFactory(),
                readerCache,
                stateCache::forComputation,
                sampler,
                streamingCounters.pendingDeltaCounters(),
                StreamingWorkerEnvironment.mapTaskToBaseNetworkFnInstance(),
                stateNameMap,
                StreamingWorkerEnvironment.getMaxSinkBytes(options)),
            new SideInputStateFetcher(getDataClient::getSideInputData, options),
            failureTracker,
            WorkFailureProcessor.create(
                workExecutor,
                failureTracker,
                () -> Optional.ofNullable(memoryMonitor.tryToDumpHeap()),
                clock),
            StreamingCommitFinalizer.create(workExecutor),
            streamingCounters,
            HotKeyLogger.create(),
            stageInfo,
            sampler,
            maxWorkItemCommitBytes);
    AtomicBoolean isRunning = new AtomicBoolean(false);
    StreamingStatusPages statusPages =
        StreamingWorkerStatusPages.forAppliance(
            clock,
            clientId,
            isRunning,
            StreamingWorkerEnvironment.createStatusPages(memoryMonitor),
            stateCache,
            computationStateCache,
            applianceWorkCommitter::currentActiveCommitBytes,
            getDataClient::printHtml,
            workExecutor);
    ActiveWorkRefresher activeWorkRefresher =
        DispatchedActiveWorkRefresher.create(
            clock,
            options.getActiveWorkRefreshPeriodMillis(),
            0, // Invalidation of stuck commits is not available in Streaming Appliance.
            computationStateCache::getAllComputations,
            sampler,
            getDataClient::refreshActiveWork,
            Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat(REFRESH_WORK_EXECUTOR).build()));
    return new ApplianceWorkerHarness(
        options,
        clientId,
        windmillServer,
        getDataClient,
        memoryMonitor,
        computationStateCache,
        streamingWorkScheduler,
        applianceWorkCommitter,
        workerStatusReporter,
        statusPages,
        sampler,
        activeWorkRefresher,
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat(MEMORY_MONITOR_EXECUTOR)
                .setPriority(Thread.MIN_PRIORITY)
                .build()),
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat(GET_WORK_EXECUTOR)
                .setPriority(Thread.MIN_PRIORITY)
                .setDaemon(true)
                .build()),
        isRunning,
        workExecutor);
  }

  @VisibleForTesting
  public static ApplianceWorkerHarness forTesting(
      BoundedQueueExecutor workExecutor,
      WindmillStateCache stateCache,
      WindmillServerStub windmillServer,
      List<MapTask> mapTasks,
      DataflowMapTaskExecutorFactory mapTaskExecutorFactory,
      WorkUnitClient workUnitClient,
      DataflowWorkerHarnessOptions options,
      boolean publishCounters,
      HotKeyLogger hotKeyLogger,
      java.util.function.Supplier<Instant> clock,
      Function<String, ScheduledExecutorService> executorSupplier,
      int maxWorkItemCommitBytesOverride,
      int localRetryTimeoutMs,
      StreamingStatusPages testStatusPages,
      StreamingCounters streamingCounters,
      ConcurrentMap<String, String> stateNameMap) {
    ConcurrentMap<String, StageInfo> stageInfo = new ConcurrentHashMap<>();
    MemoryMonitor memoryMonitor = MemoryMonitor.fromOptions(options);
    AtomicInteger maxWorkItemCommitBytes = new AtomicInteger(maxWorkItemCommitBytesOverride);
    FailureTracker failureTracker =
        StreamingApplianceFailureTracker.create(
            StreamingWorkerEnvironment.getMaxFailuresToReportInUpdate(),
            options.getMaxStackTraceDepthToReport(),
            windmillServer::reportStats);
    WorkFailureProcessor workFailureProcessor =
        WorkFailureProcessor.forTesting(
            workExecutor,
            failureTracker,
            () -> Optional.ofNullable(memoryMonitor.tryToDumpHeap()),
            clock,
            localRetryTimeoutMs);
    StreamingWorkerStatusReporter workerStatusReporter =
        StreamingWorkerStatusReporter.forTesting(
            publishCounters,
            workUnitClient,
            windmillServer::getAndResetThrottleTime,
            stageInfo::values,
            failureTracker,
            streamingCounters,
            memoryMonitor,
            workExecutor,
            executorSupplier,
            options.getWindmillHarnessUpdateReportingPeriod().getMillis(),
            options.getPerWorkerMetricsUpdateReportingPeriodMillis());
    StreamingApplianceConfigLoader streamingConfigLoader =
        new StreamingApplianceConfigLoader(
            windmillServer, response -> onConfigResponse(response, stateNameMap));
    CacheLoader<String, Optional<ComputationState>> cacheLoader =
        new StreamingApplianceComputationStateCacheLoader(
            streamingConfigLoader, workExecutor, stateCache::forComputation);
    ComputationStateCache computationStateCache = ComputationStateCache.create(cacheLoader);
    computationStateCache.putAll(
        StreamingWorkerEnvironment.createComputationMapForTesting(
            mapTasks, workExecutor, stateCache::forComputation));
    DataflowExecutionStateSampler sampler = DataflowExecutionStateSampler.instance();
    ReaderCache readerCache =
        new ReaderCache(
            Duration.standardSeconds(options.getReaderCacheTimeoutSec()),
            Executors.newCachedThreadPool());
    ExecutionStateFactory executionStateFactory =
        new ExecutionStateFactory(
            options,
            mapTaskExecutorFactory,
            readerCache,
            stateCache::forComputation,
            sampler,
            streamingCounters.pendingDeltaCounters(),
            StreamingWorkerEnvironment.mapTaskToBaseNetworkFnInstance(),
            stateNameMap,
            StreamingWorkerEnvironment.getMaxSinkBytes(options));
    MetricTrackingWindmillServerStub getDataClient =
        MetricTrackingWindmillServerStub.builder(windmillServer, memoryMonitor)
            .setUseStreamingRequests(false)
            .build();
    StreamingWorkScheduler streamingWorkScheduler =
        new StreamingWorkScheduler(
            options,
            clock,
            executionStateFactory,
            new SideInputStateFetcher(getDataClient::getSideInputData, options),
            failureTracker,
            workFailureProcessor,
            StreamingCommitFinalizer.create(workExecutor),
            streamingCounters,
            hotKeyLogger,
            stageInfo,
            sampler,
            maxWorkItemCommitBytes);
    WorkCommitter applianceWorkCommitter =
        StreamingApplianceWorkCommitter.create(
            windmillServer::commitWork,
            completeCommit ->
                onCompleteCommit(completeCommit, readerCache, stateCache, computationStateCache));
    LOG.debug("windmillServiceEnabled: {}", false);
    LOG.debug("WindmillServiceEndpoint: {}", options.getWindmillServiceEndpoint());
    LOG.debug("WindmillServicePort: {}", options.getWindmillServicePort());
    LOG.debug("LocalWindmillHostport: {}", options.getLocalWindmillHostport());
    LOG.debug("maxWorkItemCommitBytes: {}", maxWorkItemCommitBytes.get());
    return new ApplianceWorkerHarness(
        options,
        1L,
        windmillServer,
        getDataClient,
        memoryMonitor,
        computationStateCache,
        streamingWorkScheduler,
        applianceWorkCommitter,
        workerStatusReporter,
        testStatusPages,
        sampler,
        DispatchedActiveWorkRefresher.create(
            clock,
            options.getActiveWorkRefreshPeriodMillis(),
            0,
            computationStateCache::getAllComputations,
            sampler,
            getDataClient::refreshActiveWork,
            executorSupplier.apply("RefreshWork")),
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("MemoryMonitor")
                .setPriority(Thread.MIN_PRIORITY)
                .build()),
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("GetWorkDispatchThread")
                .setPriority(Thread.MIN_PRIORITY)
                .setDaemon(true)
                .build()),
        new AtomicBoolean(false),
        workExecutor);
  }

  private static boolean isInvalidGetWorkResponse(@Nullable GetWorkResponse getWorkResponse) {
    return getWorkResponse == null || getWorkResponse.getWorkCount() <= 0;
  }

  private static void onCompleteCommit(
      CompleteCommit completeCommit,
      ReaderCache readerCache,
      WindmillStateCache stateCache,
      ComputationStateCache computationStateCache) {
    if (completeCommit.status() != Windmill.CommitStatus.OK) {
      readerCache.invalidateReader(
          WindmillComputationKey.create(
              completeCommit.computationId(), completeCommit.shardedKey()));
      stateCache
          .forComputation(completeCommit.computationId())
          .invalidate(completeCommit.shardedKey());
    }

    computationStateCache
        .getComputationState(completeCommit.computationId())
        .ifPresent(
            state ->
                state.completeWorkAndScheduleNextWorkForKey(
                    completeCommit.shardedKey(), completeCommit.workId()));
  }

  private static void onConfigResponse(
      Windmill.GetConfigResponse response, Map<String, String> stateNameMap) {
    for (Windmill.GetConfigResponse.NameMapEntry entry : response.getNameMapList()) {
      stateNameMap.put(entry.getUserName(), entry.getSystemName());
    }
  }

  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public void start() {
    if (isRunning.compareAndSet(false, true)) {
      memoryMonitoryExecutor.submit(memoryMonitor);
      getWorkExecutor.submit(this::getWorkLoop);
      sampler.start();
      statusPages.start(options);
      workCommitter.start();
      workerStatusReporter.start();
      activeWorkRefresher.start();
    }
  }

  @Override
  public void stop() {
    if (isRunning.compareAndSet(true, false)) {
      try {
        ExecutorUtils.shutdownExecutors(getWorkExecutor, memoryMonitoryExecutor);
        statusPages.stop();
        activeWorkRefresher.stop();
        sampler.stop();
        workCommitter.stop();
        workerStatusReporter.stop();
        computationCache.clearAndCloseAll();
        workExecutor.shutdown();
      } catch (Exception e) {
        LOG.warn("Exception while shutting down: ", e);
      }
    }
  }

  private void getWorkLoop() {
    LOG.info("Starting get work.");
    while (isRunning.get()) {
      memoryMonitor.waitForResources("GetWork");
      GetWorkResponse workResponse = getWork();
      for (ComputationWorkItems computationProto : workResponse.getWorkList()) {
        String computationId = computationProto.getComputationId();
        Optional<ComputationState> computationState =
            computationCache.getComputationState(computationId);

        if (computationState.isPresent()) {
          scheduleWork(computationState.get(), computationProto);
        } else {
          LOG.warn(
              "Received work for unknown computation: {}. Known computations are {}",
              computationId,
              computationCache.knownComputationIds());
        }
      }
    }

    LOG.info("GetWork finished.");
  }

  @Override
  public void requestWorkerUpdate() {
    workerStatusReporter.reportWorkerUpdates();
  }

  private GetWorkResponse getWork() {
    int backoffMillis = 1;
    GetWorkResponse getWorkResponse = null;
    do {
      try {
        getWorkResponse =
            windmillServer.getWork(
                Windmill.GetWorkRequest.newBuilder()
                    .setClientId(clientId)
                    .setMaxItems(
                        StreamingWorkerEnvironment.chooseMaximumBundlesOutstanding(options))
                    .setMaxBytes(StreamingWorkerEnvironment.getMaxGetWorkFetchBytes())
                    .build());
      } catch (WindmillServerStub.RpcException e) {
        LOG.warn("GetWork failed, retrying:", e);
      }
      Uninterruptibles.sleepUninterruptibly(backoffMillis, TimeUnit.MILLISECONDS);
      backoffMillis = Math.min(1000, backoffMillis * 2);
    } while (isInvalidGetWorkResponse(getWorkResponse));

    return Preconditions.checkNotNull(getWorkResponse);
  }

  private void scheduleWork(
      ComputationState computationState, ComputationWorkItems computationProto) {
    Instant inputDataWatermark =
        Preconditions.checkNotNull(
            WindmillTimeUtils.windmillToHarnessWatermark(computationProto.getInputDataWatermark()));
    // If null, the synchronizedProcessingTime is not yet known.
    @Nullable
    Instant synchronizedProcessingTime =
        WindmillTimeUtils.windmillToHarnessWatermark(
            computationProto.getDependentRealtimeInputWatermark());
    for (Windmill.WorkItem workItem : computationProto.getWorkList()) {
      WorkProcessingContext workProcessingContext =
          WorkProcessingContext.builder(
                  computationState.getComputationId(),
                  (id, request) ->
                      Optional.ofNullable(
                          getDataClient.getStateData(computationProto.getComputationId(), request)))
              .setInputDataWatermark(Preconditions.checkNotNull(inputDataWatermark))
              .setSynchronizedProcessingTime(synchronizedProcessingTime)
              .setWorkItem(workItem)
              .setWorkCommitter(workCommitter::commit)
              .setOutputDataWatermark(
                  WindmillTimeUtils.windmillToHarnessWatermark(workItem.getOutputDataWatermark()))
              .build();
      streamingWorkScheduler.scheduleWork(
          computationState, workProcessingContext, ImmutableList.of());
    }
  }

  @VisibleForTesting
  public int numCommitThreads() {
    return workCommitter.parallelism();
  }

  @VisibleForTesting
  @Override
  public ComputationStateCache getComputationStateCache() {
    return computationCache;
  }

  @Override
  public Mode mode() {
    return Mode.APPLIANCE;
  }
}
