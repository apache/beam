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

import static org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillChannelFactory.remoteChannel;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.DataflowWorkUnitClient;
import org.apache.beam.runners.dataflow.worker.HotKeyLogger;
import org.apache.beam.runners.dataflow.worker.IntrinsicMapTaskExecutorFactory;
import org.apache.beam.runners.dataflow.worker.ReaderCache;
import org.apache.beam.runners.dataflow.worker.WindmillComputationKey;
import org.apache.beam.runners.dataflow.worker.WorkUnitClient;
import org.apache.beam.runners.dataflow.worker.status.DebugCapture;
import org.apache.beam.runners.dataflow.worker.status.WorkerStatusPages;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationStateCache;
import org.apache.beam.runners.dataflow.worker.streaming.StageInfo;
import org.apache.beam.runners.dataflow.worker.streaming.StreamingEngineComputationStateCacheLoader;
import org.apache.beam.runners.dataflow.worker.streaming.WorkHeartbeatResponseProcessor;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingConfigLoader;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingEngineConfigLoader;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingEnginePipelineConfig;
import org.apache.beam.runners.dataflow.worker.streaming.processing.ExecutionStateFactory;
import org.apache.beam.runners.dataflow.worker.streaming.processing.StreamingCommitFinalizer;
import org.apache.beam.runners.dataflow.worker.streaming.processing.StreamingWorkScheduler;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputStateFetcher;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.runners.dataflow.worker.windmill.client.CloseableStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.GetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.CompleteCommit;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.StreamingEngineWorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcDispatcherClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillStreamFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.StreamingEngineClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.StreamingEngineConnectionState;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.ChannelCache;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.ChannelCachingRemoteStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.ChannelCachingStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.IsolationChannel;
import org.apache.beam.runners.dataflow.worker.windmill.client.throttling.ThrottleTimer;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudgetDistributors;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.FailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.StreamingEngineFailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.WorkFailureProcessor;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.ActiveWorkRefresher;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.DirectActiveWorkRefresher;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming Engine worker harness that handles GetWork, GetData, and CommitWork RPC fan out from a
 * single user worker harness to multiple backend workers on the Streaming Engine backend.
 */
@Internal
@ThreadSafe
public final class FanOutStreamingEngineWorkerHarness implements StreamingWorkerHarness {
  private static final Logger LOG =
      LoggerFactory.getLogger(FanOutStreamingEngineWorkerHarness.class);

  private final DataflowWorkerHarnessOptions options;
  private final AtomicBoolean isRunning;
  private final StreamingEngineClient streamingEngineClient;
  private final StreamingConfigLoader<StreamingEnginePipelineConfig> streamingConfigLoader;
  private final ComputationStateCache computationStateCache;
  private final BoundedQueueExecutor workUnitExecutor;
  private final DataflowExecutionStateSampler sampler;
  private final ActiveWorkRefresher activeWorkRefresher;
  private final MemoryMonitor memoryMonitor;
  private final ExecutorService memoryMonitorWorker;
  private final StreamingWorkerStatusReporter workerStatusReporter;
  private final StreamingWorkerStatusPages statusPages;

  private FanOutStreamingEngineWorkerHarness(
      DataflowWorkerHarnessOptions options,
      AtomicBoolean isRunning,
      StreamingEngineClient streamingEngineClient,
      StreamingConfigLoader<StreamingEnginePipelineConfig> streamingConfigLoader,
      ComputationStateCache computationStateCache,
      BoundedQueueExecutor workUnitExecutor,
      DataflowExecutionStateSampler sampler,
      ActiveWorkRefresher activeWorkRefresher,
      MemoryMonitor memoryMonitor,
      ExecutorService memoryMonitorWorker,
      StreamingWorkerStatusReporter workerStatusReporter,
      StreamingWorkerStatusPages statusPages) {
    this.options = options;
    this.isRunning = isRunning;
    this.streamingEngineClient = streamingEngineClient;
    this.streamingConfigLoader = streamingConfigLoader;
    this.computationStateCache = computationStateCache;
    this.workUnitExecutor = workUnitExecutor;
    this.sampler = sampler;
    this.activeWorkRefresher = activeWorkRefresher;
    this.memoryMonitor = memoryMonitor;
    this.memoryMonitorWorker = memoryMonitorWorker;
    this.workerStatusReporter = workerStatusReporter;
    this.statusPages = statusPages;

    FileSystems.setDefaultPipelineOptions(options);
    LOG.debug("windmillServiceEnabled: {}", true);
    LOG.debug("WindmillServiceEndpoint: {}", options.getWindmillServiceEndpoint());
    LOG.debug("WindmillServicePort: {}", options.getWindmillServicePort());
    LOG.debug("LocalWindmillHostport: {}", options.getLocalWindmillHostport());
    LOG.debug("directPathEnabled: {}", true);
  }

  public static FanOutStreamingEngineWorkerHarness fromOptions(
      DataflowWorkerHarnessOptions options) {
    long clientId = StreamingWorkerEnvironment.newClientId();
    MemoryMonitor memoryMonitor = MemoryMonitor.fromOptions(options);
    ConcurrentMap<String, String> stateNameMap = new ConcurrentHashMap<>();
    ConcurrentMap<String, StageInfo> stageInfo = new ConcurrentHashMap<>();
    StreamingCounters streamingCounters = StreamingCounters.create();
    Function<WindmillServiceAddress, ManagedChannel> channelFactory =
        serviceAddress ->
            remoteChannel(serviceAddress, options.getWindmillServiceRpcChannelAliveTimeoutSec());
    ChannelCache channelCache =
        ChannelCache.create(
            serviceAddress ->
                // IsolationChannel will create and manage separate RPC channels to the same
                // serviceAddress via calling the channelFactory, else just directly return the
                // RPC channel.
                options.getUseWindmillIsolatedChannels()
                    ? IsolationChannel.create(() -> channelFactory.apply(serviceAddress))
                    : channelFactory.apply(serviceAddress));
    ChannelCachingStubFactory windmillStubFactory =
        ChannelCachingRemoteStubFactory.create(options.getGcpCredential(), channelCache);
    GrpcDispatcherClient grpcDispatcherClient = GrpcDispatcherClient.create(windmillStubFactory);
    JobHeader jobHeader = jobHeader(options, clientId);
    GrpcWindmillStreamFactory windmillStreamFactory =
        createWindmillStreamFactory(options, jobHeader);
    windmillStreamFactory.scheduleHealthChecks(
        options.getWindmillServiceStreamingRpcHealthCheckPeriodMs());
    FailureTracker failureTracker =
        StreamingEngineFailureTracker.create(
            StreamingWorkerEnvironment.getMaxFailuresToReportInUpdate(),
            options.getMaxStackTraceDepthToReport());
    WorkUnitClient dataflowServiceClient = new DataflowWorkUnitClient(options, LOG);
    BoundedQueueExecutor workExecutor = StreamingWorkerEnvironment.createWorkUnitExecutor(options);
    Supplier<Instant> clock = Instant::now;
    WindmillStateCache stateCache = WindmillStateCache.ofSizeMbs(options.getWorkerCacheMb());
    AtomicInteger maxWorkItemCommitBytes = new AtomicInteger(Integer.MAX_VALUE);
    StreamingEngineConfigLoader streamingConfigLoader =
        StreamingEngineConfigLoader.create(
            Optional.ofNullable(options.getGlobalConfigRefreshPeriod())
                .map(Duration::getMillis)
                .orElse(0L),
            dataflowServiceClient,
            config ->
                onDataflowServiceConfig(
                    config, stateNameMap, grpcDispatcherClient, maxWorkItemCommitBytes));
    CacheLoader<String, Optional<ComputationState>> computationStateCacheLoader =
        new StreamingEngineComputationStateCacheLoader(
            streamingConfigLoader, workExecutor, stateCache::forComputation);
    ComputationStateCache computationStateCache =
        ComputationStateCache.create(computationStateCacheLoader);
    ReaderCache readerCache =
        new ReaderCache(
            Duration.standardSeconds(options.getReaderCacheTimeoutSec()),
            Executors.newCachedThreadPool());
    AtomicBoolean isRunning = new AtomicBoolean(false);
    GetDataClient getDataClient = new GetDataClient(memoryMonitor);
    DataflowExecutionStateSampler sampler = DataflowExecutionStateSampler.instance();
    int stuckCommitDurationMillis = Math.max(options.getStuckCommitDurationMillis(), 0);
    StreamingEngineClient streamingEngineClient =
        createStreamingEngineClient(
            options,
            clock,
            jobHeader,
            grpcDispatcherClient,
            windmillStreamFactory,
            computationStateCache,
            windmillStubFactory,
            readerCache,
            stateCache,
            getDataClient,
            sampler,
            streamingCounters,
            stateNameMap,
            stageInfo,
            maxWorkItemCommitBytes,
            workExecutor,
            failureTracker,
            memoryMonitor);
    ActiveWorkRefresher directActiveWorkRefresher =
        DirectActiveWorkRefresher.create(
            clock,
            options.getActiveWorkRefreshPeriodMillis(),
            stuckCommitDurationMillis,
            computationStateCache::getAllComputations,
            sampler,
            getDataClient::refreshActiveWorkWithFanOut);
    LOG.debug("maxWorkItemCommitBytes: {}", maxWorkItemCommitBytes.get());
    WorkerStatusPages workerStatusPages =
        StreamingWorkerEnvironment.createStatusPages(memoryMonitor);
    return new FanOutStreamingEngineWorkerHarness(
        options,
        isRunning,
        streamingEngineClient,
        streamingConfigLoader,
        computationStateCache,
        workExecutor,
        sampler,
        directActiveWorkRefresher,
        memoryMonitor,
        Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().build()),
        StreamingWorkerStatusReporter.create(
            dataflowServiceClient,
            streamingEngineClient::getAndResetThrottleTimes,
            stageInfo::values,
            failureTracker,
            streamingCounters,
            memoryMonitor,
            workExecutor),
        StreamingWorkerStatusPages.forStreamingEngine(
            clock,
            jobHeader.getClientId(),
            isRunning,
            workerStatusPages,
            new DebugCapture.Manager(options, workerStatusPages.getDebugCapturePages()),
            StreamingWorkerEnvironment.createChannelZServlet(
                options, streamingEngineClient::currentWindmillEndpoints),
            stateCache,
            computationStateCache,
            streamingEngineClient::currentActiveCommitBytes,
            windmillStreamFactory,
            getDataClient::printHtml,
            workExecutor));
  }

  private static StreamingEngineClient createStreamingEngineClient(
      DataflowWorkerHarnessOptions options,
      Supplier<Instant> clock,
      JobHeader jobHeader,
      GrpcDispatcherClient dispatcherClient,
      GrpcWindmillStreamFactory windmillStreamFactory,
      ComputationStateCache computationStateCache,
      ChannelCachingStubFactory windmillStubFactory,
      ReaderCache readerCache,
      WindmillStateCache stateCache,
      GetDataClient getDataClient,
      DataflowExecutionStateSampler sampler,
      StreamingCounters streamingCounters,
      ConcurrentMap<String, String> stateNameMap,
      ConcurrentMap<String, StageInfo> stageInfo,
      AtomicInteger maxWorkItemCommitBytes,
      BoundedQueueExecutor workExecutor,
      FailureTracker failureTracker,
      MemoryMonitor memoryMonitor) {
    AtomicReference<StreamingEngineConnectionState> connectionsState =
        new AtomicReference<>(StreamingEngineConnectionState.initialState());
    WorkHeartbeatResponseProcessor workHeartbeatResponseProcessor =
        new WorkHeartbeatResponseProcessor(computationStateCache::getComputationState);
    StreamingWorkScheduler streamingWorkScheduler =
        createStreamingWorkExecutor(
            options,
            clock,
            getDataClient,
            readerCache,
            stateCache,
            sampler,
            streamingCounters,
            stateNameMap,
            stageInfo,
            connectionsState,
            dispatcherClient,
            windmillStreamFactory,
            workHeartbeatResponseProcessor,
            maxWorkItemCommitBytes,
            workExecutor,
            failureTracker,
            memoryMonitor);

    return StreamingEngineClient.create(
        jobHeader,
        GetWorkBudget.builder()
            .setItems(StreamingWorkerEnvironment.chooseMaximumBundlesOutstanding(options))
            .setBytes(StreamingWorkerEnvironment.getMaxGetWorkFetchBytes())
            .build(),
        windmillStreamFactory,
        (workProcessingContext, onWorkItemQueued, getWorkSteamLatencies) -> {
          Optional<ComputationState> computationState =
              computationStateCache.getComputationState(workProcessingContext.computationId());
          if (computationState.isPresent()) {
            streamingWorkScheduler.scheduleWork(
                computationState.get(), workProcessingContext, getWorkSteamLatencies);
            onWorkItemQueued.accept(workProcessingContext.workItem());
          } else {
            LOG.warn(
                "Computation computationId={} is unknown. Known computations:{}",
                workProcessingContext.computationId(),
                computationStateCache.knownComputationIds());
          }
        },
        windmillStubFactory,
        GetWorkBudgetDistributors.distributeEvenly(
            computationStateCache::totalCurrentActiveGetWorkBudget),
        dispatcherClient,
        commitWorkStream ->
            StreamingEngineWorkCommitter.create(
                () -> CloseableStream.create(commitWorkStream, () -> {}),
                Math.max(options.getWindmillServiceCommitThreads(), 1),
                completeCommit ->
                    onCompleteCommit(
                        completeCommit, readerCache, stateCache, computationStateCache)),
        new WorkHeartbeatResponseProcessor(computationStateCache::getComputationState),
        connectionsState);
  }

  private static StreamingWorkScheduler createStreamingWorkExecutor(
      DataflowWorkerHarnessOptions options,
      Supplier<Instant> clock,
      GetDataClient getDataClient,
      ReaderCache readerCache,
      WindmillStateCache stateCache,
      DataflowExecutionStateSampler sampler,
      StreamingCounters streamingCounters,
      ConcurrentMap<String, String> stateNameMap,
      ConcurrentMap<String, StageInfo> stageInfo,
      AtomicReference<StreamingEngineConnectionState> connectionsState,
      GrpcDispatcherClient dispatcherClient,
      GrpcWindmillStreamFactory windmillStreamFactory,
      WorkHeartbeatResponseProcessor workHeartbeatResponseProcessor,
      AtomicInteger maxWorkItemCommitBytes,
      BoundedQueueExecutor workExecutor,
      FailureTracker failureTracker,
      MemoryMonitor memoryMonitor) {
    ExecutionStateFactory executionStateFactory =
        new ExecutionStateFactory(
            options,
            IntrinsicMapTaskExecutorFactory.defaultFactory(),
            readerCache,
            stateCache::forComputation,
            sampler,
            streamingCounters.pendingDeltaCounters(),
            StreamingWorkerEnvironment.mapTaskToBaseNetworkFnInstance(),
            stateNameMap,
            StreamingWorkerEnvironment.getMaxSinkBytes(options));
    return new StreamingWorkScheduler(
        options,
        clock,
        executionStateFactory,
        createSideInputStateFetcher(
            options,
            connectionsState,
            dispatcherClient,
            getDataClient,
            windmillStreamFactory,
            workHeartbeatResponseProcessor),
        failureTracker,
        WorkFailureProcessor.create(
            workExecutor,
            failureTracker,
            () -> Optional.ofNullable(memoryMonitor.tryToDumpHeap()),
            clock),
        StreamingCommitFinalizer.create(workExecutor),
        streamingCounters,
        HotKeyLogger.ofSystemClock(),
        stageInfo,
        sampler,
        maxWorkItemCommitBytes);
  }

  private static SideInputStateFetcher createSideInputStateFetcher(
      DataflowWorkerHarnessOptions options,
      AtomicReference<StreamingEngineConnectionState> connections,
      GrpcDispatcherClient dispatcherClient,
      GetDataClient getDataClient,
      GrpcWindmillStreamFactory streamFactory,
      WorkHeartbeatResponseProcessor heartbeatResponseProcessor) {
    return new SideInputStateFetcher(
        request ->
            getDataClient.getSideInputState(
                connections
                    .get()
                    .getGlobalDataStream(request.getDataId().getTag())
                    .orElseGet(
                        () ->
                            streamFactory.createGetDataStream(
                                dispatcherClient.getWindmillServiceStub(),
                                new ThrottleTimer(),
                                false,
                                heartbeatResponseProcessor)),
                request),
        options);
  }

  private static JobHeader jobHeader(DataflowWorkerHarnessOptions options, long clientId) {
    return JobHeader.newBuilder()
        .setJobId(options.getJobId())
        .setProjectId(options.getProject())
        .setWorkerId(options.getWorkerId())
        .setClientId(clientId)
        .build();
  }

  private static GrpcWindmillStreamFactory createWindmillStreamFactory(
      DataflowWorkerHarnessOptions options, JobHeader jobHeader) {
    Duration maxBackoff =
        options.getLocalWindmillHostport() != null
            ? StreamingWorkerEnvironment.localHostMaxBackoff()
            : Duration.standardSeconds(30);
    return GrpcWindmillStreamFactory.of(jobHeader)
        .setWindmillMessagesBetweenIsReadyChecks(options.getWindmillMessagesBetweenIsReadyChecks())
        .setMaxBackOffSupplier(() -> maxBackoff)
        .setLogEveryNStreamFailures(options.getWindmillServiceStreamingLogEveryNStreamFailures())
        .setStreamingRpcBatchLimit(options.getWindmillServiceStreamingRpcBatchLimit())
        .build();
  }

  /**
   * Sends a request to get configuration from Dataflow, either for a specific computation (if
   * computation is not null) or global configuration (if computation is null).
   */
  private static void onDataflowServiceConfig(
      StreamingEnginePipelineConfig config,
      Map<String, String> stateNameMap,
      GrpcDispatcherClient grpcDispatcherClient,
      AtomicInteger maxWorkItemCommitBytes) {
    stateNameMap.putAll(config.userStepToStateFamilyNameMap());
    if (config.maxWorkItemCommitBytes() != maxWorkItemCommitBytes.get()) {
      LOG.info("Setting maxWorkItemCommitBytes to {}", maxWorkItemCommitBytes);
    }

    maxWorkItemCommitBytes.set((int) config.maxWorkItemCommitBytes());

    if (!config.windmillServiceEndpoints().isEmpty()) {
      grpcDispatcherClient.consumeWindmillDispatcherEndpoints(config.windmillServiceEndpoints());
    }
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

  @SuppressWarnings("FutureReturnValueIgnored")
  @Override
  public void start() {
    if (isRunning.compareAndSet(false, true)) {
      streamingConfigLoader.start();
      memoryMonitorWorker.submit(memoryMonitor);
      sampler.start();

      streamingEngineClient.start();
      workerStatusReporter.start(options.getWindmillHarnessUpdateReportingPeriod().getMillis());
      activeWorkRefresher.start();
      statusPages.start(options);

      StreamingWorkerEnvironment.enableBigQueryMetrics(options);
    }
  }

  @Override
  @SuppressWarnings("ReturnValueIgnored")
  public void stop() {
    if (isRunning.compareAndSet(true, false)) {
      try {
        activeWorkRefresher.stop();
        statusPages.stop();
        memoryMonitor.stop();
        memoryMonitorWorker.shutdown();
        if (!memoryMonitorWorker.isShutdown()) {
          memoryMonitorWorker.awaitTermination(300, TimeUnit.SECONDS);
        }
        workUnitExecutor.shutdown();
        computationStateCache.clearAndCloseAll();
        workerStatusReporter.stop();
      } catch (Exception e) {
        LOG.warn("Exception while shutting down: ", e);
      }
    }
  }

  @Override
  public void requestWorkerUpdate() {
    workerStatusReporter.reportWorkerUpdates();
  }

  @VisibleForTesting
  @Override
  public ComputationStateCache getComputationStateCache() {
    return computationStateCache;
  }

  @Override
  public Mode mode() {
    return Mode.FAN_OUT_STREAMING_ENGINE;
  }
}
