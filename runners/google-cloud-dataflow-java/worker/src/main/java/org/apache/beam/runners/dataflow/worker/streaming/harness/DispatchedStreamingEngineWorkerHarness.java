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

import com.google.api.services.dataflow.model.MapTask;
import java.util.Collection;
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
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.DataflowMapTaskExecutorFactory;
import org.apache.beam.runners.dataflow.worker.DataflowWorkUnitClient;
import org.apache.beam.runners.dataflow.worker.HotKeyLogger;
import org.apache.beam.runners.dataflow.worker.IntrinsicMapTaskExecutorFactory;
import org.apache.beam.runners.dataflow.worker.MetricTrackingWindmillServerStub;
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
import org.apache.beam.runners.dataflow.worker.streaming.processing.StreamingWorkExecutor;
import org.apache.beam.runners.dataflow.worker.streaming.processing.StreamingWorkItemScheduler;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputStateFetcher;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamPool;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.CompleteCommit;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.StreamingEngineWorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcDispatcherClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillServer;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillStreamFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.ChannelCache;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.ChannelCachingRemoteStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.IsolationChannel;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.FailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.StreamingEngineFailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.WorkFailureProcessor;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.ActiveWorkRefresher;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.DispatchedActiveWorkRefresher;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming Engine worker harness that communicates with Streaming Engine backend. Workers running
 * in this mode will GetWork, GetData, and CommitWork to a single address on the Streaming Engine
 * backend.
 */
@Internal
public final class DispatchedStreamingEngineWorkerHarness implements StreamingWorkerHarness {
  private static final Logger LOG =
      LoggerFactory.getLogger(DispatchedStreamingEngineWorkerHarness.class);
  private static final int NUM_COMMIT_STREAMS = 1;
  private static final int GET_WORK_STREAM_TIMEOUT_MINUTES = 3;
  private static final Duration COMMIT_STREAM_TIMEOUT = Duration.standardMinutes(1);

  private final DataflowWorkerHarnessOptions options;
  private final AtomicBoolean isRunning;
  private final long clientId;
  private final StreamingConfigLoader<StreamingEnginePipelineConfig> streamingConfigLoader;
  private final ComputationStateCache computationStateCache;
  private final WindmillServerStub windmillServer;
  private final WorkCommitter workCommitter;
  private final MetricTrackingWindmillServerStub getDataClient;
  private final StreamingWorkItemScheduler workItemScheduler;
  private final MemoryMonitor memoryMonitor;
  private final StreamingWorkerStatusReporter workerStatusReporter;
  private final StreamingStatusPages statusPages;
  private final ActiveWorkRefresher activeWorkRefresher;
  private final ExecutorService streamingGetWorkExecutor;
  private final ExecutorService memoryMonitorExecutor;

  private DispatchedStreamingEngineWorkerHarness(
      DataflowWorkerHarnessOptions options,
      long clientId,
      AtomicBoolean isRunning,
      StreamingConfigLoader<StreamingEnginePipelineConfig> streamingConfigLoader,
      ComputationStateCache computationStateCache,
      WindmillServerStub windmillServer,
      WorkCommitter workCommitter,
      MetricTrackingWindmillServerStub getDataClient,
      StreamingWorkItemScheduler workItemScheduler,
      MemoryMonitor memoryMonitor,
      StreamingWorkerStatusReporter workerStatusReporter,
      StreamingStatusPages statusPages,
      ActiveWorkRefresher activeWorkRefresher,
      ExecutorService streamingGetWorkExecutor,
      ExecutorService memoryMonitorExecutor) {
    this.options = options;
    this.isRunning = isRunning;
    this.clientId = clientId;
    this.streamingConfigLoader = streamingConfigLoader;
    this.computationStateCache = computationStateCache;
    this.windmillServer = windmillServer;
    this.workCommitter = workCommitter;
    this.getDataClient = getDataClient;
    this.workItemScheduler = workItemScheduler;
    this.memoryMonitor = memoryMonitor;
    this.workerStatusReporter = workerStatusReporter;
    this.statusPages = statusPages;
    this.activeWorkRefresher = activeWorkRefresher;
    this.streamingGetWorkExecutor = streamingGetWorkExecutor;
    this.memoryMonitorExecutor = memoryMonitorExecutor;
    // Register standard file systems.
    FileSystems.setDefaultPipelineOptions(options);
  }

  public static DispatchedStreamingEngineWorkerHarness fromOptions(
      DataflowWorkerHarnessOptions options) {
    long clientId = StreamingWorkerEnvironment.newClientId();
    MemoryMonitor memoryMonitor = MemoryMonitor.fromOptions(options);
    ConcurrentMap<String, StageInfo> stageInfo = new ConcurrentHashMap<>();
    ReaderCache readerCache =
        new ReaderCache(
            Duration.standardSeconds(options.getReaderCacheTimeoutSec()),
            Executors.newCachedThreadPool());
    StreamingCounters streamingCounters = StreamingCounters.create();
    GrpcDispatcherClient grpcDispatcherClient = createDispatcherClient(options);
    GrpcWindmillStreamFactory windmillStreamFactory =
        createWindmillStreamFactory(options, clientId);
    WorkHeartbeatResponseProcessor workHeartbeatResponseProcessor =
        new WorkHeartbeatResponseProcessor();
    WindmillServerStub windmillServer =
        GrpcWindmillServer.create(
            options, windmillStreamFactory, workHeartbeatResponseProcessor, grpcDispatcherClient);
    MetricTrackingWindmillServerStub getDataClient =
        MetricTrackingWindmillServerStub.builder(windmillServer, memoryMonitor)
            .setUseStreamingRequests(true)
            .setUseSeparateHeartbeatStreams(options.getUseSeparateWindmillHeartbeatStreams())
            .setNumGetDataStreams(options.getWindmillGetDataStreamCount())
            .build();
    FailureTracker failureTracker =
        StreamingEngineFailureTracker.create(
            StreamingWorkerEnvironment.getMaxFailuresToReportInUpdate(),
            options.getMaxStackTraceDepthToReport());
    WorkUnitClient dataflowServiceClient = new DataflowWorkUnitClient(options, LOG);
    BoundedQueueExecutor workExecutor = StreamingWorkerEnvironment.createWorkUnitExecutor(options);
    Supplier<Instant> clock = Instant::now;
    WorkFailureProcessor workFailureProcessor =
        WorkFailureProcessor.create(
            workExecutor,
            failureTracker,
            () -> Optional.ofNullable(memoryMonitor.tryToDumpHeap()),
            clock);
    StreamingWorkerStatusReporter workerStatusReporter =
        StreamingWorkerStatusReporter.create(
            dataflowServiceClient,
            windmillServer::getAndResetThrottleTime,
            stageInfo::values,
            failureTracker,
            streamingCounters,
            memoryMonitor,
            workExecutor);
    AtomicInteger maxWorkItemCommitBytes = new AtomicInteger(Integer.MAX_VALUE);
    ConcurrentMap<String, String> stateNameMap = new ConcurrentHashMap<>();
    StreamingEngineConfigLoader streamingConfigLoader =
        StreamingEngineConfigLoader.create(
            Optional.ofNullable(options.getGlobalConfigRefreshPeriod())
                .map(Duration::getMillis)
                .orElse(0L),
            dataflowServiceClient,
            config ->
                onDataflowServiceConfig(
                    config, stateNameMap, grpcDispatcherClient, maxWorkItemCommitBytes));
    WindmillStateCache stateCache = WindmillStateCache.ofSizeMbs(options.getWorkerCacheMb());
    CacheLoader<String, Optional<ComputationState>> cacheLoader =
        new StreamingEngineComputationStateCacheLoader(
            streamingConfigLoader, workExecutor, stateCache::forComputation);
    ComputationStateCache computationStateCache = ComputationStateCache.create(cacheLoader);
    workHeartbeatResponseProcessor.setComputationStateFetcher(
        computationStateCache::getComputationState);
    StreamingEngineWorkCommitter workCommitter =
        StreamingEngineWorkCommitter.create(
            WindmillStreamPool.create(
                    NUM_COMMIT_STREAMS, COMMIT_STREAM_TIMEOUT, windmillServer::commitWorkStream)
                ::getCloseableStream,
            Math.max(1, options.getWindmillServiceCommitThreads()),
            completeCommit ->
                onCompleteCommit(completeCommit, readerCache, stateCache, computationStateCache));
    DataflowExecutionStateSampler sampler = DataflowExecutionStateSampler.instance();
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
    StreamingWorkExecutor streamingWorkExecutor =
        new StreamingWorkExecutor(
            options,
            executionStateFactory,
            new SideInputStateFetcher(getDataClient::getSideInputData, options),
            failureTracker,
            workFailureProcessor,
            StreamingCommitFinalizer.create(workExecutor),
            streamingCounters,
            HotKeyLogger.ofSystemClock(),
            stageInfo,
            sampler,
            maxWorkItemCommitBytes);
    AtomicBoolean isRunning = new AtomicBoolean(false);
    WorkerStatusPages workerStatusPages =
        StreamingWorkerEnvironment.createStatusPages(memoryMonitor);
    StreamingWorkerStatusPages statusPages =
        StreamingWorkerStatusPages.forStreamingEngine(
            clock,
            clientId,
            isRunning,
            workerStatusPages,
            new DebugCapture.Manager(options, workerStatusPages.getDebugCapturePages()),
            StreamingWorkerEnvironment.createChannelZServlet(
                options, windmillServer::getWindmillServiceEndpoints),
            stateCache,
            computationStateCache,
            workCommitter::currentActiveCommitBytes,
            windmillStreamFactory,
            getDataClient::printHtml,
            workExecutor);
    int stuckCommitDurationMillis = Math.max(options.getStuckCommitDurationMillis(), 0);
    ActiveWorkRefresher activeWorkRefresher =
        DispatchedActiveWorkRefresher.create(
            clock,
            options.getActiveWorkRefreshPeriodMillis(),
            stuckCommitDurationMillis,
            computationStateCache::getAllComputations,
            sampler,
            getDataClient::refreshActiveWork,
            Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("RefreshWork").build()));
    LOG.debug("windmillServiceEnabled: {}", true);
    LOG.debug("WindmillServiceEndpoint: {}", options.getWindmillServiceEndpoint());
    LOG.debug("WindmillServicePort: {}", options.getWindmillServicePort());
    LOG.debug("LocalWindmillHostport: {}", options.getLocalWindmillHostport());
    LOG.debug("maxWorkItemCommitBytes: {}", maxWorkItemCommitBytes.get());
    LOG.debug("directPathEnabled: {}", false);
    return new DispatchedStreamingEngineWorkerHarness(
        options,
        clientId,
        isRunning,
        streamingConfigLoader,
        computationStateCache,
        windmillServer,
        workCommitter,
        getDataClient,
        new StreamingWorkItemScheduler(clock, streamingWorkExecutor),
        memoryMonitor,
        workerStatusReporter,
        statusPages,
        activeWorkRefresher,
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("StreamingGetWorkDispatchThread")
                .setPriority(Thread.MIN_PRIORITY)
                .setDaemon(true)
                .build()),
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("MemoryMonitor")
                .setPriority(Thread.MIN_PRIORITY)
                .build()));
  }

  @VisibleForTesting
  public static DispatchedStreamingEngineWorkerHarness forTesting(
      BoundedQueueExecutor workExecutor,
      WindmillStateCache stateCache,
      GrpcDispatcherClient grpcDispatcherClient,
      WindmillServerStub windmillServer,
      List<MapTask> mapTasks,
      DataflowMapTaskExecutorFactory mapTaskExecutorFactory,
      WorkUnitClient workUnitClient,
      DataflowWorkerHarnessOptions options,
      boolean publishCounters,
      HotKeyLogger hotKeyLogger,
      Supplier<Instant> clock,
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
        StreamingEngineFailureTracker.create(
            StreamingWorkerEnvironment.getMaxFailuresToReportInUpdate(),
            options.getMaxStackTraceDepthToReport());
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
            executorSupplier);
    StreamingEngineConfigLoader streamingConfigLoader =
        StreamingEngineConfigLoader.forTesting(
            true,
            Optional.ofNullable(options.getGlobalConfigRefreshPeriod())
                .map(Duration::getMillis)
                .orElse(0L),
            workUnitClient,
            executorSupplier,
            config ->
                onDataflowServiceConfig(
                    config, stateNameMap, grpcDispatcherClient, maxWorkItemCommitBytes));
    CacheLoader<String, Optional<ComputationState>> cacheLoader =
        new StreamingEngineComputationStateCacheLoader(
            streamingConfigLoader, workExecutor, stateCache::forComputation);

    ComputationStateCache computationStateCache = ComputationStateCache.create(cacheLoader);
    computationStateCache.putAll(
        StreamingWorkerEnvironment.createComputationMapForTesting(
            mapTasks, workExecutor, stateCache::forComputation));
    ReaderCache readerCache =
        new ReaderCache(
            Duration.standardSeconds(options.getReaderCacheTimeoutSec()),
            Executors.newCachedThreadPool());
    StreamingEngineWorkCommitter workCommitter =
        StreamingEngineWorkCommitter.create(
            WindmillStreamPool.create(
                    NUM_COMMIT_STREAMS, COMMIT_STREAM_TIMEOUT, windmillServer::commitWorkStream)
                ::getCloseableStream,
            Math.max(1, options.getWindmillServiceCommitThreads()),
            completeCommit ->
                onCompleteCommit(completeCommit, readerCache, stateCache, computationStateCache));
    MetricTrackingWindmillServerStub getDataClient =
        MetricTrackingWindmillServerStub.builder(windmillServer, memoryMonitor)
            .setUseStreamingRequests(true)
            .setUseSeparateHeartbeatStreams(options.getUseSeparateWindmillHeartbeatStreams())
            .setNumGetDataStreams(options.getWindmillGetDataStreamCount())
            .build();
    DataflowExecutionStateSampler sampler = DataflowExecutionStateSampler.instance();
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
    StreamingWorkExecutor streamingWorkExecutor =
        new StreamingWorkExecutor(
            options,
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
    int stuckCommitDurationMillis = Math.max(options.getStuckCommitDurationMillis(), 0);
    ActiveWorkRefresher activeWorkRefresher =
        DispatchedActiveWorkRefresher.create(
            clock,
            options.getActiveWorkRefreshPeriodMillis(),
            stuckCommitDurationMillis,
            computationStateCache::getAllComputations,
            sampler,
            getDataClient::refreshActiveWork,
            executorSupplier.apply("RefreshWork"));
    return new DispatchedStreamingEngineWorkerHarness(
        options,
        1L,
        new AtomicBoolean(false),
        streamingConfigLoader,
        computationStateCache,
        windmillServer,
        workCommitter,
        getDataClient,
        new StreamingWorkItemScheduler(clock, streamingWorkExecutor),
        memoryMonitor,
        workerStatusReporter,
        testStatusPages,
        activeWorkRefresher,
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("MemoryMonitor")
                .setPriority(Thread.MIN_PRIORITY)
                .build()),
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("StreamingGetWorkDispatchThread")
                .setPriority(Thread.MIN_PRIORITY)
                .setDaemon(true)
                .build()));
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

  private static GrpcDispatcherClient createDispatcherClient(DataflowWorkerHarnessOptions options) {
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
    WindmillStubFactory stubFactory =
        ChannelCachingRemoteStubFactory.create(options.getGcpCredential(), channelCache);
    return GrpcDispatcherClient.create(stubFactory);
  }

  private static GrpcWindmillStreamFactory createWindmillStreamFactory(
      DataflowWorkerHarnessOptions options, long clientId) {
    Duration maxBackoff =
        !options.isEnableStreamingEngine() && options.getLocalWindmillHostport() != null
            ? GrpcWindmillServer.LOCALHOST_MAX_BACKOFF
            : Duration.millis(options.getWindmillServiceStreamMaxBackoffMillis());
    return GrpcWindmillStreamFactory.of(
            Windmill.JobHeader.newBuilder()
                .setJobId(options.getJobId())
                .setProjectId(options.getProject())
                .setWorkerId(options.getWorkerId())
                .setClientId(clientId)
                .build())
        .setWindmillMessagesBetweenIsReadyChecks(options.getWindmillMessagesBetweenIsReadyChecks())
        .setMaxBackOffSupplier(() -> maxBackoff)
        .setLogEveryNStreamFailures(options.getWindmillServiceStreamingLogEveryNStreamFailures())
        .setStreamingRpcBatchLimit(options.getWindmillServiceStreamingRpcBatchLimit())
        .build();
  }

  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public void start() {
    if (isRunning.compareAndSet(false, true)) {
      workerStatusReporter.start(options.getWindmillHarnessUpdateReportingPeriod().getMillis());
      streamingConfigLoader.start();
      memoryMonitorExecutor.submit(memoryMonitor);
      workCommitter.start();
      streamingGetWorkExecutor.submit(this::streamingDispatchLoop);
      statusPages.start(options);
      activeWorkRefresher.start();
      StreamingWorkerEnvironment.enableBigQueryMetrics(options);
    }
  }

  @Override
  public void stop() {
    if (isRunning.compareAndSet(true, false)) {
      ExecutorUtils.shutdownExecutors(streamingGetWorkExecutor, memoryMonitorExecutor);
      streamingConfigLoader.stop();
      workCommitter.stop();
      activeWorkRefresher.stop();
      statusPages.stop();
      workerStatusReporter.stop();
    }
  }

  @Override
  public void requestWorkerUpdate() {
    workerStatusReporter.reportWorkerUpdates();
  }

  void streamingDispatchLoop() {
    LOG.info("Starting to fetch work.");
    while (isRunning.get()) {
      WindmillStream.GetWorkStream stream =
          windmillServer.getWorkStream(
              Windmill.GetWorkRequest.newBuilder()
                  .setClientId(clientId)
                  .setMaxItems(StreamingWorkerEnvironment.chooseMaximumBundlesOutstanding(options))
                  .setMaxBytes(StreamingWorkerEnvironment.getMaxGetWorkFetchBytes())
                  .build(),
              (String computationId,
                  @Nullable Instant inputDataWatermark,
                  @Nullable Instant synchronizedProcessingTime,
                  Windmill.WorkItem workItem,
                  Collection<Windmill.LatencyAttribution> getWorkStreamLatencies) -> {
                memoryMonitor.waitForResources("GetWork");
                Preconditions.checkArgument(
                    !computationId.isEmpty(),
                    "computationId is empty. Cannot schedule work without a computationId.");
                Optional<ComputationState> computationState =
                    computationStateCache.getComputationState(computationId);
                if (computationState.isPresent()) {
                  workItemScheduler.scheduleWork(
                      computationState.get(),
                      Preconditions.checkNotNull(inputDataWatermark),
                      synchronizedProcessingTime,
                      workItem,
                      workCommitter::commit,
                      getDataClient::getStateData,
                      getWorkStreamLatencies);
                } else {
                  LOG.warn(
                      "Computation computationId={} is unknown. Known computations:{}",
                      computationId,
                      computationStateCache.knownComputationIds());
                }
              });
      try {
        // Reconnect every now and again to enable better load balancing.
        // If at any point the server closes the stream, we will reconnect immediately; otherwise
        // we half-close the stream after some time and create a new one.
        if (!stream.awaitTermination(GET_WORK_STREAM_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
          stream.close();
        }
      } catch (InterruptedException e) {
        // Continue processing until !running.get()
      }
    }
    LOG.info("Stopped fetching work.");
  }

  @VisibleForTesting
  public int numCommitThreads() {
    return workCommitter.parallelism();
  }

  @VisibleForTesting
  @Override
  public ComputationStateCache getComputationStateCache() {
    return computationStateCache;
  }

  @Override
  public Mode mode() {
    return Mode.DISPATCHED_STREAMING_ENGINE;
  }
}
