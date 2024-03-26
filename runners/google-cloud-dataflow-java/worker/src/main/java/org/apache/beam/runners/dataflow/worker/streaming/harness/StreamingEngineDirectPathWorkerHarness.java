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

import static org.apache.beam.runners.dataflow.DataflowRunner.hasExperiment;
import static org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillChannelFactory.remoteChannel;

import com.google.api.services.dataflow.model.MapTask;
import com.google.auto.value.AutoValue;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.internal.CustomSources;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.DataflowMapTaskExecutor;
import org.apache.beam.runners.dataflow.worker.DataflowMapTaskExecutorFactory;
import org.apache.beam.runners.dataflow.worker.DataflowWorkUnitClient;
import org.apache.beam.runners.dataflow.worker.HotKeyLogger;
import org.apache.beam.runners.dataflow.worker.IntrinsicMapTaskExecutorFactory;
import org.apache.beam.runners.dataflow.worker.ReaderCache;
import org.apache.beam.runners.dataflow.worker.ReaderRegistry;
import org.apache.beam.runners.dataflow.worker.SinkRegistry;
import org.apache.beam.runners.dataflow.worker.StreamingModeExecutionContext;
import org.apache.beam.runners.dataflow.worker.WindmillComputationKey;
import org.apache.beam.runners.dataflow.worker.WindmillKeyedWorkItem;
import org.apache.beam.runners.dataflow.worker.WindmillTimeUtils;
import org.apache.beam.runners.dataflow.worker.WorkItemCancelledException;
import org.apache.beam.runners.dataflow.worker.WorkUnitClient;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.graph.Edges;
import org.apache.beam.runners.dataflow.worker.graph.Networks;
import org.apache.beam.runners.dataflow.worker.graph.Nodes;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingMDC;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler;
import org.apache.beam.runners.dataflow.worker.status.DebugCapture;
import org.apache.beam.runners.dataflow.worker.status.WorkerStatusPages;
import org.apache.beam.runners.dataflow.worker.streaming.ExecutionState;
import org.apache.beam.runners.dataflow.worker.streaming.KeyCommitTooLargeException;
import org.apache.beam.runners.dataflow.worker.streaming.ShardedKey;
import org.apache.beam.runners.dataflow.worker.streaming.StageInfo;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.streaming.WorkHeartbeatResponseProcessor;
import org.apache.beam.runners.dataflow.worker.streaming.computations.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.computations.ComputationStateCache;
import org.apache.beam.runners.dataflow.worker.streaming.computations.StreamingEngineComputationStateCacheLoader;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingConfigLoader;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingEngineConfigLoader;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingEnginePipelineConfig;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputStateFetcher;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ElementCounter;
import org.apache.beam.runners.dataflow.worker.util.common.worker.MapTaskExecutor;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputObjectAndByteCounter;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ReadOperation;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.runners.dataflow.worker.windmill.client.CloseableStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.CommitWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.Commit;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.CompleteCommit;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.StreamingEngineWorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.ChannelzServlet;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcDispatcherClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillStreamFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.StreamingEngineClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.ChannelCache;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.ChannelCachingRemoteStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.ChannelCachingStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.IsolationChannel;
import org.apache.beam.runners.dataflow.worker.windmill.state.GetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateReader;
import org.apache.beam.runners.dataflow.worker.windmill.work.ProcessWorkItemClient;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudgetDistributors;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.FailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.StreamingEngineFailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.WorkFailureProcessor;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.ActiveWorkRefresher;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.DirectActiveWorkRefresher;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.graph.MutableNetwork;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public final class StreamingEngineDirectPathWorkerHarness implements StreamingWorkerHarness {
  private static final Logger LOG =
      LoggerFactory.getLogger(StreamingEngineDirectPathWorkerHarness.class);
  // Controls processing parallelism. Maximum number of threads for processing.  Currently, each
  // thread processes one key at a time.
  private static final int MAX_PROCESSING_THREADS = 300;
  private static final long THREAD_EXPIRATION_TIME_SEC = 60;
  private static final int DEFAULT_STATUS_PORT = 8081;
  private static final String CHANNELZ_PATH = "/channelz";
  private static final String NO_MAX_SINK_BYTES_LIMIT_EXPERIMENT =
      "disable_limiting_bundle_sink_bytes";
  // Maximum size of the result of a GetWork request.
  private static final long MAX_GET_WORK_FETCH_BYTES = 64L << 20; // 64m

  /**
   * Sinks are marked 'full' in {@link StreamingModeExecutionContext} once the amount of data sinked
   * (across all the sinks, if there are more than one) reaches this limit. This serves as hint for
   * readers to stop producing more. This can be disabled with 'disable_limiting_bundle_sink_bytes'
   * experiment.
   */
  private static final int MAX_SINK_BYTES = 10_000_000;

  /** Maximum number of failure stacktraces to report in each update sent to backend. */
  private static final int MAX_FAILURES_TO_REPORT_IN_UPDATE = 1000;

  private final WindmillStateCache stateCache;
  private final StreamingConfigLoader<StreamingEnginePipelineConfig> streamingConfigLoader;
  private final ComputationStateCache computationStateCache;
  private final StreamingWorkerStatusPages statusPages;
  // Cache of tokens to commit callbacks.
  // Using Cache with time eviction policy helps us to prevent memory leak when callback ids are
  // discarded by Dataflow service and calling commitCallback is best-effort.
  private final Cache<Long, Runnable> commitCallbacks =
      CacheBuilder.newBuilder().expireAfterWrite(5L, TimeUnit.MINUTES).build();
  // Map of user state names to system state names.
  // TODO(drieber): obsolete stateNameMap. Use transformUserNameToStateFamily in
  // ComputationState instead.
  private final ConcurrentMap<String, String> stateNameMap;
  private final BoundedQueueExecutor workUnitExecutor;
  private final AtomicBoolean running;
  private final SideInputStateFetcher sideInputStateFetcher;
  private final DataflowWorkerHarnessOptions options;
  private final GetDataClient getDataClient;

  // Map from stage name to StageInfo containing metrics container registry and per stage
  // counters.
  private final ConcurrentMap<String, StageInfo> stageInfoMap;

  private final MemoryMonitor memoryMonitor;
  private final ExecutorService memoryMonitorWorker;
  // Limit on bytes sinked (committed) in a work item.
  private final long maxSinkBytes; // = MAX_SINK_BYTES unless disabled in options.
  private final ReaderCache readerCache;
  private final Function<MapTask, MutableNetwork<Nodes.Node, Edges.Edge>> mapTaskToNetwork;
  private final ReaderRegistry readerRegistry = ReaderRegistry.defaultRegistry();
  private final SinkRegistry sinkRegistry = SinkRegistry.defaultRegistry();
  private final Supplier<Instant> clock;
  private final DataflowMapTaskExecutorFactory mapTaskExecutorFactory;
  private final HotKeyLogger hotKeyLogger;
  private final DataflowExecutionStateSampler sampler;
  private final ActiveWorkRefresher activeWorkRefresher;
  private final StreamingWorkerStatusReporter workerStatusReporter;
  private final FailureTracker failureTracker;
  private final WorkFailureProcessor workFailureProcessor;
  private final StreamingCounters streamingCounters;
  private final StreamingEngineClient streamingEngineClient;
  // Possibly overridden by streaming engine config.
  private final AtomicInteger maxWorkItemCommitBytes;

  private StreamingEngineDirectPathWorkerHarness(
      DataflowWorkerHarnessOptions options,
      AtomicBoolean running,
      JobHeader jobHeader,
      WindmillStateCache stateCache,
      ReaderCache readerCache,
      StreamingConfigLoader<StreamingEnginePipelineConfig> streamingConfigLoader,
      ComputationStateCache computationStateCache,
      ConcurrentMap<String, String> stateNameMap,
      BoundedQueueExecutor workUnitExecutor,
      GetDataClient getDataClient,
      ConcurrentMap<String, StageInfo> stageInfoMap,
      MemoryMonitor memoryMonitor,
      WorkUnitClient workUnitClient,
      Supplier<Instant> clock,
      DataflowMapTaskExecutorFactory mapTaskExecutorFactory,
      HotKeyLogger hotKeyLogger,
      ActiveWorkRefresher activeWorkRefresher,
      FailureTracker failureTracker,
      WorkFailureProcessor workFailureProcessor,
      StreamingCounters streamingCounters,
      GrpcWindmillStreamFactory windmillStreamFactory,
      GrpcDispatcherClient dispatcherClient,
      ChannelCachingStubFactory windmillStubFactory,
      Function<CommitWorkStream, WorkCommitter> workCommitterFactory,
      AtomicInteger maxWorkItemCommitBytes,
      DataflowExecutionStateSampler sampler) {
    this.running = running;
    this.maxWorkItemCommitBytes = maxWorkItemCommitBytes;
    this.stateCache = stateCache;
    this.streamingConfigLoader = streamingConfigLoader;
    this.computationStateCache = computationStateCache;
    this.stateNameMap = stateNameMap;
    this.workUnitExecutor = workUnitExecutor;
    this.options = options;
    this.getDataClient = getDataClient;
    this.stageInfoMap = stageInfoMap;
    this.memoryMonitor = memoryMonitor;
    this.memoryMonitorWorker =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setPriority(Thread.MIN_PRIORITY)
                .setNameFormat("MemoryMonitor")
                .build());
    this.maxSinkBytes =
        hasExperiment(options, NO_MAX_SINK_BYTES_LIMIT_EXPERIMENT)
            ? Long.MAX_VALUE
            : MAX_SINK_BYTES;
    this.readerCache = readerCache;
    this.mapTaskToNetwork = StreamingEnvironment.mapTaskToBaseNetworkFnInstance();
    this.clock = clock;
    this.mapTaskExecutorFactory = mapTaskExecutorFactory;
    this.hotKeyLogger = hotKeyLogger;
    this.activeWorkRefresher = activeWorkRefresher;
    this.failureTracker = failureTracker;
    this.workFailureProcessor = workFailureProcessor;
    this.streamingCounters = streamingCounters;
    this.streamingEngineClient =
        StreamingEngineClient.create(
            jobHeader,
            // totalGetWorkBudget =
            GetWorkBudget.builder()
                .setItems(chooseMaximumBundlesOutstanding(options))
                .setBytes(MAX_GET_WORK_FETCH_BYTES)
                .build(),
            windmillStreamFactory,
            this::scheduleWorkItemDirectPath,
            windmillStubFactory,
            GetWorkBudgetDistributors.distributeEvenly(
                computationStateCache::totalCurrentActiveGetWorkBudget),
            dispatcherClient,
            workCommitterFactory,
            new WorkHeartbeatResponseProcessor(computationStateCache::getComputationState));
    this.sideInputStateFetcher =
        new SideInputStateFetcher(
            request ->
                getDataClient.getSideInputState(
                    streamingEngineClient.getGlobalDataStream(request.getDataId().getTag()),
                    request),
            options);
    WorkerStatusPages workerStatusPages =
        WorkerStatusPages.create(DEFAULT_STATUS_PORT, memoryMonitor, () -> true);
    this.statusPages =
        StreamingWorkerStatusPages.create(
            clock,
            jobHeader.getClientId(),
            running,
            workerStatusPages,
            new DebugCapture.Manager(options, workerStatusPages.getDebugCapturePages()),
            new ChannelzServlet(
                CHANNELZ_PATH, options, streamingEngineClient::currentWindmillEndpoints),
            stateCache,
            computationStateCache,
            streamingEngineClient,
            windmillStreamFactory,
            getDataClient,
            workUnitExecutor);
    this.workerStatusReporter =
        StreamingWorkerStatusReporter.create(
            workUnitClient,
            streamingEngineClient::getAndResetThrottleTimes,
            stageInfoMap::values,
            failureTracker,
            streamingCounters,
            memoryMonitor,
            workUnitExecutor);
    this.sampler = sampler;

    FileSystems.setDefaultPipelineOptions(options);
    LOG.debug("windmillServiceEnabled: {}", true);
    LOG.debug("WindmillServiceEndpoint: {}", options.getWindmillServiceEndpoint());
    LOG.debug("WindmillServicePort: {}", options.getWindmillServicePort());
    LOG.debug("LocalWindmillHostport: {}", options.getLocalWindmillHostport());
    LOG.debug("maxWorkItemCommitBytes: {}", maxWorkItemCommitBytes);
    LOG.debug("directPathEnabled: {}", true);
  }

  public static StreamingEngineDirectPathWorkerHarness fromOptions(
      DataflowWorkerHarnessOptions options) {
    long clientId = StreamingEnvironment.newClientId();
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
    Duration maxBackoff =
        options.getLocalWindmillHostport() != null
            ? StreamingEnvironment.localHostMaxBackoff()
            : Duration.standardSeconds(30);
    JobHeader jobHeader = jobHeader(options, clientId);
    GrpcWindmillStreamFactory windmillStreamFactory =
        GrpcWindmillStreamFactory.of(jobHeader)
            .setWindmillMessagesBetweenIsReadyChecks(
                options.getWindmillMessagesBetweenIsReadyChecks())
            .setMaxBackOffSupplier(() -> maxBackoff)
            .setLogEveryNStreamFailures(
                options.getWindmillServiceStreamingLogEveryNStreamFailures())
            .setStreamingRpcBatchLimit(options.getWindmillServiceStreamingRpcBatchLimit())
            .build();
    windmillStreamFactory.scheduleHealthChecks(
        options.getWindmillServiceStreamingRpcHealthCheckPeriodMs());
    FailureTracker failureTracker =
        StreamingEngineFailureTracker.create(
            MAX_FAILURES_TO_REPORT_IN_UPDATE, options.getMaxStackTraceDepthToReport());
    WorkUnitClient dataflowServiceClient = new DataflowWorkUnitClient(options, LOG);
    BoundedQueueExecutor workExecutor = createWorkUnitExecutor(options);
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
    Function<CommitWorkStream, WorkCommitter> workCommitterFactory =
        commitWorkStream ->
            StreamingEngineWorkCommitter.create(
                () -> CloseableStream.create(commitWorkStream, () -> {}),
                Math.max(options.getWindmillServiceCommitThreads(), 1),
                completeCommit ->
                    onCompleteCommit(
                        completeCommit, readerCache, stateCache, computationStateCache));
    AtomicBoolean isRunning = new AtomicBoolean(false);
    GetDataClient getDataClient = new GetDataClient(memoryMonitor);

    DataflowExecutionStateSampler sampler = DataflowExecutionStateSampler.instance();
    int stuckCommitDurationMillis = Math.max(options.getStuckCommitDurationMillis(), 0);
    return new StreamingEngineDirectPathWorkerHarness(
        options,
        isRunning,
        jobHeader,
        stateCache,
        readerCache,
        streamingConfigLoader,
        computationStateCache,
        stateNameMap,
        workExecutor,
        getDataClient,
        stageInfo,
        memoryMonitor,
        dataflowServiceClient,
        clock,
        IntrinsicMapTaskExecutorFactory.defaultFactory(),
        HotKeyLogger.ofSystemClock(),
        DirectActiveWorkRefresher.create(
            clock,
            options.getActiveWorkRefreshPeriodMillis(),
            stuckCommitDurationMillis,
            computationStateCache::getAllComputations,
            sampler,
            getDataClient::refreshActiveWork),
        failureTracker,
        WorkFailureProcessor.create(
            workExecutor,
            failureTracker,
            () -> Optional.ofNullable(memoryMonitor.tryToDumpHeap()),
            clock),
        streamingCounters,
        windmillStreamFactory,
        grpcDispatcherClient,
        windmillStubFactory,
        workCommitterFactory,
        maxWorkItemCommitBytes,
        sampler);
  }

  private static JobHeader jobHeader(DataflowWorkerHarnessOptions options, long clientId) {
    return JobHeader.newBuilder()
        .setJobId(options.getJobId())
        .setProjectId(options.getProject())
        .setWorkerId(options.getWorkerId())
        .setClientId(clientId)
        .build();
  }

  private static BoundedQueueExecutor createWorkUnitExecutor(DataflowWorkerHarnessOptions options) {
    return new BoundedQueueExecutor(
        chooseMaxThreads(options),
        THREAD_EXPIRATION_TIME_SEC,
        TimeUnit.SECONDS,
        chooseMaxBundlesOutstanding(options),
        chooseMaxBytesOutstanding(options),
        new ThreadFactoryBuilder().setNameFormat("DataflowWorkUnits-%d").setDaemon(true).build());
  }

  /** Sets the stage name and workId of the current Thread for logging. */
  private static void setUpWorkLoggingContext(String workLatencyTrackingId, String computationId) {
    DataflowWorkerLoggingMDC.setWorkId(workLatencyTrackingId);
    DataflowWorkerLoggingMDC.setStageName(computationId);
  }

  private static int chooseMaximumBundlesOutstanding(DataflowWorkerHarnessOptions options) {
    int maxBundles = options.getMaxBundlesFromWindmillOutstanding();
    if (maxBundles > 0) {
      return maxBundles;
    }
    return chooseMaxThreads(options) + 100;
  }

  private static int chooseMaxThreads(DataflowWorkerHarnessOptions options) {
    if (options.getNumberOfWorkerHarnessThreads() != 0) {
      return options.getNumberOfWorkerHarnessThreads();
    }
    return MAX_PROCESSING_THREADS;
  }

  private static int chooseMaxBundlesOutstanding(DataflowWorkerHarnessOptions options) {
    int maxBundles = options.getMaxBundlesFromWindmillOutstanding();
    return maxBundles > 0 ? maxBundles : chooseMaxThreads(options) + 100;
  }

  private static long chooseMaxBytesOutstanding(DataflowWorkerHarnessOptions options) {
    long maxMem = options.getMaxBytesFromWindmillOutstanding();
    return maxMem > 0 ? maxMem : (Runtime.getRuntime().maxMemory() / 2);
  }

  private static ShardedKey createShardedKey(Work work) {
    return ShardedKey.create(work.getWorkItem().getKey(), work.getWorkItem().getShardingKey());
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

  private static ReadOperation getValidatedReadOperation(MapTaskExecutor mapTaskExecutor) {
    ReadOperation readOperation = mapTaskExecutor.getReadOperation();
    // Disable progress updates since its results are unused for streaming
    // and involves starting a thread.
    readOperation.setProgressUpdatePeriodMs(ReadOperation.DONT_UPDATE_PERIODICALLY);
    Preconditions.checkState(
        mapTaskExecutor.supportsRestart(),
        "Streaming runner requires all operations support restart.");
    return readOperation;
  }

  private static ParallelInstructionNode extractReadNode(
      MutableNetwork<Nodes.Node, Edges.Edge> mapTaskNetwork) {
    return (ParallelInstructionNode)
        Iterables.find(
            mapTaskNetwork.nodes(),
            node ->
                node instanceof ParallelInstructionNode
                    && ((ParallelInstructionNode) node).getParallelInstruction().getRead() != null);
  }

  private static boolean isCustomSource(ParallelInstructionNode readNode) {
    return CustomSources.class
        .getName()
        .equals(readNode.getParallelInstruction().getRead().getSource().getSpec().get("@type"));
  }

  private static void trackAutoscalingBytesRead(
      MapTask mapTask,
      ParallelInstructionNode readNode,
      Coder<?> readCoder,
      ReadOperation readOperation,
      MapTaskExecutor mapTaskExecutor,
      String counterName) {
    NameContext nameContext =
        NameContext.create(
            mapTask.getStageName(),
            readNode.getParallelInstruction().getOriginalName(),
            readNode.getParallelInstruction().getSystemName(),
            readNode.getParallelInstruction().getName());
    readOperation.receivers[0].addOutputCounter(
        counterName,
        new OutputObjectAndByteCounter(
                new IntrinsicMapTaskExecutorFactory.ElementByteSizeObservableCoder<>(readCoder),
                mapTaskExecutor.getOutputCounters(),
                nameContext)
            .setSamplingPeriod(100)
            .countBytes(counterName));
  }

  private static long computeShuffleBytesRead(Windmill.WorkItem workItem) {
    return workItem.getMessageBundlesList().stream()
        .flatMap(bundle -> bundle.getMessagesList().stream())
        .map(Windmill.Message::getSerializedSize)
        .map(size -> (long) size)
        .reduce(0L, Long::sum);
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  @Override
  public void start() {
    running.set(true);
    streamingConfigLoader.start();
    memoryMonitorWorker.submit(memoryMonitor);
    sampler.start();

    streamingEngineClient.start();
    workerStatusReporter.start(options.getWindmillHarnessUpdateReportingPeriod().getMillis());
    activeWorkRefresher.start();
    if (options.getPeriodicStatusPageOutputDirectory() != null) {
      long delay = 60;
      LOG.info("Scheduling status page dump every {} seconds", delay);
      statusPages.scheduleStatusPageDump(
          options.getPeriodicStatusPageOutputDirectory(), options.getWorkerId(), delay);
    } else {
      LOG.info(
          "Status page output directory was not set, "
              + "status pages will not be periodically dumped. "
              + "If this was not intended check pipeline options.");
    }
  }

  @Override
  public void startStatusPages() {
    statusPages.start();
  }

  @Override
  @SuppressWarnings("ReturnValueIgnored")
  public void stop() {
    try {
      activeWorkRefresher.stop();
      statusPages.stop();

      running.set(false);

      memoryMonitor.stop();
      memoryMonitorWorker.shutdown();
      if (!memoryMonitorWorker.isShutdown()) {
        memoryMonitorWorker.awaitTermination(300, TimeUnit.SECONDS);
      }
      workUnitExecutor.shutdown();
      computationStateCache.close();
      workerStatusReporter.stop();
    } catch (Exception e) {
      LOG.warn("Exception while shutting down: ", e);
    }
  }

  private void scheduleWorkItemDirectPath(
      String computationId,
      @Nullable Instant inputDataWatermark,
      @Nullable Instant synchronizedProcessingTime,
      ProcessWorkItemClient wrappedWorkItem,
      Consumer<Windmill.WorkItem> ackWorkItemQueued,
      Collection<Windmill.LatencyAttribution> getWorkStreamLatencies) {
    Preconditions.checkNotNull(inputDataWatermark);
    Windmill.WorkItem workItem = wrappedWorkItem.workItem();
    // May be null if output watermark not yet known.
    @Nullable
    Instant outputDataWatermark =
        WindmillTimeUtils.windmillToHarnessWatermark(workItem.getOutputDataWatermark());
    Preconditions.checkState(
        outputDataWatermark == null || !outputDataWatermark.isAfter(inputDataWatermark));
    ComputationState computationState =
        computationStateCache
            .getComputationState(computationId)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "No matching computation state for computationId=" + computationId));
    Work scheduledWork =
        Work.create(
            wrappedWorkItem,
            clock,
            getWorkStreamLatencies,
            work ->
                directPathProcessWork(
                    computationState,
                    inputDataWatermark,
                    outputDataWatermark,
                    synchronizedProcessingTime,
                    work,
                    wrappedWorkItem));
    computationState.activateWork(
        ShardedKey.create(workItem.getKey(), workItem.getShardingKey()), scheduledWork);
    ackWorkItemQueued.accept(workItem);
  }

  /**
   * Extracts the userland key coder, if any, from the coder used in the initial read step of a
   * stage. This encodes many assumptions about how the streaming execution context works.
   */
  private @Nullable Coder<?> extractKeyCoder(Coder<?> readCoder) {
    if (!(readCoder instanceof WindowedValue.WindowedValueCoder)) {
      throw new RuntimeException(
          String.format(
              "Expected coder for streaming read to be %s, but received %s",
              WindowedValue.WindowedValueCoder.class.getSimpleName(), readCoder));
    }

    // Note that TimerOrElementCoder is a backwards-compatibility class
    // that is really a FakeKeyedWorkItemCoder
    Coder<?> valueCoder = ((WindowedValue.WindowedValueCoder<?>) readCoder).getValueCoder();

    if (valueCoder instanceof KvCoder<?, ?>) {
      return ((KvCoder<?, ?>) valueCoder).getKeyCoder();
    }
    if (!(valueCoder instanceof WindmillKeyedWorkItem.FakeKeyedWorkItemCoder<?, ?>)) {
      return null;
    }

    return ((WindmillKeyedWorkItem.FakeKeyedWorkItemCoder<?, ?>) valueCoder).getKeyCoder();
  }

  private void callFinalizeCallbacks(Windmill.WorkItem work) {
    for (Long callbackId : work.getSourceState().getFinalizeIdsList()) {
      final Runnable callback = commitCallbacks.getIfPresent(callbackId);
      // NOTE: It is possible the same callback id may be removed twice if
      // windmill restarts.
      // TODO: It is also possible for an earlier finalized id to be lost.
      // We should automatically discard all older callbacks for the same computation and key.
      if (callback != null) {
        commitCallbacks.invalidate(callbackId);
        workUnitExecutor.forceExecute(
            () -> {
              try {
                callback.run();
              } catch (Throwable t) {
                LOG.error("Source checkpoint finalization failed:", t);
              }
            },
            0);
      }
    }
  }

  private Windmill.WorkItemCommitRequest.Builder initializeOutputBuilder(
      final ByteString key, final Windmill.WorkItem workItem) {
    return Windmill.WorkItemCommitRequest.newBuilder()
        .setKey(key)
        .setShardingKey(workItem.getShardingKey())
        .setWorkToken(workItem.getWorkToken())
        .setCacheToken(workItem.getCacheToken());
  }

  private void directPathProcessWork(
      final ComputationState computationState,
      final Instant inputDataWatermark,
      final @Nullable Instant outputDataWatermark,
      final @Nullable Instant synchronizedProcessingTime,
      final Work work,
      final ProcessWorkItemClient processWorkItemClient) {
    final Windmill.WorkItem workItem = work.getWorkItem();
    final String computationId = computationState.getComputationId();
    final ByteString key = workItem.getKey();
    work.setState(Work.State.PROCESSING);

    setUpWorkLoggingContext(work.getLatencyTrackingId(), computationId);

    LOG.debug("Starting processing for {}:\n{}", computationId, work);

    // Before any processing starts, call any pending OnCommit callbacks.  Nothing that requires
    // cleanup should be done before this, since we might exit early here.
    if (isPipelineBeingDrained(
        work,
        initializeOutputBuilder(key, workItem),
        processWorkItemClient::queueCommit,
        computationState)) {
      return;
    }

    long processingStartTimeNanos = System.nanoTime();
    final MapTask mapTask = computationState.getMapTask();

    StageInfo stageInfo =
        stageInfoMap.computeIfAbsent(
            mapTask.getStageName(), s -> StageInfo.create(s, mapTask.getSystemName()));

    try {
      if (work.isFailed()) {
        throw new WorkItemCancelledException(workItem.getShardingKey());
      }

      ExecuteWorkResult executeWorkResult =
          executeWork(
              work,
              stageInfo,
              mapTask,
              computationState,
              inputDataWatermark,
              outputDataWatermark,
              synchronizedProcessingTime);

      Windmill.WorkItemCommitRequest.Builder outputBuilder = executeWorkResult.commitWorkRequest();

      // Add the output to the commit queue.
      work.setState(Work.State.COMMIT_QUEUED);
      outputBuilder.addAllPerWorkItemLatencyAttributions(
          work.getLatencyAttributions(false, sampler));

      Windmill.WorkItemCommitRequest commitRequest = outputBuilder.build();
      int byteLimit = maxWorkItemCommitBytes.get();
      int commitSize = commitRequest.getSerializedSize();
      int estimatedCommitSize = commitSize < 0 ? Integer.MAX_VALUE : commitSize;

      // Detect overflow of integer serialized size or if the byte limit was exceeded.
      streamingCounters.windmillMaxObservedWorkItemCommitBytes().addValue(estimatedCommitSize);
      if (commitSize < 0 || commitSize > byteLimit) {
        KeyCommitTooLargeException e =
            KeyCommitTooLargeException.causedBy(computationId, byteLimit, commitRequest);
        failureTracker.trackFailure(computationId, workItem, e);
        LOG.error(e.toString());

        // Drop the current request in favor of a new, minimal one requesting truncation.
        // Messages, timers, counters, and other commit content will not be used by the service
        // so, we're purposefully dropping them here
        commitRequest = buildWorkItemTruncationRequest(key, workItem, estimatedCommitSize);
      }

      processWorkItemClient.queueCommit(Commit.create(commitRequest, computationState, work));
      recordProcessingStats(outputBuilder, workItem, executeWorkResult);
      LOG.debug("Processing done for work token: {}", workItem.getWorkToken());
    } catch (Throwable t) {
      workFailureProcessor.logAndProcessFailure(
          computationId,
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
      DataflowWorkerLoggingMDC.setWorkId(null);
      DataflowWorkerLoggingMDC.setStageName(null);
    }
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
      Work work,
      StageInfo stageInfo,
      MapTask mapTask,
      ComputationState computationState,
      Instant inputDataWatermark,
      @Nullable Instant outputDataWatermark,
      @Nullable Instant synchronizedProcessingTime)
      throws Exception {
    ProcessWorkItemClient processWorkItemClient = work.getProcessWorkItemClient();
    String computationId = computationState.getComputationId();
    Windmill.WorkItem workItem = work.getWorkItem();
    ByteString key = workItem.getKey();
    Windmill.WorkItemCommitRequest.Builder outputBuilder = initializeOutputBuilder(key, workItem);
    String counterName = "dataflow_source_bytes_processed-" + mapTask.getSystemName();
    ExecutionState executionState =
        getOrCreateExecutionState(mapTask, stageInfo, work, computationState, counterName);

    try {
      WindmillStateReader stateReader =
          createWindmillStateReader(
              key,
              work,
              (Windmill.KeyedGetDataRequest request) ->
                  Optional.ofNullable(
                      getDataClient.getState(
                          processWorkItemClient.getDataStream(), computationId, request)));
      SideInputStateFetcher localSideInputStateFetcher = sideInputStateFetcher.byteTrackingView();

      // If the read output KVs, then we can decode Windmill's byte key into userland
      // key object and provide it to the execution context for use with per-key state.
      // Otherwise, we pass null.
      //
      // The coder type that will be present is:
      //     WindowedValueCoder(TimerOrElementCoder(KvCoder))
      Optional<Coder<?>> keyCoder = executionState.keyCoder();
      @SuppressWarnings("deprecation")
      @Nullable
      Object executionKey =
          !keyCoder.isPresent() ? null : keyCoder.get().decode(key.newInput(), Coder.Context.OUTER);

      if (workItem.hasHotKeyInfo()) {
        Windmill.HotKeyInfo hotKeyInfo = workItem.getHotKeyInfo();
        Duration hotKeyAge = Duration.millis(hotKeyInfo.getHotKeyAgeUsec() / 1000);

        // The MapTask instruction is ordered by dependencies, such that the first element is
        // always going to be the shuffle task.
        String stepName = computationState.getMapTask().getInstructions().get(0).getName();
        if (options.isHotKeyLoggingEnabled() && keyCoder.isPresent()) {
          hotKeyLogger.logHotKeyDetection(stepName, hotKeyAge, executionKey);
        } else {
          hotKeyLogger.logHotKeyDetection(stepName, hotKeyAge);
        }
      }

      executionState
          .context()
          .start(
              executionKey,
              workItem,
              inputDataWatermark,
              outputDataWatermark,
              synchronizedProcessingTime,
              stateReader,
              localSideInputStateFetcher,
              outputBuilder,
              work::isFailed);

      // Blocks while executing work.
      executionState.workExecutor().execute();

      if (work.isFailed()) {
        throw new WorkItemCancelledException(workItem.getShardingKey());
      }

      // Reports source bytes processed to WorkItemCommitRequest if available.
      try {
        long sourceBytesProcessed = 0;
        HashMap<String, ElementCounter> counters =
            ((DataflowMapTaskExecutor) executionState.workExecutor())
                .getReadOperation()
                .receivers[0]
                .getOutputCounters();
        if (counters.containsKey(counterName)) {
          sourceBytesProcessed =
              ((OutputObjectAndByteCounter) counters.get(counterName)).getByteCount().getAndReset();
        }
        outputBuilder.setSourceBytesProcessed(sourceBytesProcessed);
      } catch (Exception e) {
        LOG.error(e.toString());
      }

      commitCallbacks.putAll(executionState.context().flushState());

      // Release the execution state for another thread to use.
      computationState.releaseExecutionState(executionState);

      return ExecuteWorkResult.create(
          outputBuilder, stateReader.getBytesRead() + localSideInputStateFetcher.getBytesRead());
    } catch (Throwable t) {
      // If processing failed due to a thrown exception, close the executionState. Do not
      // return/release the executionState back to computationState as that will lead to this
      // executionState instance being reused.
      executionState.close();

      // Re-throw the exception, it will be caught and handled by workFailureProcessor downstream.
      throw t;
    }
  }

  private DataflowMapTaskExecutor createMapTaskExecutor(
      StreamingModeExecutionContext context,
      MapTask mapTask,
      MutableNetwork<Nodes.Node, Edges.Edge> mapTaskNetwork) {
    return mapTaskExecutorFactory.create(
        mapTaskNetwork,
        options,
        mapTask.getStageName(),
        readerRegistry,
        sinkRegistry,
        context,
        streamingCounters.pendingDeltaCounters(),
        StreamingEnvironment.idGeneratorInstance());
  }

  private StreamingModeExecutionContext createExecutionContext(
      ComputationState computationState,
      StageInfo stageInfo,
      DataflowExecutionContext.DataflowExecutionStateTracker executionStateTracker) {
    String computationId = computationState.getComputationId();
    return new StreamingModeExecutionContext(
        streamingCounters.pendingDeltaCounters(),
        computationId,
        readerCache,
        !computationState.getTransformUserNameToStateFamily().isEmpty()
            ? computationState.getTransformUserNameToStateFamily()
            : stateNameMap,
        stateCache.forComputation(computationId),
        stageInfo.metricsContainerRegistry(),
        executionStateTracker,
        stageInfo.executionStateRegistry(),
        maxSinkBytes);
  }

  private DataflowExecutionContext.DataflowExecutionStateTracker createExecutionStateTracker(
      StageInfo stageInfo, MapTask mapTask, Work work) {
    return new DataflowExecutionContext.DataflowExecutionStateTracker(
        sampler,
        stageInfo
            .executionStateRegistry()
            .getState(
                NameContext.forStage(mapTask.getStageName()),
                "other",
                null,
                ScopedProfiler.INSTANCE.emptyScope()),
        stageInfo.deltaCounters(),
        options,
        work.getLatencyTrackingId());
  }

  private ExecutionState getOrCreateExecutionState(
      MapTask mapTask,
      StageInfo stageInfo,
      Work work,
      ComputationState computationState,
      String counterName) {
    Optional<ExecutionState> existingExecutionState = computationState.getExecutionState();
    if (existingExecutionState.isPresent()) {
      return existingExecutionState.get();
    }

    MutableNetwork<Nodes.Node, Edges.Edge> mapTaskNetwork = mapTaskToNetwork.apply(mapTask);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Network as Graphviz .dot: {}", Networks.toDot(mapTaskNetwork));
    }

    ParallelInstructionNode readNode = extractReadNode(mapTaskNetwork);
    Nodes.InstructionOutputNode readOutputNode =
        (Nodes.InstructionOutputNode) Iterables.getOnlyElement(mapTaskNetwork.successors(readNode));

    DataflowExecutionContext.DataflowExecutionStateTracker executionStateTracker =
        createExecutionStateTracker(stageInfo, mapTask, work);
    StreamingModeExecutionContext context =
        createExecutionContext(computationState, stageInfo, executionStateTracker);
    DataflowMapTaskExecutor mapTaskExecutor =
        createMapTaskExecutor(context, mapTask, mapTaskNetwork);
    ReadOperation readOperation = getValidatedReadOperation(mapTaskExecutor);

    Coder<?> readCoder =
        CloudObjects.coderFromCloudObject(
            CloudObject.fromSpec(readOutputNode.getInstructionOutput().getCodec()));
    Coder<?> keyCoder = extractKeyCoder(readCoder);

    // If using a custom source, count bytes read for autoscaling.
    if (isCustomSource(readNode)) {
      trackAutoscalingBytesRead(
          mapTask, readNode, readCoder, readOperation, mapTaskExecutor, counterName);
    }

    ExecutionState.Builder executionStateBuilder =
        ExecutionState.builder()
            .setWorkExecutor(mapTaskExecutor)
            .setContext(context)
            .setExecutionStateTracker(executionStateTracker);

    if (keyCoder != null) {
      executionStateBuilder.setKeyCoder(keyCoder);
    }

    return executionStateBuilder.build();
  }

  private WindmillStateReader createWindmillStateReader(
      ByteString key,
      Work work,
      Function<Windmill.KeyedGetDataRequest, Optional<Windmill.KeyedGetDataResponse>>
          getKeyedDataFn) {
    return new WindmillStateReader(
        getKeyedDataFn::apply,
        key,
        work.getWorkItem().getShardingKey(),
        work.getWorkItem().getWorkToken(),
        () -> {
          work.setState(Work.State.READING);
          return () -> work.setState(Work.State.PROCESSING);
        },
        work::isFailed);
  }

  private boolean isPipelineBeingDrained(
      Work work,
      Windmill.WorkItemCommitRequest.Builder outputBuilder,
      Consumer<Commit> committer,
      ComputationState computationState) {
    callFinalizeCallbacks(work.getWorkItem());
    if (work.getWorkItem().getSourceState().getOnlyFinalize()) {
      outputBuilder.setSourceStateUpdates(Windmill.SourceState.newBuilder().setOnlyFinalize(true));
      work.setState(Work.State.QUEUED);
      committer.accept(Commit.create(outputBuilder.build(), computationState, work));
      return true;
    }

    return false;
  }

  private Windmill.WorkItemCommitRequest buildWorkItemTruncationRequest(
      final ByteString key, final Windmill.WorkItem workItem, final int estimatedCommitSize) {
    Windmill.WorkItemCommitRequest.Builder outputBuilder = initializeOutputBuilder(key, workItem);
    outputBuilder.setExceedsMaxWorkItemCommitBytes(true);
    outputBuilder.setEstimatedWorkItemCommitBytes(estimatedCommitSize);
    return outputBuilder.build();
  }

  @AutoValue
  abstract static class ExecuteWorkResult {
    private static ExecuteWorkResult create(
        Windmill.WorkItemCommitRequest.Builder commitWorkRequest, long stateBytesRead) {
      return new AutoValue_StreamingEngineDirectPathWorkerHarness_ExecuteWorkResult(
          commitWorkRequest, stateBytesRead);
    }

    abstract Windmill.WorkItemCommitRequest.Builder commitWorkRequest();

    abstract long stateBytesRead();
  }
}
