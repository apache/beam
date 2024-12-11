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
package org.apache.beam.runners.dataflow.worker;

import static org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillChannelFactory.remoteChannel;

import com.google.api.services.dataflow.model.MapTask;
import com.google.auto.value.AutoValue;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.metrics.MetricsLogger;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.status.DebugCapture;
import org.apache.beam.runners.dataflow.worker.status.WorkerStatusPages;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationStateCache;
import org.apache.beam.runners.dataflow.worker.streaming.StageInfo;
import org.apache.beam.runners.dataflow.worker.streaming.WeightedSemaphore;
import org.apache.beam.runners.dataflow.worker.streaming.WorkHeartbeatResponseProcessor;
import org.apache.beam.runners.dataflow.worker.streaming.config.ComputationConfig;
import org.apache.beam.runners.dataflow.worker.streaming.config.FixedGlobalConfigHandle;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingApplianceComputationConfigFetcher;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingEngineComputationConfigFetcher;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingGlobalConfig;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingGlobalConfigHandle;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingGlobalConfigHandleImpl;
import org.apache.beam.runners.dataflow.worker.streaming.harness.FanOutStreamingEngineWorkerHarness;
import org.apache.beam.runners.dataflow.worker.streaming.harness.SingleSourceWorkerHarness;
import org.apache.beam.runners.dataflow.worker.streaming.harness.SingleSourceWorkerHarness.GetWorkSender;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingCounters;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingWorkerHarness;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingWorkerStatusPages;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingWorkerStatusReporter;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.windmill.ApplianceWindmillClient;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.runners.dataflow.worker.windmill.appliance.JniWindmillApplianceServer;
import org.apache.beam.runners.dataflow.worker.windmill.client.CloseableStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamPool;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.Commit;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.Commits;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.CompleteCommit;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.StreamingApplianceWorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.StreamingEngineWorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.ApplianceGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.GetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.StreamPoolGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.ThrottlingGetDataMetricTracker;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.ChannelzServlet;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcDispatcherClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillServer;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillStreamFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.ChannelCache;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.ChannelCachingRemoteStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.ChannelCachingStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.IsolationChannel;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillStubFactoryFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillStubFactoryFactoryImpl;
import org.apache.beam.runners.dataflow.worker.windmill.client.throttling.ThrottledTimeTracker;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudgetDistributors;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.StreamingWorkScheduler;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.FailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.StreamingApplianceFailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.StreamingEngineFailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.WorkFailureProcessor;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.ActiveWorkRefresher;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.ApplianceHeartbeatSender;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.StreamPoolHeartbeatSender;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.fn.JvmInitializers;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.bigquery.BigQuerySinkMetrics;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.util.construction.CoderTranslation;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheStats;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <b>For internal use only.</b>
 *
 * <p>Implements a Streaming Dataflow worker.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@Internal
public final class StreamingDataflowWorker {

  /**
   * Sinks are marked 'full' in {@link StreamingModeExecutionContext} once the amount of data sinked
   * (across all the sinks, if there are more than one) reaches this limit. This serves as hint for
   * readers to stop producing more. This can be disabled with 'disable_limiting_bundle_sink_bytes'
   * experiment.
   */
  public static final int MAX_SINK_BYTES = 10_000_000;

  private static final Logger LOG = LoggerFactory.getLogger(StreamingDataflowWorker.class);

  /**
   * Maximum number of threads for processing. Currently, each thread processes one key at a time.
   */
  private static final int MAX_PROCESSING_THREADS = 300;

  /** The idGenerator to generate unique id globally. */
  private static final IdGenerator ID_GENERATOR = IdGenerators.decrementingLongs();

  /** Maximum size of the result of a GetWork request. */
  private static final long MAX_GET_WORK_FETCH_BYTES = 64L << 20; // 64m

  /** Maximum number of failure stacktraces to report in each update sent to backend. */
  private static final int MAX_FAILURES_TO_REPORT_IN_UPDATE = 1000;

  private static final long THREAD_EXPIRATION_TIME_SEC = 60;
  private static final Duration COMMIT_STREAM_TIMEOUT = Duration.standardMinutes(1);
  private static final Duration GET_DATA_STREAM_TIMEOUT = Duration.standardSeconds(30);
  private static final int DEFAULT_STATUS_PORT = 8081;
  private static final Random CLIENT_ID_GENERATOR = new Random();
  private static final String CHANNELZ_PATH = "/channelz";
  private static final String BEAM_FN_API_EXPERIMENT = "beam_fn_api";
  private static final String ENABLE_IPV6_EXPERIMENT = "enable_private_ipv6_google_access";
  private static final String STREAMING_ENGINE_USE_JOB_SETTINGS_FOR_HEARTBEAT_POOL_EXPERIMENT =
      "streaming_engine_use_job_settings_for_heartbeat_pool";

  private final WindmillStateCache stateCache;
  private final StreamingWorkerStatusPages statusPages;
  private final ComputationConfig.Fetcher configFetcher;
  private final ComputationStateCache computationStateCache;
  private final BoundedQueueExecutor workUnitExecutor;
  private final StreamingWorkerHarness streamingWorkerHarness;
  private final AtomicBoolean running = new AtomicBoolean();
  private final DataflowWorkerHarnessOptions options;
  private final BackgroundMemoryMonitor memoryMonitor;
  private final ReaderCache readerCache;
  private final DataflowExecutionStateSampler sampler = DataflowExecutionStateSampler.instance();
  private final ActiveWorkRefresher activeWorkRefresher;
  private final StreamingWorkerStatusReporter workerStatusReporter;
  private final int numCommitThreads;

  private StreamingDataflowWorker(
      WindmillServerStub windmillServer,
      long clientId,
      ComputationConfig.Fetcher configFetcher,
      ComputationStateCache computationStateCache,
      WindmillStateCache windmillStateCache,
      BoundedQueueExecutor workUnitExecutor,
      DataflowMapTaskExecutorFactory mapTaskExecutorFactory,
      DataflowWorkerHarnessOptions options,
      HotKeyLogger hotKeyLogger,
      Supplier<Instant> clock,
      StreamingWorkerStatusReporterFactory streamingWorkerStatusReporterFactory,
      FailureTracker failureTracker,
      WorkFailureProcessor workFailureProcessor,
      StreamingCounters streamingCounters,
      MemoryMonitor memoryMonitor,
      GrpcWindmillStreamFactory windmillStreamFactory,
      ScheduledExecutorService activeWorkRefreshExecutorFn,
      ConcurrentMap<String, StageInfo> stageInfoMap,
      @Nullable GrpcDispatcherClient dispatcherClient) {
    // Register standard file systems.
    FileSystems.setDefaultPipelineOptions(options);
    this.configFetcher = configFetcher;
    this.computationStateCache = computationStateCache;
    this.stateCache = windmillStateCache;
    this.readerCache =
        new ReaderCache(
            Duration.standardSeconds(options.getReaderCacheTimeoutSec()),
            Executors.newCachedThreadPool());
    this.options = options;
    this.workUnitExecutor = workUnitExecutor;
    this.memoryMonitor = BackgroundMemoryMonitor.create(memoryMonitor);
    this.numCommitThreads =
        options.isEnableStreamingEngine()
            ? Math.max(options.getWindmillServiceCommitThreads(), 1)
            : 1;

    StreamingWorkScheduler streamingWorkScheduler =
        StreamingWorkScheduler.create(
            options,
            clock,
            readerCache,
            mapTaskExecutorFactory,
            workUnitExecutor,
            stateCache::forComputation,
            failureTracker,
            workFailureProcessor,
            streamingCounters,
            hotKeyLogger,
            sampler,
            ID_GENERATOR,
            configFetcher.getGlobalConfigHandle(),
            stageInfoMap);
    ThrottlingGetDataMetricTracker getDataMetricTracker =
        new ThrottlingGetDataMetricTracker(memoryMonitor);
    // Status page members. Different implementations on whether the harness is streaming engine
    // direct path, streaming engine cloud path, or streaming appliance.
    @Nullable ChannelzServlet channelzServlet = null;
    Consumer<PrintWriter> getDataStatusProvider;
    Supplier<Long> currentActiveCommitBytesProvider;
    if (isDirectPathPipeline(options)) {
      WeightedSemaphore<Commit> maxCommitByteSemaphore = Commits.maxCommitByteSemaphore();
      FanOutStreamingEngineWorkerHarness fanOutStreamingEngineWorkerHarness =
          FanOutStreamingEngineWorkerHarness.create(
              createJobHeader(options, clientId),
              GetWorkBudget.builder()
                  .setItems(chooseMaxBundlesOutstanding(options))
                  .setBytes(MAX_GET_WORK_FETCH_BYTES)
                  .build(),
              windmillStreamFactory,
              (workItem, watermarks, processingContext, getWorkStreamLatencies) ->
                  computationStateCache
                      .get(processingContext.computationId())
                      .ifPresent(
                          computationState -> {
                            memoryMonitor.waitForResources("GetWork");
                            streamingWorkScheduler.scheduleWork(
                                computationState,
                                workItem,
                                watermarks,
                                processingContext,
                                getWorkStreamLatencies);
                          }),
              createFanOutStubFactory(options),
              GetWorkBudgetDistributors.distributeEvenly(),
              Preconditions.checkNotNull(dispatcherClient),
              commitWorkStream ->
                  StreamingEngineWorkCommitter.builder()
                      // Share the commitByteSemaphore across all created workCommitters.
                      .setCommitByteSemaphore(maxCommitByteSemaphore)
                      .setBackendWorkerToken(commitWorkStream.backendWorkerToken())
                      .setOnCommitComplete(this::onCompleteCommit)
                      .setNumCommitSenders(Math.max(options.getWindmillServiceCommitThreads(), 1))
                      .setCommitWorkStreamFactory(
                          () -> CloseableStream.create(commitWorkStream, () -> {}))
                      .build(),
              getDataMetricTracker);
      getDataStatusProvider = getDataMetricTracker::printHtml;
      currentActiveCommitBytesProvider =
          fanOutStreamingEngineWorkerHarness::currentActiveCommitBytes;
      channelzServlet =
          createChannelzServlet(
              options, fanOutStreamingEngineWorkerHarness::currentWindmillEndpoints);
      this.streamingWorkerHarness = fanOutStreamingEngineWorkerHarness;
    } else {
      // Non-direct path pipelines.
      Windmill.GetWorkRequest request =
          Windmill.GetWorkRequest.newBuilder()
              .setClientId(clientId)
              .setMaxItems(chooseMaxBundlesOutstanding(options))
              .setMaxBytes(MAX_GET_WORK_FETCH_BYTES)
              .build();
      GetDataClient getDataClient;
      HeartbeatSender heartbeatSender;
      WorkCommitter workCommitter;
      GetWorkSender getWorkSender;
      if (options.isEnableStreamingEngine()) {
        WindmillStreamPool<GetDataStream> getDataStreamPool =
            WindmillStreamPool.create(
                Math.max(1, options.getWindmillGetDataStreamCount()),
                GET_DATA_STREAM_TIMEOUT,
                windmillServer::getDataStream);
        getDataClient = new StreamPoolGetDataClient(getDataMetricTracker, getDataStreamPool);
        heartbeatSender =
            createStreamingEngineHeartbeatSender(
                options, windmillServer, getDataStreamPool, configFetcher.getGlobalConfigHandle());
        channelzServlet =
            createChannelzServlet(options, windmillServer::getWindmillServiceEndpoints);
        workCommitter =
            StreamingEngineWorkCommitter.builder()
                .setCommitWorkStreamFactory(
                    WindmillStreamPool.create(
                            numCommitThreads,
                            COMMIT_STREAM_TIMEOUT,
                            windmillServer::commitWorkStream)
                        ::getCloseableStream)
                .setCommitByteSemaphore(Commits.maxCommitByteSemaphore())
                .setNumCommitSenders(numCommitThreads)
                .setOnCommitComplete(this::onCompleteCommit)
                .build();
        getWorkSender =
            GetWorkSender.forStreamingEngine(
                receiver -> windmillServer.getWorkStream(request, receiver));
      } else {
        getDataClient = new ApplianceGetDataClient(windmillServer, getDataMetricTracker);
        heartbeatSender = new ApplianceHeartbeatSender(windmillServer::getData);
        workCommitter =
            StreamingApplianceWorkCommitter.create(
                windmillServer::commitWork, this::onCompleteCommit);
        getWorkSender = GetWorkSender.forAppliance(() -> windmillServer.getWork(request));
      }

      getDataStatusProvider = getDataClient::printHtml;
      currentActiveCommitBytesProvider = workCommitter::currentActiveCommitBytes;

      this.streamingWorkerHarness =
          SingleSourceWorkerHarness.builder()
              .setStreamingWorkScheduler(streamingWorkScheduler)
              .setWorkCommitter(workCommitter)
              .setGetDataClient(getDataClient)
              .setComputationStateFetcher(this.computationStateCache::get)
              .setWaitForResources(() -> memoryMonitor.waitForResources("GetWork"))
              .setHeartbeatSender(heartbeatSender)
              .setThrottledTimeTracker(windmillServer::getAndResetThrottleTime)
              .setGetWorkSender(getWorkSender)
              .build();
    }

    this.workerStatusReporter =
        streamingWorkerStatusReporterFactory.createStatusReporter(streamingWorkerHarness);
    this.activeWorkRefresher =
        new ActiveWorkRefresher(
            clock,
            options.getActiveWorkRefreshPeriodMillis(),
            options.isEnableStreamingEngine()
                ? Math.max(options.getStuckCommitDurationMillis(), 0)
                : 0,
            computationStateCache::getAllPresentComputations,
            sampler,
            activeWorkRefreshExecutorFn,
            getDataMetricTracker::trackHeartbeats);

    this.statusPages =
        createStatusPageBuilder(options, windmillStreamFactory, memoryMonitor)
            .setClock(clock)
            .setClientId(clientId)
            .setIsRunning(running)
            .setStateCache(stateCache)
            .setComputationStateCache(this.computationStateCache)
            .setWorkUnitExecutor(workUnitExecutor)
            .setGlobalConfigHandle(configFetcher.getGlobalConfigHandle())
            .setChannelzServlet(channelzServlet)
            .setGetDataStatusProvider(getDataStatusProvider)
            .setCurrentActiveCommitBytes(currentActiveCommitBytesProvider)
            .build();

    LOG.debug("isDirectPathEnabled: {}", options.getIsWindmillServiceDirectPathEnabled());
    LOG.debug("windmillServiceEnabled: {}", options.isEnableStreamingEngine());
    LOG.debug("WindmillServiceEndpoint: {}", options.getWindmillServiceEndpoint());
    LOG.debug("WindmillServicePort: {}", options.getWindmillServicePort());
    LOG.debug("LocalWindmillHostport: {}", options.getLocalWindmillHostport());
  }

  private static StreamingWorkerStatusPages.Builder createStatusPageBuilder(
      DataflowWorkerHarnessOptions options,
      GrpcWindmillStreamFactory windmillStreamFactory,
      MemoryMonitor memoryMonitor) {
    WorkerStatusPages workerStatusPages =
        WorkerStatusPages.create(DEFAULT_STATUS_PORT, memoryMonitor);

    StreamingWorkerStatusPages.Builder streamingStatusPages =
        StreamingWorkerStatusPages.builder().setStatusPages(workerStatusPages);

    return options.isEnableStreamingEngine()
        ? streamingStatusPages
            .setDebugCapture(
                new DebugCapture.Manager(options, workerStatusPages.getDebugCapturePages()))
            .setWindmillStreamFactory(windmillStreamFactory)
        : streamingStatusPages;
  }

  private static ChannelzServlet createChannelzServlet(
      DataflowWorkerHarnessOptions options,
      Supplier<ImmutableSet<HostAndPort>> windmillEndpointProvider) {
    return new ChannelzServlet(CHANNELZ_PATH, options, windmillEndpointProvider);
  }

  private static HeartbeatSender createStreamingEngineHeartbeatSender(
      DataflowWorkerHarnessOptions options,
      WindmillServerStub windmillClient,
      WindmillStreamPool<GetDataStream> getDataStreamPool,
      StreamingGlobalConfigHandle globalConfigHandle) {
    // Experiment gates the logic till backend changes are rollback safe
    if (!DataflowRunner.hasExperiment(
            options, STREAMING_ENGINE_USE_JOB_SETTINGS_FOR_HEARTBEAT_POOL_EXPERIMENT)
        || options.getUseSeparateWindmillHeartbeatStreams() != null) {
      return StreamPoolHeartbeatSender.create(
          Boolean.TRUE.equals(options.getUseSeparateWindmillHeartbeatStreams())
              ? WindmillStreamPool.create(1, GET_DATA_STREAM_TIMEOUT, windmillClient::getDataStream)
              : getDataStreamPool);

    } else {
      return StreamPoolHeartbeatSender.create(
          WindmillStreamPool.create(1, GET_DATA_STREAM_TIMEOUT, windmillClient::getDataStream),
          getDataStreamPool,
          globalConfigHandle);
    }
  }

  public static StreamingDataflowWorker fromOptions(DataflowWorkerHarnessOptions options) {
    long clientId = CLIENT_ID_GENERATOR.nextLong();
    MemoryMonitor memoryMonitor = MemoryMonitor.fromOptions(options);
    ConcurrentMap<String, StageInfo> stageInfo = new ConcurrentHashMap<>();
    StreamingCounters streamingCounters = StreamingCounters.create();
    WorkUnitClient dataflowServiceClient = new DataflowWorkUnitClient(options, LOG);
    BoundedQueueExecutor workExecutor = createWorkUnitExecutor(options);
    WindmillStateCache windmillStateCache =
        WindmillStateCache.builder()
            .setSizeMb(options.getWorkerCacheMb())
            .setSupportMapViaMultimap(options.isEnableStreamingEngine())
            .build();

    GrpcWindmillStreamFactory.Builder windmillStreamFactoryBuilder =
        createGrpcwindmillStreamFactoryBuilder(options, clientId);

    ConfigFetcherComputationStateCacheAndWindmillClient
        configFetcherComputationStateCacheAndWindmillClient =
            createConfigFetcherComputationStateCacheAndWindmillClient(
                options,
                dataflowServiceClient,
                windmillStreamFactoryBuilder,
                configFetcher ->
                    ComputationStateCache.create(
                        configFetcher,
                        workExecutor,
                        windmillStateCache::forComputation,
                        ID_GENERATOR));

    ComputationStateCache computationStateCache =
        configFetcherComputationStateCacheAndWindmillClient.computationStateCache();
    WindmillServerStub windmillServer =
        configFetcherComputationStateCacheAndWindmillClient.windmillServer();

    FailureTracker failureTracker =
        options.isEnableStreamingEngine()
            ? StreamingEngineFailureTracker.create(
                MAX_FAILURES_TO_REPORT_IN_UPDATE, options.getMaxStackTraceDepthToReport())
            : StreamingApplianceFailureTracker.create(
                MAX_FAILURES_TO_REPORT_IN_UPDATE,
                options.getMaxStackTraceDepthToReport(),
                windmillServer::reportStats);

    Supplier<Instant> clock = Instant::now;
    WorkFailureProcessor workFailureProcessor =
        WorkFailureProcessor.create(
            workExecutor,
            failureTracker,
            () -> Optional.ofNullable(memoryMonitor.tryToDumpHeap()),
            clock);
    StreamingWorkerStatusReporterFactory workerStatusReporterFactory =
        throttleTimeSupplier ->
            StreamingWorkerStatusReporter.builder()
                .setDataflowServiceClient(dataflowServiceClient)
                .setWindmillQuotaThrottleTime(throttleTimeSupplier)
                .setAllStageInfo(stageInfo::values)
                .setFailureTracker(failureTracker)
                .setStreamingCounters(streamingCounters)
                .setMemoryMonitor(memoryMonitor)
                .setWorkExecutor(workExecutor)
                .setWindmillHarnessUpdateReportingPeriodMillis(
                    options.getWindmillHarnessUpdateReportingPeriod().getMillis())
                .setPerWorkerMetricsUpdateReportingPeriodMillis(
                    options.getPerWorkerMetricsUpdateReportingPeriodMillis())
                .build();

    return new StreamingDataflowWorker(
        windmillServer,
        clientId,
        configFetcherComputationStateCacheAndWindmillClient.configFetcher(),
        computationStateCache,
        windmillStateCache,
        workExecutor,
        IntrinsicMapTaskExecutorFactory.defaultFactory(),
        options,
        new HotKeyLogger(),
        clock,
        workerStatusReporterFactory,
        failureTracker,
        workFailureProcessor,
        streamingCounters,
        memoryMonitor,
        configFetcherComputationStateCacheAndWindmillClient.windmillStreamFactory(),
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("RefreshWork").build()),
        stageInfo,
        configFetcherComputationStateCacheAndWindmillClient.windmillDispatcherClient());
  }

  /**
   * {@link ComputationConfig.Fetcher}, {@link ComputationStateCache}, and {@link
   * WindmillServerStub} are constructed in different orders due to cyclic dependencies depending on
   * the underlying implementation. This method simplifies creating them and returns an object with
   * all of these dependencies initialized.
   */
  private static ConfigFetcherComputationStateCacheAndWindmillClient
      createConfigFetcherComputationStateCacheAndWindmillClient(
          DataflowWorkerHarnessOptions options,
          WorkUnitClient dataflowServiceClient,
          GrpcWindmillStreamFactory.Builder windmillStreamFactoryBuilder,
          Function<ComputationConfig.Fetcher, ComputationStateCache> computationStateCacheFactory) {
    if (options.isEnableStreamingEngine()) {
      GrpcDispatcherClient dispatcherClient =
          GrpcDispatcherClient.create(options, new WindmillStubFactoryFactoryImpl(options));
      ComputationConfig.Fetcher configFetcher =
          StreamingEngineComputationConfigFetcher.create(
              options.getGlobalConfigRefreshPeriod().getMillis(), dataflowServiceClient);
      configFetcher.getGlobalConfigHandle().registerConfigObserver(dispatcherClient::onJobConfig);
      ComputationStateCache computationStateCache =
          computationStateCacheFactory.apply(configFetcher);
      GrpcWindmillStreamFactory windmillStreamFactory =
          windmillStreamFactoryBuilder
              .setProcessHeartbeatResponses(
                  new WorkHeartbeatResponseProcessor(computationStateCache::get))
              .setHealthCheckIntervalMillis(
                  options.getWindmillServiceStreamingRpcHealthCheckPeriodMs())
              .build();
      return ConfigFetcherComputationStateCacheAndWindmillClient.builder()
          .setWindmillDispatcherClient(dispatcherClient)
          .setConfigFetcher(configFetcher)
          .setComputationStateCache(computationStateCache)
          .setWindmillStreamFactory(windmillStreamFactory)
          .setWindmillServer(
              GrpcWindmillServer.create(options, windmillStreamFactory, dispatcherClient))
          .build();
    }

    // Build with local Windmill client.
    if (options.getWindmillServiceEndpoint() != null
        || options.getLocalWindmillHostport().startsWith("grpc:")) {
      GrpcDispatcherClient dispatcherClient =
          GrpcDispatcherClient.create(options, new WindmillStubFactoryFactoryImpl(options));
      GrpcWindmillStreamFactory windmillStreamFactory =
          windmillStreamFactoryBuilder
              .setHealthCheckIntervalMillis(
                  options.getWindmillServiceStreamingRpcHealthCheckPeriodMs())
              .build();
      GrpcWindmillServer windmillServer =
          GrpcWindmillServer.create(options, windmillStreamFactory, dispatcherClient);
      ComputationConfig.Fetcher configFetcher =
          createApplianceComputationConfigFetcher(windmillServer);
      return ConfigFetcherComputationStateCacheAndWindmillClient.builder()
          .setWindmillDispatcherClient(dispatcherClient)
          .setWindmillServer(windmillServer)
          .setWindmillStreamFactory(windmillStreamFactory)
          .setConfigFetcher(configFetcher)
          .setComputationStateCache(computationStateCacheFactory.apply(configFetcher))
          .build();
    }

    WindmillServerStub windmillServer =
        new JniWindmillApplianceServer(options.getLocalWindmillHostport());
    ComputationConfig.Fetcher configFetcher =
        createApplianceComputationConfigFetcher(windmillServer);
    return ConfigFetcherComputationStateCacheAndWindmillClient.builder()
        .setWindmillStreamFactory(windmillStreamFactoryBuilder.build())
        .setWindmillServer(windmillServer)
        .setConfigFetcher(configFetcher)
        .setComputationStateCache(computationStateCacheFactory.apply(configFetcher))
        .build();
  }

  private static StreamingApplianceComputationConfigFetcher createApplianceComputationConfigFetcher(
      ApplianceWindmillClient windmillClient) {
    return new StreamingApplianceComputationConfigFetcher(
        windmillClient::getConfig,
        new FixedGlobalConfigHandle(StreamingGlobalConfig.builder().build()));
  }

  private static boolean isDirectPathPipeline(DataflowWorkerHarnessOptions options) {
    if (options.isEnableStreamingEngine() && options.getIsWindmillServiceDirectPathEnabled()) {
      boolean isIpV6Enabled =
          Optional.ofNullable(options.getDataflowServiceOptions())
              .map(serviceOptions -> serviceOptions.contains(ENABLE_IPV6_EXPERIMENT))
              .orElse(false);

      if (isIpV6Enabled) {
        return true;
      }

      LOG.warn(
          "DirectPath is currently only supported with IPv6 networking stack. This requires setting "
              + "\"enable_private_ipv6_google_access\" in experimental pipeline options. "
              + "For information on how to set experimental pipeline options see "
              + "https://cloud.google.com/dataflow/docs/guides/setting-pipeline-options#experimental. "
              + "Defaulting to CloudPath.");
    }

    return false;
  }

  private static void validateWorkerOptions(DataflowWorkerHarnessOptions options) {
    Preconditions.checkArgument(
        options.isStreaming(),
        "%s instantiated with options indicating batch use",
        StreamingDataflowWorker.class.getName());

    Preconditions.checkArgument(
        !DataflowRunner.hasExperiment(options, BEAM_FN_API_EXPERIMENT),
        "%s cannot be main() class with beam_fn_api enabled",
        StreamingDataflowWorker.class.getSimpleName());
  }

  private static ChannelCachingStubFactory createFanOutStubFactory(
      DataflowWorkerHarnessOptions workerOptions) {
    return ChannelCachingRemoteStubFactory.create(
        workerOptions.getGcpCredential(),
        ChannelCache.create(
            serviceAddress ->
                // IsolationChannel will create and manage separate RPC channels to the same
                // serviceAddress.
                IsolationChannel.create(
                    () ->
                        remoteChannel(
                            serviceAddress,
                            workerOptions.getWindmillServiceRpcChannelAliveTimeoutSec()))));
  }

  @VisibleForTesting
  static StreamingDataflowWorker forTesting(
      Map<String, String> prePopulatedStateNameMappings,
      WindmillServerStub windmillServer,
      List<MapTask> mapTasks,
      DataflowMapTaskExecutorFactory mapTaskExecutorFactory,
      WorkUnitClient workUnitClient,
      DataflowWorkerHarnessOptions options,
      boolean publishCounters,
      HotKeyLogger hotKeyLogger,
      Supplier<Instant> clock,
      Function<String, ScheduledExecutorService> executorSupplier,
      StreamingGlobalConfigHandleImpl globalConfigHandle,
      int localRetryTimeoutMs,
      StreamingCounters streamingCounters,
      WindmillStubFactoryFactory stubFactory) {
    ConcurrentMap<String, StageInfo> stageInfo = new ConcurrentHashMap<>();
    BoundedQueueExecutor workExecutor = createWorkUnitExecutor(options);
    WindmillStateCache stateCache =
        WindmillStateCache.builder()
            .setSizeMb(options.getWorkerCacheMb())
            .setSupportMapViaMultimap(options.isEnableStreamingEngine())
            .build();
    ComputationConfig.Fetcher configFetcher =
        options.isEnableStreamingEngine()
            ? StreamingEngineComputationConfigFetcher.forTesting(
                /* hasReceivedGlobalConfig= */ true,
                options.getGlobalConfigRefreshPeriod().getMillis(),
                workUnitClient,
                globalConfigHandle,
                executorSupplier)
            : new StreamingApplianceComputationConfigFetcher(
                windmillServer::getConfig, globalConfigHandle);
    configFetcher
        .getGlobalConfigHandle()
        .registerConfigObserver(
            config -> {
              if (config.windmillServiceEndpoints().isEmpty()) {
                LOG.warn("Received empty windmill service endpoints");
                return;
              }
              windmillServer.setWindmillServiceEndpoints(config.windmillServiceEndpoints());
            });
    ConcurrentMap<String, String> stateNameMap =
        new ConcurrentHashMap<>(prePopulatedStateNameMappings);
    ComputationStateCache computationStateCache =
        ComputationStateCache.forTesting(
            configFetcher, workExecutor, stateCache::forComputation, ID_GENERATOR, stateNameMap);
    computationStateCache.loadCacheForTesting(
        mapTasks,
        mapTask ->
            new ComputationState(
                mapTask.getStageName(),
                mapTask,
                workExecutor,
                stateNameMap,
                stateCache.forComputation(mapTask.getStageName())));
    MemoryMonitor memoryMonitor = MemoryMonitor.fromOptions(options);
    FailureTracker failureTracker =
        options.isEnableStreamingEngine()
            ? StreamingEngineFailureTracker.create(
                MAX_FAILURES_TO_REPORT_IN_UPDATE, options.getMaxStackTraceDepthToReport())
            : StreamingApplianceFailureTracker.create(
                MAX_FAILURES_TO_REPORT_IN_UPDATE,
                options.getMaxStackTraceDepthToReport(),
                windmillServer::reportStats);
    WorkFailureProcessor workFailureProcessor =
        WorkFailureProcessor.forTesting(
            workExecutor,
            failureTracker,
            () -> Optional.ofNullable(memoryMonitor.tryToDumpHeap()),
            clock,
            localRetryTimeoutMs);
    StreamingWorkerStatusReporterFactory workerStatusReporterFactory =
        throttleTimeSupplier ->
            StreamingWorkerStatusReporter.builder()
                .setPublishCounters(publishCounters)
                .setDataflowServiceClient(workUnitClient)
                .setWindmillQuotaThrottleTime(throttleTimeSupplier)
                .setAllStageInfo(stageInfo::values)
                .setFailureTracker(failureTracker)
                .setStreamingCounters(streamingCounters)
                .setMemoryMonitor(memoryMonitor)
                .setWorkExecutor(workExecutor)
                .setExecutorFactory(executorSupplier)
                .setWindmillHarnessUpdateReportingPeriodMillis(
                    options.getWindmillHarnessUpdateReportingPeriod().getMillis())
                .setPerWorkerMetricsUpdateReportingPeriodMillis(
                    options.getPerWorkerMetricsUpdateReportingPeriodMillis())
                .build();

    GrpcWindmillStreamFactory.Builder windmillStreamFactory =
        createGrpcwindmillStreamFactoryBuilder(options, 1)
            .setProcessHeartbeatResponses(
                new WorkHeartbeatResponseProcessor(computationStateCache::get));

    return new StreamingDataflowWorker(
        windmillServer,
        1L,
        configFetcher,
        computationStateCache,
        stateCache,
        workExecutor,
        mapTaskExecutorFactory,
        options,
        hotKeyLogger,
        clock,
        workerStatusReporterFactory,
        failureTracker,
        workFailureProcessor,
        streamingCounters,
        memoryMonitor,
        options.isEnableStreamingEngine()
            ? windmillStreamFactory
                .setHealthCheckIntervalMillis(
                    options.getWindmillServiceStreamingRpcHealthCheckPeriodMs())
                .build()
            : windmillStreamFactory.build(),
        executorSupplier.apply("RefreshWork"),
        stageInfo,
        GrpcDispatcherClient.create(options, stubFactory));
  }

  private static GrpcWindmillStreamFactory.Builder createGrpcwindmillStreamFactoryBuilder(
      DataflowWorkerHarnessOptions options, long clientId) {
    Duration maxBackoff =
        !options.isEnableStreamingEngine() && options.getLocalWindmillHostport() != null
            ? GrpcWindmillServer.LOCALHOST_MAX_BACKOFF
            : Duration.millis(options.getWindmillServiceStreamMaxBackoffMillis());
    return GrpcWindmillStreamFactory.of(createJobHeader(options, clientId))
        .setWindmillMessagesBetweenIsReadyChecks(options.getWindmillMessagesBetweenIsReadyChecks())
        .setMaxBackOffSupplier(() -> maxBackoff)
        .setLogEveryNStreamFailures(options.getWindmillServiceStreamingLogEveryNStreamFailures())
        .setStreamingRpcBatchLimit(options.getWindmillServiceStreamingRpcBatchLimit())
        .setSendKeyedGetDataRequests(
            !options.isEnableStreamingEngine()
                || DataflowRunner.hasExperiment(
                    options, "streaming_engine_disable_new_heartbeat_requests"));
  }

  private static JobHeader createJobHeader(DataflowWorkerHarnessOptions options, long clientId) {
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

  public static void main(String[] args) throws Exception {
    JvmInitializers.runOnStartup();

    DataflowWorkerHarnessHelper.initializeLogging(StreamingDataflowWorker.class);
    DataflowWorkerHarnessOptions options =
        DataflowWorkerHarnessHelper.initializeGlobalStateAndPipelineOptions(
            StreamingDataflowWorker.class, DataflowWorkerHarnessOptions.class);
    DataflowWorkerHarnessHelper.configureLogging(options);
    validateWorkerOptions(options);

    CoderTranslation.verifyModelCodersRegistered();

    LOG.debug("Creating StreamingDataflowWorker from options: {}", options);
    StreamingDataflowWorker worker = StreamingDataflowWorker.fromOptions(options);

    // Use the MetricsLogger container which is used by BigQueryIO to periodically log process-wide
    // metrics.
    MetricsEnvironment.setProcessWideContainer(new MetricsLogger(null));

    if (options.isEnableStreamingEngine()
        && !DataflowRunner.hasExperiment(options, "disable_per_worker_metrics")) {
      enableBigQueryMetrics();
    }

    JvmInitializers.runBeforeProcessing(options);
    worker.startStatusPages();
    worker.start();
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

  private static void enableBigQueryMetrics() {
    // When enabled, the Pipeline will record Per-Worker metrics that will be piped to DFE.
    StreamingStepMetricsContainer.setEnablePerWorkerMetrics(true);
    // StreamingStepMetricsContainer automatically deletes perWorkerCounters if they are zero-valued
    // for longer than 5 minutes.
    BigQuerySinkMetrics.setSupportMetricsDeletion(true);
    // Support metrics for BigQuery's Streaming Inserts write method.
    BigQuerySinkMetrics.setSupportStreamingInsertsMetrics(true);
  }

  @VisibleForTesting
  void reportPeriodicWorkerUpdatesForTest() {
    workerStatusReporter.reportPeriodicWorkerUpdates();
  }

  @VisibleForTesting
  public boolean workExecutorIsEmpty() {
    return workUnitExecutor.executorQueueIsEmpty();
  }

  @VisibleForTesting
  int numCommitThreads() {
    return numCommitThreads;
  }

  @VisibleForTesting
  CacheStats getStateCacheStats() {
    return stateCache.getCacheStats();
  }

  @VisibleForTesting
  ComputationStateCache getComputationStateCache() {
    return computationStateCache;
  }

  public void start() {
    running.set(true);
    configFetcher.start();
    memoryMonitor.start();
    streamingWorkerHarness.start();
    sampler.start();
    workerStatusReporter.start();
    activeWorkRefresher.start();
  }

  /** Starts the status page server for debugging. May be omitted for lighter weight testing. */
  private void startStatusPages() {
    statusPages.start(options);
  }

  @VisibleForTesting
  void stop() {
    try {
      configFetcher.stop();
      activeWorkRefresher.stop();
      statusPages.stop();
      running.set(false);
      streamingWorkerHarness.shutdown();
      memoryMonitor.shutdown();
      workUnitExecutor.shutdown();
      computationStateCache.closeAndInvalidateAll();
      workerStatusReporter.stop();
    } catch (Exception e) {
      LOG.warn("Exception while shutting down: ", e);
    }
  }

  private void onCompleteCommit(CompleteCommit completeCommit) {
    if (completeCommit.status() != Windmill.CommitStatus.OK) {
      readerCache.invalidateReader(
          WindmillComputationKey.create(
              completeCommit.computationId(), completeCommit.shardedKey()));
      stateCache
          .forComputation(completeCommit.computationId())
          .invalidate(completeCommit.shardedKey());
    }

    computationStateCache
        .getIfPresent(completeCommit.computationId())
        .ifPresent(
            state ->
                state.completeWorkAndScheduleNextWorkForKey(
                    completeCommit.shardedKey(), completeCommit.workId()));
  }

  @FunctionalInterface
  private interface StreamingWorkerStatusReporterFactory {
    StreamingWorkerStatusReporter createStatusReporter(ThrottledTimeTracker throttledTimeTracker);
  }

  @AutoValue
  abstract static class ConfigFetcherComputationStateCacheAndWindmillClient {

    private static Builder builder() {
      return new AutoValue_StreamingDataflowWorker_ConfigFetcherComputationStateCacheAndWindmillClient
          .Builder();
    }

    abstract ComputationConfig.Fetcher configFetcher();

    abstract ComputationStateCache computationStateCache();

    abstract WindmillServerStub windmillServer();

    abstract GrpcWindmillStreamFactory windmillStreamFactory();

    abstract @Nullable GrpcDispatcherClient windmillDispatcherClient();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConfigFetcher(ComputationConfig.Fetcher value);

      abstract Builder setComputationStateCache(ComputationStateCache value);

      abstract Builder setWindmillServer(WindmillServerStub value);

      abstract Builder setWindmillStreamFactory(GrpcWindmillStreamFactory value);

      abstract Builder setWindmillDispatcherClient(GrpcDispatcherClient value);

      abstract ConfigFetcherComputationStateCacheAndWindmillClient build();
    }
  }

  /**
   * Monitors memory pressure on a background executor. May be used to throttle calls, blocking if
   * there is memory pressure.
   */
  @AutoValue
  abstract static class BackgroundMemoryMonitor {

    private static BackgroundMemoryMonitor create(MemoryMonitor memoryMonitor) {
      return new AutoValue_StreamingDataflowWorker_BackgroundMemoryMonitor(
          memoryMonitor,
          Executors.newSingleThreadScheduledExecutor(
              new ThreadFactoryBuilder()
                  .setNameFormat("MemoryMonitor")
                  .setPriority(Thread.MIN_PRIORITY)
                  .build()));
    }

    abstract MemoryMonitor memoryMonitor();

    abstract ExecutorService executor();

    private void start() {
      executor().execute(memoryMonitor());
    }

    private void shutdown() {
      memoryMonitor().stop();
      executor().shutdown();
    }
  }
}
