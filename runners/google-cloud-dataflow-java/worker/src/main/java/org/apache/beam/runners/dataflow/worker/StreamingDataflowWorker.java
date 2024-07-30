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
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.MapTask;
import com.google.auto.value.AutoValue;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.runners.core.metrics.MetricsLogger;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor;
import org.apache.beam.runners.dataflow.worker.status.DebugCapture;
import org.apache.beam.runners.dataflow.worker.status.WorkerStatusPages;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationStateCache;
import org.apache.beam.runners.dataflow.worker.streaming.StageInfo;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.streaming.WorkHeartbeatResponseProcessor;
import org.apache.beam.runners.dataflow.worker.streaming.config.ComputationConfig;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingApplianceComputationConfigFetcher;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingEngineComputationConfigFetcher;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingEnginePipelineConfig;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingCounters;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingWorkerStatusPages;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingWorkerStatusReporter;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.LatencyAttribution;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.runners.dataflow.worker.windmill.appliance.JniWindmillApplianceServer;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamPool;
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
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.StreamingWorkScheduler;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.FailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.StreamingApplianceFailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.StreamingEngineFailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.WorkFailureProcessor;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.ActiveWorkRefresher;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.ApplianceHeartbeatSender;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.StreamPoolHeartbeatSender;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.fn.JvmInitializers;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.bigquery.BigQuerySinkMetrics;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.util.construction.CoderTranslation;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implements a Streaming Dataflow worker. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class StreamingDataflowWorker {
  /**
   * Sinks are marked 'full' in {@link StreamingModeExecutionContext} once the amount of data sinked
   * (across all the sinks, if there are more than one) reaches this limit. This serves as hint for
   * readers to stop producing more. This can be disabled with 'disable_limiting_bundle_sink_bytes'
   * experiment.
   */
  public static final int MAX_SINK_BYTES = 10_000_000;

  // Maximum number of threads for processing.  Currently, each thread processes one key at a time.
  static final int MAX_PROCESSING_THREADS = 300;
  static final long THREAD_EXPIRATION_TIME_SEC = 60;
  static final int GET_WORK_STREAM_TIMEOUT_MINUTES = 3;
  static final Duration COMMIT_STREAM_TIMEOUT = Duration.standardMinutes(1);
  private static final Logger LOG = LoggerFactory.getLogger(StreamingDataflowWorker.class);
  private static final Duration GET_DATA_STREAM_TIMEOUT = Duration.standardSeconds(30);

  /** The idGenerator to generate unique id globally. */
  private static final IdGenerator ID_GENERATOR = IdGenerators.decrementingLongs();

  private static final int DEFAULT_STATUS_PORT = 8081;
  // Maximum size of the result of a GetWork request.
  private static final long MAX_GET_WORK_FETCH_BYTES = 64L << 20; // 64m

  /** Maximum number of failure stacktraces to report in each update sent to backend. */
  private static final int MAX_FAILURES_TO_REPORT_IN_UPDATE = 1000;

  private static final Random clientIdGenerator = new Random();
  private static final String CHANNELZ_PATH = "/channelz";
  final WindmillStateCache stateCache;
  private final StreamingWorkerStatusPages statusPages;
  private final ComputationConfig.Fetcher configFetcher;
  private final ComputationStateCache computationStateCache;
  private final BoundedQueueExecutor workUnitExecutor;
  private final WindmillServerStub windmillServer;
  private final Thread dispatchThread;
  private final AtomicBoolean running = new AtomicBoolean();
  private final DataflowWorkerHarnessOptions options;
  private final long clientId;
  private final GetDataClient getDataClient;
  private final MemoryMonitor memoryMonitor;
  private final Thread memoryMonitorThread;
  private final ReaderCache readerCache;
  private final DataflowExecutionStateSampler sampler = DataflowExecutionStateSampler.instance();
  private final ActiveWorkRefresher activeWorkRefresher;
  private final WorkCommitter workCommitter;
  private final StreamingWorkerStatusReporter workerStatusReporter;
  private final StreamingCounters streamingCounters;
  private final StreamingWorkScheduler streamingWorkScheduler;
  private final HeartbeatSender heartbeatSender;

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
      StreamingWorkerStatusReporter workerStatusReporter,
      FailureTracker failureTracker,
      WorkFailureProcessor workFailureProcessor,
      StreamingCounters streamingCounters,
      MemoryMonitor memoryMonitor,
      AtomicReference<OperationalLimits> operationalLimits,
      GrpcWindmillStreamFactory windmillStreamFactory,
      Function<String, ScheduledExecutorService> executorSupplier,
      ConcurrentMap<String, StageInfo> stageInfoMap) {
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

    boolean windmillServiceEnabled = options.isEnableStreamingEngine();

    int numCommitThreads = 1;
    if (windmillServiceEnabled && options.getWindmillServiceCommitThreads() > 0) {
      numCommitThreads = options.getWindmillServiceCommitThreads();
    }

    this.workCommitter =
        windmillServiceEnabled
            ? StreamingEngineWorkCommitter.builder()
                .setCommitWorkStreamFactory(
                    WindmillStreamPool.create(
                            numCommitThreads,
                            COMMIT_STREAM_TIMEOUT,
                            windmillServer::commitWorkStream)
                        ::getCloseableStream)
                .setNumCommitSenders(numCommitThreads)
                .setOnCommitComplete(this::onCompleteCommit)
                .build()
            : StreamingApplianceWorkCommitter.create(
                windmillServer::commitWork, this::onCompleteCommit);

    this.workUnitExecutor = workUnitExecutor;

    memoryMonitorThread = new Thread(memoryMonitor);
    memoryMonitorThread.setPriority(Thread.MIN_PRIORITY);
    memoryMonitorThread.setName("MemoryMonitor");

    dispatchThread =
        new Thread(
            () -> {
              LOG.info("Dispatch starting");
              if (windmillServiceEnabled) {
                streamingDispatchLoop();
              } else {
                dispatchLoop();
              }
              LOG.info("Dispatch done");
            });
    dispatchThread.setDaemon(true);
    dispatchThread.setPriority(Thread.MIN_PRIORITY);
    dispatchThread.setName("DispatchThread");
    this.clientId = clientId;
    this.windmillServer = windmillServer;

    ThrottlingGetDataMetricTracker getDataMetricTracker =
        new ThrottlingGetDataMetricTracker(memoryMonitor);

    int stuckCommitDurationMillis;
    if (windmillServiceEnabled) {
      WindmillStreamPool<GetDataStream> getDataStreamPool =
          WindmillStreamPool.create(
              Math.max(1, options.getWindmillGetDataStreamCount()),
              GET_DATA_STREAM_TIMEOUT,
              windmillServer::getDataStream);
      this.getDataClient = new StreamPoolGetDataClient(getDataMetricTracker, getDataStreamPool);
      this.heartbeatSender =
          new StreamPoolHeartbeatSender(
              options.getUseSeparateWindmillHeartbeatStreams()
                  ? WindmillStreamPool.create(
                      1, GET_DATA_STREAM_TIMEOUT, windmillServer::getDataStream)
                  : getDataStreamPool);
      stuckCommitDurationMillis =
          options.getStuckCommitDurationMillis() > 0 ? options.getStuckCommitDurationMillis() : 0;
    } else {
      this.getDataClient = new ApplianceGetDataClient(windmillServer, getDataMetricTracker);
      this.heartbeatSender = new ApplianceHeartbeatSender(windmillServer::getData);
      stuckCommitDurationMillis = 0;
    }

    this.activeWorkRefresher =
        new ActiveWorkRefresher(
            clock,
            options.getActiveWorkRefreshPeriodMillis(),
            stuckCommitDurationMillis,
            computationStateCache::getAllPresentComputations,
            sampler,
            executorSupplier.apply("RefreshWork"),
            getDataMetricTracker::trackHeartbeats);

    WorkerStatusPages workerStatusPages =
        WorkerStatusPages.create(DEFAULT_STATUS_PORT, memoryMonitor);
    StreamingWorkerStatusPages.Builder statusPagesBuilder =
        StreamingWorkerStatusPages.builder()
            .setClock(clock)
            .setClientId(clientId)
            .setIsRunning(running)
            .setStatusPages(workerStatusPages)
            .setStateCache(stateCache)
            .setComputationStateCache(computationStateCache)
            .setCurrentActiveCommitBytes(workCommitter::currentActiveCommitBytes)
            .setGetDataStatusProvider(getDataClient::printHtml)
            .setWorkUnitExecutor(workUnitExecutor);

    this.statusPages =
        windmillServiceEnabled
            ? statusPagesBuilder
                .setDebugCapture(
                    new DebugCapture.Manager(options, workerStatusPages.getDebugCapturePages()))
                .setChannelzServlet(new ChannelzServlet(CHANNELZ_PATH, options, windmillServer))
                .setWindmillStreamFactory(windmillStreamFactory)
                .build()
            : statusPagesBuilder.build();

    this.workerStatusReporter = workerStatusReporter;
    this.streamingCounters = streamingCounters;
    this.memoryMonitor = memoryMonitor;
    this.streamingWorkScheduler =
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
            operationalLimits,
            ID_GENERATOR,
            stageInfoMap);

    LOG.debug("windmillServiceEnabled: {}", windmillServiceEnabled);
    LOG.debug("WindmillServiceEndpoint: {}", options.getWindmillServiceEndpoint());
    LOG.debug("WindmillServicePort: {}", options.getWindmillServicePort());
    LOG.debug("LocalWindmillHostport: {}", options.getLocalWindmillHostport());
  }

  public static StreamingDataflowWorker fromOptions(DataflowWorkerHarnessOptions options) {
    long clientId = clientIdGenerator.nextLong();
    MemoryMonitor memoryMonitor = MemoryMonitor.fromOptions(options);
    ConcurrentMap<String, StageInfo> stageInfo = new ConcurrentHashMap<>();
    StreamingCounters streamingCounters = StreamingCounters.create();
    WorkUnitClient dataflowServiceClient = new DataflowWorkUnitClient(options, LOG);
    BoundedQueueExecutor workExecutor = createWorkUnitExecutor(options);
    AtomicReference<OperationalLimits> operationalLimits =
        new AtomicReference<>(OperationalLimits.builder().build());
    WindmillStateCache windmillStateCache =
        WindmillStateCache.builder()
            .setSizeMb(options.getWorkerCacheMb())
            .setSupportMapViaMultimap(options.isEnableStreamingEngine())
            .build();
    Function<String, ScheduledExecutorService> executorSupplier =
        threadName ->
            Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat(threadName).build());
    GrpcWindmillStreamFactory.Builder windmillStreamFactoryBuilder =
        createGrpcwindmillStreamFactoryBuilder(options, clientId);

    ConfigFetcherComputationStateCacheAndWindmillClient
        configFetcherComputationStateCacheAndWindmillClient =
            createConfigFetcherComputationStateCacheAndWindmillClient(
                options,
                dataflowServiceClient,
                operationalLimits,
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
        workerStatusReporter,
        failureTracker,
        workFailureProcessor,
        streamingCounters,
        memoryMonitor,
        operationalLimits,
        configFetcherComputationStateCacheAndWindmillClient.windmillStreamFactory(),
        executorSupplier,
        stageInfo);
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
          AtomicReference<OperationalLimits> operationalLimits,
          GrpcWindmillStreamFactory.Builder windmillStreamFactoryBuilder,
          Function<ComputationConfig.Fetcher, ComputationStateCache> computationStateCacheFactory) {
    ComputationConfig.Fetcher configFetcher;
    WindmillServerStub windmillServer;
    ComputationStateCache computationStateCache;
    GrpcDispatcherClient dispatcherClient = GrpcDispatcherClient.create(createStubFactory(options));
    GrpcWindmillStreamFactory windmillStreamFactory;
    if (options.isEnableStreamingEngine()) {
      configFetcher =
          StreamingEngineComputationConfigFetcher.create(
              options.getGlobalConfigRefreshPeriod().getMillis(),
              dataflowServiceClient,
              config ->
                  onPipelineConfig(
                      config,
                      options,
                      dispatcherClient::consumeWindmillDispatcherEndpoints,
                      operationalLimits::set));
      computationStateCache = computationStateCacheFactory.apply(configFetcher);
      windmillStreamFactory =
          windmillStreamFactoryBuilder
              .setProcessHeartbeatResponses(
                  new WorkHeartbeatResponseProcessor(computationStateCache::get))
              .setHealthCheckIntervalMillis(
                  options.getWindmillServiceStreamingRpcHealthCheckPeriodMs())
              .build();
      windmillServer = GrpcWindmillServer.create(options, windmillStreamFactory, dispatcherClient);
    } else {
      if (options.getWindmillServiceEndpoint() != null
          || options.getLocalWindmillHostport().startsWith("grpc:")) {
        windmillStreamFactory =
            windmillStreamFactoryBuilder
                .setHealthCheckIntervalMillis(
                    options.getWindmillServiceStreamingRpcHealthCheckPeriodMs())
                .build();
        windmillServer =
            GrpcWindmillServer.create(options, windmillStreamFactory, dispatcherClient);
      } else {
        windmillStreamFactory = windmillStreamFactoryBuilder.build();
        windmillServer = new JniWindmillApplianceServer(options.getLocalWindmillHostport());
      }

      configFetcher = new StreamingApplianceComputationConfigFetcher(windmillServer::getConfig);
      computationStateCache = computationStateCacheFactory.apply(configFetcher);
    }

    return ConfigFetcherComputationStateCacheAndWindmillClient.create(
        configFetcher, computationStateCache, windmillServer, windmillStreamFactory);
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
      int localRetryTimeoutMs,
      OperationalLimits limits) {
    ConcurrentMap<String, StageInfo> stageInfo = new ConcurrentHashMap<>();
    AtomicReference<OperationalLimits> operationalLimits = new AtomicReference<>(limits);
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
                executorSupplier,
                config ->
                    onPipelineConfig(
                        config,
                        options,
                        windmillServer::setWindmillServiceEndpoints,
                        operationalLimits::set))
            : new StreamingApplianceComputationConfigFetcher(windmillServer::getConfig);
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
    StreamingCounters streamingCounters = StreamingCounters.create();
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
        workerStatusReporter,
        failureTracker,
        workFailureProcessor,
        streamingCounters,
        memoryMonitor,
        operationalLimits,
        options.isEnableStreamingEngine()
            ? windmillStreamFactory
                .setHealthCheckIntervalMillis(
                    options.getWindmillServiceStreamingRpcHealthCheckPeriodMs())
                .build()
            : windmillStreamFactory.build(),
        executorSupplier,
        stageInfo);
  }

  private static void onPipelineConfig(
      StreamingEnginePipelineConfig config,
      DataflowWorkerHarnessOptions options,
      Consumer<ImmutableSet<HostAndPort>> consumeWindmillServiceEndpoints,
      Consumer<OperationalLimits> operationalLimits) {

    operationalLimits.accept(
        OperationalLimits.builder()
            .setMaxWorkItemCommitBytes(config.maxWorkItemCommitBytes())
            .setMaxOutputKeyBytes(config.maxOutputKeyBytes())
            .setMaxOutputValueBytes(config.maxOutputValueBytes())
            .setThrowExceptionOnLargeOutput(
                DataflowRunner.hasExperiment(options, "throw_exceptions_on_large_output"))
            .build());

    if (!config.windmillServiceEndpoints().isEmpty()) {
      consumeWindmillServiceEndpoints.accept(config.windmillServiceEndpoints());
    }
  }

  private static GrpcWindmillStreamFactory.Builder createGrpcwindmillStreamFactoryBuilder(
      DataflowWorkerHarnessOptions options, long clientId) {
    Duration maxBackoff =
        !options.isEnableStreamingEngine() && options.getLocalWindmillHostport() != null
            ? GrpcWindmillServer.LOCALHOST_MAX_BACKOFF
            : Duration.millis(options.getWindmillServiceStreamMaxBackoffMillis());
    return GrpcWindmillStreamFactory.of(
            JobHeader.newBuilder()
                .setJobId(options.getJobId())
                .setProjectId(options.getProject())
                .setWorkerId(options.getWorkerId())
                .setClientId(clientId)
                .build())
        .setWindmillMessagesBetweenIsReadyChecks(options.getWindmillMessagesBetweenIsReadyChecks())
        .setMaxBackOffSupplier(() -> maxBackoff)
        .setLogEveryNStreamFailures(options.getWindmillServiceStreamingLogEveryNStreamFailures())
        .setStreamingRpcBatchLimit(options.getWindmillServiceStreamingRpcBatchLimit())
        .setSendKeyedGetDataRequests(
            !options.isEnableStreamingEngine()
                || DataflowRunner.hasExperiment(
                    options, "streaming_engine_disable_new_heartbeat_requests"));
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
    checkArgument(
        options.isStreaming(),
        "%s instantiated with options indicating batch use",
        StreamingDataflowWorker.class.getName());

    checkArgument(
        !DataflowRunner.hasExperiment(options, "beam_fn_api"),
        "%s cannot be main() class with beam_fn_api enabled",
        StreamingDataflowWorker.class.getSimpleName());

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

  private static ChannelCachingStubFactory createStubFactory(
      DataflowWorkerHarnessOptions workerOptions) {
    Function<WindmillServiceAddress, ManagedChannel> channelFactory =
        serviceAddress ->
            remoteChannel(
                serviceAddress, workerOptions.getWindmillServiceRpcChannelAliveTimeoutSec());
    ChannelCache channelCache =
        ChannelCache.create(
            serviceAddress ->
                // IsolationChannel will create and manage separate RPC channels to the same
                // serviceAddress via calling the channelFactory, else just directly return the
                // RPC channel.
                workerOptions.getUseWindmillIsolatedChannels()
                    ? IsolationChannel.create(() -> channelFactory.apply(serviceAddress))
                    : channelFactory.apply(serviceAddress));
    return ChannelCachingRemoteStubFactory.create(workerOptions.getGcpCredential(), channelCache);
  }

  private static void sleep(int millis) {
    Uninterruptibles.sleepUninterruptibly(millis, TimeUnit.MILLISECONDS);
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
  final void reportPeriodicWorkerUpdatesForTest() {
    workerStatusReporter.reportPeriodicWorkerUpdates();
  }

  private int chooseMaximumNumberOfThreads() {
    if (options.getNumberOfWorkerHarnessThreads() != 0) {
      return options.getNumberOfWorkerHarnessThreads();
    }
    return MAX_PROCESSING_THREADS;
  }

  private int chooseMaximumBundlesOutstanding() {
    int maxBundles = options.getMaxBundlesFromWindmillOutstanding();
    if (maxBundles > 0) {
      return maxBundles;
    }
    return chooseMaximumNumberOfThreads() + 100;
  }

  @VisibleForTesting
  public boolean workExecutorIsEmpty() {
    return workUnitExecutor.executorQueueIsEmpty();
  }

  @VisibleForTesting
  int numCommitThreads() {
    return workCommitter.parallelism();
  }

  @VisibleForTesting
  ComputationStateCache getComputationStateCache() {
    return computationStateCache;
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  public void start() {
    running.set(true);

    configFetcher.start();

    memoryMonitorThread.start();
    dispatchThread.start();
    sampler.start();

    workCommitter.start();
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
      dispatchThread.interrupt();
      dispatchThread.join();

      workCommitter.stop();
      memoryMonitor.stop();
      memoryMonitorThread.join();
      workUnitExecutor.shutdown();

      computationStateCache.closeAndInvalidateAll();

      workerStatusReporter.stop();
    } catch (Exception e) {
      LOG.warn("Exception while shutting down: ", e);
    }
  }

  private void dispatchLoop() {
    while (running.get()) {
      memoryMonitor.waitForResources("GetWork");

      int backoff = 1;
      Windmill.GetWorkResponse workResponse = null;
      do {
        try {
          workResponse = getWork();
          if (workResponse.getWorkCount() > 0) {
            break;
          }
        } catch (WindmillServerStub.RpcException e) {
          LOG.warn("GetWork failed, retrying:", e);
        }
        sleep(backoff);
        backoff = Math.min(1000, backoff * 2);
      } while (running.get());
      for (final Windmill.ComputationWorkItems computationWork : workResponse.getWorkList()) {
        final String computationId = computationWork.getComputationId();
        Optional<ComputationState> maybeComputationState = computationStateCache.get(computationId);
        if (!maybeComputationState.isPresent()) {
          continue;
        }

        final ComputationState computationState = maybeComputationState.get();
        final Instant inputDataWatermark =
            WindmillTimeUtils.windmillToHarnessWatermark(computationWork.getInputDataWatermark());
        Watermarks.Builder watermarks =
            Watermarks.builder()
                .setInputDataWatermark(Preconditions.checkNotNull(inputDataWatermark))
                .setSynchronizedProcessingTime(
                    WindmillTimeUtils.windmillToHarnessWatermark(
                        computationWork.getDependentRealtimeInputWatermark()));

        for (final Windmill.WorkItem workItem : computationWork.getWorkList()) {
          streamingWorkScheduler.scheduleWork(
              computationState,
              workItem,
              watermarks.setOutputDataWatermark(workItem.getOutputDataWatermark()).build(),
              Work.createProcessingContext(
                  computationId, getDataClient, workCommitter::commit, heartbeatSender),
              /* getWorkStreamLatencies= */ Collections.emptyList());
        }
      }
    }
  }

  void streamingDispatchLoop() {
    while (running.get()) {
      GetWorkStream stream =
          windmillServer.getWorkStream(
              Windmill.GetWorkRequest.newBuilder()
                  .setClientId(clientId)
                  .setMaxItems(chooseMaximumBundlesOutstanding())
                  .setMaxBytes(MAX_GET_WORK_FETCH_BYTES)
                  .build(),
              (String computation,
                  Instant inputDataWatermark,
                  Instant synchronizedProcessingTime,
                  Windmill.WorkItem workItem,
                  Collection<LatencyAttribution> getWorkStreamLatencies) ->
                  computationStateCache
                      .get(computation)
                      .ifPresent(
                          computationState -> {
                            memoryMonitor.waitForResources("GetWork");
                            streamingWorkScheduler.scheduleWork(
                                computationState,
                                workItem,
                                Watermarks.builder()
                                    .setInputDataWatermark(inputDataWatermark)
                                    .setSynchronizedProcessingTime(synchronizedProcessingTime)
                                    .setOutputDataWatermark(workItem.getOutputDataWatermark())
                                    .build(),
                                Work.createProcessingContext(
                                    computationState.getComputationId(),
                                    getDataClient,
                                    workCommitter::commit,
                                    heartbeatSender),
                                getWorkStreamLatencies);
                          }));
      try {
        // Reconnect every now and again to enable better load balancing.
        // If at any point the server closes the stream, we will reconnect immediately; otherwise
        // we half-close the stream after some time and create a new one.
        if (!stream.awaitTermination(GET_WORK_STREAM_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
          stream.halfClose();
        }
      } catch (InterruptedException e) {
        // Continue processing until !running.get()
      }
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

  private Windmill.GetWorkResponse getWork() {
    return windmillServer.getWork(
        Windmill.GetWorkRequest.newBuilder()
            .setClientId(clientId)
            .setMaxItems(chooseMaximumBundlesOutstanding())
            .setMaxBytes(MAX_GET_WORK_FETCH_BYTES)
            .build());
  }

  @VisibleForTesting
  public Iterable<CounterUpdate> buildCounters() {
    return Iterables.concat(
        streamingCounters
            .pendingDeltaCounters()
            .extractModifiedDeltaUpdates(DataflowCounterUpdateExtractor.INSTANCE),
        streamingCounters
            .pendingCumulativeCounters()
            .extractUpdates(false, DataflowCounterUpdateExtractor.INSTANCE));
  }

  @AutoValue
  abstract static class ConfigFetcherComputationStateCacheAndWindmillClient {

    private static ConfigFetcherComputationStateCacheAndWindmillClient create(
        ComputationConfig.Fetcher configFetcher,
        ComputationStateCache computationStateCache,
        WindmillServerStub windmillServer,
        GrpcWindmillStreamFactory windmillStreamFactory) {
      return new AutoValue_StreamingDataflowWorker_ConfigFetcherComputationStateCacheAndWindmillClient(
          configFetcher, computationStateCache, windmillServer, windmillStreamFactory);
    }

    abstract ComputationConfig.Fetcher configFetcher();

    abstract ComputationStateCache computationStateCache();

    abstract WindmillServerStub windmillServer();

    abstract GrpcWindmillStreamFactory windmillStreamFactory();
  }
}
