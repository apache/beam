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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toConcurrentMap;
import static org.apache.beam.runners.dataflow.DataflowRunner.hasExperiment;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.MapTask;
import com.google.api.services.dataflow.model.StreamingComputationConfig;
import com.google.api.services.dataflow.model.StreamingConfigTask;
import com.google.api.services.dataflow.model.WorkItem;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.beam.runners.core.metrics.MetricsLogger;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.internal.CustomSources;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.worker.apiary.FixMultiOutputInfosOnParDoInstructions;
import org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.MapTaskToNetworkFunction;
import org.apache.beam.runners.dataflow.worker.graph.Networks;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.InstructionOutputNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingMDC;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler;
import org.apache.beam.runners.dataflow.worker.status.BaseStatusServlet;
import org.apache.beam.runners.dataflow.worker.status.DebugCapture;
import org.apache.beam.runners.dataflow.worker.status.DebugCapture.Capturable;
import org.apache.beam.runners.dataflow.worker.status.LastExceptionDataProvider;
import org.apache.beam.runners.dataflow.worker.status.StatusDataProvider;
import org.apache.beam.runners.dataflow.worker.status.WorkerStatusPages;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.ExecutionState;
import org.apache.beam.runners.dataflow.worker.streaming.KeyCommitTooLargeException;
import org.apache.beam.runners.dataflow.worker.streaming.ShardedKey;
import org.apache.beam.runners.dataflow.worker.streaming.StageInfo;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.streaming.Work.State;
import org.apache.beam.runners.dataflow.worker.streaming.WorkHeartbeatResponseProcessor;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingCounters;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingWorkerStatusReporter;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputStateFetcher;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ElementCounter;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputObjectAndByteCounter;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ReadOperation;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.LatencyAttribution;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItemCommitRequest;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.runners.dataflow.worker.windmill.appliance.JniWindmillApplianceServer;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamPool;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.Commit;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.CompleteCommit;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.StreamingApplianceWorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.StreamingEngineWorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.ChannelzServlet;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillServer;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillStreamFactory;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateReader;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.FailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.StreamingApplianceFailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.StreamingEngineFailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.WorkFailureProcessor;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.ActiveWorkRefresher;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.ActiveWorkRefreshers;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.fn.JvmInitializers;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.bigquery.BigQuerySinkMetrics;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.util.construction.CoderTranslation;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.*;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.graph.MutableNetwork;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implements a Streaming Dataflow worker. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class StreamingDataflowWorker {

  // TODO(https://github.com/apache/beam/issues/19632): Update throttling counters to use generic
  // throttling-msecs metric.
  public static final MetricName BIGQUERY_STREAMING_INSERT_THROTTLE_TIME =
      MetricName.named(
          "org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesImpl$DatasetServiceImpl",
          "throttling-msecs");
  // Maximum number of threads for processing.  Currently each thread processes one key at a time.
  static final int MAX_PROCESSING_THREADS = 300;
  static final long THREAD_EXPIRATION_TIME_SEC = 60;
  static final int NUM_COMMIT_STREAMS = 1;
  static final int GET_WORK_STREAM_TIMEOUT_MINUTES = 3;
  static final Duration COMMIT_STREAM_TIMEOUT = Duration.standardMinutes(1);

  /**
   * Sinks are marked 'full' in {@link StreamingModeExecutionContext} once the amount of data sinked
   * (across all the sinks, if there are more than one) reaches this limit. This serves as hint for
   * readers to stop producing more. This can be disabled with 'disable_limiting_bundle_sink_bytes'
   * experiment.
   */
  static final int MAX_SINK_BYTES = 10_000_000;

  private static final Logger LOG = LoggerFactory.getLogger(StreamingDataflowWorker.class);
  /** The idGenerator to generate unique id globally. */
  private static final IdGenerator idGenerator = IdGenerators.decrementingLongs();
  /**
   * Fix up MapTask representation because MultiOutputInfos are missing from system generated
   * ParDoInstructions.
   */
  private static final Function<MapTask, MapTask> fixMultiOutputInfos =
      new FixMultiOutputInfosOnParDoInstructions(idGenerator);
  /**
   * Function which converts map tasks to their network representation for execution.
   *
   * <ul>
   *   <li>Translate the map task to a network representation.
   *   <li>Remove flatten instructions by rewiring edges.
   * </ul>
   */
  private static final Function<MapTask, MutableNetwork<Node, Edge>> mapTaskToBaseNetwork =
      new MapTaskToNetworkFunction(idGenerator);

  private static final int DEFAULT_STATUS_PORT = 8081;
  // Maximum size of the result of a GetWork request.
  private static final long MAX_GET_WORK_FETCH_BYTES = 64L << 20; // 64m
  /** Maximum number of failure stacktraces to report in each update sent to backend. */
  private static final int MAX_FAILURES_TO_REPORT_IN_UPDATE = 1000;

  private static final Random clientIdGenerator = new Random();
  private static final String CHANNELZ_PATH = "/channelz";
  final WindmillStateCache stateCache;
  // Maps from computation ids to per-computation state.
  private final ConcurrentMap<String, ComputationState> computationMap;
  // Cache of tokens to commit callbacks.
  // Using Cache with time eviction policy helps us to prevent memory leak when callback ids are
  // discarded by Dataflow service and calling commitCallback is best-effort.
  private final Cache<Long, Runnable> commitCallbacks =
      CacheBuilder.newBuilder().expireAfterWrite(5L, TimeUnit.MINUTES).build();
  // Map of user state names to system state names.
  // TODO(drieber): obsolete stateNameMap. Use transformUserNameToStateFamily in
  // ComputationState instead.
  private final ConcurrentMap<String, String> stateNameMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, String> systemNameToComputationIdMap =
      new ConcurrentHashMap<>();
  private final BoundedQueueExecutor workUnitExecutor;
  private final WindmillServerStub windmillServer;
  private final Thread dispatchThread;
  private final AtomicBoolean running = new AtomicBoolean();
  private final SideInputStateFetcher sideInputStateFetcher;
  private final DataflowWorkerHarnessOptions options;
  private final boolean windmillServiceEnabled;
  private final long clientId;
  private final MetricTrackingWindmillServerStub metricTrackingWindmillServer;

  private final java.util.concurrent.ConcurrentLinkedQueue<CounterUpdate> pendingMonitoringInfos =
      new ConcurrentLinkedQueue<>();
  // Map from stage name to StageInfo containing metrics container registry and per stage counters.
  private final ConcurrentMap<String, StageInfo> stageInfoMap;

  private final MemoryMonitor memoryMonitor;
  private final Thread memoryMonitorThread;
  private final WorkerStatusPages statusPages;
  // Limit on bytes sinked (committed) in a work item.
  private final long maxSinkBytes; // = MAX_SINK_BYTES unless disabled in options.
  private final ReaderCache readerCache;
  private final WorkUnitClient workUnitClient;
  private final CompletableFuture<Void> isDoneFuture;
  private final Function<MapTask, MutableNetwork<Node, Edge>> mapTaskToNetwork;
  private final ReaderRegistry readerRegistry = ReaderRegistry.defaultRegistry();
  private final SinkRegistry sinkRegistry = SinkRegistry.defaultRegistry();
  private final Supplier<Instant> clock;
  private final Function<String, ScheduledExecutorService> executorSupplier;
  private final DataflowMapTaskExecutorFactory mapTaskExecutorFactory;
  private final HotKeyLogger hotKeyLogger;
  // Periodic sender of debug information to the debug capture service.
  private final DebugCapture.@Nullable Manager debugCaptureManager;
  // Collection of ScheduledExecutorServices that are running periodic functions.
  private final ArrayList<ScheduledExecutorService> scheduledExecutors = new ArrayList<>();
  // Periodically fires a global config request to dataflow service. Only used when windmill service
  // is enabled.
  // Possibly overridden by streaming engine config.
  private int maxWorkItemCommitBytes = Integer.MAX_VALUE;

  private final DataflowExecutionStateSampler sampler = DataflowExecutionStateSampler.instance();
  private final ActiveWorkRefresher activeWorkRefresher;
  private final WorkCommitter workCommitter;
  private final StreamingWorkerStatusReporter workerStatusReporter;
  private final FailureTracker failureTracker;
  private final WorkFailureProcessor workFailureProcessor;
  private final StreamingCounters streamingCounters;

  private StreamingDataflowWorker(
      WindmillServerStub windmillServer,
      long clientId,
      ConcurrentMap<String, ComputationState> computationMap,
      ConcurrentMap<String, StageInfo> stageInfoMap,
      WindmillStateCache windmillStateCache,
      BoundedQueueExecutor workUnitExecutor,
      DataflowMapTaskExecutorFactory mapTaskExecutorFactory,
      WorkUnitClient workUnitClient,
      DataflowWorkerHarnessOptions options,
      HotKeyLogger hotKeyLogger,
      Supplier<Instant> clock,
      StreamingWorkerStatusReporter workerStatusReporter,
      FailureTracker failureTracker,
      WorkFailureProcessor workFailureProcessor,
      StreamingCounters streamingCounters,
      MemoryMonitor memoryMonitor,
      Function<String, ScheduledExecutorService> executorSupplier) {
    this.computationMap = computationMap;
    this.stageInfoMap = stageInfoMap;
    this.stateCache = windmillStateCache;
    this.readerCache =
        new ReaderCache(
            Duration.standardSeconds(options.getReaderCacheTimeoutSec()),
            Executors.newCachedThreadPool());
    this.mapTaskExecutorFactory = mapTaskExecutorFactory;
    this.workUnitClient = workUnitClient;
    this.options = options;
    this.hotKeyLogger = hotKeyLogger;
    this.clock = clock;
    this.executorSupplier = executorSupplier;
    this.windmillServiceEnabled = options.isEnableStreamingEngine();
    this.statusPages = WorkerStatusPages.create(DEFAULT_STATUS_PORT, memoryMonitor, () -> true);
    if (windmillServiceEnabled) {
      this.debugCaptureManager =
          new DebugCapture.Manager(options, statusPages.getDebugCapturePages());
    } else {
      this.debugCaptureManager = null;
    }

    this.isDoneFuture = new CompletableFuture<>();
    this.workUnitExecutor = workUnitExecutor;

    maxSinkBytes =
        hasExperiment(options, "disable_limiting_bundle_sink_bytes")
            ? Long.MAX_VALUE
            : MAX_SINK_BYTES;

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
    this.metricTrackingWindmillServer =
        MetricTrackingWindmillServerStub.builder(windmillServer, memoryMonitor)
            .setUseStreamingRequests(windmillServiceEnabled)
            .setUseSeparateHeartbeatStreams(options.getUseSeparateWindmillHeartbeatStreams())
            .setNumGetDataStreams(options.getWindmillGetDataStreamCount())
            .build();

    this.sideInputStateFetcher =
        new SideInputStateFetcher(metricTrackingWindmillServer::getSideInputData, options);
    int numCommitThreads = 1;
    if (windmillServiceEnabled && options.getWindmillServiceCommitThreads() > 0) {
      numCommitThreads = options.getWindmillServiceCommitThreads();
    }

    this.workCommitter =
        windmillServiceEnabled
            ? StreamingEngineWorkCommitter.create(
                WindmillStreamPool.create(
                        NUM_COMMIT_STREAMS, COMMIT_STREAM_TIMEOUT, windmillServer::commitWorkStream)
                    ::getCloseableStream,
                numCommitThreads,
                this::onCompleteCommit)
            : StreamingApplianceWorkCommitter.create(
                windmillServer::commitWork, this::onCompleteCommit);

    // Register standard file systems.
    FileSystems.setDefaultPipelineOptions(options);

    this.mapTaskToNetwork = mapTaskToBaseNetwork;

    int stuckCommitDurationMillis =
        windmillServiceEnabled && options.getStuckCommitDurationMillis() > 0
            ? options.getStuckCommitDurationMillis()
            : 0;
    this.activeWorkRefresher =
        ActiveWorkRefreshers.createDispatchedActiveWorkRefresher(
            clock,
            options.getActiveWorkRefreshPeriodMillis(),
            stuckCommitDurationMillis,
            () -> Collections.unmodifiableCollection(computationMap.values()),
            sampler,
            metricTrackingWindmillServer::refreshActiveWork,
            executorSupplier.apply("RefreshWork"));
    this.workerStatusReporter = workerStatusReporter;
    this.failureTracker = failureTracker;
    this.workFailureProcessor = workFailureProcessor;
    this.streamingCounters = streamingCounters;
    this.memoryMonitor = memoryMonitor;

    LOG.debug("windmillServiceEnabled: {}", windmillServiceEnabled);
    LOG.debug("WindmillServiceEndpoint: {}", options.getWindmillServiceEndpoint());
    LOG.debug("WindmillServicePort: {}", options.getWindmillServicePort());
    LOG.debug("LocalWindmillHostport: {}", options.getLocalWindmillHostport());
    LOG.debug("maxWorkItemCommitBytes: {}", maxWorkItemCommitBytes);
  }

  public static StreamingDataflowWorker fromOptions(DataflowWorkerHarnessOptions options) {
    long clientId = clientIdGenerator.nextLong();
    MemoryMonitor memoryMonitor = MemoryMonitor.fromOptions(options);
    ConcurrentMap<String, ComputationState> computationMap = new ConcurrentHashMap<>();
    ConcurrentMap<String, StageInfo> stageInfo = new ConcurrentHashMap<>();
    StreamingCounters streamingCounters = StreamingCounters.create();

    GrpcWindmillStreamFactory windmillStreamFactory =
        createWindmillStreamFactory(options, clientId);
    WindmillServerStub windmillServer =
        createWindmillServerStub(
            options,
            windmillStreamFactory,
            new WorkHeartbeatResponseProcessor(
                computationId -> Optional.ofNullable(computationMap.get(computationId))));

    FailureTracker failureTracker =
        options.isEnableStreamingEngine()
            ? StreamingEngineFailureTracker.create(
                MAX_FAILURES_TO_REPORT_IN_UPDATE, options.getMaxStackTraceDepthToReport())
            : StreamingApplianceFailureTracker.create(
                MAX_FAILURES_TO_REPORT_IN_UPDATE,
                options.getMaxStackTraceDepthToReport(),
                windmillServer::reportStats);

    WorkUnitClient dataflowServiceClient = new DataflowWorkUnitClient(options, LOG);
    BoundedQueueExecutor workExecutor = createWorkUnitExecutor(options);
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
        computationMap,
        stageInfo,
        WindmillStateCache.ofSizeMbs(options.getWorkerCacheMb()),
        workExecutor,
        IntrinsicMapTaskExecutorFactory.defaultFactory(),
        dataflowServiceClient,
        options,
        new HotKeyLogger(),
        clock,
        workerStatusReporter,
        failureTracker,
        workFailureProcessor,
        streamingCounters,
        memoryMonitor,
        threadName ->
            Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat(threadName).build()));
  }

  @VisibleForTesting
  static StreamingDataflowWorker forTesting(
      ConcurrentMap<String, ComputationState> computationMap,
      WindmillServerStub windmillServer,
      List<MapTask> mapTasks,
      DataflowMapTaskExecutorFactory mapTaskExecutorFactory,
      WorkUnitClient workUnitClient,
      DataflowWorkerHarnessOptions options,
      boolean publishCounters,
      HotKeyLogger hotKeyLogger,
      Supplier<Instant> clock,
      Function<String, ScheduledExecutorService> executorSupplier,
      int localRetryTimeoutMs) {
    ConcurrentMap<String, StageInfo> stageInfo = new ConcurrentHashMap<>();
    BoundedQueueExecutor workExecutor = createWorkUnitExecutor(options);
    WindmillStateCache stateCache = WindmillStateCache.ofSizeMbs(options.getWorkerCacheMb());
    computationMap.putAll(
        createComputationMapForTesting(mapTasks, workExecutor, stateCache::forComputation));
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
    return new StreamingDataflowWorker(
        windmillServer,
        1L,
        computationMap,
        stageInfo,
        stateCache,
        workExecutor,
        mapTaskExecutorFactory,
        workUnitClient,
        options,
        hotKeyLogger,
        clock,
        workerStatusReporter,
        failureTracker,
        workFailureProcessor,
        streamingCounters,
        memoryMonitor,
        executorSupplier);
  }

  private static GrpcWindmillStreamFactory createWindmillStreamFactory(
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
        .build();
  }

  @VisibleForTesting
  final void reportPeriodicWorkerUpdatesForTest() {
    workerStatusReporter.reportPeriodicWorkerUpdates();
  }

  private static ConcurrentMap<String, ComputationState> createComputationMapForTesting(
      List<MapTask> mapTasks,
      BoundedQueueExecutor workUnitExecutor,
      Function<String, WindmillStateCache.ForComputation> forComputationStateCacheFactory) {
    return mapTasks.stream()
        .map(fixMultiOutputInfos)
        .map(
            mapTask -> {
              LOG.info("Adding config for {}: {}", mapTask.getSystemName(), mapTask);
              String computationId = mapTask.getStageName();
              return new ComputationState(
                  computationId,
                  mapTask,
                  workUnitExecutor,
                  ImmutableMap.of(),
                  forComputationStateCacheFactory.apply(computationId));
            })
        .collect(toConcurrentMap(ComputationState::getComputationId, Function.identity()));
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

  private static MapTask parseMapTask(String input) throws IOException {
    return Transport.getJsonFactory().fromString(input, MapTask.class);
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
        && DataflowRunner.hasExperiment(options, "enable_per_worker_metrics")) {
      enableBigQueryMetrics();
    }

    JvmInitializers.runBeforeProcessing(options);
    worker.startStatusPages();
    worker.start();
  }

  private static WindmillServerStub createWindmillServerStub(
      DataflowWorkerHarnessOptions options,
      GrpcWindmillStreamFactory windmillStreamFactory,
      Consumer<List<Windmill.ComputationHeartbeatResponse>> processHeartbeatResponses) {
    if (options.getWindmillServiceEndpoint() != null
        || options.isEnableStreamingEngine()
        || options.getLocalWindmillHostport().startsWith("grpc:")) {
      try {
        windmillStreamFactory.scheduleHealthChecks(
            options.getWindmillServiceStreamingRpcHealthCheckPeriodMs());
        return GrpcWindmillServer.create(options, windmillStreamFactory, processHeartbeatResponses);
      } catch (IOException e) {
        throw new RuntimeException("Failed to create GrpcWindmillServer: ", e);
      }
    } else {
      return new JniWindmillApplianceServer(options.getLocalWindmillHostport());
    }
  }

  private static void sleep(int millis) {
    Uninterruptibles.sleepUninterruptibly(millis, TimeUnit.MILLISECONDS);
  }

  /** Sets the stage name and workId of the current Thread for logging. */
  private static void setUpWorkLoggingContext(String workId, String computationId) {
    DataflowWorkerLoggingMDC.setWorkId(workId);
    DataflowWorkerLoggingMDC.setStageName(computationId);
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

  void addStateNameMappings(Map<String, String> nameMap) {
    stateNameMap.putAll(nameMap);
  }

  @VisibleForTesting
  public void setMaxWorkItemCommitBytes(int maxWorkItemCommitBytes) {
    if (maxWorkItemCommitBytes != this.maxWorkItemCommitBytes) {
      LOG.info("Setting maxWorkItemCommitBytes to {}", maxWorkItemCommitBytes);
    }
    this.maxWorkItemCommitBytes = maxWorkItemCommitBytes;
  }

  @VisibleForTesting
  public boolean workExecutorIsEmpty() {
    return workUnitExecutor.executorQueueIsEmpty();
  }

  @VisibleForTesting
  int numCommitThreads() {
    return workCommitter.parallelism();
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  public void start() {
    running.set(true);

    if (windmillServiceEnabled) {
      // Schedule the background getConfig thread. Blocks until windmillServer stub is ready.
      schedulePeriodicGlobalConfigRequests();
    }

    memoryMonitorThread.start();
    dispatchThread.start();
    sampler.start();

    if (options.getPeriodicStatusPageOutputDirectory() != null) {
      ScheduledExecutorService statusPageTimer = executorSupplier.apply("DumpStatusPages");
      statusPageTimer.scheduleWithFixedDelay(
          () -> {
            Collection<Capturable> pages = statusPages.getDebugCapturePages();
            if (pages.isEmpty()) {
              LOG.warn("No captured status pages.");
            }
            long timestamp = clock.get().getMillis();
            for (Capturable page : pages) {
              PrintWriter writer = null;
              try {
                File outputFile =
                    new File(
                        options.getPeriodicStatusPageOutputDirectory(),
                        ("StreamingDataflowWorker"
                                + options.getWorkerId()
                                + "_"
                                + page.pageName()
                                + timestamp
                                + ".html")
                            .replaceAll("/", "_"));
                writer = new PrintWriter(outputFile, UTF_8.name());
                page.captureData(writer);
              } catch (IOException e) {
                LOG.warn("Error dumping status page.", e);
              } finally {
                if (writer != null) {
                  writer.close();
                }
              }
            }
          },
          60,
          60,
          TimeUnit.SECONDS);
      scheduledExecutors.add(statusPageTimer);
    }
    workCommitter.start();
    workerStatusReporter.start();
    activeWorkRefresher.start();
  }

  public void startStatusPages() {
    if (debugCaptureManager != null) {
      debugCaptureManager.start();
    }

    if (windmillServiceEnabled) {
      ChannelzServlet channelzServlet = new ChannelzServlet(CHANNELZ_PATH, options, windmillServer);
      statusPages.addServlet(channelzServlet);
      statusPages.addCapturePage(channelzServlet);
    }

    statusPages.addServlet(stateCache.statusServlet());
    statusPages.addServlet(new SpecsServlet());

    statusPages.addStatusDataProvider("harness", "Harness", new HarnessDataProvider());
    statusPages.addStatusDataProvider("metrics", "Metrics", new MetricsDataProvider());
    statusPages.addStatusDataProvider(
        "exception", "Last Exception", new LastExceptionDataProvider());
    statusPages.addStatusDataProvider("cache", "State Cache", stateCache);
    statusPages.addStatusDataProvider("streaming", "Streaming Rpcs", windmillServer);

    statusPages.start();
  }

  public void stop() {
    try {
      for (ScheduledExecutorService timer : scheduledExecutors) {
        if (timer != null) {
          timer.shutdown();
        }
      }
      for (ScheduledExecutorService timer : scheduledExecutors) {
        if (timer != null) {
          timer.awaitTermination(300, TimeUnit.SECONDS);
        }
      }

      activeWorkRefresher.stop();
      statusPages.stop();
      if (debugCaptureManager != null) {
        debugCaptureManager.stop();
      }
      running.set(false);
      dispatchThread.interrupt();
      dispatchThread.join();

      workCommitter.stop();
      memoryMonitor.stop();
      memoryMonitorThread.join();
      workUnitExecutor.shutdown();
      for (ComputationState state : computationMap.values()) {
        state.close();
      }

      workerStatusReporter.stop();
    } catch (Exception e) {
      LOG.warn("Exception while shutting down: ", e);
    }
    setIsDone();
  }

  // null is the only value of type Void, but findbugs thinks
  // it violates the contract of CompletableFuture.complete
  @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
  private void setIsDone() {
    isDoneFuture.complete(null);
  }

  private synchronized void addComputation(
      String computationId,
      MapTask originalMapTask,
      Map<String, String> transformUserNameToStateFamily) {
    // Map task instances are shared amongst multiple threads during computation hence
    // we fix the map task before we add a new computation state that would reference it.
    MapTask mapTask = fixMultiOutputInfos.apply(originalMapTask);
    if (!computationMap.containsKey(computationId)) {
      LOG.info("Adding config for {}: {}", computationId, mapTask);
      computationMap.put(
          computationId,
          new ComputationState(
              computationId,
              mapTask,
              workUnitExecutor,
              transformUserNameToStateFamily,
              stateCache.forComputation(computationId)));
    }
  }

  /**
   * If the computation is not yet known about, configuration for it will be fetched. This can still
   * return null if there is no configuration fetched for the computation.
   */
  private ComputationState getComputationState(String computationId) {
    ComputationState state = computationMap.get(computationId);
    if (state == null) {
      getConfig(computationId);
      state = computationMap.get(computationId);
    }
    return state;
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
        final ComputationState computationState = getComputationState(computationId);
        if (computationState == null) {
          LOG.warn(
              "Received work for unknown computation: {}. Known computations are {}",
              computationId,
              computationMap.keySet());
          continue;
        }

        final Instant inputDataWatermark =
            WindmillTimeUtils.windmillToHarnessWatermark(computationWork.getInputDataWatermark());
        Preconditions.checkNotNull(inputDataWatermark);
        final @Nullable Instant synchronizedProcessingTime =
            WindmillTimeUtils.windmillToHarnessWatermark(
                computationWork.getDependentRealtimeInputWatermark());
        for (final Windmill.WorkItem workItem : computationWork.getWorkList()) {
          scheduleWorkItem(
              computationState,
              inputDataWatermark,
              synchronizedProcessingTime,
              workItem,
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
                  Collection<LatencyAttribution> getWorkStreamLatencies) -> {
                memoryMonitor.waitForResources("GetWork");
                scheduleWorkItem(
                    getComputationState(computation),
                    inputDataWatermark,
                    synchronizedProcessingTime,
                    workItem,
                    getWorkStreamLatencies);
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
  }

  private void scheduleWorkItem(
      final ComputationState computationState,
      final Instant inputDataWatermark,
      final Instant synchronizedProcessingTime,
      final Windmill.WorkItem workItem,
      final Collection<LatencyAttribution> getWorkStreamLatencies) {
    Preconditions.checkNotNull(inputDataWatermark);
    // May be null if output watermark not yet known.
    final @Nullable Instant outputDataWatermark =
        WindmillTimeUtils.windmillToHarnessWatermark(workItem.getOutputDataWatermark());
    Preconditions.checkState(
        outputDataWatermark == null || !outputDataWatermark.isAfter(inputDataWatermark));
    Work scheduledWork =
        Work.create(
            workItem,
            clock,
            getWorkStreamLatencies,
            work ->
                process(
                    computationState,
                    inputDataWatermark,
                    outputDataWatermark,
                    synchronizedProcessingTime,
                    work));
    computationState.activateWork(
        ShardedKey.create(workItem.getKey(), workItem.getShardingKey()), scheduledWork);
  }

  /**
   * Extracts the userland key coder, if any, from the coder used in the initial read step of a
   * stage. This encodes many assumptions about how the streaming execution context works.
   */
  private @Nullable Coder<?> extractKeyCoder(Coder<?> readCoder) {
    if (!(readCoder instanceof WindowedValueCoder)) {
      throw new RuntimeException(
          String.format(
              "Expected coder for streaming read to be %s, but received %s",
              WindowedValueCoder.class.getSimpleName(), readCoder));
    }

    // Note that TimerOrElementCoder is a backwards-compatibility class
    // that is really a FakeKeyedWorkItemCoder
    Coder<?> valueCoder = ((WindowedValueCoder<?>) readCoder).getValueCoder();

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

  private void process(
      final ComputationState computationState,
      final Instant inputDataWatermark,
      final @Nullable Instant outputDataWatermark,
      final @Nullable Instant synchronizedProcessingTime,
      final Work work) {
    final Windmill.WorkItem workItem = work.getWorkItem();
    final String computationId = computationState.getComputationId();
    final ByteString key = workItem.getKey();
    work.setState(State.PROCESSING);

    setUpWorkLoggingContext(work.getLatencyTrackingId(), computationId);

    LOG.debug("Starting processing for {}:\n{}", computationId, work);

    Windmill.WorkItemCommitRequest.Builder outputBuilder = initializeOutputBuilder(key, workItem);

    // Before any processing starts, call any pending OnCommit callbacks.  Nothing that requires
    // cleanup should be done before this, since we might exit early here.
    callFinalizeCallbacks(workItem);
    if (workItem.getSourceState().getOnlyFinalize()) {
      outputBuilder.setSourceStateUpdates(Windmill.SourceState.newBuilder().setOnlyFinalize(true));
      work.setState(State.COMMIT_QUEUED);
      workCommitter.commit(Commit.create(outputBuilder.build(), computationState, work));
      return;
    }

    long processingStartTimeNanos = System.nanoTime();

    final MapTask mapTask = computationState.getMapTask();

    StageInfo stageInfo =
        stageInfoMap.computeIfAbsent(
            mapTask.getStageName(), s -> StageInfo.create(s, mapTask.getSystemName()));

    ExecutionState executionState = null;
    String counterName = "dataflow_source_bytes_processed-" + mapTask.getSystemName();

    try {
      if (work.isFailed()) {
        throw new WorkItemCancelledException(workItem.getShardingKey());
      }
      executionState = computationState.getExecutionStateQueue().poll();
      if (executionState == null) {
        MutableNetwork<Node, Edge> mapTaskNetwork = mapTaskToNetwork.apply(mapTask);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Network as Graphviz .dot: {}", Networks.toDot(mapTaskNetwork));
        }
        ParallelInstructionNode readNode =
            (ParallelInstructionNode)
                Iterables.find(
                    mapTaskNetwork.nodes(),
                    node ->
                        node instanceof ParallelInstructionNode
                            && ((ParallelInstructionNode) node).getParallelInstruction().getRead()
                                != null);
        InstructionOutputNode readOutputNode =
            (InstructionOutputNode) Iterables.getOnlyElement(mapTaskNetwork.successors(readNode));
        DataflowExecutionContext.DataflowExecutionStateTracker executionStateTracker =
            new DataflowExecutionContext.DataflowExecutionStateTracker(
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
        StreamingModeExecutionContext context =
            new StreamingModeExecutionContext(
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
        DataflowMapTaskExecutor mapTaskExecutor =
            mapTaskExecutorFactory.create(
                mapTaskNetwork,
                options,
                mapTask.getStageName(),
                readerRegistry,
                sinkRegistry,
                context,
                streamingCounters.pendingDeltaCounters(),
                idGenerator);
        ReadOperation readOperation = mapTaskExecutor.getReadOperation();
        // Disable progress updates since its results are unused  for streaming
        // and involves starting a thread.
        readOperation.setProgressUpdatePeriodMs(ReadOperation.DONT_UPDATE_PERIODICALLY);
        Preconditions.checkState(
            mapTaskExecutor.supportsRestart(),
            "Streaming runner requires all operations support restart.");

        Coder<?> readCoder;
        readCoder =
            CloudObjects.coderFromCloudObject(
                CloudObject.fromSpec(readOutputNode.getInstructionOutput().getCodec()));
        Coder<?> keyCoder = extractKeyCoder(readCoder);

        // If using a custom source, count bytes read for autoscaling.
        if (CustomSources.class
            .getName()
            .equals(
                readNode.getParallelInstruction().getRead().getSource().getSpec().get("@type"))) {
          NameContext nameContext =
              NameContext.create(
                  mapTask.getStageName(),
                  readNode.getParallelInstruction().getOriginalName(),
                  readNode.getParallelInstruction().getSystemName(),
                  readNode.getParallelInstruction().getName());
          readOperation.receivers[0].addOutputCounter(
              counterName,
              new OutputObjectAndByteCounter(
                      new IntrinsicMapTaskExecutorFactory.ElementByteSizeObservableCoder<>(
                          readCoder),
                      mapTaskExecutor.getOutputCounters(),
                      nameContext)
                  .setSamplingPeriod(100)
                  .countBytes(counterName));
        }

        ExecutionState.Builder executionStateBuilder =
            ExecutionState.builder()
                .setWorkExecutor(mapTaskExecutor)
                .setContext(context)
                .setExecutionStateTracker(executionStateTracker);

        if (keyCoder != null) {
          executionStateBuilder.setKeyCoder(keyCoder);
        }

        executionState = executionStateBuilder.build();
      }

      WindmillStateReader stateReader =
          new WindmillStateReader(
              (request) ->
                  Optional.ofNullable(
                      metricTrackingWindmillServer.getStateData(computationId, request)),
              key,
              workItem.getShardingKey(),
              workItem.getWorkToken(),
              () -> {
                work.setState(State.READING);
                return () -> work.setState(State.PROCESSING);
              },
              work::isFailed);
      SideInputStateFetcher localSideInputStateFetcher = sideInputStateFetcher.byteTrackingView();

      // If the read output KVs, then we can decode Windmill's byte key into a userland
      // key object and provide it to the execution context for use with per-key state.
      // Otherwise, we pass null.
      //
      // The coder type that will be present is:
      //     WindowedValueCoder(TimerOrElementCoder(KvCoder))
      Optional<Coder<?>> keyCoder = executionState.keyCoder();
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

      Iterables.addAll(
          this.pendingMonitoringInfos, executionState.workExecutor().extractMetricUpdates());

      commitCallbacks.putAll(executionState.context().flushState());

      // Release the execution state for another thread to use.
      computationState.getExecutionStateQueue().offer(executionState);
      executionState = null;

      // Add the output to the commit queue.
      work.setState(State.COMMIT_QUEUED);
      outputBuilder.addAllPerWorkItemLatencyAttributions(
          work.getLatencyAttributions(false, work.getLatencyTrackingId(), sampler));

      WorkItemCommitRequest commitRequest = outputBuilder.build();
      int byteLimit = maxWorkItemCommitBytes;
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
        // so we're purposefully dropping them here
        commitRequest = buildWorkItemTruncationRequest(key, workItem, estimatedCommitSize);
      }

      workCommitter.commit(Commit.create(commitRequest, computationState, work));

      // Compute shuffle and state byte statistics these will be flushed asynchronously.
      long stateBytesWritten =
          outputBuilder
              .clearOutputMessages()
              .clearPerWorkItemLatencyAttributions()
              .build()
              .getSerializedSize();
      long shuffleBytesRead = 0;
      for (Windmill.InputMessageBundle bundle : workItem.getMessageBundlesList()) {
        for (Windmill.Message message : bundle.getMessagesList()) {
          shuffleBytesRead += message.getSerializedSize();
        }
      }
      long stateBytesRead = stateReader.getBytesRead() + localSideInputStateFetcher.getBytesRead();
      streamingCounters.windmillShuffleBytesRead().addValue(shuffleBytesRead);
      streamingCounters.windmillStateBytesRead().addValue(stateBytesRead);
      streamingCounters.windmillStateBytesWritten().addValue(stateBytesWritten);

      LOG.debug("Processing done for work token: {}", workItem.getWorkToken());
    } catch (Throwable t) {
      if (executionState != null) {
        try {
          executionState.context().invalidateCache();
          executionState.workExecutor().close();
        } catch (Exception e) {
          LOG.warn("Failed to close map task executor: ", e);
        } finally {
          // Release references to potentially large objects early.
          executionState = null;
        }
      }

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

  private static ShardedKey createShardedKey(Work work) {
    return ShardedKey.create(work.getWorkItem().getKey(), work.getWorkItem().getShardingKey());
  }

  private WorkItemCommitRequest buildWorkItemTruncationRequest(
      final ByteString key, final Windmill.WorkItem workItem, final int estimatedCommitSize) {
    Windmill.WorkItemCommitRequest.Builder outputBuilder = initializeOutputBuilder(key, workItem);
    outputBuilder.setExceedsMaxWorkItemCommitBytes(true);
    outputBuilder.setEstimatedWorkItemCommitBytes(estimatedCommitSize);
    return outputBuilder.build();
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

    Optional.ofNullable(computationMap.get(completeCommit.computationId()))
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

  private void getConfigFromWindmill(String computation) {
    Windmill.GetConfigRequest request =
        Windmill.GetConfigRequest.newBuilder().addComputations(computation).build();

    Windmill.GetConfigResponse response = windmillServer.getConfig(request);
    // The max work item commit bytes should be modified to be dynamic once it is available in
    // the request.
    for (Windmill.GetConfigResponse.SystemNameToComputationIdMapEntry entry :
        response.getSystemNameToComputationIdMapList()) {
      systemNameToComputationIdMap.put(entry.getSystemName(), entry.getComputationId());
    }

    // Outer keys are computation ids. Outer values are map from transform username to state family.
    Map<String, Map<String, String>> transformUserNameToStateFamilyByComputationId =
        new HashMap<>();
    for (Windmill.GetConfigResponse.ComputationConfigMapEntry computationConfig :
        response.getComputationConfigMapList()) {
      Map<String, String> transformUserNameToStateFamily =
          transformUserNameToStateFamilyByComputationId.computeIfAbsent(
              computationConfig.getComputationId(), k -> new HashMap<>());
      for (Windmill.ComputationConfig.TransformUserNameToStateFamilyEntry entry :
          computationConfig.getComputationConfig().getTransformUserNameToStateFamilyList()) {
        transformUserNameToStateFamily.put(entry.getTransformUserName(), entry.getStateFamily());
      }
    }

    for (String serializedMapTask : response.getCloudWorksList()) {
      try {
        MapTask mapTask = parseMapTask(serializedMapTask);
        String computationId =
            systemNameToComputationIdMap.containsKey(mapTask.getSystemName())
                ? systemNameToComputationIdMap.get(mapTask.getSystemName())
                : mapTask.getSystemName();
        addComputation(
            computationId,
            mapTask,
            transformUserNameToStateFamilyByComputationId.getOrDefault(
                computationId, ImmutableMap.of()));
      } catch (IOException e) {
        LOG.warn("Parsing MapTask failed: {}", serializedMapTask);
        LOG.warn("Error: ", e);
      }
    }
    for (Windmill.GetConfigResponse.NameMapEntry entry : response.getNameMapList()) {
      stateNameMap.put(entry.getUserName(), entry.getSystemName());
    }
  }

  /**
   * Sends a request to get configuration from Dataflow, either for a specific computation (if
   * computation is not null) or global configuration (if computation is null).
   *
   * @throws IOException if the RPC fails.
   */
  private void getConfigFromDataflowService(@Nullable String computation) throws IOException {
    Optional<WorkItem> workItem =
        computation != null
            ? workUnitClient.getStreamingConfigWorkItem(computation)
            : workUnitClient.getGlobalStreamingConfigWorkItem();

    if (!workItem.isPresent()) {
      return;
    }
    StreamingConfigTask config = workItem.get().getStreamingConfigTask();
    Preconditions.checkState(config != null);
    if (config.getUserStepToStateFamilyNameMap() != null) {
      stateNameMap.putAll(config.getUserStepToStateFamilyNameMap());
    }
    if (computation == null) {
      if (config.getMaxWorkItemCommitBytes() != null
          && config.getMaxWorkItemCommitBytes() > 0
          && config.getMaxWorkItemCommitBytes() <= Integer.MAX_VALUE) {
        setMaxWorkItemCommitBytes(config.getMaxWorkItemCommitBytes().intValue());
      } else {
        setMaxWorkItemCommitBytes(180 << 20);
      }
    }
    List<StreamingComputationConfig> configs = config.getStreamingComputationConfigs();
    if (configs != null) {
      for (StreamingComputationConfig computationConfig : configs) {
        MapTask mapTask = new MapTask();
        mapTask.setSystemName(computationConfig.getSystemName());
        mapTask.setStageName(computationConfig.getStageName());
        mapTask.setInstructions(computationConfig.getInstructions());
        addComputation(
            computationConfig.getComputationId(),
            mapTask,
            Optional.ofNullable(computationConfig.getTransformUserNameToStateFamily())
                .orElseGet(ImmutableMap::of));
      }
    }

    if (config.getWindmillServiceEndpoint() != null
        && !config.getWindmillServiceEndpoint().isEmpty()) {
      int port = 443;
      if (config.getWindmillServicePort() != null && config.getWindmillServicePort() != 0) {
        port = config.getWindmillServicePort().intValue();
      }
      HashSet<HostAndPort> endpoints = new HashSet<>();
      for (String endpoint : Splitter.on(',').split(config.getWindmillServiceEndpoint())) {
        endpoints.add(HostAndPort.fromString(endpoint).withDefaultPort(port));
      }
      windmillServer.setWindmillServiceEndpoints(endpoints);
    }
  }

  /**
   * Schedules a background thread that periodically sends getConfig requests to Dataflow Service to
   * obtain the windmill service endpoint. Blocks until the windmillServer is ready.
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  private void schedulePeriodicGlobalConfigRequests() {
    Preconditions.checkState(windmillServiceEnabled);
    if (!windmillServer.isReady()) {
      // Get the initial global configuration. This will initialize the windmillServer stub.
      while (true) {
        LOG.info("Sending request to get global configuration for this worker");
        getGlobalConfig();
        if (windmillServer.isReady()) {
          break;
        }
        LOG.info("windmillServerStub is not ready yet, will retry in 5 seconds");
        sleep(5000);
      }
    }
    LOG.info("windmillServerStub is now ready");

    // Now start a thread that periodically refreshes the windmill service endpoint.
    ScheduledExecutorService configRefreshTimer =
        executorSupplier.apply("GlobalConfigRefreshTimer");
    configRefreshTimer.scheduleWithFixedDelay(
        this::getGlobalConfig,
        0,
        options.getGlobalConfigRefreshPeriod().getMillis(),
        TimeUnit.MILLISECONDS);
    scheduledExecutors.add(configRefreshTimer);
  }

  private void getGlobalConfig() {
    // No need to pass a computation since we are only interested in the global config.
    getConfig(null);
  }

  // Attempts to populate computationMap with an entry for the computation. Upon
  // error or shutdown computationMap may remain unchanged.
  private void getConfig(String computation) {
    BackOff backoff =
        FluentBackoff.DEFAULT
            .withInitialBackoff(Duration.millis(100))
            .withMaxBackoff(Duration.standardMinutes(1))
            .withMaxCumulativeBackoff(Duration.standardMinutes(5))
            .backoff();
    while (running.get()) {
      try {
        if (windmillServiceEnabled) {
          getConfigFromDataflowService(computation);
        } else {
          getConfigFromWindmill(computation);
        }
        return;
      } catch (IllegalArgumentException | IOException e) {
        LOG.warn("Error fetching config: ", e);
        try {
          if (!BackOffUtils.next(Sleeper.DEFAULT, backoff)) {
            return;
          }
        } catch (IOException ioe) {
          LOG.warn("Error backing off, will not retry: ", ioe);
          return;
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
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

  private class HarnessDataProvider implements StatusDataProvider {
    @Override
    public void appendSummaryHtml(PrintWriter writer) {
      writer.println("Running: " + running.get() + "<br>");
      writer.println("ID: " + clientId + "<br>");
    }
  }

  private class SpecsServlet extends BaseStatusServlet {

    public SpecsServlet() {
      super("/specs");
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
      PrintWriter writer = response.getWriter();
      writer.println("<h1>Specs</h1>");
      for (Map.Entry<String, ComputationState> entry : computationMap.entrySet()) {
        writer.println("<h3>" + entry.getKey() + "</h3>");
        writer.print("<script>document.write(JSON.stringify(");
        writer.print(entry.getValue().getMapTask().toString());
        writer.println(", null, \"&nbsp&nbsp\").replace(/\\n/g, \"<br>\"))</script>");
      }
    }
  }

  private class MetricsDataProvider implements StatusDataProvider {

    @Override
    public void appendSummaryHtml(PrintWriter writer) {
      writer.println(workUnitExecutor.summaryHtml());

      writer.print("Active commit: ");
      appendHumanizedBytes(workCommitter.currentActiveCommitBytes(), writer);
      writer.println("<br>");

      metricTrackingWindmillServer.printHtml(writer);

      writer.println("<br>");

      writer.println("Active Keys: <br>");
      for (Map.Entry<String, ComputationState> computationEntry : computationMap.entrySet()) {
        writer.print(computationEntry.getKey());
        writer.print(":<br>");
        computationEntry.getValue().printActiveWork(writer);
        writer.println("<br>");
      }
    }

    private void appendHumanizedBytes(long bytes, PrintWriter writer) {
      if (bytes < (4 << 10)) {
        writer.print(bytes);
        writer.print("B");
      } else if (bytes < (4 << 20)) {
        writer.print("~");
        writer.print(bytes >> 10);
        writer.print("KB");
      } else {
        writer.print("~");
        writer.print(bytes >> 20);
        writer.print("MB");
      }
    }
  }
}
