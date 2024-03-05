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
import com.google.api.services.dataflow.model.PerStepNamespaceMetrics;
import com.google.api.services.dataflow.model.PerWorkerMetrics;
import com.google.api.services.dataflow.model.Status;
import com.google.api.services.dataflow.model.StreamingComputationConfig;
import com.google.api.services.dataflow.model.StreamingConfigTask;
import com.google.api.services.dataflow.model.StreamingScalingReport;
import com.google.api.services.dataflow.model.WorkItem;
import com.google.api.services.dataflow.model.WorkItemStatus;
import com.google.api.services.dataflow.model.WorkerMessage;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
import org.apache.beam.runners.dataflow.worker.DataflowSystemMetrics.StreamingSystemCounterNames;
import org.apache.beam.runners.dataflow.worker.apiary.FixMultiOutputInfosOnParDoInstructions;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingMDC;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler;
import org.apache.beam.runners.dataflow.worker.status.BaseStatusServlet;
import org.apache.beam.runners.dataflow.worker.status.DebugCapture;
import org.apache.beam.runners.dataflow.worker.status.DebugCapture.Capturable;
import org.apache.beam.runners.dataflow.worker.status.LastExceptionDataProvider;
import org.apache.beam.runners.dataflow.worker.status.StatusDataProvider;
import org.apache.beam.runners.dataflow.worker.status.WorkerStatusPages;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.ShardedKey;
import org.apache.beam.runners.dataflow.worker.streaming.StageInfo;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.streaming.WorkHeartbeatResponseProcessor;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputStateFetcher;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.LatencyAttribution;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.runners.dataflow.worker.windmill.appliance.JniWindmillApplianceServer;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamPool;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.CompleteCommit;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.StreamingApplianceWorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.StreamingEngineWorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.ChannelzServlet;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillServer;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillStreamFactory;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.CommitCallbackCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.ExecutionStateFactory;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.StreamingWorkExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.FailureReporter;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.StreamingApplianceFailureReporter;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.StreamingEngineFailureReporter;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.WorkFailureProcessor;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.ActiveWorkRefresher;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.ActiveWorkRefreshers;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.*;
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
  static final long TARGET_COMMIT_BUNDLE_BYTES = 32 << 20;
  static final int MAX_COMMIT_QUEUE_BYTES = 500 << 20; // 500MB
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

  private static final int DEFAULT_STATUS_PORT = 8081;
  // Maximum size of the result of a GetWork request.
  private static final long MAX_GET_WORK_FETCH_BYTES = 64L << 20; // 64m
  // Reserved ID for counter updates.
  // Matches kWindmillCounterUpdate in workflow_worker_service_multi_hubs.cc.
  private static final String WINDMILL_COUNTER_UPDATE_WORK_ID = "3";
  /** Maximum number of failure stacktraces to report in each update sent to backend. */
  private static final int MAX_FAILURES_TO_REPORT_IN_UPDATE = 1000;

  private static final Random clientIdGenerator = new Random();
  private static final String CHANNELZ_PATH = "/channelz";
  final WindmillStateCache stateCache;
  // Maps from computation ids to per-computation state.
  private final ConcurrentMap<String, ComputationState> computationMap;

  // Map of user state names to system state names.
  // TODO(drieber): obsolete stateNameMap. Use transformUserNameToStateFamily in
  // ComputationState instead.
  private final ConcurrentMap<String, String> stateNameMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, String> systemNameToComputationIdMap =
      new ConcurrentHashMap<>();
  private final BoundedQueueExecutor workUnitExecutor;
  private final WindmillServerStub windmillServer;
  private final Thread dispatchThread;
  private final AtomicLong previousTimeAtMaxThreads = new AtomicLong();
  private final AtomicBoolean running = new AtomicBoolean();
  private final DataflowWorkerHarnessOptions options;
  private final boolean windmillServiceEnabled;
  private final long clientId;
  private final MetricTrackingWindmillServerStub metricTrackingWindmillServer;
  private final CounterSet pendingDeltaCounters = new CounterSet();
  private final CounterSet pendingCumulativeCounters = new CounterSet();

  // Map from stage name to StageInfo containing metrics container registry and per stage counters.
  private final ConcurrentMap<String, StageInfo> stageInfoMap = new ConcurrentHashMap<>();
  // Built-in delta counters.
  private final Counter<Long, Long> windmillShuffleBytesRead;
  private final Counter<Long, Long> windmillStateBytesRead;
  private final Counter<Long, Long> windmillStateBytesWritten;
  private final Counter<Long, Long> windmillQuotaThrottling;
  private final Counter<Long, Long> timeAtMaxActiveThreads;
  // Built-in cumulative counters.
  private final Counter<Long, Long> javaHarnessUsedMemory;
  private final Counter<Long, Long> javaHarnessMaxMemory;
  private final Counter<Integer, Integer> activeThreads;
  private final Counter<Integer, Integer> totalAllocatedThreads;
  private final Counter<Long, Long> outstandingBytes;
  private final Counter<Long, Long> maxOutstandingBytes;
  private final Counter<Long, Long> outstandingBundles;
  private final Counter<Long, Long> maxOutstandingBundles;
  private final Counter<Integer, Integer> windmillMaxObservedWorkItemCommitBytes;
  private final Counter<Integer, Integer> memoryThrashing;
  private final boolean publishCounters;
  private final MemoryMonitor memoryMonitor;
  private final Thread memoryMonitorThread;
  private final WorkerStatusPages statusPages;
  // Limit on bytes sinked (committed) in a work item.
  private final long maxSinkBytes; // = MAX_SINK_BYTES unless disabled in options.
  private final ReaderCache readerCache;
  private final WorkUnitClient workUnitClient;
  private final CompletableFuture<Void> isDoneFuture;
  private final Supplier<Instant> clock;
  private final Function<String, ScheduledExecutorService> executorSupplier;
  // Periodic sender of debug information to the debug capture service.
  private final DebugCapture.@Nullable Manager debugCaptureManager;
  // Collection of ScheduledExecutorServices that are running periodic functions.
  private final ArrayList<ScheduledExecutorService> scheduledExecutors = new ArrayList<>();
  private int retryLocallyDelayMs = 10000;
  // Periodically fires a global config request to dataflow service. Only used when windmill service
  // is enabled.
  // Possibly overridden by streaming engine config.
  private int maxWorkItemCommitBytes = Integer.MAX_VALUE;

  private final DataflowExecutionStateSampler sampler = DataflowExecutionStateSampler.instance();
  private final ActiveWorkRefresher activeWorkRefresher;
  private final WorkCommitter workCommitter;
  private final StreamingWorkExecutor streamingWorkExecutor;
  private final Supplier<ImmutableList<Status>> failuresToReport;

  private StreamingDataflowWorker(
      WindmillServerStub windmillServer,
      long clientId,
      ConcurrentMap<String, ComputationState> computationMap,
      WindmillStateCache windmillStateCache,
      BoundedQueueExecutor workUnitExecutor,
      DataflowMapTaskExecutorFactory mapTaskExecutorFactory,
      WorkUnitClient workUnitClient,
      DataflowWorkerHarnessOptions options,
      boolean publishCounters,
      HotKeyLogger hotKeyLogger,
      Supplier<Instant> clock,
      Function<String, ScheduledExecutorService> executorSupplier) {
    this.computationMap = computationMap;
    this.stateCache = windmillStateCache;
    this.readerCache =
        new ReaderCache(
            Duration.standardSeconds(options.getReaderCacheTimeoutSec()),
            Executors.newCachedThreadPool());
    this.workUnitClient = workUnitClient;
    this.options = options;
    this.clock = clock;
    this.executorSupplier = executorSupplier;
    this.windmillServiceEnabled = options.isEnableStreamingEngine();
    this.memoryMonitor = MemoryMonitor.fromOptions(options);
    this.statusPages = WorkerStatusPages.create(DEFAULT_STATUS_PORT, memoryMonitor, () -> true);
    if (windmillServiceEnabled) {
      this.debugCaptureManager =
          new DebugCapture.Manager(options, statusPages.getDebugCapturePages());
    } else {
      this.debugCaptureManager = null;
    }
    this.windmillShuffleBytesRead =
        pendingDeltaCounters.longSum(
            StreamingSystemCounterNames.WINDMILL_SHUFFLE_BYTES_READ.counterName());
    this.windmillStateBytesRead =
        pendingDeltaCounters.longSum(
            StreamingSystemCounterNames.WINDMILL_STATE_BYTES_READ.counterName());
    this.windmillStateBytesWritten =
        pendingDeltaCounters.longSum(
            StreamingSystemCounterNames.WINDMILL_STATE_BYTES_WRITTEN.counterName());
    this.windmillQuotaThrottling =
        pendingDeltaCounters.longSum(
            StreamingSystemCounterNames.WINDMILL_QUOTA_THROTTLING.counterName());
    this.timeAtMaxActiveThreads =
        pendingDeltaCounters.longSum(
            StreamingSystemCounterNames.TIME_AT_MAX_ACTIVE_THREADS.counterName());
    this.javaHarnessUsedMemory =
        pendingCumulativeCounters.longSum(
            StreamingSystemCounterNames.JAVA_HARNESS_USED_MEMORY.counterName());
    this.javaHarnessMaxMemory =
        pendingCumulativeCounters.longSum(
            StreamingSystemCounterNames.JAVA_HARNESS_MAX_MEMORY.counterName());
    this.activeThreads =
        pendingCumulativeCounters.intSum(StreamingSystemCounterNames.ACTIVE_THREADS.counterName());
    this.outstandingBytes =
        pendingCumulativeCounters.longSum(
            StreamingSystemCounterNames.OUTSTANDING_BYTES.counterName());
    this.maxOutstandingBytes =
        pendingCumulativeCounters.longSum(
            StreamingSystemCounterNames.MAX_OUTSTANDING_BYTES.counterName());
    this.outstandingBundles =
        pendingCumulativeCounters.longSum(
            StreamingSystemCounterNames.OUTSTANDING_BUNDLES.counterName());
    this.maxOutstandingBundles =
        pendingCumulativeCounters.longSum(
            StreamingSystemCounterNames.MAX_OUTSTANDING_BUNDLES.counterName());
    this.totalAllocatedThreads =
        pendingCumulativeCounters.intSum(
            StreamingSystemCounterNames.TOTAL_ALLOCATED_THREADS.counterName());
    this.windmillMaxObservedWorkItemCommitBytes =
        pendingCumulativeCounters.intMax(
            StreamingSystemCounterNames.WINDMILL_MAX_WORK_ITEM_COMMIT_BYTES.counterName());
    this.memoryThrashing =
        pendingCumulativeCounters.intSum(
            StreamingSystemCounterNames.MEMORY_THRASHING.counterName());
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

    this.publishCounters = publishCounters;
    this.clientId = clientId;
    this.windmillServer = windmillServer;
    this.metricTrackingWindmillServer =
        MetricTrackingWindmillServerStub.builder(windmillServer, memoryMonitor)
            .setUseStreamingRequests(windmillServiceEnabled)
            .setUseSeparateHeartbeatStreams(options.getUseSeparateWindmillHeartbeatStreams())
            .setNumGetDataStreams(options.getWindmillGetDataStreamCount())
            .build();

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
                this::onStreamingCommitComplete)
            : StreamingApplianceWorkCommitter.create(
                windmillServer::commitWork, this::onStreamingCommitComplete);

    FailureReporter failureReporter =
        windmillServiceEnabled
            ? StreamingEngineFailureReporter.create(
                MAX_FAILURES_TO_REPORT_IN_UPDATE, options.getMaxStackTraceDepthToReport())
            : StreamingApplianceFailureReporter.create(
                MAX_FAILURES_TO_REPORT_IN_UPDATE,
                options.getMaxStackTraceDepthToReport(),
                windmillServer::reportStats);

    SideInputStateFetcher sideInputStateFetcher =
        new SideInputStateFetcher(metricTrackingWindmillServer::getSideInputData, options);
    this.streamingWorkExecutor =
        new StreamingWorkExecutor(
            workCommitter,
            stageName ->
                stageInfoMap.computeIfAbsent(stageName, s -> StageInfo.create(s, stageName)),
            sampler,
            options,
            (computationId, request) ->
                Optional.ofNullable(
                    metricTrackingWindmillServer.getStateData(computationId, request)),
            sideInputStateFetcher::byteTrackingView,
            new CommitCallbackCache(workUnitExecutor, java.time.Duration.ofMinutes(5L)),
            failureReporter,
            new WorkFailureProcessor(
                workUnitExecutor,
                failureReporter,
                () -> Optional.ofNullable(memoryMonitor.tryToDumpHeap()),
                clock,
                retryLocallyDelayMs),
            hotKeyLogger,
            maxWorkItemCommitBytes,
            windmillShuffleBytesRead,
            windmillStateBytesRead,
            windmillStateBytesWritten,
            windmillMaxObservedWorkItemCommitBytes,
            ExecutionStateFactory.createDefault(
                () -> readerCache,
                () -> stateNameMap,
                stateCache::forComputation,
                options,
                pendingDeltaCounters,
                idGenerator,
                sampler,
                mapTaskExecutorFactory,
                maxSinkBytes));

    this.failuresToReport = failureReporter;

    // Register standard file systems.
    FileSystems.setDefaultPipelineOptions(options);

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

    LOG.debug("windmillServiceEnabled: {}", windmillServiceEnabled);
    LOG.debug("WindmillServiceEndpoint: {}", options.getWindmillServiceEndpoint());
    LOG.debug("WindmillServicePort: {}", options.getWindmillServicePort());
    LOG.debug("LocalWindmillHostport: {}", options.getLocalWindmillHostport());
    LOG.debug("maxWorkItemCommitBytes: {}", maxWorkItemCommitBytes);
  }

  public static StreamingDataflowWorker fromOptions(DataflowWorkerHarnessOptions options) {
    ConcurrentMap<String, ComputationState> computationMap = new ConcurrentHashMap<>();
    long clientId = clientIdGenerator.nextLong();
    return new StreamingDataflowWorker(
        createWindmillServerStub(
            options,
            clientId,
            new WorkHeartbeatResponseProcessor(
                computationId -> Optional.ofNullable(computationMap.get(computationId)))),
        clientId,
        computationMap,
        WindmillStateCache.ofSizeMbs(options.getWorkerCacheMb()),
        createWorkUnitExecutor(options),
        IntrinsicMapTaskExecutorFactory.defaultFactory(),
        new DataflowWorkUnitClient(options, LOG),
        options,
        /* publishCounters= */ true,
        new HotKeyLogger(),
        Instant::now,
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
      Function<String, ScheduledExecutorService> executorSupplier) {
    BoundedQueueExecutor boundedQueueExecutor = createWorkUnitExecutor(options);
    WindmillStateCache stateCache = WindmillStateCache.ofSizeMbs(options.getWorkerCacheMb());
    computationMap.putAll(
        createComputationMapForTesting(mapTasks, boundedQueueExecutor, stateCache::forComputation));
    return new StreamingDataflowWorker(
        windmillServer,
        1L,
        computationMap,
        stateCache,
        boundedQueueExecutor,
        mapTaskExecutorFactory,
        workUnitClient,
        options,
        publishCounters,
        hotKeyLogger,
        clock,
        executorSupplier);
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

    LOG.debug("Creating StreamingDataflowWorker from options: {}", options);
    StreamingDataflowWorker worker = StreamingDataflowWorker.fromOptions(options);

    // Use the MetricsLogger container which is used by BigQueryIO to periodically log process-wide
    // metrics.
    MetricsEnvironment.setProcessWideContainer(new MetricsLogger(null));

    // When enabled, the Pipeline will record Per-Worker metrics that will be piped to DFE.
    StreamingStepMetricsContainer.setEnablePerWorkerMetrics(
        options.isEnableStreamingEngine()
            && DataflowRunner.hasExperiment(options, "enable_per_worker_metrics"));
    // StreamingStepMetricsContainer automatically deletes perWorkerCounters if they are zero-valued
    // for longer than 5 minutes.
    BigQuerySinkMetrics.setSupportMetricsDeletion(true);

    JvmInitializers.runBeforeProcessing(options);
    worker.startStatusPages();
    worker.start();
  }

  private static WindmillServerStub createWindmillServerStub(
      DataflowWorkerHarnessOptions options,
      long clientId,
      Consumer<List<Windmill.ComputationHeartbeatResponse>> processHeartbeatResponses) {
    if (options.getWindmillServiceEndpoint() != null
        || options.isEnableStreamingEngine()
        || options.getLocalWindmillHostport().startsWith("grpc:")) {
      try {
        Duration maxBackoff =
            !options.isEnableStreamingEngine() && options.getLocalWindmillHostport() != null
                ? GrpcWindmillServer.LOCALHOST_MAX_BACKOFF
                : Duration.millis(options.getWindmillServiceStreamMaxBackoffMillis());
        GrpcWindmillStreamFactory windmillStreamFactory =
            GrpcWindmillStreamFactory.of(
                    JobHeader.newBuilder()
                        .setJobId(options.getJobId())
                        .setProjectId(options.getProject())
                        .setWorkerId(options.getWorkerId())
                        .setClientId(clientId)
                        .build())
                .setWindmillMessagesBetweenIsReadyChecks(
                    options.getWindmillMessagesBetweenIsReadyChecks())
                .setMaxBackOffSupplier(() -> maxBackoff)
                .setLogEveryNStreamFailures(
                    options.getWindmillServiceStreamingLogEveryNStreamFailures())
                .setStreamingRpcBatchLimit(options.getWindmillServiceStreamingRpcBatchLimit())
                .build();
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

  void addStateNameMappings(Map<String, String> nameMap) {
    stateNameMap.putAll(nameMap);
  }

  @VisibleForTesting
  public void setRetryLocallyDelayMs(int retryLocallyDelayMs) {
    this.retryLocallyDelayMs = retryLocallyDelayMs;
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

    // Periodically report workers counters and other updates.
    ScheduledExecutorService workerUpdateTimer = executorSupplier.apply("GlobalWorkerUpdates");
    workerUpdateTimer.scheduleWithFixedDelay(
        this::reportPeriodicWorkerUpdates,
        0,
        options.getWindmillHarnessUpdateReportingPeriod().getMillis(),
        TimeUnit.MILLISECONDS);
    scheduledExecutors.add(workerUpdateTimer);

    ScheduledExecutorService workerMessageTimer = executorSupplier.apply("ReportWorkerMessage");
    if (options.getWindmillHarnessUpdateReportingPeriod().getMillis() > 0) {
      workerMessageTimer.scheduleWithFixedDelay(
          this::reportPeriodicWorkerMessage,
          0,
          options.getWindmillHarnessUpdateReportingPeriod().getMillis(),
          TimeUnit.MILLISECONDS);
      scheduledExecutors.add(workerMessageTimer);
    }

    activeWorkRefresher.start();

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
    reportHarnessStartup();
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

  public void addWorkerStatusPage(BaseStatusServlet page) {
    statusPages.addServlet(page);
    if (page instanceof Capturable) {
      statusPages.addCapturePage((Capturable) page);
    }
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

      // one last send
      reportPeriodicWorkerUpdates();
      reportPeriodicWorkerMessage();
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
                streamingWorkExecutor.execute(
                    computationState,
                    inputDataWatermark,
                    outputDataWatermark,
                    synchronizedProcessingTime,
                    work));
    computationState.activateWork(
        ShardedKey.create(workItem.getKey(), workItem.getShardingKey()), scheduledWork);
  }

  private void onStreamingCommitComplete(CompleteCommit completeCommit) {
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
        pendingDeltaCounters.extractModifiedDeltaUpdates(DataflowCounterUpdateExtractor.INSTANCE),
        pendingCumulativeCounters.extractUpdates(false, DataflowCounterUpdateExtractor.INSTANCE));
  }

  private void reportHarnessStartup() {
    DataflowWorkerLoggingMDC.setStageName("startup");
    CounterSet restartCounter = new CounterSet();
    restartCounter
        .longSum(StreamingSystemCounterNames.JAVA_HARNESS_RESTARTS.counterName())
        .addValue(1L);
    try {
      // Sending a one time update. Use empty counter set for cumulativeCounters (2nd arg).
      sendWorkerUpdatesToDataflowService(restartCounter, new CounterSet());
    } catch (IOException e) {
      LOG.warn("Failed to send harness startup counter", e);
    }
  }

  /** Updates VM metrics like memory and CPU utilization. */
  private void updateVMMetrics() {
    Runtime rt = Runtime.getRuntime();
    long usedMemory = rt.totalMemory() - rt.freeMemory();
    long maxMemory = rt.maxMemory();

    javaHarnessUsedMemory.getAndReset();
    javaHarnessUsedMemory.addValue(usedMemory);
    javaHarnessMaxMemory.getAndReset();
    javaHarnessMaxMemory.addValue(maxMemory);
  }

  private void updateThreadMetrics() {
    timeAtMaxActiveThreads.getAndReset();
    long allThreadsActiveTime = workUnitExecutor.allThreadsActiveTime();
    timeAtMaxActiveThreads.addValue(allThreadsActiveTime - previousTimeAtMaxThreads.get());
    previousTimeAtMaxThreads.set(allThreadsActiveTime);
    activeThreads.getAndReset();
    activeThreads.addValue(workUnitExecutor.activeCount());
    totalAllocatedThreads.getAndReset();
    totalAllocatedThreads.addValue(chooseMaximumNumberOfThreads());
    outstandingBytes.getAndReset();
    outstandingBytes.addValue(workUnitExecutor.bytesOutstanding());
    maxOutstandingBytes.getAndReset();
    maxOutstandingBytes.addValue(workUnitExecutor.maximumBytesOutstanding());
    outstandingBundles.getAndReset();
    outstandingBundles.addValue((long) workUnitExecutor.elementsOutstanding());
    maxOutstandingBundles.getAndReset();
    maxOutstandingBundles.addValue((long) workUnitExecutor.maximumElementsOutstanding());
  }

  private WorkerMessage createWorkerMessageForStreamingScalingReport() {
    StreamingScalingReport activeThreadsReport =
        new StreamingScalingReport()
            .setActiveThreadCount(workUnitExecutor.activeCount())
            .setActiveBundleCount(workUnitExecutor.elementsOutstanding())
            .setOutstandingBytes(workUnitExecutor.bytesOutstanding())
            .setMaximumThreadCount(chooseMaximumNumberOfThreads())
            .setMaximumBundleCount(workUnitExecutor.maximumElementsOutstanding())
            .setMaximumBytes(workUnitExecutor.maximumBytesOutstanding());
    return workUnitClient.createWorkerMessageFromStreamingScalingReport(activeThreadsReport);
  }

  private Optional<WorkerMessage> createWorkerMessageForPerWorkerMetrics() {
    List<PerStepNamespaceMetrics> metrics = new ArrayList<>();
    stageInfoMap.values().forEach(s -> metrics.addAll(s.extractPerWorkerMetricValues()));

    if (metrics.isEmpty()) {
      return Optional.empty();
    }

    PerWorkerMetrics perWorkerMetrics = new PerWorkerMetrics().setPerStepNamespaceMetrics(metrics);
    return Optional.of(workUnitClient.createWorkerMessageFromPerWorkerMetrics(perWorkerMetrics));
  }

  private void sendWorkerMessage() throws IOException {
    List<WorkerMessage> workerMessages = new ArrayList<WorkerMessage>(2);
    workerMessages.add(createWorkerMessageForStreamingScalingReport());

    if (StreamingStepMetricsContainer.getEnablePerWorkerMetrics()) {
      Optional<WorkerMessage> metricsMsg = createWorkerMessageForPerWorkerMetrics();
      if (metricsMsg.isPresent()) {
        workerMessages.add(metricsMsg.get());
      }
    }

    workUnitClient.reportWorkerMessage(workerMessages);
  }

  @VisibleForTesting
  public void reportPeriodicWorkerUpdates() {
    updateVMMetrics();
    updateThreadMetrics();
    try {
      sendWorkerUpdatesToDataflowService(pendingDeltaCounters, pendingCumulativeCounters);
    } catch (IOException e) {
      LOG.warn("Failed to send periodic counter updates", e);
    } catch (Exception e) {
      LOG.error("Unexpected exception while trying to send counter updates", e);
    }
  }

  @VisibleForTesting
  public void reportPeriodicWorkerMessage() {
    try {
      sendWorkerMessage();
    } catch (IOException e) {
      LOG.warn("Failed to send worker messages", e);
    } catch (Exception e) {
      LOG.error("Unexpected exception while trying to send worker messages", e);
    }
  }

  /**
   * Returns key for a counter update. It is a String in case of legacy counter and
   * CounterStructuredName in the case of a structured counter.
   */
  private Object getCounterUpdateKey(CounterUpdate counterUpdate) {
    Object key = null;
    if (counterUpdate.getNameAndKind() != null) {
      key = counterUpdate.getNameAndKind().getName();
    } else if (counterUpdate.getStructuredNameAndMetadata() != null) {
      key = counterUpdate.getStructuredNameAndMetadata().getName();
    }
    checkArgument(key != null, "Could not find name for CounterUpdate: %s", counterUpdate);
    return key;
  }

  /** Sends counter updates to Dataflow backend. */
  private void sendWorkerUpdatesToDataflowService(
      CounterSet deltaCounters, CounterSet cumulativeCounters) throws IOException {
    // Throttle time is tracked by the windmillServer but is reported to DFE here.
    windmillQuotaThrottling.addValue(windmillServer.getAndResetThrottleTime());
    if (memoryMonitor.isThrashing()) {
      memoryThrashing.addValue(1);
    }

    List<CounterUpdate> counterUpdates = new ArrayList<>(128);

    if (publishCounters) {
      stageInfoMap.values().forEach(s -> counterUpdates.addAll(s.extractCounterUpdates()));
      counterUpdates.addAll(
          cumulativeCounters.extractUpdates(false, DataflowCounterUpdateExtractor.INSTANCE));
      counterUpdates.addAll(
          deltaCounters.extractModifiedDeltaUpdates(DataflowCounterUpdateExtractor.INSTANCE));
    }

    // Handle duplicate counters from different stages. Store all the counters in a multi-map and
    // send the counters that appear multiple times in separate RPCs. Same logical counter could
    // appear in multiple stages if a step runs in multiple stages (as with flatten-unzipped stages)
    // especially if the counter definition does not set execution_step_name.
    ListMultimap<Object, CounterUpdate> counterMultimap =
        MultimapBuilder.hashKeys(counterUpdates.size()).linkedListValues().build();
    boolean hasDuplicates = false;

    for (CounterUpdate c : counterUpdates) {
      Object key = getCounterUpdateKey(c);
      if (counterMultimap.containsKey(key)) {
        hasDuplicates = true;
      }
      counterMultimap.put(key, c);
    }

    // Clears counterUpdates and enqueues unique counters from counterMultimap. If a counter
    // appears more than once, one of them is extracted leaving the remaining in the map.
    Runnable extractUniqueCounters =
        () -> {
          counterUpdates.clear();
          for (Iterator<Object> iter = counterMultimap.keySet().iterator(); iter.hasNext(); ) {
            List<CounterUpdate> counters = counterMultimap.get(iter.next());
            counterUpdates.add(counters.get(0));
            if (counters.size() == 1) {
              // There is single value. Remove the entry through the iterator.
              iter.remove();
            } else {
              // Otherwise remove the first value.
              counters.remove(0);
            }
          }
        };

    if (hasDuplicates) {
      extractUniqueCounters.run();
    } else { // Common case: no duplicates. We can just send counterUpdates, empty the multimap.
      counterMultimap.clear();
    }

    WorkItemStatus workItemStatus =
        new WorkItemStatus()
            .setWorkItemId(WINDMILL_COUNTER_UPDATE_WORK_ID)
            .setErrors(failuresToReport.get())
            .setCounterUpdates(counterUpdates);

    workUnitClient.reportWorkItemStatus(workItemStatus);

    // Send any counters appearing more than once in subsequent RPCs:
    while (!counterMultimap.isEmpty()) {
      extractUniqueCounters.run();
      workUnitClient.reportWorkItemStatus(
          new WorkItemStatus()
              .setWorkItemId(WINDMILL_COUNTER_UPDATE_WORK_ID)
              .setCounterUpdates(counterUpdates));
    }
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
