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
import static org.apache.beam.runners.dataflow.DataflowRunner.hasExperiment;
import static org.apache.beam.runners.dataflow.worker.DataflowSystemMetrics.THROTTLING_MSECS_METRIC_NAME;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.dataflow.model.CounterStructuredName;
import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.MapTask;
import com.google.api.services.dataflow.model.Status;
import com.google.api.services.dataflow.model.StreamingComputationConfig;
import com.google.api.services.dataflow.model.StreamingConfigTask;
import com.google.api.services.dataflow.model.WorkItem;
import com.google.api.services.dataflow.model.WorkItemStatus;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RemoteGrpcPort;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.internal.CustomSources;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.worker.DataflowSystemMetrics.StreamingPerStageSystemCounterNames;
import org.apache.beam.runners.dataflow.worker.DataflowSystemMetrics.StreamingSystemCounterNames;
import org.apache.beam.runners.dataflow.worker.SdkHarnessRegistry.SdkWorkerHarness;
import org.apache.beam.runners.dataflow.worker.StreamingDataflowWorker.Work.State;
import org.apache.beam.runners.dataflow.worker.StreamingModeExecutionContext.StreamingModeExecutionStateRegistry;
import org.apache.beam.runners.dataflow.worker.apiary.FixMultiOutputInfosOnParDoInstructions;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.CounterUpdateAggregators;
import org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.graph.CloneAmbiguousFlattensFunction;
import org.apache.beam.runners.dataflow.worker.graph.CreateRegisterFnOperationFunction;
import org.apache.beam.runners.dataflow.worker.graph.DeduceFlattenLocationsFunction;
import org.apache.beam.runners.dataflow.worker.graph.DeduceNodeLocationsFunction;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.InsertFetchAndFilterStreamingSideInputNodes;
import org.apache.beam.runners.dataflow.worker.graph.LengthPrefixUnknownCoders;
import org.apache.beam.runners.dataflow.worker.graph.MapTaskToNetworkFunction;
import org.apache.beam.runners.dataflow.worker.graph.Networks;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.InstructionOutputNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.RemoteGrpcPortNode;
import org.apache.beam.runners.dataflow.worker.graph.RegisterNodeFunction;
import org.apache.beam.runners.dataflow.worker.graph.ReplacePgbkWithPrecombineFunction;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingMDC;
import org.apache.beam.runners.dataflow.worker.options.StreamingDataflowWorkerOptions;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler;
import org.apache.beam.runners.dataflow.worker.status.BaseStatusServlet;
import org.apache.beam.runners.dataflow.worker.status.DebugCapture;
import org.apache.beam.runners.dataflow.worker.status.DebugCapture.Capturable;
import org.apache.beam.runners.dataflow.worker.status.LastExceptionDataProvider;
import org.apache.beam.runners.dataflow.worker.status.StatusDataProvider;
import org.apache.beam.runners.dataflow.worker.status.WorkerStatusPages;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputObjectAndByteCounter;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ReadOperation;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItemCommitRequest;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub.CommitWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub.GetWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub.StreamPool;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.fn.JvmInitializers;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.TextFormat;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.EvictingQueue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.MultimapBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.MutableNetwork;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.net.HostAndPort;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implements a Streaming Dataflow worker. */
public class StreamingDataflowWorker {
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

  private static Random clientIdGenerator = new Random();

  // Maximum number of threads for processing.  Currently each thread processes one key at a time.
  static final int MAX_PROCESSING_THREADS = 300;
  static final long THREAD_EXPIRATION_TIME_SEC = 60;
  // Maximum work units retrieved from Windmill and queued before processing. Limiting this delays
  // retrieving extra work from Windmill without working on it, leading to better
  // prioritization / utilization.
  static final int MAX_WORK_UNITS_QUEUED = 100;
  static final long TARGET_COMMIT_BUNDLE_BYTES = 32 << 20;
  static final int MAX_COMMIT_QUEUE_BYTES = 500 << 20; // 500MB
  static final int NUM_COMMIT_STREAMS = 1;
  static final int GET_WORK_STREAM_TIMEOUT_MINUTES = 3;
  static final Duration COMMIT_STREAM_TIMEOUT = Duration.standardMinutes(1);

  private static final int DEFAULT_STATUS_PORT = 8081;

  // Maximum size of the result of a GetWork request.
  private static final long MAX_GET_WORK_FETCH_BYTES = 64L << 20; // 64m

  // Reserved ID for counter updates.
  // Matches kWindmillCounterUpdate in workflow_worker_service_multi_hubs.cc.
  private static final String WINDMILL_COUNTER_UPDATE_WORK_ID = "3";

  /** Maximum number of items to return in a GetWork request. */
  private static final long MAX_GET_WORK_ITEMS = MAX_WORK_UNITS_QUEUED + MAX_PROCESSING_THREADS;

  /** Maximum number of failure stacktraces to report in each update sent to backend. */
  private static final int MAX_FAILURES_TO_REPORT_IN_UPDATE = 1000;

  private final AtomicLong counterAggregationErrorCount = new AtomicLong();

  /** Returns whether an exception was caused by a {@link OutOfMemoryError}. */
  private static boolean isOutOfMemoryError(Throwable t) {
    while (t != null) {
      if (t instanceof OutOfMemoryError) {
        return true;
      }
      t = t.getCause();
    }
    return false;
  }

  private static class KeyCommitTooLargeException extends Exception {
    public static KeyCommitTooLargeException causedBy(
        String computationId, long byteLimit, WorkItemCommitRequest request) {
      StringBuilder message = new StringBuilder();
      message.append("Commit request for stage ");
      message.append(computationId);
      message.append(" and key ");
      message.append(request.getKey().toStringUtf8());
      if (request.getSerializedSize() > 0) {
        message.append(
            " has size "
                + request.getSerializedSize()
                + " which is more than the limit of "
                + byteLimit);
      } else {
        message.append(" is larger than 2GB and cannot be processed");
      }
      message.append(
          ". This may be caused by grouping a very "
              + "large amount of data in a single window without using Combine,"
              + " or by producing a large amount of data from a single input element.");
      return new KeyCommitTooLargeException(message.toString());
    }

    private KeyCommitTooLargeException(String message) {
      super(message);
    }
  }

  private static MapTask parseMapTask(String input) throws IOException {
    return Transport.getJsonFactory().fromString(input, MapTask.class);
  }

  public static void main(String[] args) throws Exception {
    JvmInitializers.runOnStartup();

    DataflowWorkerHarnessHelper.initializeLogging(StreamingDataflowWorker.class);
    DataflowWorkerHarnessOptions options =
        DataflowWorkerHarnessHelper.initializeGlobalStateAndPipelineOptions(
            StreamingDataflowWorker.class);
    DataflowWorkerHarnessHelper.configureLogging(options);
    checkArgument(
        options.isStreaming(),
        "%s instantiated with options indicating batch use",
        StreamingDataflowWorker.class.getName());

    checkArgument(
        !DataflowRunner.hasExperiment(options, "beam_fn_api"),
        "%s cannot be main() class with beam_fn_api enabled",
        StreamingDataflowWorker.class.getSimpleName());

    // Create a non fnapi registry
    SdkHarnessRegistry sdkHarnessRegistry = SdkHarnessRegistries.emptySdkHarnessRegistry();
    StreamingDataflowWorker worker =
        StreamingDataflowWorker.fromDataflowWorkerHarnessOptions(options, sdkHarnessRegistry);

    JvmInitializers.runBeforeProcessing(options);
    worker.startStatusPages();
    worker.start();
  }

  /** Bounded set of queues, with a maximum total weight. */
  private static class WeightedBoundedQueue<V> {
    private final LinkedBlockingQueue<V> queue = new LinkedBlockingQueue<>();
    private final int maxWeight;
    private final Semaphore limit;
    private final Function<V, Integer> weigher;

    public WeightedBoundedQueue(int maxWeight, Function<V, Integer> weigher) {
      this.maxWeight = maxWeight;
      this.limit = new Semaphore(maxWeight, true);
      this.weigher = weigher;
    }

    /**
     * Adds the value to the queue, blocking if this would cause the overall weight to exceed the
     * limit.
     */
    public void put(V value) {
      limit.acquireUninterruptibly(weigher.apply(value));
      queue.add(value);
    }

    /** Returns and removes the next value, or null if there is no such value. */
    @Nullable
    public V poll() {
      V result = queue.poll();
      if (result != null) {
        limit.release(weigher.apply(result));
      }
      return result;
    }

    /**
     * Retrieves and removes the head of this queue, waiting up to the specified wait time if
     * necessary for an element to become available.
     *
     * @param timeout how long to wait before giving up, in units of {@code unit}
     * @param unit a {@code TimeUnit} determining how to interpret the {@code timeout} parameter
     * @return the head of this queue, or {@code null} if the specified waiting time elapses before
     *     an element is available
     * @throws InterruptedException if interrupted while waiting
     */
    @Nullable
    public V poll(long timeout, TimeUnit unit) throws InterruptedException {
      V result = queue.poll(timeout, unit);
      if (result != null) {
        limit.release(weigher.apply(result));
      }
      return result;
    }

    /** Returns and removes the next value, or blocks until one is available. */
    @Nullable
    public V take() throws InterruptedException {
      V result = queue.take();
      limit.release(weigher.apply(result));
      return result;
    }

    /** Returns the current weight of the queue. */
    public int weight() {
      return maxWeight - limit.availablePermits();
    }

    public int size() {
      return queue.size();
    }
  }

  // Value class for a queued commit.
  static class Commit {
    private Windmill.WorkItemCommitRequest request;
    private ComputationState computationState;
    private Work work;

    public Commit(
        Windmill.WorkItemCommitRequest request, ComputationState computationState, Work work) {
      this.request = request;
      assert request.getSerializedSize() > 0;
      this.computationState = computationState;
      this.work = work;
    }

    public Windmill.WorkItemCommitRequest getRequest() {
      return request;
    }

    public ComputationState getComputationState() {
      return computationState;
    }

    public Work getWork() {
      return work;
    }

    public int getSize() {
      return request.getSerializedSize();
    }
  }

  // Maps from computation ids to per-computation state.
  private final ConcurrentMap<String, ComputationState> computationMap = new ConcurrentHashMap<>();
  private final WeightedBoundedQueue<Commit> commitQueue =
      new WeightedBoundedQueue<>(
          MAX_COMMIT_QUEUE_BYTES, commit -> Math.min(MAX_COMMIT_QUEUE_BYTES, commit.getSize()));

  // Map of tokens to commit callbacks.
  private final ConcurrentMap<Long, Runnable> commitCallbacks = new ConcurrentHashMap<>();

  // Map of user state names to system state names.
  // TODO(drieber): obsolete stateNameMap. Use transformUserNameToStateFamily in
  // ComputationState instead.
  private final ConcurrentMap<String, String> stateNameMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, String> systemNameToComputationIdMap =
      new ConcurrentHashMap<>();

  private final WindmillStateCache stateCache = new WindmillStateCache();

  private final ThreadFactory threadFactory;
  private DataflowMapTaskExecutorFactory mapTaskExecutorFactory;
  private final BoundedQueueExecutor workUnitExecutor;
  private final WindmillServerStub windmillServer;
  private final Thread dispatchThread;
  private final Thread commitThread;
  private final AtomicLong activeCommitBytes = new AtomicLong();
  private final AtomicBoolean running = new AtomicBoolean();
  private final StateFetcher stateFetcher;
  private final StreamingDataflowWorkerOptions options;
  private final boolean windmillServiceEnabled;
  private final long clientId;

  private final MetricTrackingWindmillServerStub metricTrackingWindmillServer;
  private final CounterSet pendingDeltaCounters = new CounterSet();
  private final CounterSet pendingCumulativeCounters = new CounterSet();
  private final java.util.concurrent.ConcurrentLinkedQueue<CounterUpdate> pendingMonitoringInfos =
      new ConcurrentLinkedQueue<>();

  // Map from stage name to StageInfo containing metrics container registry and per stage counters.
  private final ConcurrentMap<String, StageInfo> stageInfoMap = new ConcurrentHashMap();

  // Built-in delta counters.
  private final Counter<Long, Long> windmillShuffleBytesRead;
  private final Counter<Long, Long> windmillStateBytesRead;
  private final Counter<Long, Long> windmillStateBytesWritten;
  private final Counter<Long, Long> windmillQuotaThrottling;
  // Built-in cumulative counters.
  private final Counter<Long, Long> javaHarnessUsedMemory;
  private final Counter<Long, Long> javaHarnessMaxMemory;
  private final Counter<Integer, Integer> windmillMaxObservedWorkItemCommitBytes;
  private final Counter<Integer, Integer> memoryThrashing;
  private Timer refreshWorkTimer;
  private Timer statusPageTimer;

  private final boolean publishCounters;
  private Timer globalWorkerUpdatesTimer;
  private int retryLocallyDelayMs = 10000;

  // Periodically fires a global config request to dataflow service. Only used when windmill service
  // is enabled.
  private Timer globalConfigRefreshTimer;

  private final MemoryMonitor memoryMonitor;
  private final Thread memoryMonitorThread;

  private final WorkerStatusPages statusPages;
  // Periodic sender of debug information to the debug capture service.
  private DebugCapture.Manager debugCaptureManager = null;

  // Limit on bytes sinked (committed) in a work item.
  private final long maxSinkBytes; // = MAX_SINK_BYTES unless disabled in options.
  // Possibly overridden by streaming engine config.
  private int maxWorkItemCommitBytes = Integer.MAX_VALUE;

  private final EvictingQueue<String> pendingFailuresToReport =
      EvictingQueue.<String>create(MAX_FAILURES_TO_REPORT_IN_UPDATE);

  private final ReaderCache readerCache = new ReaderCache();

  private final WorkUnitClient workUnitClient;
  private final CompletableFuture<Void> isDoneFuture;
  private final SdkHarnessRegistry sdkHarnessRegistry;
  private final Function<MapTask, MutableNetwork<Node, Edge>> mapTaskToNetwork;

  /**
   * Sinks are marked 'full' in {@link StreamingModeExecutionContext} once the amount of data sinked
   * (across all the sinks, if there are more than one) reaches this limit. This serves as hint for
   * readers to stop producing more. This can be disabled with 'disable_limiting_bundle_sink_bytes'
   * experiment.
   */
  static final int MAX_SINK_BYTES = 10_000_000;

  private final ReaderRegistry readerRegistry = ReaderRegistry.defaultRegistry();
  private final SinkRegistry sinkRegistry = SinkRegistry.defaultRegistry();

  private HotKeyLogger hotKeyLogger;

  /** Contains a few of the stage specific fields. E.g. metrics container registry, counters etc. */
  private static class StageInfo {

    final String stageName;
    final String systemName;
    final MetricsContainerRegistry<StreamingStepMetricsContainer> metricsContainerRegistry;
    final StreamingModeExecutionStateRegistry executionStateRegistry;
    final CounterSet deltaCounters;
    final Counter<Long, Long> throttledMsecs;
    final Counter<Long, Long> totalProcessingMsecs;
    final Counter<Long, Long> timerProcessingMsecs;

    StageInfo(String stageName, String systemName, StreamingDataflowWorker worker) {
      this.stageName = stageName;
      this.systemName = systemName;
      metricsContainerRegistry = StreamingStepMetricsContainer.createRegistry();
      executionStateRegistry = new StreamingModeExecutionStateRegistry(worker);
      NameContext nameContext = NameContext.create(stageName, null, systemName, null);
      deltaCounters = new CounterSet();
      throttledMsecs =
          deltaCounters.longSum(
              StreamingPerStageSystemCounterNames.THROTTLED_MSECS.counterName(nameContext));
      totalProcessingMsecs =
          deltaCounters.longSum(
              StreamingPerStageSystemCounterNames.TOTAL_PROCESSING_MSECS.counterName(nameContext));
      timerProcessingMsecs =
          deltaCounters.longSum(
              StreamingPerStageSystemCounterNames.TIMER_PROCESSING_MSECS.counterName(nameContext));
    }

    List<CounterUpdate> extractCounterUpdates() {
      List<CounterUpdate> counterUpdates = new ArrayList<>();
      Iterables.addAll(
          counterUpdates,
          StreamingStepMetricsContainer.extractMetricUpdates(metricsContainerRegistry));
      Iterables.addAll(counterUpdates, executionStateRegistry.extractUpdates(false));
      for (CounterUpdate counterUpdate : counterUpdates) {
        translateKnownStepCounters(counterUpdate);
      }
      counterUpdates.addAll(
          deltaCounters.extractModifiedDeltaUpdates(DataflowCounterUpdateExtractor.INSTANCE));
      return counterUpdates;
    }

    // Checks if the step counter affects any per-stage counters. Currently 'throttled_millis'
    // is the only counter updated.
    private void translateKnownStepCounters(CounterUpdate stepCounterUpdate) {
      CounterStructuredName structuredName =
          stepCounterUpdate.getStructuredNameAndMetadata().getName();
      if (THROTTLING_MSECS_METRIC_NAME.getNamespace().equals(structuredName.getOriginNamespace())
          && THROTTLING_MSECS_METRIC_NAME.getName().equals(structuredName.getName())) {
        long msecs = DataflowCounterUpdateExtractor.splitIntToLong(stepCounterUpdate.getInteger());
        if (msecs > 0) {
          throttledMsecs.addValue(msecs);
        }
      }
    }
  }

  public static StreamingDataflowWorker forStreamingFnWorkerHarness(
      List<MapTask> mapTasks,
      WorkUnitClient workUnitClient,
      DataflowWorkerHarnessOptions options,
      @Nullable RunnerApi.Pipeline pipeline,
      SdkHarnessRegistry sdkHarnessRegistry)
      throws IOException {
    return new StreamingDataflowWorker(
        mapTasks,
        BeamFnMapTaskExecutorFactory.defaultFactory(),
        workUnitClient,
        options.as(StreamingDataflowWorkerOptions.class),
        pipeline,
        sdkHarnessRegistry,
        true,
        new HotKeyLogger());
  }

  public static StreamingDataflowWorker fromDataflowWorkerHarnessOptions(
      DataflowWorkerHarnessOptions options, SdkHarnessRegistry sdkHarnessRegistry)
      throws IOException {

    return new StreamingDataflowWorker(
        Collections.emptyList(),
        IntrinsicMapTaskExecutorFactory.defaultFactory(),
        new DataflowWorkUnitClient(options, LOG),
        options.as(StreamingDataflowWorkerOptions.class),
        null,
        sdkHarnessRegistry,
        true,
        new HotKeyLogger());
  }

  @VisibleForTesting
  StreamingDataflowWorker(
      List<MapTask> mapTasks,
      DataflowMapTaskExecutorFactory mapTaskExecutorFactory,
      WorkUnitClient workUnitClient,
      StreamingDataflowWorkerOptions options,
      @Nullable RunnerApi.Pipeline pipeline,
      SdkHarnessRegistry sdkHarnessRegistry,
      boolean publishCounters,
      HotKeyLogger hotKeyLogger)
      throws IOException {
    this.mapTaskExecutorFactory = mapTaskExecutorFactory;
    this.workUnitClient = workUnitClient;
    this.options = options;
    this.sdkHarnessRegistry = sdkHarnessRegistry;
    this.hotKeyLogger = hotKeyLogger;
    this.windmillServiceEnabled = options.isEnableStreamingEngine();
    this.memoryMonitor = MemoryMonitor.fromOptions(options);
    this.statusPages =
        WorkerStatusPages.create(
            DEFAULT_STATUS_PORT, memoryMonitor, sdkHarnessRegistry::sdkHarnessesAreHealthy);
    if (windmillServiceEnabled) {
      this.debugCaptureManager =
          new DebugCapture.Manager(options, statusPages.getDebugCapturePages());
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
    this.javaHarnessUsedMemory =
        pendingCumulativeCounters.longSum(
            StreamingSystemCounterNames.JAVA_HARNESS_USED_MEMORY.counterName());
    this.javaHarnessMaxMemory =
        pendingCumulativeCounters.longSum(
            StreamingSystemCounterNames.JAVA_HARNESS_MAX_MEMORY.counterName());
    this.windmillMaxObservedWorkItemCommitBytes =
        pendingCumulativeCounters.intMax(
            StreamingSystemCounterNames.WINDMILL_MAX_WORK_ITEM_COMMIT_BYTES.counterName());
    this.memoryThrashing =
        pendingCumulativeCounters.intSum(
            StreamingSystemCounterNames.MEMORY_THRASHING.counterName());
    this.isDoneFuture = new CompletableFuture<>();

    this.threadFactory =
        r -> {
          Thread t = new Thread(r);
          t.setDaemon(true);
          return t;
        };
    this.workUnitExecutor =
        new BoundedQueueExecutor(
            chooseMaximumNumberOfThreads(),
            THREAD_EXPIRATION_TIME_SEC,
            TimeUnit.SECONDS,
            MAX_WORK_UNITS_QUEUED,
            threadFactory);

    maxSinkBytes =
        hasExperiment(options, "disable_limiting_bundle_sink_bytes")
            ? Long.MAX_VALUE
            : MAX_SINK_BYTES;

    memoryMonitorThread = new Thread(memoryMonitor);
    memoryMonitorThread.setPriority(Thread.MIN_PRIORITY);
    memoryMonitorThread.setName("MemoryMonitor");

    dispatchThread =
        threadFactory.newThread(
            new Runnable() {
              @Override
              public void run() {
                LOG.info("Dispatch starting");
                if (windmillServiceEnabled) {
                  streamingDispatchLoop();
                } else {
                  dispatchLoop();
                }
                LOG.info("Dispatch done");
              }
            });
    dispatchThread.setPriority(Thread.MIN_PRIORITY);
    dispatchThread.setName("DispatchThread");

    commitThread =
        threadFactory.newThread(
            new Runnable() {
              @Override
              public void run() {
                if (windmillServiceEnabled) {
                  streamingCommitLoop();
                } else {
                  commitLoop();
                }
              }
            });
    commitThread.setPriority(Thread.MAX_PRIORITY);
    commitThread.setName("CommitThread");

    this.publishCounters = publishCounters;
    this.windmillServer = options.getWindmillServerStub();
    this.metricTrackingWindmillServer =
        new MetricTrackingWindmillServerStub(windmillServer, memoryMonitor, windmillServiceEnabled);
    this.metricTrackingWindmillServer.start();
    this.stateFetcher = new StateFetcher(metricTrackingWindmillServer);
    this.clientId = clientIdGenerator.nextLong();

    for (MapTask mapTask : mapTasks) {
      addComputation(mapTask.getSystemName(), mapTask, ImmutableMap.of());
    }

    // Register standard file systems.
    FileSystems.setDefaultPipelineOptions(options);

    if (hasExperiment(options, "beam_fn_api")) {
      Function<MutableNetwork<Node, Edge>, Node> sdkFusedStage =
          pipeline == null
              ? RegisterNodeFunction.withoutPipeline(
                  idGenerator, sdkHarnessRegistry.beamFnStateApiServiceDescriptor())
              : RegisterNodeFunction.forPipeline(
                  pipeline, idGenerator, sdkHarnessRegistry.beamFnStateApiServiceDescriptor());
      Function<MutableNetwork<Node, Edge>, MutableNetwork<Node, Edge>> lengthPrefixUnknownCoders =
          LengthPrefixUnknownCoders::forSdkNetwork;
      Function<MutableNetwork<Node, Edge>, MutableNetwork<Node, Edge>>
          addStreamingSideInputHandlers =
              InsertFetchAndFilterStreamingSideInputNodes.with(pipeline)::forNetwork;

      Function<MutableNetwork<Node, Edge>, MutableNetwork<Node, Edge>> transformToRunnerNetwork =
          new CreateRegisterFnOperationFunction(
              idGenerator,
              this::createPortNode,
              lengthPrefixUnknownCoders.andThen(sdkFusedStage),
              false);

      mapTaskToNetwork =
          mapTaskToBaseNetwork
              .andThen(new ReplacePgbkWithPrecombineFunction())
              .andThen(new DeduceNodeLocationsFunction())
              .andThen(new DeduceFlattenLocationsFunction())
              .andThen(new CloneAmbiguousFlattensFunction())
              .andThen(addStreamingSideInputHandlers)
              .andThen(transformToRunnerNetwork)
              .andThen(LengthPrefixUnknownCoders::andReplaceForRunnerNetwork);
    } else {
      mapTaskToNetwork = mapTaskToBaseNetwork;
    }

    LOG.debug("windmillServiceEnabled: {}", windmillServiceEnabled);
    LOG.debug("WindmillServiceEndpoint: {}", options.getWindmillServiceEndpoint());
    LOG.debug("WindmillServicePort: {}", options.getWindmillServicePort());
    LOG.debug("LocalWindmillHostport: {}", options.getLocalWindmillHostport());
    LOG.debug("maxWorkItemCommitBytes: {}", maxWorkItemCommitBytes);
  }

  private Node createPortNode() {
    return RemoteGrpcPortNode.create(
        RemoteGrpcPort.newBuilder()
            .setApiServiceDescriptor(sdkHarnessRegistry.beamFnDataApiServiceDescriptor())
            .build(),
        idGenerator.getId());
  }

  private int chooseMaximumNumberOfThreads() {
    if (options.getNumberOfWorkerHarnessThreads() != 0) {
      return options.getNumberOfWorkerHarnessThreads();
    }
    return MAX_PROCESSING_THREADS;
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
    return workUnitExecutor.getQueue().isEmpty();
  }

  public void start() {
    running.set(true);

    if (windmillServiceEnabled) {
      // Schedule the background getConfig thread. Blocks until windmillServer stub is ready.
      schedulePeriodicGlobalConfigRequests();
    }

    memoryMonitorThread.start();
    dispatchThread.start();
    commitThread.start();
    ExecutionStateSampler.instance().start();

    // Periodically report workers counters and other updates.
    globalWorkerUpdatesTimer = new Timer("GlobalWorkerUpdatesTimer");
    globalWorkerUpdatesTimer.schedule(
        new TimerTask() {
          @Override
          public void run() {
            reportPeriodicWorkerUpdates();
          }
        },
        0,
        options.getWindmillHarnessUpdateReportingPeriod().getMillis());

    refreshWorkTimer = new Timer("RefreshWork");
    if (options.getActiveWorkRefreshPeriodMillis() > 0) {
      refreshWorkTimer.schedule(
          new TimerTask() {
            @Override
            public void run() {
              refreshActiveWork();
            }
          },
          options.getActiveWorkRefreshPeriodMillis(),
          options.getActiveWorkRefreshPeriodMillis());
    }
    if (windmillServiceEnabled && options.getStuckCommitDurationMillis() > 0) {
      int periodMillis = Math.max(options.getStuckCommitDurationMillis() / 10, 100);
      refreshWorkTimer.schedule(
          new TimerTask() {
            @Override
            public void run() {
              invalidateStuckCommits();
            }
          },
          periodMillis,
          periodMillis);
    }

    if (options.getPeriodicStatusPageOutputDirectory() != null) {
      statusPageTimer = new Timer("DumpStatusPages");
      statusPageTimer.schedule(
          new TimerTask() {
            @Override
            public void run() {
              List<Capturable> pages = statusPages.getDebugCapturePages();
              if (pages.isEmpty()) {
                LOG.warn("No captured status pages.");
              }
              Long timestamp = Instant.now().getMillis();
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
                                  + timestamp.toString())
                              .replaceAll("/", "_"));
                  writer = new PrintWriter(outputFile, UTF_8.name());
                  page.captureData(writer);
                } catch (IOException e) {
                  LOG.warn("Error dumping status page., {}", e);
                } finally {
                  if (writer != null) {
                    writer.close();
                  }
                }
              }
            }
          },
          60 * 1000,
          60 * 1000);
    }

    reportHarnessStartup();
  }

  public void startStatusPages() {
    if (debugCaptureManager != null) {
      debugCaptureManager.start();
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
      if (globalConfigRefreshTimer != null) {
        globalConfigRefreshTimer.cancel();
      }
      globalWorkerUpdatesTimer.cancel();
      if (refreshWorkTimer != null) {
        refreshWorkTimer.cancel();
      }
      if (statusPageTimer != null) {
        statusPageTimer.cancel();
      }
      statusPages.stop();
      if (debugCaptureManager != null) {
        debugCaptureManager.stop();
      }
      running.set(false);
      dispatchThread.interrupt();
      dispatchThread.join();
      // We need to interrupt the commitThread in case it is blocking on pulling
      // from the commitQueue.
      commitThread.interrupt();
      commitThread.join();
      memoryMonitor.stop();
      memoryMonitorThread.join();
      workUnitExecutor.shutdown();
      if (!workUnitExecutor.awaitTermination(5, TimeUnit.MINUTES)) {
        throw new RuntimeException("Work executor did not terminate within 5 minutes");
      }
      for (ComputationState state : computationMap.values()) {
        state.close();
      }

      // one last send
      reportPeriodicWorkerUpdates();
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

  public void waitTillExecutionFinishes() throws InterruptedException {
    try {
      isDoneFuture.get();
    } catch (ExecutionException e) {
      throw new IllegalStateException(e);
    }
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

  private static void sleep(int millis) {
    Uninterruptibles.sleepUninterruptibly(millis, TimeUnit.MILLISECONDS);
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
        @Nullable
        final Instant synchronizedProcessingTime =
            WindmillTimeUtils.windmillToHarnessWatermark(
                computationWork.getDependentRealtimeInputWatermark());
        for (final Windmill.WorkItem workItem : computationWork.getWorkList()) {
          scheduleWorkItem(
              computationState, inputDataWatermark, synchronizedProcessingTime, workItem);
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
                  .setMaxItems(MAX_GET_WORK_ITEMS)
                  .setMaxBytes(MAX_GET_WORK_FETCH_BYTES)
                  .build(),
              (String computation,
                  @Nullable Instant inputDataWatermark,
                  Instant synchronizedProcessingTime,
                  Windmill.WorkItem workItem) -> {
                memoryMonitor.waitForResources("GetWork");
                scheduleWorkItem(
                    getComputationState(computation),
                    inputDataWatermark,
                    synchronizedProcessingTime,
                    workItem);
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
      final Windmill.WorkItem workItem) {
    Preconditions.checkNotNull(inputDataWatermark);
    // May be null if output watermark not yet known.
    @Nullable
    final Instant outputDataWatermark =
        WindmillTimeUtils.windmillToHarnessWatermark(workItem.getOutputDataWatermark());
    Preconditions.checkState(
        outputDataWatermark == null || !outputDataWatermark.isAfter(inputDataWatermark));
    SdkWorkerHarness worker = sdkHarnessRegistry.getAvailableWorkerAndAssignWork();

    if (workItem.hasHotKeyInfo()) {
      Windmill.HotKeyInfo hotKeyInfo = workItem.getHotKeyInfo();
      Duration hotKeyAge = Duration.millis(hotKeyInfo.getHotKeyAgeUsec() / 1000);

      // The MapTask instruction is ordered by dependencies, such that the first element is
      // always going to be the shuffle task.
      String stepName = computationState.getMapTask().getInstructions().get(0).getName();
      hotKeyLogger.logHotKeyDetection(stepName, hotKeyAge);
    }

    Work work =
        new Work(workItem) {
          @Override
          public void run() {
            try {
              process(
                  worker,
                  computationState,
                  inputDataWatermark,
                  outputDataWatermark,
                  synchronizedProcessingTime,
                  this);
            } finally {
              // Reduce the work associated with the worker
              sdkHarnessRegistry.completeWork(worker);
            }
          }
        };
    if (!computationState.activateWork(workItem.getKey(), work)) {
      // Free worker if the work was not activated.
      // This can happen if it's duplicate work or some other reason.
      sdkHarnessRegistry.completeWork(worker);
    }
  }

  abstract static class Work implements Runnable {
    enum State {
      QUEUED,
      PROCESSING,
      COMMIT_QUEUED,
      COMMITTING
    }

    private final Windmill.WorkItem workItem;
    private final Instant startTime;
    private Instant stateStartTime;
    private State state;

    public Work(Windmill.WorkItem workItem) {
      this.workItem = workItem;
      this.startTime = this.stateStartTime = Instant.now();
      this.state = State.QUEUED;
    }

    public Windmill.WorkItem getWorkItem() {
      return workItem;
    }

    public Instant getStartTime() {
      return startTime;
    }

    public State getState() {
      return state;
    }

    public void setState(State state) {
      this.state = state;
      this.stateStartTime = Instant.now();
    }

    public Instant getStateStartTime() {
      return stateStartTime;
    }
  }

  /**
   * Extracts the userland key coder, if any, from the coder used in the initial read step of a
   * stage. This encodes many assumptions about how the streaming execution context works.
   */
  @Nullable
  private Coder<?> extractKeyCoder(Coder<?> readCoder) {
    if (!(readCoder instanceof WindowedValueCoder)) {
      throw new RuntimeException(
          String.format(
              "Expected coder for streaming read to be %s, but received %s",
              WindowedValueCoder.class.getSimpleName(), readCoder));
    }

    // Note that TimerOrElementCoder is a backwards-compatibility class
    // that is really a FakeKeyedWorkItemCoder
    Coder<?> valueCoder = ((WindowedValueCoder<?>) readCoder).getValueCoder();
    if (!(valueCoder instanceof WindmillKeyedWorkItem.FakeKeyedWorkItemCoder<?, ?>)) {
      return null;
    }

    return ((WindmillKeyedWorkItem.FakeKeyedWorkItemCoder<?, ?>) valueCoder).getKeyCoder();
  }

  private void callFinalizeCallbacks(Windmill.WorkItem work) {
    for (Long callbackId : work.getSourceState().getFinalizeIdsList()) {
      final Runnable callback = commitCallbacks.remove(callbackId);
      // NOTE: It is possible the same callback id may be removed twice if
      // windmill restarts.
      // TODO: It is also possible for an earlier finalized id to be lost.
      // We should automatically discard all older callbacks for the same computation and key.
      if (callback != null) {
        workUnitExecutor.forceExecute(
            () -> {
              try {
                callback.run();
              } catch (Throwable t) {
                LOG.error("Source checkpoint finalization failed:", t);
              }
            });
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
      final SdkWorkerHarness worker,
      final ComputationState computationState,
      final Instant inputDataWatermark,
      @Nullable final Instant outputDataWatermark,
      @Nullable final Instant synchronizedProcessingTime,
      final Work work) {
    final Windmill.WorkItem workItem = work.getWorkItem();
    final String computationId = computationState.getComputationId();
    final ByteString key = workItem.getKey();
    work.setState(State.PROCESSING);
    DataflowWorkerLoggingMDC.setWorkId(
        TextFormat.escapeBytes(key) + "-" + Long.toString(workItem.getWorkToken()));
    DataflowWorkerLoggingMDC.setStageName(computationId);
    LOG.debug("Starting processing for {}:\n{}", computationId, work);

    Windmill.WorkItemCommitRequest.Builder outputBuilder = initializeOutputBuilder(key, workItem);

    // Before any processing starts, call any pending OnCommit callbacks.  Nothing that requires
    // cleanup should be done before this, since we might exit early here.
    callFinalizeCallbacks(workItem);
    if (workItem.getSourceState().getOnlyFinalize()) {
      outputBuilder.setSourceStateUpdates(Windmill.SourceState.newBuilder().setOnlyFinalize(true));
      work.setState(State.COMMIT_QUEUED);
      commitQueue.put(new Commit(outputBuilder.build(), computationState, work));
      return;
    }

    long processingStartTimeNanos = System.nanoTime();

    final MapTask mapTask = computationState.getMapTask();

    StageInfo stageInfo =
        stageInfoMap.computeIfAbsent(
            mapTask.getStageName(), s -> new StageInfo(s, mapTask.getSystemName(), this));

    ExecutionState executionState = null;

    try {
      executionState = computationState.getExecutionStateQueue(worker).poll();
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
                ExecutionStateSampler.instance(),
                stageInfo.executionStateRegistry.getState(
                    NameContext.forStage(mapTask.getStageName()),
                    "other",
                    null,
                    ScopedProfiler.INSTANCE.emptyScope()),
                stageInfo.deltaCounters,
                options,
                computationId);
        StreamingModeExecutionContext context =
            new StreamingModeExecutionContext(
                pendingDeltaCounters,
                computationId,
                readerCache,
                !computationState.getTransformUserNameToStateFamily().isEmpty()
                    ? computationState.getTransformUserNameToStateFamily()
                    : stateNameMap,
                stateCache.forComputation(computationId),
                stageInfo.metricsContainerRegistry,
                executionStateTracker,
                stageInfo.executionStateRegistry,
                maxSinkBytes);
        DataflowMapTaskExecutor mapTaskExecutor =
            mapTaskExecutorFactory.create(
                worker.getControlClientHandler(),
                worker.getGrpcDataFnServer(),
                sdkHarnessRegistry.beamFnDataApiServiceDescriptor(),
                worker.getGrpcStateFnServer(),
                mapTaskNetwork,
                options,
                mapTask.getStageName(),
                readerRegistry,
                sinkRegistry,
                context,
                pendingDeltaCounters,
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
              new OutputObjectAndByteCounter(
                      new IntrinsicMapTaskExecutorFactory.ElementByteSizeObservableCoder<>(
                          readCoder),
                      mapTaskExecutor.getOutputCounters(),
                      nameContext)
                  .setSamplingPeriod(100)
                  .countBytes("dataflow_input_size-" + mapTask.getSystemName()));
        }
        executionState =
            new ExecutionState(mapTaskExecutor, context, keyCoder, executionStateTracker);
      }

      WindmillStateReader stateReader =
          new WindmillStateReader(
              metricTrackingWindmillServer,
              computationId,
              key,
              workItem.getShardingKey(),
              workItem.getWorkToken());
      StateFetcher localStateFetcher = stateFetcher.byteTrackingView();

      // If the read output KVs, then we can decode Windmill's byte key into a userland
      // key object and provide it to the execution context for use with per-key state.
      // Otherwise, we pass null.
      //
      // The coder type that will be present is:
      //     WindowedValueCoder(TimerOrElementCoder(KvCoder))
      @Nullable Coder<?> keyCoder = executionState.getKeyCoder();
      @Nullable
      Object executionKey =
          keyCoder == null ? null : keyCoder.decode(key.newInput(), Coder.Context.OUTER);

      executionState
          .getContext()
          .start(
              executionKey,
              workItem,
              inputDataWatermark,
              outputDataWatermark,
              synchronizedProcessingTime,
              stateReader,
              localStateFetcher,
              outputBuilder);

      // Blocks while executing work.
      executionState.getWorkExecutor().execute();

      Iterables.addAll(
          this.pendingMonitoringInfos, executionState.getWorkExecutor().extractMetricUpdates());

      commitCallbacks.putAll(executionState.getContext().flushState());

      // Release the execution state for another thread to use.
      computationState.getExecutionStateQueue(worker).offer(executionState);
      executionState = null;

      // Add the output to the commit queue.
      work.setState(State.COMMIT_QUEUED);

      WorkItemCommitRequest commitRequest = outputBuilder.build();
      int byteLimit = maxWorkItemCommitBytes;
      int commitSize = commitRequest.getSerializedSize();
      int estimatedCommitSize = commitSize < 0 ? Integer.MAX_VALUE : commitSize;

      // Detect overflow of integer serialized size or if the byte limit was exceeded.
      windmillMaxObservedWorkItemCommitBytes.addValue(estimatedCommitSize);
      if (estimatedCommitSize > byteLimit) {
        KeyCommitTooLargeException e =
            KeyCommitTooLargeException.causedBy(computationId, byteLimit, commitRequest);
        reportFailure(computationId, workItem, e);
        LOG.error(e.toString());

        // Drop the current request in favor of a new, minimal one requesting truncation.
        // Messages, timers, counters, and other commit content will not be used by the service
        // so we're purposefully dropping them here
        commitRequest = buildWorkItemTruncationRequest(key, workItem, estimatedCommitSize);
      }

      commitQueue.put(new Commit(commitRequest, computationState, work));

      // Compute shuffle and state byte statistics these will be flushed asynchronously.
      long stateBytesWritten = outputBuilder.clearOutputMessages().build().getSerializedSize();
      long shuffleBytesRead = 0;
      for (Windmill.InputMessageBundle bundle : workItem.getMessageBundlesList()) {
        for (Windmill.Message message : bundle.getMessagesList()) {
          shuffleBytesRead += message.getSerializedSize();
        }
      }
      long stateBytesRead = stateReader.getBytesRead() + localStateFetcher.getBytesRead();
      windmillShuffleBytesRead.addValue(shuffleBytesRead);
      windmillStateBytesRead.addValue(stateBytesRead);
      windmillStateBytesWritten.addValue(stateBytesWritten);

      LOG.debug("Processing done for work token: {}", workItem.getWorkToken());
    } catch (Throwable t) {
      if (executionState != null) {
        try {
          executionState.getContext().invalidateCache();
          executionState.getWorkExecutor().close();
        } catch (Exception e) {
          LOG.warn("Failed to close map task executor: ", e);
        } finally {
          // Release references to potentially large objects early.
          executionState = null;
        }
      }

      t = t instanceof UserCodeException ? t.getCause() : t;

      boolean retryLocally = false;
      if (KeyTokenInvalidException.isKeyTokenInvalidException(t)) {
        LOG.debug(
            "Execution of work for {} for key {} failed due to token expiration. "
                + "Will not retry locally.",
            computationId,
            key.toStringUtf8());
      } else {
        LOG.error("Uncaught exception: ", t);
        LastExceptionDataProvider.reportException(t);
        LOG.debug("Failed work: {}", work);
        if (!reportFailure(computationId, workItem, t)) {
          LOG.error(
              "Execution of work for {} for key {} failed, and Windmill "
                  + "indicated not to retry locally.",
              computationId,
              key.toStringUtf8());
        } else if (isOutOfMemoryError(t)) {
          File heapDump = memoryMonitor.tryToDumpHeap();
          LOG.error(
              "Execution of work for {} for key {} failed with out-of-memory. "
                  + "Will not retry locally. Heap dump {}.",
              computationId,
              key.toStringUtf8(),
              heapDump == null ? "not written" : ("written to '" + heapDump + "'"));
        } else {
          LOG.error(
              "Execution of work for {} for key {} failed. Will retry locally.",
              computationId,
              key.toStringUtf8());
          retryLocally = true;
        }
      }
      if (retryLocally) {
        // Try again after some delay and at the end of the queue to avoid a tight loop.
        sleep(retryLocallyDelayMs);
        workUnitExecutor.forceExecute(work);
      } else {
        // Consider the item invalid. It will eventually be retried by Windmill if it still needs to
        // be processed.
        computationState.completeWork(key, workItem.getWorkToken());
      }
    } finally {
      // Update total processing time counters. Updating in finally clause ensures that
      // work items causing exceptions are also accounted in time spent.
      long processingTimeMsecs =
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - processingStartTimeNanos);
      stageInfo.totalProcessingMsecs.addValue(processingTimeMsecs);

      // Attribute all the processing to timers if the work item contains any timers.
      // Tests show that work items rarely contain both timers and message bundles. It should
      // be a fairly close approximation.
      // Another option: Derive time split between messages and timers based on recent totals.
      // either here or in DFE.
      if (work.getWorkItem().hasTimers()) {
        stageInfo.timerProcessingMsecs.addValue(processingTimeMsecs);
      }

      DataflowWorkerLoggingMDC.setWorkId(null);
      DataflowWorkerLoggingMDC.setStageName(null);
    }
  }

  private WorkItemCommitRequest buildWorkItemTruncationRequest(
      final ByteString key, final Windmill.WorkItem workItem, final int estimatedCommitSize) {
    Windmill.WorkItemCommitRequest.Builder outputBuilder = initializeOutputBuilder(key, workItem);
    outputBuilder.setExceedsMaxWorkItemCommitBytes(true);
    outputBuilder.setEstimatedWorkItemCommitBytes(estimatedCommitSize);
    return outputBuilder.build();
  }

  private void commitLoop() {
    Map<ComputationState, Windmill.ComputationCommitWorkRequest.Builder> computationRequestMap =
        new HashMap<>();
    while (running.get()) {
      computationRequestMap.clear();
      Windmill.CommitWorkRequest.Builder commitRequestBuilder =
          Windmill.CommitWorkRequest.newBuilder();
      long commitBytes = 0;
      // Block until we have a commit, then batch with additional commits.
      Commit commit = null;
      try {
        commit = commitQueue.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        continue;
      }
      while (commit != null) {
        ComputationState computationState = commit.getComputationState();
        commit.getWork().setState(State.COMMITTING);
        Windmill.ComputationCommitWorkRequest.Builder computationRequestBuilder =
            computationRequestMap.get(computationState);
        if (computationRequestBuilder == null) {
          computationRequestBuilder = commitRequestBuilder.addRequestsBuilder();
          computationRequestBuilder.setComputationId(computationState.getComputationId());
          computationRequestMap.put(computationState, computationRequestBuilder);
        }
        computationRequestBuilder.addRequests(commit.getRequest());
        // Send the request if we've exceeded the bytes or there is no more
        // pending work.  commitBytes is a long, so this cannot overflow.
        commitBytes += commit.getSize();
        if (commitBytes >= TARGET_COMMIT_BUNDLE_BYTES) {
          break;
        }
        commit = commitQueue.poll();
      }
      Windmill.CommitWorkRequest commitRequest = commitRequestBuilder.build();
      LOG.trace("Commit: {}", commitRequest);
      activeCommitBytes.set(commitBytes);
      windmillServer.commitWork(commitRequest);
      activeCommitBytes.set(0);
      for (Map.Entry<ComputationState, Windmill.ComputationCommitWorkRequest.Builder> entry :
          computationRequestMap.entrySet()) {
        ComputationState computationState = entry.getKey();
        for (Windmill.WorkItemCommitRequest workRequest : entry.getValue().getRequestsList()) {
          computationState.completeWork(workRequest.getKey(), workRequest.getWorkToken());
        }
      }
    }
  }

  // Adds the commit to the commitStream if it fits, returning true iff it is consumed.
  private boolean addCommitToStream(Commit commit, CommitWorkStream commitStream) {
    Preconditions.checkNotNull(commit);
    final ComputationState state = commit.getComputationState();
    final Windmill.WorkItemCommitRequest request = commit.getRequest();
    final int size = commit.getSize();
    commit.getWork().setState(State.COMMITTING);
    activeCommitBytes.addAndGet(size);
    if (commitStream.commitWorkItem(
        state.computationId,
        request,
        (Windmill.CommitStatus status) -> {
          if (status != Windmill.CommitStatus.OK) {
            stateCache.forComputation(state.computationId).invalidate(request.getKey());
          }
          activeCommitBytes.addAndGet(-size);
          // This may throw an exception if the commit was not active, which is possible if it
          // was deemed stuck.
          state.completeWork(request.getKey(), request.getWorkToken());
        })) {
      return true;
    } else {
      // Back out the stats changes since the commit wasn't consumed.
      commit.getWork().setState(State.COMMIT_QUEUED);
      activeCommitBytes.addAndGet(-size);
      return false;
    }
  }

  // Helper to batch additional commits into the commit stream as long as they fit.
  // Returns a commit that was removed from the queue but not consumed or null.
  private Commit batchCommitsToStream(CommitWorkStream commitStream) {
    int commits = 1;
    while (running.get()) {
      Commit commit;
      try {
        if (commits < 5) {
          commit = commitQueue.poll(10 - 2 * commits, TimeUnit.MILLISECONDS);
        } else {
          commit = commitQueue.poll();
        }
      } catch (InterruptedException e) {
        // Continue processing until !running.get()
        continue;
      }
      if (commit == null || !addCommitToStream(commit, commitStream)) {
        return commit;
      }
      commits++;
    }
    return null;
  }

  private void streamingCommitLoop() {
    StreamPool<CommitWorkStream> streamPool =
        new StreamPool<>(
            NUM_COMMIT_STREAMS, COMMIT_STREAM_TIMEOUT, windmillServer::commitWorkStream);
    Commit initialCommit = null;
    while (running.get()) {
      if (initialCommit == null) {
        try {
          initialCommit = commitQueue.take();
        } catch (InterruptedException e) {
          continue;
        }
      }
      // We initialize the commit stream only after we have a commit to make sure it is fresh.
      CommitWorkStream commitStream = streamPool.getStream();
      if (!addCommitToStream(initialCommit, commitStream)) {
        throw new AssertionError("Initial commit on flushed stream should always be accepted.");
      }
      // Batch additional commits to the stream and possibly make an un-batched commit the next
      // initial commit.
      initialCommit = batchCommitsToStream(commitStream);
      commitStream.flush();
      streamPool.releaseStream(commitStream);
    }
  }

  private Windmill.GetWorkResponse getWork() {
    return windmillServer.getWork(
        Windmill.GetWorkRequest.newBuilder()
            .setClientId(clientId)
            .setMaxItems(MAX_GET_WORK_ITEMS)
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
            transformUserNameToStateFamilyByComputationId.get(computationId));
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
    Optional<WorkItem> workItem;
    if (computation != null) {
      workItem = workUnitClient.getStreamingConfigWorkItem(computation);
    } else {
      workItem = workUnitClient.getGlobalStreamingConfigWorkItem();
    }
    if (workItem == null || !workItem.isPresent() || workItem.get() == null) {
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
            computationConfig.getTransformUserNameToStateFamily());
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
    globalConfigRefreshTimer = new Timer("GlobalConfigRefreshTimer");
    globalConfigRefreshTimer.schedule(
        new TimerTask() {
          @Override
          public void run() {
            getGlobalConfig();
          }
        },
        0,
        options.getGlobalConfigRefreshPeriod().getMillis());
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

  private Windmill.Exception buildExceptionReport(Throwable t) {
    Windmill.Exception.Builder builder = Windmill.Exception.newBuilder();

    builder.addStackFrames(t.toString());
    for (StackTraceElement frame : t.getStackTrace()) {
      builder.addStackFrames(frame.toString());
    }
    if (t.getCause() != null) {
      builder.setCause(buildExceptionReport(t.getCause()));
    }

    return builder.build();
  }

  private String buildExceptionStackTrace(Throwable t, final int maxDepth) {
    StringBuilder builder = new StringBuilder(1024);
    Throwable cur = t;
    for (int depth = 0; cur != null && depth < maxDepth; cur = cur.getCause()) {
      if (depth > 0) {
        builder.append("\nCaused by: ");
      }
      builder.append(cur);
      depth++;
      for (StackTraceElement frame : cur.getStackTrace()) {
        if (depth < maxDepth) {
          builder.append("\n        ");
          builder.append(frame);
          depth++;
        }
      }
    }
    if (cur != null) {
      builder.append("\nStack trace truncated. Please see Cloud Logging for the entire trace.");
    }
    return builder.toString();
  }

  // Returns true if reporting the exception is successful and the work should be retried.
  private boolean reportFailure(String computation, Windmill.WorkItem work, Throwable t) {
    // Enqueue the errors to be sent to DFE in periodic updates
    addFailure(buildExceptionStackTrace(t, options.getMaxStackTraceDepthToReport()));
    if (windmillServiceEnabled) {
      return true;
    } else {
      Windmill.ReportStatsResponse response =
          windmillServer.reportStats(
              Windmill.ReportStatsRequest.newBuilder()
                  .setComputationId(computation)
                  .setKey(work.getKey())
                  .setShardingKey(work.getShardingKey())
                  .setWorkToken(work.getWorkToken())
                  .build());
      return !response.getFailed();
    }
  }

  /**
   * Adds the given failure message to the queue of messages to be reported to DFE in periodic
   * updates.
   */
  public void addFailure(String failureMessage) {
    synchronized (pendingFailuresToReport) {
      pendingFailuresToReport.add(failureMessage);
    }
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

  @VisibleForTesting
  public void reportPeriodicWorkerUpdates() {
    updateVMMetrics();
    try {
      sendWorkerUpdatesToDataflowService(pendingDeltaCounters, pendingCumulativeCounters);
    } catch (IOException e) {
      LOG.warn("Failed to send periodic counter updates", e);
    } catch (Exception e) {
      LOG.error("Unexpected exception while trying to send counter updates", e);
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
      if (hasExperiment(options, "beam_fn_api")) {
        Map<Object, List<CounterUpdate>> fnApiCounters = new HashMap<>();

        while (!this.pendingMonitoringInfos.isEmpty()) {
          final CounterUpdate item = this.pendingMonitoringInfos.poll();

          // This change will treat counter as delta.
          // This is required because we receive cumulative results from FnAPI harness,
          // while streaming job is expected to receive delta updates to counters on same
          // WorkItem.
          if (item.getCumulative()) {
            item.setCumulative(false);
            // Group counterUpdates by counterUpdateKey so they can be aggregated before sending to
            // dataflow service.
            fnApiCounters
                .computeIfAbsent(getCounterUpdateKey(item), k -> new ArrayList<>())
                .add(item);
          } else {
            // In current world all counters coming from FnAPI are cumulative.
            // This is a safety check in case new counter type appears in FnAPI.
            throw new UnsupportedOperationException(
                "FnApi counters are expected to provide cumulative values."
                    + " Please, update conversion to delta logic"
                    + " if non-cumulative counter type is required.");
          }
        }

        // Aggregates counterUpdates with same counterUpdateKey to single CounterUpdate if possible
        // so we can avoid excessive I/Os for reporting to dataflow service.
        for (List<CounterUpdate> counterUpdateList : fnApiCounters.values()) {
          if (counterUpdateList.isEmpty()) {
            continue;
          }
          List<CounterUpdate> aggregatedCounterUpdateList =
              CounterUpdateAggregators.aggregate(counterUpdateList);

          // Log a warning message if encountered enough non-aggregatable counter updates since this
          // can lead to a severe performance penalty if dataflow service can not handle the
          // updates.
          if (aggregatedCounterUpdateList.size() > 10) {
            CounterUpdate head = aggregatedCounterUpdateList.get(0);
            this.counterAggregationErrorCount.getAndIncrement();
            // log warning message only when error count is the power of 2 to avoid spamming.
            if (this.counterAggregationErrorCount.get() > 10
                && Long.bitCount(this.counterAggregationErrorCount.get()) == 1) {
              LOG.warn(
                  "Found non-aggregated counter updates of size {} with kind {}, this will likely "
                      + "cause performance degradation and excessive GC if size is large.",
                  counterUpdateList.size(),
                  MoreObjects.firstNonNull(
                      head.getNameAndKind(), head.getStructuredNameAndMetadata()));
            }
          }

          counterUpdates.addAll(aggregatedCounterUpdateList);
        }
      }
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

    List<Status> errors;
    synchronized (pendingFailuresToReport) {
      errors = new ArrayList<>(pendingFailuresToReport.size());
      for (String stackTrace : pendingFailuresToReport) {
        errors.add(
            new Status()
                .setCode(2) // rpc.Code.UNKNOWN
                .setMessage(stackTrace));
      }
      pendingFailuresToReport.clear(); // Best effort only, no need to wait till successfully sent.
    }

    WorkItemStatus workItemStatus =
        new WorkItemStatus()
            .setWorkItemId(WINDMILL_COUNTER_UPDATE_WORK_ID)
            .setErrors(errors)
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

  /**
   * Sends a GetData request to Windmill for all sufficiently old active work.
   *
   * <p>This informs Windmill that processing is ongoing and the work should not be retried. The age
   * threshold is determined by {@link
   * StreamingDataflowWorkerOptions#getActiveWorkRefreshPeriodMillis}.
   */
  private void refreshActiveWork() {
    Map<String, List<Windmill.KeyedGetDataRequest>> active = new HashMap<>();
    Instant refreshDeadline =
        Instant.now().minus(Duration.millis(options.getActiveWorkRefreshPeriodMillis()));

    for (Map.Entry<String, ComputationState> entry : computationMap.entrySet()) {
      active.put(entry.getKey(), entry.getValue().getKeysToRefresh(refreshDeadline));
    }

    metricTrackingWindmillServer.refreshActiveWork(active);
  }

  private void invalidateStuckCommits() {
    Instant stuckCommitDeadline =
        Instant.now().minus(Duration.millis(options.getStuckCommitDurationMillis()));
    for (Map.Entry<String, ComputationState> entry : computationMap.entrySet()) {
      entry.getValue().invalidateStuckCommits(stuckCommitDeadline);
    }
  }

  /**
   * Class representing the state of a computation.
   *
   * <p>This class is synchronized, but only used from the dispatch and commit threads, so should
   * not be heavily contended. Still, blocking work should not be done by it.
   */
  static class ComputationState implements AutoCloseable {
    private final String computationId;
    private final MapTask mapTask;
    private final ImmutableMap<String, String> transformUserNameToStateFamily;
    // Map from key to work for the key.  The first item in the queue is
    // actively processing.  Synchronized by itself.
    private final Map<ByteString, Deque<Work>> activeWork = new HashMap<>();
    private final BoundedQueueExecutor executor;
    private final ConcurrentMap<SdkWorkerHarness, ConcurrentLinkedQueue<ExecutionState>>
        executionStateQueues = new ConcurrentHashMap<>();
    private final WindmillStateCache.ForComputation computationStateCache;

    public ComputationState(
        String computationId,
        MapTask mapTask,
        BoundedQueueExecutor executor,
        Map<String, String> transformUserNameToStateFamily,
        WindmillStateCache.ForComputation computationStateCache) {
      this.computationId = computationId;
      this.mapTask = mapTask;
      this.executor = executor;
      this.transformUserNameToStateFamily =
          transformUserNameToStateFamily != null
              ? ImmutableMap.copyOf(transformUserNameToStateFamily)
              : ImmutableMap.of();
      this.computationStateCache = computationStateCache;
      Preconditions.checkNotNull(mapTask.getStageName());
      Preconditions.checkNotNull(mapTask.getSystemName());
    }

    public String getComputationId() {
      return computationId;
    }

    public MapTask getMapTask() {
      return mapTask;
    }

    public ImmutableMap<String, String> getTransformUserNameToStateFamily() {
      return transformUserNameToStateFamily;
    }

    public ConcurrentLinkedQueue<ExecutionState> getExecutionStateQueue(
        SdkWorkerHarness sdkWorkerHarness) {
      return executionStateQueues.computeIfAbsent(
          sdkWorkerHarness, key -> new ConcurrentLinkedQueue<>());
    }

    /** Mark the given key and work as active. */
    public boolean activateWork(ByteString key, Work work) {
      synchronized (activeWork) {
        Deque<Work> queue = activeWork.get(key);
        if (queue != null) {
          Preconditions.checkState(!queue.isEmpty());
          // Ensure we don't already have this work token queueud.
          for (Work queuedWork : queue) {
            if (queuedWork.getWorkItem().getWorkToken() == work.getWorkItem().getWorkToken()) {
              return false;
            }
          }
          // Queue the work for later processing.
          queue.addLast(work);
          return true;
        } else {
          queue = new ArrayDeque<>();
          queue.addLast(work);
          activeWork.put(key, queue);
          // Fall through to execute without the lock held.
        }
      }
      executor.execute(work);
      return true;
    }

    /** Marks the work for a the given key as complete. Schedules queued work for the key if any. */
    public void completeWork(ByteString key, long workToken) {
      Work nextWork;
      synchronized (activeWork) {
        Queue<Work> queue = activeWork.get(key);
        Preconditions.checkNotNull(queue);
        Work completedWork = queue.peek();
        // avoid Preconditions.checkNotNull and checkState here to prevent eagerly evaluating the
        // format string parameters for the error message.
        if (completedWork == null) {
          throw new NullPointerException(
              String.format(
                  "No active state for key %s, expected token %s",
                  TextFormat.escapeBytes(key), workToken));
        }
        if (completedWork.getWorkItem().getWorkToken() != workToken) {
          throw new IllegalStateException(
              String.format(
                  "Token mismatch for key %s: %s and %s",
                  TextFormat.escapeBytes(key),
                  completedWork.getWorkItem().getWorkToken(),
                  workToken));
        }
        queue.remove(); // We consumed the matching work item.
        nextWork = queue.peek();
        if (nextWork == null) {
          Preconditions.checkState(queue == activeWork.remove(key));
        }
      }
      if (nextWork != null) {
        executor.forceExecute(nextWork);
      }
    }

    public void invalidateStuckCommits(Instant stuckCommitDeadline) {
      synchronized (activeWork) {
        // Determine the stuck commit keys but complete them outside of iterating over
        // activeWork as completeWork may delete the entry from activeWork.
        Map<ByteString, Long> stuckCommits = new HashMap<>();
        for (Map.Entry<ByteString, Deque<Work>> entry : activeWork.entrySet()) {
          ByteString key = entry.getKey();
          Work work = entry.getValue().peek();
          if (work.getState() == State.COMMITTING
              && work.getStateStartTime().isBefore(stuckCommitDeadline)) {
            LOG.error(
                "Detected key with sharding key {} stuck in COMMITTING state, completing it with error. Key ",
                work.workItem.getShardingKey());
            stuckCommits.put(key, work.getWorkItem().getWorkToken());
          }
        }
        for (Map.Entry<ByteString, Long> stuckCommit : stuckCommits.entrySet()) {
          computationStateCache.invalidate(stuckCommit.getKey());
          completeWork(stuckCommit.getKey(), stuckCommit.getValue());
        }
      }
    }

    /** Adds any work started before the refreshDeadline to the GetDataRequest builder. */
    public List<Windmill.KeyedGetDataRequest> getKeysToRefresh(Instant refreshDeadline) {
      List<Windmill.KeyedGetDataRequest> result = new ArrayList<>();
      synchronized (activeWork) {
        for (Map.Entry<ByteString, Deque<Work>> entry : activeWork.entrySet()) {
          ByteString key = entry.getKey();
          for (Work work : entry.getValue()) {
            if (work.getStartTime().isBefore(refreshDeadline)) {
              result.add(
                  Windmill.KeyedGetDataRequest.newBuilder()
                      .setKey(key)
                      .setShardingKey(work.getWorkItem().getShardingKey())
                      .setWorkToken(work.getWorkItem().getWorkToken())
                      .build());
            }
          }
        }
      }
      return result;
    }

    private String elapsedString(Instant start, Instant end) {
      Duration activeFor = new Duration(start, end);
      // Duration's toString always starts with "PT"; remove that here.
      return activeFor.toString().substring(2);
    }

    public void printActiveWork(PrintWriter writer) {
      final Instant now = Instant.now();
      // The max number of keys in COMMITTING or COMMIT_QUEUED status to be shown.
      final int maxCommitPending = 50;
      int commitPendingCount = 0;
      writer.println(
          "<table border=\"1\" "
              + "style=\"border-collapse:collapse;padding:5px;border-spacing:5px;border:1px\">");
      writer.println(
          "<tr><th>Key</th><th>Token</th><th>Queued</th><th>Active For</th><th>State</th><th>State Active For</th></tr>");
      // We use a StringBuilder in the synchronized section to buffer writes since the provided
      // PrintWriter may block when flushing.
      StringBuilder builder = new StringBuilder();
      synchronized (activeWork) {
        for (Map.Entry<ByteString, Deque<Work>> entry : activeWork.entrySet()) {
          Queue<Work> queue = entry.getValue();
          Preconditions.checkNotNull(queue);
          Work work = queue.peek();
          Preconditions.checkNotNull(work);
          Windmill.WorkItem workItem = work.getWorkItem();
          State state = work.getState();
          if (state == State.COMMITTING || state == State.COMMIT_QUEUED) {
            if (++commitPendingCount >= maxCommitPending) {
              continue;
            }
          }
          builder.append("<tr>");
          builder.append("<td>");
          builder.append(String.format("%016x", workItem.getShardingKey()));
          builder.append("</td><td>");
          builder.append(workItem.getWorkToken());
          builder.append("</td><td>");
          builder.append(queue.size() - 1);
          builder.append("</td><td>");
          builder.append(elapsedString(work.getStartTime(), now));
          builder.append("</td><td>");
          builder.append(state);
          builder.append("</td></tr>\n");
          builder.append(elapsedString(work.getStateStartTime(), now));
          builder.append("</td></tr>\n");
        }
      }
      writer.print(builder.toString());
      writer.println("</table>");
      if (commitPendingCount >= maxCommitPending) {
        writer.println("<br>");
        writer.print("Skipped keys in COMMITTING/COMMIT_QUEUED: ");
        writer.println(commitPendingCount - maxCommitPending);
        writer.println("<br>");
      }
    }

    @Override
    public void close() throws Exception {
      ExecutionState executionState;
      for (ConcurrentLinkedQueue<ExecutionState> queue : executionStateQueues.values()) {
        while ((executionState = queue.poll()) != null) {
          executionState.getWorkExecutor().close();
        }
      }
      executionStateQueues.clear();
    }
  }

  private static class ExecutionState {
    public final DataflowWorkExecutor workExecutor;
    public final StreamingModeExecutionContext context;
    @Nullable public final Coder<?> keyCoder;
    private final ExecutionStateTracker executionStateTracker;

    public ExecutionState(
        DataflowWorkExecutor workExecutor,
        StreamingModeExecutionContext context,
        Coder<?> keyCoder,
        ExecutionStateTracker executionStateTracker) {
      this.workExecutor = workExecutor;
      this.context = context;
      this.keyCoder = keyCoder;
      this.executionStateTracker = executionStateTracker;
    }

    public DataflowWorkExecutor getWorkExecutor() {
      return workExecutor;
    }

    public StreamingModeExecutionContext getContext() {
      return context;
    }

    public ExecutionStateTracker getExecutionStateTracker() {
      return executionStateTracker;
    }

    @Nullable
    public Coder<?> getKeyCoder() {
      return keyCoder;
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
      writer.println(
          "Worker Threads: "
              + workUnitExecutor.getPoolSize()
              + "/"
              + workUnitExecutor.getMaximumPoolSize()
              + "<br>");
      writer.println("Active Threads: " + workUnitExecutor.getActiveCount() + "<br>");
      writer.println(
          "Work Queue Size: "
              + workUnitExecutor.getQueue().size()
              + "/"
              + MAX_WORK_UNITS_QUEUED
              + "<br>");
      writer.print("Commit Queue: ");
      appendHumanizedBytes(commitQueue.weight(), writer);
      writer.print(", ");
      writer.print(commitQueue.size());
      writer.println(" elements<br>");

      writer.print("Active commit: ");
      appendHumanizedBytes(activeCommitBytes.get(), writer);
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
