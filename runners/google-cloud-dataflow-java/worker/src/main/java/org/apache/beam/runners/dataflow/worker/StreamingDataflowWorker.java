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

import static org.apache.beam.runners.dataflow.worker.streaming.harness.environment.WorkerHarnessInjector.createHarnessInjector;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.dataflow.model.MapTask;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.runners.core.metrics.MetricsLogger;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationStateCache;
import org.apache.beam.runners.dataflow.worker.streaming.config.ComputationConfig;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingApplianceComputationConfigFetcher;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingEngineComputationConfigFetcher;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingGlobalConfigHandleImpl;
import org.apache.beam.runners.dataflow.worker.streaming.harness.BackgroundMemoryMonitor;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingCounters;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingWorkerHarness;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingWorkerStatusPages;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingWorkerStatusReporter;
import org.apache.beam.runners.dataflow.worker.streaming.harness.environment.Environment;
import org.apache.beam.runners.dataflow.worker.streaming.harness.environment.WorkerHarnessInjector;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.ThrottlingGetDataMetricTracker;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.ActiveWorkRefresher;
import org.apache.beam.sdk.fn.JvmInitializers;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.bigquery.BigQuerySinkMetrics;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.util.construction.CoderTranslation;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Constructs a streaming pipeline worker.
 *
 * @implNote Holds {@link #main(String[])} method for the streaming pipeline worker program.
 */
final class StreamingDataflowWorker {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingDataflowWorker.class);

  private final StreamingWorkerStatusPages statusPages;
  private final ComputationConfig.Fetcher configFetcher;
  private final ComputationStateCache computationStateCache;
  private final BoundedQueueExecutor workUnitExecutor;
  private final StreamingWorkerHarness streamingWorkerHarness;
  private final AtomicBoolean running = new AtomicBoolean();
  private final DataflowWorkerHarnessOptions options;
  private final BackgroundMemoryMonitor memoryMonitor;
  private final ActiveWorkRefresher activeWorkRefresher;
  private final int commitParallelism;
  private final StreamingWorkerStatusReporter workerStatusReporter;
  private final StreamingCounters streamingCounters;

  private StreamingDataflowWorker(
      long clientId,
      WindmillStateCache windmillStateCache,
      BoundedQueueExecutor workUnitExecutor,
      DataflowWorkerHarnessOptions options,
      Supplier<Instant> clock,
      StreamingWorkerStatusReporter workerStatusReporter,
      StreamingCounters streamingCounters,
      MemoryMonitor memoryMonitor,
      ActiveWorkRefresher activeWorkRefresher,
      WorkerHarnessInjector workerHarnessInjector) {
    this.commitParallelism = workerHarnessInjector.workCommitter().parallelism();
    this.configFetcher = workerHarnessInjector.configFetcher();
    this.computationStateCache = workerHarnessInjector.computationStateCache();
    this.options = options;
    this.workUnitExecutor = workUnitExecutor;
    this.workerStatusReporter = workerStatusReporter;
    this.streamingCounters = streamingCounters;
    this.memoryMonitor = BackgroundMemoryMonitor.create(memoryMonitor);
    this.activeWorkRefresher = activeWorkRefresher;
    this.statusPages =
        workerHarnessInjector
            .statusPages()
            .setClock(clock)
            .setClientId(clientId)
            .setIsRunning(running)
            .setStateCache(windmillStateCache)
            .setComputationStateCache(workerHarnessInjector.computationStateCache())
            .setCurrentActiveCommitBytes(
                workerHarnessInjector.workCommitter()::currentActiveCommitBytes)
            .setWorkUnitExecutor(workUnitExecutor)
            .setGlobalConfigHandle(workerHarnessInjector.configFetcher().getGlobalConfigHandle())
            .setGetDataStatusProvider(workerHarnessInjector.getDataClient()::printHtml)
            .build();

    this.streamingWorkerHarness = workerHarnessInjector.harness();

    LOG.debug("windmillServiceEnabled: {}", options.isEnableStreamingEngine());
    LOG.debug("WindmillServiceEndpoint: {}", options.getWindmillServiceEndpoint());
    LOG.debug("WindmillServicePort: {}", options.getWindmillServicePort());
    LOG.debug("LocalWindmillHostport: {}", options.getLocalWindmillHostport());
  }

  private static StreamingDataflowWorker fromOptions(DataflowWorkerHarnessOptions options) {
    MemoryMonitor memoryMonitor = MemoryMonitor.fromOptions(options);
    WorkUnitClient dataflowServiceClient = new DataflowWorkUnitClient(options, LOG);
    ThrottlingGetDataMetricTracker getDataMetricTracker =
        new ThrottlingGetDataMetricTracker(memoryMonitor);
    WorkerHarnessInjector harnessInjector =
        createHarnessInjector(options, dataflowServiceClient)
            .setMemoryMonitor(memoryMonitor)
            .setGetDataMetricTracker(getDataMetricTracker)
            .build();

    return new StreamingDataflowWorker(
        harnessInjector.clientId(),
        harnessInjector.stateCache(),
        harnessInjector.workExecutor(),
        options,
        harnessInjector.clock(),
        StreamingWorkerStatusReporter.create(
            dataflowServiceClient,
            harnessInjector.windmillQuotaThrottleTimeSupplier(),
            harnessInjector.stageInfo()::values,
            harnessInjector.failureTracker(),
            harnessInjector.streamingCounters(),
            memoryMonitor,
            harnessInjector.workExecutor(),
            options.getWindmillHarnessUpdateReportingPeriod().getMillis(),
            options.getPerWorkerMetricsUpdateReportingPeriodMillis()),
        harnessInjector.streamingCounters(),
        memoryMonitor,
        new ActiveWorkRefresher(
            harnessInjector.clock(),
            options.getActiveWorkRefreshPeriodMillis(),
            options.isEnableStreamingEngine()
                ? Math.max(options.getStuckCommitDurationMillis(), 0)
                : 0,
            harnessInjector.computationStateCache()::getAllPresentComputations,
            Environment.sampler(),
            Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("RefreshWork").build()),
            getDataMetricTracker::trackHeartbeats),
        harnessInjector);
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
      WindmillStateCache stateCache) {
    BoundedQueueExecutor workExecutor = Environment.WorkExecution.createWorkExecutor(options);
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
            configFetcher,
            workExecutor,
            stateCache::forComputation,
            Environment.globalIdGenerator(),
            stateNameMap);
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
    ThrottlingGetDataMetricTracker getDataMetricTracker =
        new ThrottlingGetDataMetricTracker(memoryMonitor);

    WorkerHarnessInjector harnessInjector =
        createHarnessInjector(options, workUnitClient)
            .setWindmillServerOverride(windmillServer)
            .setConfigFetcherOverride(configFetcher)
            .setComputationStateCacheOverride(computationStateCache)
            .setWindmillStateCacheOverride(stateCache)
            .setWorkExecutorOverride(workExecutor)
            .setClientId(1)
            .setMemoryMonitor(memoryMonitor)
            .setGetDataMetricTracker(getDataMetricTracker)
            .setRetryLocallyDelayMs(localRetryTimeoutMs)
            .setHotKeyLogger(hotKeyLogger)
            .setMapTaskExecutorFactory(mapTaskExecutorFactory)
            .setClock(clock)
            .build();

    ActiveWorkRefresher activeWorkRefresher =
        new ActiveWorkRefresher(
            clock,
            options.getActiveWorkRefreshPeriodMillis(),
            options.isEnableStreamingEngine()
                ? Math.max(options.getStuckCommitDurationMillis(), 0)
                : 0,
            harnessInjector.computationStateCache()::getAllPresentComputations,
            Environment.sampler(),
            executorSupplier.apply("RefreshWork"),
            getDataMetricTracker::trackHeartbeats);

    return new StreamingDataflowWorker(
        1L,
        stateCache,
        harnessInjector.workExecutor(),
        options,
        clock,
        StreamingWorkerStatusReporter.forTesting(
            publishCounters,
            workUnitClient,
            windmillServer::getAndResetThrottleTime,
            harnessInjector.stageInfo()::values,
            harnessInjector.failureTracker(),
            harnessInjector.streamingCounters(),
            memoryMonitor,
            harnessInjector.workExecutor(),
            executorSupplier,
            options.getWindmillHarnessUpdateReportingPeriod().getMillis(),
            options.getPerWorkerMetricsUpdateReportingPeriodMillis()),
        harnessInjector.streamingCounters(),
        memoryMonitor,
        activeWorkRefresher,
        harnessInjector);
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
  boolean workExecutorIsEmpty() {
    return workUnitExecutor.executorQueueIsEmpty();
  }

  @VisibleForTesting
  int numCommitThreads() {
    return commitParallelism;
  }

  @VisibleForTesting
  ComputationStateCache getComputationStateCache() {
    return computationStateCache;
  }

  @VisibleForTesting
  void start() {
    // Register standard file systems.
    FileSystems.setDefaultPipelineOptions(options);
    running.set(true);
    configFetcher.start();
    memoryMonitor.start();
    streamingWorkerHarness.start();
    Environment.sampler().start();
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

  @VisibleForTesting
  StreamingCounters getStreamingCounters() {
    return streamingCounters;
  }
}
