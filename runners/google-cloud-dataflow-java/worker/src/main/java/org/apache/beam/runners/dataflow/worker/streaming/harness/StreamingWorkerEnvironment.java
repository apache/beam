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

import static java.util.stream.Collectors.toConcurrentMap;
import static org.apache.beam.runners.dataflow.DataflowRunner.hasExperiment;

import com.google.api.services.dataflow.model.MapTask;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowStreamingPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.StreamingModeExecutionContext;
import org.apache.beam.runners.dataflow.worker.StreamingStepMetricsContainer;
import org.apache.beam.runners.dataflow.worker.apiary.FixMultiOutputInfosOnParDoInstructions;
import org.apache.beam.runners.dataflow.worker.graph.Edges;
import org.apache.beam.runners.dataflow.worker.graph.MapTaskToNetworkFunction;
import org.apache.beam.runners.dataflow.worker.graph.Nodes;
import org.apache.beam.runners.dataflow.worker.status.WorkerStatusPages;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.ChannelzServlet;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.io.gcp.bigquery.BigQuerySinkMetrics;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.graph.MutableNetwork;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities and static factory methods for common components and environmental parameters for
 * {@link StreamingWorkerHarness}.
 */
public final class StreamingWorkerEnvironment {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingWorkerEnvironment.class);

  private static final String ENABLE_BIG_QUERY_METRICS_EXPERIMENT = "enable_per_worker_metrics";
  private static final String DISABLE_SINK_BYTES_LIMIT_EXPERIMENT =
      "disable_limiting_bundle_sink_bytes";
  private static final String CHANNELZ_PATH = "/channelz";
  /* The idGenerator to generate unique id globally. */
  private static final IdGenerator ID_GENERATOR = IdGenerators.decrementingLongs();
  private static final Random CLIENT_ID_GENERATOR = new Random();
  private static final int MAX_PROCESSING_THREADS = 300;
  private static final int DEFAULT_STATUS_PORT = 8081;
  // Maximum size of the result of a GetWork request.
  private static final long MAX_GET_WORK_FETCH_BYTES = 64L << 20; // 64m

  /** Maximum number of failure stacktraces to report in each update sent to backend. */
  private static final int MAX_FAILURES_TO_REPORT_IN_UPDATE = 1000;

  private static final long THREAD_EXPIRATION_TIME_SEC = 60;

  /**
   * Sinks are marked 'full' in {@link StreamingModeExecutionContext} once the amount of data sinked
   * (across all the sinks, if there are more than one) reaches this limit. This serves as hint for
   * readers to stop producing more. This can be disabled with 'disable_limiting_bundle_sink_bytes'
   * experiment.
   */
  private static final int MAX_SINK_BYTES = 10_000_000;

  /**
   * Fix up MapTask representation because MultiOutputInfos are missing from system generated
   * ParDoInstructions.
   */
  private static final Function<MapTask, MapTask> FIX_MULTI_OUTPUT_INFOS =
      new FixMultiOutputInfosOnParDoInstructions(ID_GENERATOR);

  /**
   * Function which converts map tasks to their network representation for execution.
   *
   * <ul>
   *   <li>Translate the map task to a network representation.
   *   <li>Remove flatten instructions by rewiring edges.
   * </ul>
   */
  private static final Function<MapTask, MutableNetwork<Nodes.Node, Edges.Edge>>
      MAP_TASK_TO_BASE_NETWORK = new MapTaskToNetworkFunction(ID_GENERATOR);

  public static IdGenerator idGeneratorInstance() {
    return ID_GENERATOR;
  }

  public static Function<MapTask, MapTask> fixMapTaskMultiOutputInfoFnInstance() {
    return FIX_MULTI_OUTPUT_INFOS;
  }

  static Function<MapTask, MutableNetwork<Nodes.Node, Edges.Edge>>
      mapTaskToBaseNetworkFnInstance() {
    return MAP_TASK_TO_BASE_NETWORK;
  }

  static Duration localHostMaxBackoff() {
    return Duration.millis(500);
  }

  static long newClientId() {
    return CLIENT_ID_GENERATOR.nextLong();
  }

  static int chooseMaximumBundlesOutstanding(DataflowWorkerHarnessOptions options) {
    int maxBundles = options.getMaxBundlesFromWindmillOutstanding();
    if (maxBundles > 0) {
      return maxBundles;
    }
    return chooseMaximumNumberOfThreads(options) + 100;
  }

  private static int chooseMaximumNumberOfThreads(DataflowWorkerHarnessOptions options) {
    if (options.getNumberOfWorkerHarnessThreads() != 0) {
      return options.getNumberOfWorkerHarnessThreads();
    }
    return MAX_PROCESSING_THREADS;
  }

  static long getMaxSinkBytes(DataflowWorkerHarnessOptions options) {
    return hasExperiment(options, DISABLE_SINK_BYTES_LIMIT_EXPERIMENT)
        ? Long.MAX_VALUE
        : MAX_SINK_BYTES;
  }

  static int getMaxFailuresToReportInUpdate() {
    return MAX_FAILURES_TO_REPORT_IN_UPDATE;
  }

  public static BoundedQueueExecutor createWorkUnitExecutor(DataflowWorkerHarnessOptions options) {
    return new BoundedQueueExecutor(
        chooseMaximumNumberOfThreads(options),
        THREAD_EXPIRATION_TIME_SEC,
        TimeUnit.SECONDS,
        chooseMaximumBundlesOutstanding(options),
        chooseMaxBytesOutstanding(options),
        new ThreadFactoryBuilder().setNameFormat("DataflowWorkUnits-%d").setDaemon(true).build());
  }

  static WorkerStatusPages createStatusPages(MemoryMonitor memoryMonitor) {
    return WorkerStatusPages.create(DEFAULT_STATUS_PORT, memoryMonitor, () -> true);
  }

  static void enableBigQueryMetrics(DataflowWorkerHarnessOptions options) {
    Preconditions.checkState(options.isEnableStreamingEngine());
    if (DataflowRunner.hasExperiment(options, ENABLE_BIG_QUERY_METRICS_EXPERIMENT)) {
      // When enabled, the Pipeline will record Per-Worker metrics that will be piped to DFE.
      StreamingStepMetricsContainer.setEnablePerWorkerMetrics(true);
      // StreamingStepMetricsContainer automatically deletes perWorkerCounters if they are
      // zero-valued
      // for longer than 5 minutes.
      BigQuerySinkMetrics.setSupportMetricsDeletion(true);
      // Support metrics for BigQuery's Streaming Inserts write method.
      BigQuerySinkMetrics.setSupportStreamingInsertsMetrics(true);
    }
  }

  static long getMaxGetWorkFetchBytes() {
    return MAX_GET_WORK_FETCH_BYTES;
  }

  static ChannelzServlet createChannelZServlet(
      DataflowStreamingPipelineOptions options,
      Supplier<ImmutableSet<HostAndPort>> currentWindmillEndpoints) {
    return new ChannelzServlet(CHANNELZ_PATH, options, currentWindmillEndpoints);
  }

  static ConcurrentMap<String, ComputationState> createComputationMapForTesting(
      List<MapTask> mapTasks,
      BoundedQueueExecutor workUnitExecutor,
      Function<String, WindmillStateCache.ForComputation> forComputationStateCacheFactory) {
    return mapTasks.stream()
        .map(StreamingWorkerEnvironment.fixMapTaskMultiOutputInfoFnInstance())
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

  public static int getMaxSinkBytes() {
    return MAX_SINK_BYTES;
  }

  private static long chooseMaxBytesOutstanding(DataflowWorkerHarnessOptions options) {
    long maxMem = options.getMaxBytesFromWindmillOutstanding();
    return maxMem > 0 ? maxMem : (Runtime.getRuntime().maxMemory() / 2);
  }
}
