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
package org.apache.beam.runners.dataflow.worker.streaming.harness.environment;

import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.DataflowMapTaskExecutorFactory;
import org.apache.beam.runners.dataflow.worker.HotKeyLogger;
import org.apache.beam.runners.dataflow.worker.ReaderCache;
import org.apache.beam.runners.dataflow.worker.StreamingModeExecutionContext;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationStateCache;
import org.apache.beam.runners.dataflow.worker.streaming.StageInfo;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingGlobalConfigHandle;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingCounters;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.CommitCompleter;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillServer;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillStreamFactory;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.StreamingWorkScheduler;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.FailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.WorkFailureProcessor;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** Globals and utilities for streaming pipeline worker. */
@Internal
public final class Environment {

  /** Maximum number of failure stacktraces to report in each update sent to backend. */
  private static final int MAX_FAILURES_TO_REPORT_IN_UPDATE = 1000;

  private static final IdGenerator ID_GENERATOR = IdGenerators.decrementingLongs();
  private static final int DEFAULT_STATUS_PORT = 8081;
  private static final Random CLIENT_ID_GENERATOR = new Random();

  /**
   * Sinks are marked 'full' in {@link StreamingModeExecutionContext} once the amount of data sinked
   * (across all the sinks, if there are more than one) reaches this limit. This serves as hint for
   * readers to stop producing more. This can be disabled with 'disable_limiting_bundle_sink_bytes'
   * experiment.
   */
  private static final int MAX_SINK_BYTES = 10_000_000;

  private Environment() {}

  static int getDefaultStatusPort() {
    return DEFAULT_STATUS_PORT;
  }

  static int getMaxFailuresToReportInUpdate() {
    return MAX_FAILURES_TO_REPORT_IN_UPDATE;
  }

  public static IdGenerator globalIdGenerator() {
    return ID_GENERATOR;
  }

  public static long getNewClientId() {
    return CLIENT_ID_GENERATOR.nextLong();
  }

  public static DataflowExecutionStateSampler sampler() {
    return DataflowExecutionStateSampler.instance();
  }

  public static int maxSinkBytes() {
    return MAX_SINK_BYTES;
  }

  static CommitCompleter createCommitCompleter(
      ReaderCache readerCache,
      WindmillStateCache stateCache,
      ComputationStateCache computationStateCache) {
    return new CommitCompleter(
        readerCache::invalidateReader, stateCache::forComputation, computationStateCache::get);
  }

  static GrpcWindmillStreamFactory.Builder createGrpcwindmillStreamFactoryBuilder(
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
        .setSendKeyedGetDataRequests(
            !options.isEnableStreamingEngine()
                || DataflowRunner.hasExperiment(
                    options, "streaming_engine_disable_new_heartbeat_requests"));
  }

  @Internal
  public static final class WorkExecution {
    /**
     * Maximum number of threads for processing. Currently, each thread processes one key at a time.
     */
    private static final int MAX_PROCESSING_THREADS = 300;

    /** Maximum size of the result of a GetWork request. */
    private static final long MAX_GET_WORK_FETCH_BYTES = 64L << 20; // 64m

    private static final long THREAD_EXPIRATION_TIME_SEC = 60;

    private WorkExecution() {}

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

    static Windmill.GetWorkRequest createGetWorkRequest(
        long clientId, DataflowWorkerHarnessOptions options) {
      return Windmill.GetWorkRequest.newBuilder()
          .setClientId(clientId)
          .setMaxItems(chooseMaxBundlesOutstanding(options))
          .setMaxBytes(MAX_GET_WORK_FETCH_BYTES)
          .build();
    }

    public static BoundedQueueExecutor createWorkExecutor(DataflowWorkerHarnessOptions options) {
      return new BoundedQueueExecutor(
          chooseMaxThreads(options),
          THREAD_EXPIRATION_TIME_SEC,
          TimeUnit.SECONDS,
          chooseMaxBundlesOutstanding(options),
          chooseMaxBytesOutstanding(options),
          new ThreadFactoryBuilder().setNameFormat("DataflowWorkUnits-%d").setDaemon(true).build());
    }

    static StreamingWorkScheduler createStreamingWorkScheduler(
        DataflowWorkerHarnessOptions options,
        Supplier<Instant> clock,
        ReaderCache readerCache,
        DataflowMapTaskExecutorFactory mapTaskExecutorFactory,
        BoundedQueueExecutor workExecutor,
        WorkFailureProcessor workFailureProcessor,
        WindmillStateCache stateCache,
        StreamingCounters streamingCounters,
        HotKeyLogger hotKeyLogger,
        FailureTracker failureTracker,
        StreamingGlobalConfigHandle globalConfigHandle,
        ConcurrentMap<String, StageInfo> stageInfo) {
      return StreamingWorkScheduler.create(
          options,
          clock,
          readerCache,
          mapTaskExecutorFactory,
          workExecutor,
          stateCache::forComputation,
          failureTracker,
          workFailureProcessor,
          streamingCounters,
          hotKeyLogger,
          sampler(),
          globalIdGenerator(),
          globalConfigHandle,
          stageInfo);
    }
  }
}
