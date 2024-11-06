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

import com.google.auto.value.AutoBuilder;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.DataflowMapTaskExecutorFactory;
import org.apache.beam.runners.dataflow.worker.HotKeyLogger;
import org.apache.beam.runners.dataflow.worker.WorkUnitClient;
import org.apache.beam.runners.dataflow.worker.status.DebugCapture;
import org.apache.beam.runners.dataflow.worker.status.WorkerStatusPages;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationStateCache;
import org.apache.beam.runners.dataflow.worker.streaming.WorkHeartbeatResponseProcessor;
import org.apache.beam.runners.dataflow.worker.streaming.config.ComputationConfig;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingEngineComputationConfigFetcher;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingGlobalConfig;
import org.apache.beam.runners.dataflow.worker.streaming.harness.SingleSourceWorkerHarness;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingWorkerStatusPages;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamPool;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.StreamingEngineWorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.GetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.StreamPoolGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.ThrottlingGetDataMetricTracker;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.ChannelzServlet;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcDispatcherClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillServer;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillStubFactoryFactoryImpl;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.FailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.StreamingEngineFailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.WorkFailureProcessor;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.StreamPoolHeartbeatSender;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** Creates a {@link WorkerHarnessInjector} for Streaming Engine. */
final class StreamingEngineHarnessInjector {
  private static final Duration COMMIT_STREAM_TIMEOUT = Duration.standardMinutes(1);
  private static final Duration GET_DATA_STREAM_TIMEOUT = Duration.standardSeconds(30);

  /** @implNote Visible for {@link AutoBuilder}. */
  static WorkerHarnessInjector createStreamingEngineWorkerHarnessInjector(
      long clientId,
      Supplier<Instant> clock,
      DataflowWorkerHarnessOptions options,
      MemoryMonitor memoryMonitor,
      WorkUnitClient dataflowServiceClient,
      ThrottlingGetDataMetricTracker getDataMetricTracker,
      DataflowMapTaskExecutorFactory mapTaskExecutorFactory,
      int retryLocallyDelayMs,
      HotKeyLogger hotKeyLogger,
      @Nullable WindmillServerStub windmillServerOverride,
      @Nullable ComputationStateCache computationStateCacheOverride,
      @Nullable ComputationConfig.Fetcher configFetcherOverride,
      @Nullable WindmillStateCache windmillStateCacheOverride,
      @Nullable BoundedQueueExecutor workExecutorOverride) {
    WorkerHarnessInjector.Builder injectorBuilder =
        WorkerHarnessInjector.builder(options)
            .withStateCacheOverride(windmillStateCacheOverride)
            .withWorkExecutorOverride(workExecutorOverride);
    GrpcDispatcherClient dispatcherClient =
        GrpcDispatcherClient.create(options, new WindmillStubFactoryFactoryImpl(options));
    ComputationConfig.Fetcher configFetcher =
        configFetcherOverride != null
            ? configFetcherOverride
            : createStreamingEngineConfigFetcher(
                options, dataflowServiceClient, dispatcherClient::onJobConfig);
    ComputationStateCache computationStateCache =
        computationStateCacheOverride != null
            ? computationStateCacheOverride
            : ComputationStateCache.create(
                configFetcher,
                injectorBuilder.workExecutor(),
                injectorBuilder.stateCache()::forComputation,
                Environment.globalIdGenerator());
    WindmillServerStub windmillServer =
        windmillServerOverride != null
            ? windmillServerOverride
            : GrpcWindmillServer.create(
                options,
                Environment.createGrpcwindmillStreamFactoryBuilder(options, clientId)
                    .setProcessHeartbeatResponses(
                        new WorkHeartbeatResponseProcessor(computationStateCache::get))
                    .setHealthCheckIntervalMillis(
                        options.getWindmillServiceStreamingRpcHealthCheckPeriodMs())
                    .build(),
                dispatcherClient);
    WindmillStreamPool<WindmillStream.GetDataStream> getDataStreamPool =
        WindmillStreamPool.create(
            Math.max(1, options.getWindmillGetDataStreamCount()),
            GET_DATA_STREAM_TIMEOUT,
            windmillServer::getDataStream);
    GetDataClient getDataClient =
        new StreamPoolGetDataClient(getDataMetricTracker, getDataStreamPool);
    int numCommitThreads = Math.max(1, options.getWindmillServiceCommitThreads());
    WorkCommitter workCommitter =
        StreamingEngineWorkCommitter.builder()
            .setCommitWorkStreamFactory(
                WindmillStreamPool.create(
                        numCommitThreads, COMMIT_STREAM_TIMEOUT, windmillServer::commitWorkStream)
                    ::getCloseableStream)
            .setNumCommitSenders(numCommitThreads)
            .setOnCommitComplete(
                Environment.createCommitCompleter(
                    injectorBuilder.readerCache(),
                    injectorBuilder.stateCache(),
                    computationStateCache))
            .build();
    WorkerStatusPages workerStatusPages =
        WorkerStatusPages.create(Environment.getDefaultStatusPort(), memoryMonitor);
    FailureTracker failureTracker =
        StreamingEngineFailureTracker.create(
            Environment.getMaxFailuresToReportInUpdate(), options.getMaxStackTraceDepthToReport());

    return injectorBuilder
        .setClientId(clientId)
        .setConfigFetcher(configFetcher)
        .setClock(clock)
        .setWorkCommitter(workCommitter)
        .setStatusPages(
            StreamingWorkerStatusPages.builder()
                .setStatusPages(workerStatusPages)
                .setDebugCapture(
                    new DebugCapture.Manager(options, workerStatusPages.getDebugCapturePages()))
                .setChannelzServlet(
                    new ChannelzServlet(options, windmillServer::getWindmillServiceEndpoints)))
        .setComputationStateCache(computationStateCache)
        .setGetDataClient(getDataClient)
        .setFailureTracker(failureTracker)
        .setWindmillQuotaThrottleTimeSupplier(windmillServer::getAndResetThrottleTime)
        .setHarness(
            SingleSourceWorkerHarness.builder()
                .setStreamingWorkScheduler(
                    Environment.WorkExecution.createStreamingWorkScheduler(
                        options,
                        clock,
                        injectorBuilder.readerCache(),
                        mapTaskExecutorFactory,
                        injectorBuilder.workExecutor(),
                        WorkFailureProcessor.builder()
                            .setWorkUnitExecutor(injectorBuilder.workExecutor())
                            .setFailureTracker(failureTracker)
                            .setHeapDumper(() -> Optional.ofNullable(memoryMonitor.tryToDumpHeap()))
                            .setClock(clock)
                            .setRetryLocallyDelayMs(retryLocallyDelayMs)
                            .build(),
                        injectorBuilder.stateCache(),
                        injectorBuilder.streamingCounters(),
                        hotKeyLogger,
                        failureTracker,
                        configFetcher.getGlobalConfigHandle(),
                        injectorBuilder.stageInfo()))
                .setWorkCommitter(workCommitter)
                .setGetDataClient(getDataClient)
                .setComputationStateFetcher(computationStateCache::get)
                .setWaitForResources(() -> memoryMonitor.waitForResources("GetWork"))
                .setHeartbeatSender(
                    options.getUseSeparateWindmillHeartbeatStreams() != null
                        ? StreamPoolHeartbeatSender.create(
                            Boolean.TRUE.equals(options.getUseSeparateWindmillHeartbeatStreams())
                                ? separateHeartbeatPool(windmillServer)
                                : getDataStreamPool)
                        : StreamPoolHeartbeatSender.create(
                            separateHeartbeatPool(windmillServer),
                            getDataStreamPool,
                            configFetcher.getGlobalConfigHandle()))
                .setGetWorkSender(
                    SingleSourceWorkerHarness.GetWorkSender.forStreamingEngine(
                        receiver ->
                            windmillServer.getWorkStream(
                                Environment.WorkExecution.createGetWorkRequest(clientId, options),
                                receiver)))
                .build())
        .build();
  }

  private static StreamingEngineComputationConfigFetcher createStreamingEngineConfigFetcher(
      DataflowWorkerHarnessOptions options,
      WorkUnitClient dataflowServiceClient,
      Consumer<StreamingGlobalConfig> configObserver) {
    StreamingEngineComputationConfigFetcher configFetcher =
        StreamingEngineComputationConfigFetcher.create(
            options.getGlobalConfigRefreshPeriod().getMillis(), dataflowServiceClient);
    configFetcher.getGlobalConfigHandle().registerConfigObserver(configObserver);
    return configFetcher;
  }

  private static WindmillStreamPool<WindmillStream.GetDataStream> separateHeartbeatPool(
      WindmillServerStub windmillServer) {
    return WindmillStreamPool.create(1, GET_DATA_STREAM_TIMEOUT, windmillServer::getDataStream);
  }

  static StreamingEngineHarnessInjector.Builder builder() {
    return new AutoBuilder_StreamingEngineHarnessInjector_Builder();
  }

  @AutoBuilder(callMethod = "createStreamingEngineWorkerHarnessInjector")
  interface Builder extends WorkerHarnessInjector.HarnessInjectorBuilder {
    WorkerHarnessInjector.HarnessInjectorBuilder setDataflowServiceClient(
        WorkUnitClient dataflowServiceClient);
  }
}
