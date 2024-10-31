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
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.DataflowMapTaskExecutorFactory;
import org.apache.beam.runners.dataflow.worker.HotKeyLogger;
import org.apache.beam.runners.dataflow.worker.status.WorkerStatusPages;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationStateCache;
import org.apache.beam.runners.dataflow.worker.streaming.config.ComputationConfig;
import org.apache.beam.runners.dataflow.worker.streaming.config.FixedGlobalConfigHandle;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingApplianceComputationConfigFetcher;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingGlobalConfig;
import org.apache.beam.runners.dataflow.worker.streaming.harness.SingleSourceWorkerHarness;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingWorkerStatusPages;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.runners.dataflow.worker.windmill.appliance.JniWindmillApplianceServer;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.StreamingApplianceWorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.ApplianceGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.GetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.ThrottlingGetDataMetricTracker;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcDispatcherClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillServer;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillStubFactoryFactoryImpl;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.FailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.StreamingApplianceFailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.WorkFailureProcessor;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.ApplianceHeartbeatSender;
import org.joda.time.Instant;

/** Creates a {@link WorkerHarnessInjector} for Streaming Engine. */
final class ApplianceHarnessInjector {
  private static final String GRPC_ENDPOINT_PREFIX = "grpc:";

  /** @implNote Visible for {@link AutoBuilder}. */
  static WorkerHarnessInjector createApplianceWorkerHarnessInjector(
      long clientId,
      Supplier<Instant> clock,
      DataflowWorkerHarnessOptions options,
      MemoryMonitor memoryMonitor,
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
    WindmillServerStub windmillServer =
        windmillServerOverride != null
            ? windmillServerOverride
            : createApplianceWindmillServer(options, clientId);
    ComputationConfig.Fetcher configFetcher =
        configFetcherOverride != null
            ? configFetcherOverride
            : new StreamingApplianceComputationConfigFetcher(
                windmillServer::getConfig,
                new FixedGlobalConfigHandle(StreamingGlobalConfig.builder().build()));
    ComputationStateCache computationStateCache =
        computationStateCacheOverride != null
            ? computationStateCacheOverride
            : ComputationStateCache.create(
                configFetcher,
                injectorBuilder.workExecutor(),
                injectorBuilder.stateCache()::forComputation,
                Environment.globalIdGenerator());
    GetDataClient getDataClient = new ApplianceGetDataClient(windmillServer, getDataMetricTracker);
    FailureTracker failureTracker =
        StreamingApplianceFailureTracker.create(
            Environment.getMaxFailuresToReportInUpdate(),
            options.getMaxStackTraceDepthToReport(),
            windmillServer::reportStats);
    WorkCommitter workCommitter =
        StreamingApplianceWorkCommitter.create(
            windmillServer::commitWork,
            Environment.createCommitCompleter(
                injectorBuilder.readerCache(),
                injectorBuilder.stateCache(),
                computationStateCache));
    return injectorBuilder
        .setComputationStateCache(computationStateCache)
        .setClientId(clientId)
        .setClock(clock)
        .setWorkCommitter(workCommitter)
        .setGetDataClient(getDataClient)
        .setConfigFetcher(configFetcher)
        .setFailureTracker(failureTracker)
        .setWindmillQuotaThrottleTimeSupplier(windmillServer::getAndResetThrottleTime)
        .setStatusPages(
            StreamingWorkerStatusPages.builder()
                .setStatusPages(
                    WorkerStatusPages.create(Environment.getDefaultStatusPort(), memoryMonitor)))
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
                .setHeartbeatSender(new ApplianceHeartbeatSender(windmillServer::getData))
                .setGetWorkSender(
                    SingleSourceWorkerHarness.GetWorkSender.forAppliance(
                        () ->
                            windmillServer.getWork(
                                Environment.WorkExecution.createGetWorkRequest(clientId, options))))
                .build())
        .build();
  }

  private static boolean hasLocalWindmillService(DataflowWorkerHarnessOptions options) {
    return options.getWindmillServiceEndpoint() != null
        || options.getLocalWindmillHostport().startsWith(GRPC_ENDPOINT_PREFIX);
  }

  private static WindmillServerStub createApplianceWindmillServer(
      DataflowWorkerHarnessOptions options, long clientId) {
    if (hasLocalWindmillService(options)) {
      return GrpcWindmillServer.create(
          options,
          Environment.createGrpcwindmillStreamFactoryBuilder(options, clientId)
              .setHealthCheckIntervalMillis(
                  options.getWindmillServiceStreamingRpcHealthCheckPeriodMs())
              .build(),
          GrpcDispatcherClient.create(options, new WindmillStubFactoryFactoryImpl(options)));
    }
    return new JniWindmillApplianceServer(options.getLocalWindmillHostport());
  }

  static Builder builder() {
    return new AutoBuilder_ApplianceHarnessInjector_Builder();
  }

  @AutoBuilder(callMethod = "createApplianceWorkerHarnessInjector")
  interface Builder extends WorkerHarnessInjector.HarnessInjectorBuilder {}
}
