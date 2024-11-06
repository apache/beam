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

import com.google.auto.value.AutoValue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.DataflowMapTaskExecutorFactory;
import org.apache.beam.runners.dataflow.worker.HotKeyLogger;
import org.apache.beam.runners.dataflow.worker.IntrinsicMapTaskExecutorFactory;
import org.apache.beam.runners.dataflow.worker.ReaderCache;
import org.apache.beam.runners.dataflow.worker.WorkUnitClient;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationStateCache;
import org.apache.beam.runners.dataflow.worker.streaming.StageInfo;
import org.apache.beam.runners.dataflow.worker.streaming.config.ComputationConfig;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingCounters;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingWorkerHarness;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingWorkerStatusPages;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.GetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.ThrottlingGetDataMetricTracker;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.FailureTracker;
import org.apache.beam.sdk.annotations.Internal;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** Injector for to create {@link StreamingWorkerHarness} implementations and dependencies. */
@AutoValue
@Internal
public abstract class WorkerHarnessInjector {

  static Builder builder(DataflowWorkerHarnessOptions options) {
    return new AutoValue_WorkerHarnessInjector.Builder()
        .setStreamingCounters(StreamingCounters.create())
        .setStageInfo(new ConcurrentHashMap<>())
        .setStateCache(
            WindmillStateCache.builder()
                .setSizeMb(options.getWorkerCacheMb())
                .setSupportMapViaMultimap(options.isEnableStreamingEngine())
                .build())
        .setReaderCache(
            new ReaderCache(
                Duration.standardSeconds(options.getReaderCacheTimeoutSec()),
                Executors.newCachedThreadPool()))
        .setWorkExecutor(Environment.WorkExecution.createWorkExecutor(options));
  }

  public static WorkerHarnessInjector.HarnessInjectorBuilder createHarnessInjector(
      DataflowWorkerHarnessOptions options, WorkUnitClient dataflowServiceClient) {
    return (options.isEnableStreamingEngine()
            ? StreamingEngineHarnessInjector.builder()
                .setDataflowServiceClient(dataflowServiceClient)
            : ApplianceHarnessInjector.builder())
        .setHotKeyLogger(new HotKeyLogger())
        .setMapTaskExecutorFactory(IntrinsicMapTaskExecutorFactory.defaultFactory())
        .setRetryLocallyDelayMs(-1)
        .setClock(Instant::now)
        .setOptions(options)
        .setClientId(Environment.getNewClientId());
  }

  public abstract long clientId();

  public abstract WorkCommitter workCommitter();

  public abstract StreamingWorkerHarness harness();

  public abstract GetDataClient getDataClient();

  public abstract WindmillStateCache stateCache();

  public abstract ReaderCache readerCache();

  public abstract BoundedQueueExecutor workExecutor();

  public abstract StreamingWorkerStatusPages.Builder statusPages();

  public abstract ComputationStateCache computationStateCache();

  public abstract ComputationConfig.Fetcher configFetcher();

  public abstract FailureTracker failureTracker();

  public abstract Supplier<Long> windmillQuotaThrottleTimeSupplier();

  public abstract ConcurrentMap<String, StageInfo> stageInfo();

  public abstract StreamingCounters streamingCounters();

  public abstract Supplier<Instant> clock();

  public interface HarnessInjectorBuilder {

    HarnessInjectorBuilder setClientId(long clientId);

    HarnessInjectorBuilder setOptions(DataflowWorkerHarnessOptions options);

    HarnessInjectorBuilder setMemoryMonitor(MemoryMonitor memoryMonitor);

    HarnessInjectorBuilder setGetDataMetricTracker(
        ThrottlingGetDataMetricTracker getDataMetricTracker);

    HarnessInjectorBuilder setWindmillServerOverride(@Nullable WindmillServerStub windmillServer);

    HarnessInjectorBuilder setComputationStateCacheOverride(
        @Nullable ComputationStateCache computationStateCache);

    HarnessInjectorBuilder setConfigFetcherOverride(
        @Nullable ComputationConfig.Fetcher configFetcherOverride);

    HarnessInjectorBuilder setWindmillStateCacheOverride(
        @Nullable WindmillStateCache windmillStateCache);

    HarnessInjectorBuilder setWorkExecutorOverride(@Nullable BoundedQueueExecutor workExecutor);

    HarnessInjectorBuilder setMapTaskExecutorFactory(
        DataflowMapTaskExecutorFactory dataflowMapTaskExecutorFactory);

    HarnessInjectorBuilder setRetryLocallyDelayMs(int retryLocallyDelayMs);

    HarnessInjectorBuilder setHotKeyLogger(HotKeyLogger hotKeyLogger);

    HarnessInjectorBuilder setClock(Supplier<Instant> clock);

    WorkerHarnessInjector build();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setClientId(long clientId);

    abstract Builder setWorkCommitter(WorkCommitter workCommitter);

    abstract Builder setHarness(StreamingWorkerHarness harness);

    abstract Builder setGetDataClient(GetDataClient getDataClient);

    abstract Builder setComputationStateCache(ComputationStateCache computationStateCache);

    abstract Builder setStatusPages(StreamingWorkerStatusPages.Builder statusPages);

    abstract Builder setConfigFetcher(ComputationConfig.Fetcher configFetcher);

    abstract Builder setFailureTracker(FailureTracker failureTracker);

    abstract ReaderCache readerCache();

    abstract Builder setReaderCache(ReaderCache readerCache);

    abstract WindmillStateCache stateCache();

    abstract Builder setStateCache(WindmillStateCache windmillStateCache);

    abstract BoundedQueueExecutor workExecutor();

    abstract Builder setWorkExecutor(BoundedQueueExecutor workExecutor);

    abstract StreamingCounters streamingCounters();

    abstract Builder setStreamingCounters(StreamingCounters streamingCounters);

    abstract ConcurrentMap<String, StageInfo> stageInfo();

    abstract Builder setStageInfo(ConcurrentMap<String, StageInfo> stageInfo);

    abstract Builder setWindmillQuotaThrottleTimeSupplier(
        Supplier<Long> windmillQuotaThrottleTimeSupplier);

    abstract Builder setClock(Supplier<Instant> clock);

    abstract WorkerHarnessInjector build();

    final Builder withStateCacheOverride(@Nullable WindmillStateCache stateCacheOverride) {
      return stateCacheOverride != null ? setStateCache(stateCacheOverride) : this;
    }

    final Builder withWorkExecutorOverride(@Nullable BoundedQueueExecutor workExecutorOverride) {
      return workExecutorOverride != null ? setWorkExecutor(workExecutorOverride) : this;
    }
  }
}
