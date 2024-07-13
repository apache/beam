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
package org.apache.beam.runners.dataflow.worker.windmill.work.provider;

import com.google.auto.value.AutoValue;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.GetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemReceiver;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.StreamingWorkScheduler;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link WorkProvider} implementations that fetch {@link
 * org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem}(s) from a single source.
 */
@Internal
public abstract class SingleSourceWorkProvider implements WorkProvider {
  private static final Logger LOG = LoggerFactory.getLogger(SingleSourceWorkProvider.class);
  protected final AtomicBoolean isRunning;
  protected final WorkCommitter workCommitter;
  protected final GetDataClient getDataClient;
  protected final HeartbeatSender heartbeatSender;
  protected final StreamingWorkScheduler streamingWorkScheduler;
  protected final Runnable waitForResources;
  protected final Function<String, Optional<ComputationState>> computationStateFetcher;
  private final ExecutorService workProviderExecutor;

  protected SingleSourceWorkProvider(
      WorkCommitter workCommitter,
      GetDataClient getDataClient,
      HeartbeatSender heartbeatSender,
      StreamingWorkScheduler streamingWorkScheduler,
      Runnable waitForResources,
      Function<String, Optional<ComputationState>> computationStateFetcher) {
    this.workCommitter = workCommitter;
    this.getDataClient = getDataClient;
    this.heartbeatSender = heartbeatSender;
    this.streamingWorkScheduler = streamingWorkScheduler;
    this.waitForResources = waitForResources;
    this.computationStateFetcher = computationStateFetcher;
    this.workProviderExecutor =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setPriority(Thread.MIN_PRIORITY)
                .setNameFormat(debugName() + "DispatchThread")
                .build());
    this.isRunning = new AtomicBoolean(false);
  }

  public static SingleSourceWorkProviderBuilder.Builder builder() {
    return new AutoValue_SingleWorkProviderBuilder.Builder();
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  @Override
  public final void start() {
    if (isRunning.compareAndSet(true, false) && !workProviderExecutor.isShutdown()) {
      workProviderExecutor.submit(
          () -> {
            LOG.info("Dispatch starting");
            dispatchLoop();
            LOG.info("Dispatch done");
          });
      workCommitter.start();
    }
  }

  @Override
  public final void shutdown() {
    if (isRunning.get() && !workProviderExecutor.isShutdown()) {
      workProviderExecutor.shutdown();
      boolean isTerminated = false;
      try {
        isTerminated = workProviderExecutor.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOG.warn("Unable to shutdown WorkProvider");
      }

      if (!isTerminated) {
        workProviderExecutor.shutdownNow();
      }
      workCommitter.stop();
      isRunning.set(false);
    }
  }

  protected abstract void dispatchLoop();

  protected abstract String debugName();

  @AutoValue
  public abstract static class SingleSourceWorkProviderBuilder {
    public abstract WorkCommitter workCommitter();

    public abstract GetDataClient getDataClient();

    public abstract HeartbeatSender heartbeatSender();

    public abstract StreamingWorkScheduler streamingWorkScheduler();

    public abstract Runnable waitForResources();

    public abstract Function<String, Optional<ComputationState>> computationStateFetcher();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setWorkCommitter(WorkCommitter value);

      public abstract Builder setGetDataClient(GetDataClient value);

      public abstract Builder setHeartbeatSender(HeartbeatSender value);

      public abstract Builder setStreamingWorkScheduler(StreamingWorkScheduler value);

      public abstract Builder setWaitForResources(Runnable value);

      public abstract Builder setComputationStateFetcher(
          Function<String, Optional<ComputationState>> value);

      abstract SingleSourceWorkProviderBuilder autoBuild();

      public final WorkProvider buildForAppliance(Supplier<Windmill.GetWorkResponse> getWorkFn) {
        SingleSourceWorkProviderBuilder params = autoBuild();
        return new ApplianceWorkProvider(
            params.workCommitter(),
            params.getDataClient(),
            params.heartbeatSender(),
            params.streamingWorkScheduler(),
            params.waitForResources(),
            params.computationStateFetcher(),
            getWorkFn);
      }

      public final WorkProvider buildForStreamingEngine(
          Function<WorkItemReceiver, WindmillStream.GetWorkStream> getWorkStreamFactory) {
        SingleSourceWorkProviderBuilder params = autoBuild();
        return new StreamingEngineWorkProvider(
            params.workCommitter(),
            params.getDataClient(),
            params.heartbeatSender(),
            params.streamingWorkScheduler(),
            params.waitForResources(),
            params.computationStateFetcher(),
            getWorkStreamFactory);
      }
    }
  }
}
