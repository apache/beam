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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillConnection;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.GetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillStreamFactory;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemReceiver;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.StreamingWorkScheduler;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns and maintains a pool of streams used to fetch {@link
 * org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem}(s) from a specific source.
 */
@Internal
@ThreadSafe
public final class WindmillStreamPoolSender implements WindmillStreamSender {
  private static final Logger LOG = LoggerFactory.getLogger(WindmillStreamPoolSender.class);
  private static final int GET_WORK_STREAM_TIMEOUT_MINUTES = 3;
  private final AtomicReference<GetWorkBudget> getWorkBudget;
  private final WindmillConnection connection;
  private final GetWorkRequest getWorkRequest;
  private final GrpcWindmillStreamFactory streamingEngineStreamFactory;
  private final WorkCommitter workCommitter;
  private final GetDataClient getDataClient;
  private final HeartbeatSender heartbeatSender;
  private final StreamingWorkScheduler streamingWorkScheduler;
  private final Runnable waitForResources;
  private final Function<String, Optional<ComputationState>> computationStateFetcher;
  private final ExecutorService workProviderExecutor;
  private final AtomicBoolean isRunning;
  private final AtomicBoolean hasGetWorkStreamStarted;
  private @Nullable GetWorkStream getWorkStream;

  private WindmillStreamPoolSender(
      WindmillConnection connection,
      GetWorkRequest getWorkRequest,
      AtomicReference<GetWorkBudget> getWorkBudget,
      GrpcWindmillStreamFactory streamingEngineStreamFactory,
      WorkCommitter workCommitter,
      GetDataClient getDataClient,
      HeartbeatSender heartbeatSender,
      StreamingWorkScheduler streamingWorkScheduler,
      Runnable waitForResources,
      Function<String, Optional<ComputationState>> computationStateFetcher) {
    this.isRunning = new AtomicBoolean(false);
    this.hasGetWorkStreamStarted = new AtomicBoolean(false);
    this.connection = connection;
    this.getWorkRequest = getWorkRequest;
    this.getWorkBudget = getWorkBudget;
    this.streamingEngineStreamFactory = streamingEngineStreamFactory;
    this.getDataClient = getDataClient;
    this.heartbeatSender = heartbeatSender;
    this.streamingWorkScheduler = streamingWorkScheduler;
    this.waitForResources = waitForResources;
    this.computationStateFetcher = computationStateFetcher;
    this.workCommitter = workCommitter;
    this.workProviderExecutor =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setPriority(Thread.MIN_PRIORITY)
                .setNameFormat("DispatchThread")
                .build());
  }

  public static WindmillStreamPoolSender create(
      WindmillConnection connection,
      GetWorkRequest getWorkRequest,
      GetWorkBudget getWorkBudget,
      GrpcWindmillStreamFactory streamingEngineStreamFactory,
      WorkCommitter workCommitter,
      GetDataClient getDataClient,
      HeartbeatSender heartbeatSender,
      StreamingWorkScheduler streamingWorkScheduler,
      Runnable waitForResources,
      Function<String, Optional<ComputationState>> computationStateFetcher) {
    return new WindmillStreamPoolSender(
        connection,
        getWorkRequest,
        new AtomicReference<>(getWorkBudget),
        streamingEngineStreamFactory,
        workCommitter,
        getDataClient,
        heartbeatSender,
        streamingWorkScheduler,
        waitForResources,
        computationStateFetcher);
  }

  private void dispatchLoop() {
    while (isRunning.get()) {
      this.getWorkStream =
          streamingEngineStreamFactory.createGetWorkStream(
              connection.currentStub(), getWorkRequest, getWorkItemReceiver());
      this.getWorkStream.start();
      this.hasGetWorkStreamStarted.set(true);

      try {
        // Reconnect every now and again to enable better load balancing.
        // If at any point the server closes the stream, we will reconnect immediately;
        // otherwise
        // we half-close the stream after some time and create a new one.
        if (this.getWorkStream != null
            && !this.getWorkStream.awaitTermination(
                GET_WORK_STREAM_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
          if (this.getWorkStream != null) {
            this.getWorkStream.halfClose();
          }
        }
      } catch (InterruptedException e) {
        // Continue processing
      }
    }
  }

  private WorkItemReceiver getWorkItemReceiver() {
    return (computationId,
        inputDataWatermark,
        synchronizedProcessingTime,
        workItem,
        serializedWorkItemSize,
        getWorkStreamLatencies) ->
        this.computationStateFetcher
            .apply(computationId)
            .ifPresent(
                computationState -> {
                  this.waitForResources.run();
                  this.streamingWorkScheduler.scheduleWork(
                      computationState,
                      workItem,
                      serializedWorkItemSize,
                      Watermarks.builder()
                          .setInputDataWatermark(Preconditions.checkNotNull(inputDataWatermark))
                          .setSynchronizedProcessingTime(synchronizedProcessingTime)
                          .setOutputDataWatermark(workItem.getOutputDataWatermark())
                          .build(),
                      Work.createProcessingContext(
                          computationId,
                          this.getDataClient,
                          workCommitter::commit,
                          this.heartbeatSender),
                      getWorkStreamLatencies);
                });
  }

  @SuppressWarnings("ReturnValueIgnored")
  @Override
  public synchronized void start() {
    if (!isRunning.get()) {
      checkState(
          !workProviderExecutor.isShutdown(),
          "WindmillStreamPoolSender has already been shutdown.");
      workCommitter.start();
      isRunning.set(true);
      workProviderExecutor.execute(
          () -> {
            LOG.info("Starting dispatch.");
            dispatchLoop();
            LOG.info("Dispatch done");
          });
    }
  }

  @Override
  public synchronized void close() {
    if (isRunning.get() && this.getWorkStream != null) {
      getWorkStream.shutdown();
      workProviderExecutor.shutdownNow();
      workCommitter.stop();
      isRunning.set(false);
      hasGetWorkStreamStarted.set(false);
    }
  }

  @Override
  public synchronized void setBudget(long items, long bytes) {
    GetWorkBudget budget = GetWorkBudget.builder().setItems(items).setBytes(bytes).build();
    getWorkBudget.set(budget);
    if (isRunning.get() && this.getWorkStream != null) {
      getWorkStream.setBudget(budget);
    }
  }

  @Override
  public long getCurrentActiveCommitBytes() {
    return workCommitter.currentActiveCommitBytes();
  }

  @VisibleForTesting
  public boolean hasGetWorkStreamStarted() {
    return hasGetWorkStreamStarted.get();
  }
}
