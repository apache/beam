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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

import com.google.auto.value.AutoBuilder;
import com.google.auto.value.AutoOneOf;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.WindmillTimeUtils;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub.RpcException;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.GetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemReceiver;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.StreamingWorkScheduler;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link StreamingWorkerHarness} implementations that fetch {@link
 * org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem}(s) from a single source.
 */
@Internal
public final class SingleSourceWorkerHarness implements StreamingWorkerHarness {
  private static final Logger LOG = LoggerFactory.getLogger(SingleSourceWorkerHarness.class);
  private static final int GET_WORK_STREAM_TIMEOUT_MINUTES = 3;

  private final AtomicBoolean isRunning;
  private final WorkCommitter workCommitter;
  private final GetDataClient getDataClient;
  private final HeartbeatSender heartbeatSender;
  private final StreamingWorkScheduler streamingWorkScheduler;
  private final Runnable waitForResources;
  private final Function<String, Optional<ComputationState>> computationStateFetcher;
  private final ExecutorService workProviderExecutor;
  private final GetWorkSender getWorkSender;
  private final ThrottledTimeTracker throttleTimeTracker;

  SingleSourceWorkerHarness(
      WorkCommitter workCommitter,
      GetDataClient getDataClient,
      HeartbeatSender heartbeatSender,
      StreamingWorkScheduler streamingWorkScheduler,
      Runnable waitForResources,
      Function<String, Optional<ComputationState>> computationStateFetcher,
      GetWorkSender getWorkSender,
      ThrottledTimeTracker throttleTimeTracker) {
    this.workCommitter = workCommitter;
    this.getDataClient = getDataClient;
    this.heartbeatSender = heartbeatSender;
    this.streamingWorkScheduler = streamingWorkScheduler;
    this.waitForResources = waitForResources;
    this.computationStateFetcher = computationStateFetcher;
    this.workProviderExecutor =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setPriority(Thread.MIN_PRIORITY)
                .setNameFormat("DispatchThread")
                .build());
    this.isRunning = new AtomicBoolean(false);
    this.getWorkSender = getWorkSender;
    this.throttleTimeTracker = throttleTimeTracker;
  }

  public static SingleSourceWorkerHarness.Builder builder() {
    return new AutoBuilder_SingleSourceWorkerHarness_Builder();
  }

  @Override
  public void start() {
    Preconditions.checkState(
        isRunning.compareAndSet(false, true),
        "Multiple calls to {}.start() are not allowed.",
        getClass());
    workCommitter.start();
    workProviderExecutor.execute(
        () -> {
          getDispatchLoop().run();
          LOG.info("Dispatch done");
        });
  }

  private Runnable getDispatchLoop() {
    switch (getWorkSender.getKind()) {
      case APPLIANCE:
        LOG.info("Starting Dispatch in Appliance mode.");
        return () -> applianceDispatchLoop(getWorkSender.appliance());
      case STREAMING_ENGINE:
        LOG.info("Starting Dispatch in Streaming Engine mode.");
        return () -> streamingEngineDispatchLoop(getWorkSender.streamingEngine());
      default:
        // Will never happen switch is exhaustive.
        throw new IllegalStateException("Invalid GetWorkSender.Kind: " + getWorkSender.getKind());
    }
  }

  @Override
  public void shutdown() {
    Preconditions.checkState(
        isRunning.compareAndSet(true, false),
        "Multiple calls to {}.shutdown() are not allowed.",
        getClass());
    workProviderExecutor.shutdown();
    boolean isTerminated = false;
    try {
      isTerminated = workProviderExecutor.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Unable to shutdown {}", getClass());
    }

    if (!isTerminated) {
      workProviderExecutor.shutdownNow();
    }
    workCommitter.stop();
  }

  @Override
  public long getAndResetThrottleTime() {
    return throttleTimeTracker.getAndResetThrottleTime();
  }

  private void streamingEngineDispatchLoop(
      Function<WorkItemReceiver, WindmillStream.GetWorkStream> getWorkStreamFactory) {
    while (isRunning.get()) {
      WindmillStream.GetWorkStream stream =
          getWorkStreamFactory.apply(
              (computationId,
                  inputDataWatermark,
                  synchronizedProcessingTime,
                  workItem,
                  getWorkStreamLatencies) ->
                  computationStateFetcher
                      .apply(computationId)
                      .ifPresent(
                          computationState -> {
                            waitForResources.run();
                            streamingWorkScheduler.scheduleWork(
                                computationState,
                                workItem,
                                Watermarks.builder()
                                    .setInputDataWatermark(
                                        Preconditions.checkNotNull(inputDataWatermark))
                                    .setSynchronizedProcessingTime(synchronizedProcessingTime)
                                    .setOutputDataWatermark(workItem.getOutputDataWatermark())
                                    .build(),
                                Work.createProcessingContext(
                                    computationId,
                                    getDataClient,
                                    workCommitter::commit,
                                    heartbeatSender),
                                getWorkStreamLatencies);
                          }));
      try {
        // Reconnect every now and again to enable better load balancing.
        // If at any point the server closes the stream, we will reconnect immediately; otherwise
        // we half-close the stream after some time and create a new one.
        if (!stream.awaitTermination(GET_WORK_STREAM_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
          stream.halfClose();
        }
      } catch (InterruptedException e) {
        // Continue processing until !running.get()
      }
    }
  }

  private void applianceDispatchLoop(Supplier<Windmill.GetWorkResponse> getWorkFn) {
    while (isRunning.get()) {
      waitForResources.run();
      int backoff = 1;
      Windmill.GetWorkResponse workResponse = null;
      do {
        try {
          workResponse = getWorkFn.get();
          if (workResponse.getWorkCount() > 0) {
            break;
          }
        } catch (RpcException e) {
          LOG.warn("GetWork failed, retrying:", e);
        }
        sleepUninterruptibly(backoff, TimeUnit.MILLISECONDS);
        backoff = Math.min(1000, backoff * 2);
      } while (isRunning.get());
      for (Windmill.ComputationWorkItems computationWork :
          Preconditions.checkNotNull(workResponse).getWorkList()) {
        String computationId = computationWork.getComputationId();
        Optional<ComputationState> maybeComputationState =
            computationStateFetcher.apply(computationId);
        if (!maybeComputationState.isPresent()) {
          continue;
        }

        ComputationState computationState = maybeComputationState.get();
        Instant inputDataWatermark =
            WindmillTimeUtils.windmillToHarnessWatermark(computationWork.getInputDataWatermark());
        Watermarks.Builder watermarks =
            Watermarks.builder()
                .setInputDataWatermark(Preconditions.checkNotNull(inputDataWatermark))
                .setSynchronizedProcessingTime(
                    WindmillTimeUtils.windmillToHarnessWatermark(
                        computationWork.getDependentRealtimeInputWatermark()));

        for (Windmill.WorkItem workItem : computationWork.getWorkList()) {
          streamingWorkScheduler.scheduleWork(
              computationState,
              workItem,
              watermarks.setOutputDataWatermark(workItem.getOutputDataWatermark()).build(),
              Work.createProcessingContext(
                  computationId, getDataClient, workCommitter::commit, heartbeatSender),
              /* getWorkStreamLatencies= */ Collections.emptyList());
        }
      }
    }
  }

  @AutoBuilder
  public interface Builder {
    Builder setWorkCommitter(WorkCommitter workCommitter);

    Builder setGetDataClient(GetDataClient getDataClient);

    Builder setHeartbeatSender(HeartbeatSender heartbeatSender);

    Builder setStreamingWorkScheduler(StreamingWorkScheduler streamingWorkScheduler);

    Builder setWaitForResources(Runnable waitForResources);

    Builder setComputationStateFetcher(
        Function<String, Optional<ComputationState>> computationStateFetcher);

    Builder setGetWorkSender(GetWorkSender getWorkSender);

    Builder setThrottleTimeTracker(ThrottledTimeTracker throttleTimeTracker);

    SingleSourceWorkerHarness build();
  }

  @AutoOneOf(GetWorkSender.Kind.class)
  public abstract static class GetWorkSender {

    public static GetWorkSender forStreamingEngine(
        Function<WorkItemReceiver, WindmillStream.GetWorkStream> getWorkStreamFactory) {
      return AutoOneOf_SingleSourceWorkerHarness_GetWorkSender.streamingEngine(
          getWorkStreamFactory);
    }

    public static GetWorkSender forAppliance(Supplier<Windmill.GetWorkResponse> getWorkFn) {
      return AutoOneOf_SingleSourceWorkerHarness_GetWorkSender.appliance(getWorkFn);
    }

    abstract Function<WorkItemReceiver, WindmillStream.GetWorkStream> streamingEngine();

    abstract Supplier<Windmill.GetWorkResponse> appliance();

    abstract Kind getKind();

    enum Kind {
      STREAMING_ENGINE,
      APPLIANCE
    }
  }
}
