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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillConnection;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.CommitWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.GetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillStreamFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.throttling.StreamingEngineThrottleTimers;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemScheduler;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudgetSpender;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.FixedStreamHeartbeatSender;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns and maintains a set of streams used to communicate with a specific Windmill worker.
 *
 * <p>Once started, the underlying streams are "alive" until they are manually closed via {@link
 * #close()}.
 *
 * <p>If closed, it means that the backend endpoint is no longer in the worker set. Once closed,
 * these instances are not reused.
 *
 * @implNote Does not manage streams for fetching {@link
 *     org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalData} for side inputs.
 */
@Internal
@ThreadSafe
final class WindmillStreamSender implements GetWorkBudgetSpender, StreamSender {
  private static final Logger LOG = LoggerFactory.getLogger(WindmillStreamSender.class);
  private static final String STREAM_MANAGER_THREAD_NAME_FORMAT = "WindmillStreamManagerThread";
  private static final int GET_WORK_STREAM_TTL_MINUTES = 45;
  private static final int TERMINATION_TIMEOUT_SECONDS = 5;

  private final AtomicBoolean isRunning = new AtomicBoolean(false);
  private final GetDataStream getDataStream;
  private final CommitWorkStream commitWorkStream;
  private final WorkCommitter workCommitter;
  private final StreamingEngineThrottleTimers streamingEngineThrottleTimers;
  private final ExecutorService streamStarter;
  private final String backendWorkerToken;

  @GuardedBy("activeGetWorkStream")
  private final AtomicReference<GetWorkStream> activeGetWorkStream;

  @GuardedBy("activeGetWorkStream")
  private final AtomicReference<GetWorkBudget> getWorkBudget;

  @GuardedBy("activeGetWorkStream")
  private final Supplier<GetWorkStream> getWorkStreamFactory;

  private WindmillStreamSender(
      WindmillConnection connection,
      GetWorkRequest getWorkRequest,
      AtomicReference<GetWorkBudget> getWorkBudget,
      GrpcWindmillStreamFactory streamingEngineStreamFactory,
      WorkItemScheduler workItemScheduler,
      Function<GetDataStream, GetDataClient> getDataClientFactory,
      Function<CommitWorkStream, WorkCommitter> workCommitterFactory) {
    this.backendWorkerToken = connection.backendWorkerToken();
    this.getWorkBudget = getWorkBudget;
    this.streamingEngineThrottleTimers = StreamingEngineThrottleTimers.create();
    // Stream instances connect/reconnect internally, so we can reuse the same instance through the
    // entire lifecycle of WindmillStreamSender.
    this.getDataStream =
        streamingEngineStreamFactory.createDirectGetDataStream(
            connection, streamingEngineThrottleTimers.getDataThrottleTimer());
    this.commitWorkStream =
        streamingEngineStreamFactory.createDirectCommitWorkStream(
            connection, streamingEngineThrottleTimers.commitWorkThrottleTimer());
    this.workCommitter = workCommitterFactory.apply(commitWorkStream);
    this.activeGetWorkStream = new AtomicReference<>();
    this.getWorkStreamFactory =
        () ->
            streamingEngineStreamFactory.createDirectGetWorkStream(
                connection,
                withRequestBudget(getWorkRequest, getWorkBudget.get()),
                streamingEngineThrottleTimers.getWorkThrottleTimer(),
                FixedStreamHeartbeatSender.create(getDataStream),
                getDataClientFactory.apply(getDataStream),
                workCommitter,
                workItemScheduler);
    // 3 threads, 1 for each stream type (GetWork, GetData, CommitWork).
    this.streamStarter =
        Executors.newFixedThreadPool(
            3,
            new ThreadFactoryBuilder()
                .setNameFormat(STREAM_MANAGER_THREAD_NAME_FORMAT + "-" + backendWorkerToken + "-%d")
                .build());
  }

  static WindmillStreamSender create(
      WindmillConnection connection,
      GetWorkRequest getWorkRequest,
      GetWorkBudget getWorkBudget,
      GrpcWindmillStreamFactory streamingEngineStreamFactory,
      WorkItemScheduler workItemScheduler,
      Function<GetDataStream, GetDataClient> getDataClientFactory,
      Function<CommitWorkStream, WorkCommitter> workCommitterFactory) {
    return new WindmillStreamSender(
        connection,
        getWorkRequest,
        new AtomicReference<>(getWorkBudget),
        streamingEngineStreamFactory,
        workItemScheduler,
        getDataClientFactory,
        workCommitterFactory);
  }

  private static GetWorkRequest withRequestBudget(GetWorkRequest request, GetWorkBudget budget) {
    return request.toBuilder().setMaxItems(budget.items()).setMaxBytes(budget.bytes()).build();
  }

  synchronized void start() {
    if (isRunning.compareAndSet(false, true)) {
      checkState(!streamStarter.isShutdown(), "WindmillStreamSender has already been shutdown.");
      // Start these 3 streams in parallel since they each may perform blocking IO.
      CountDownLatch waitForInitialStream = new CountDownLatch(1);
      streamStarter.execute(() -> getWorkStreamLoop(waitForInitialStream));
      CompletableFuture.allOf(
              CompletableFuture.runAsync(getDataStream::start, streamStarter),
              CompletableFuture.runAsync(commitWorkStream::start, streamStarter))
          .join();
      try {
        waitForInitialStream.await();
      } catch (InterruptedException e) {
        close();
        LOG.error("GetWorkStream to {} was never able to start.", backendWorkerToken);
        throw new IllegalStateException("GetWorkStream unable to start aborting.", e);
      }
      workCommitter.start();
    }
  }

  @Override
  public synchronized void close() {
    isRunning.set(false);
    streamStarter.shutdownNow();
    getDataStream.shutdown();
    workCommitter.stop();
    commitWorkStream.shutdown();
    try {
      if (!Preconditions.checkNotNull(streamStarter)
          .awaitTermination(TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        streamStarter.shutdownNow();
      }
      Preconditions.checkNotNull(getDataStream)
          .awaitTermination(TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      Preconditions.checkNotNull(commitWorkStream)
          .awaitTermination(TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while waiting for streams to terminate", e);
    }
  }

  @Override
  public void setBudget(long items, long bytes) {
    synchronized (activeGetWorkStream) {
      GetWorkBudget budget = GetWorkBudget.builder().setItems(items).setBytes(bytes).build();
      getWorkBudget.set(budget);
      if (isRunning.get()) {
        @Nullable GetWorkStream stream = activeGetWorkStream.get();
        // activeGetWorkStream could be null if start() was called but activeGetWorkStream was not
        // populated yet. Populating activeGetWorkStream and setting the budget are guaranteed to
        // execute serially since both operations synchronize on activeGetWorkStream.
        if (stream != null) {
          stream.setBudget(budget);
        }
      }
    }
  }

  long getAndResetThrottleTime() {
    return streamingEngineThrottleTimers.getAndResetThrottleTime();
  }

  long getCurrentActiveCommitBytes() {
    return workCommitter.currentActiveCommitBytes();
  }

  /**
   * Creates, starts, and gracefully terminates {@link GetWorkStream} before the clientside deadline
   * to prevent {@link org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Status#DEADLINE_EXCEEDED} errors.
   * If at any point the server closes the stream, reconnects immediately.
   */
  private void getWorkStreamLoop(CountDownLatch waitForInitialStream) {
    @Nullable GetWorkStream newStream = null;
    while (isRunning.get()) {
      synchronized (activeGetWorkStream) {
        newStream = getWorkStreamFactory.get();
        newStream.start();
        waitForInitialStream.countDown();
        activeGetWorkStream.set(newStream);
      }
      try {
        // Try to gracefully terminate the stream.
        if (!newStream.awaitTermination(GET_WORK_STREAM_TTL_MINUTES, TimeUnit.MINUTES)) {
          newStream.halfClose();
        }

        // If graceful termination is unsuccessful, forcefully shutdown.
        if (!newStream.awaitTermination(30, TimeUnit.SECONDS)) {
          newStream.shutdown();
        }

      } catch (InterruptedException e) {
        // continue until !isRunning.
      }
    }

    if (newStream != null) {
      newStream.shutdown();
    }
  }
}
