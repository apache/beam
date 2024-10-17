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

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;

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
final class WindmillStreamSender implements GetWorkBudgetSpender, Closeable {
  private static final String STREAM_STARTER_THREAD_NAME = "StartWindmillStreamThread-%d";
  private final AtomicBoolean started;
  private final AtomicReference<GetWorkBudget> getWorkBudget;
  private final GetWorkStream getWorkStream;
  private final GetDataStream getDataStream;
  private final CommitWorkStream commitWorkStream;
  private final WorkCommitter workCommitter;
  private final StreamingEngineThrottleTimers streamingEngineThrottleTimers;
  private final ExecutorService streamStarter;

  private WindmillStreamSender(
      WindmillConnection connection,
      GetWorkRequest getWorkRequest,
      AtomicReference<GetWorkBudget> getWorkBudget,
      GrpcWindmillStreamFactory streamingEngineStreamFactory,
      WorkItemScheduler workItemScheduler,
      Function<GetDataStream, GetDataClient> getDataClientFactory,
      Function<CommitWorkStream, WorkCommitter> workCommitterFactory) {
    this.started = new AtomicBoolean(false);
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
    this.getWorkStream =
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
            3, new ThreadFactoryBuilder().setNameFormat(STREAM_STARTER_THREAD_NAME).build());
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
    if (!started.get()) {
      // Start these 3 streams in parallel since they each may perform blocking IO.
      CompletableFuture.allOf(
              CompletableFuture.runAsync(getWorkStream::start, streamStarter),
              CompletableFuture.runAsync(getDataStream::start, streamStarter),
              CompletableFuture.runAsync(commitWorkStream::start, streamStarter))
          .join();
      workCommitter.start();
      started.set(true);
    }
  }

  @Override
  public synchronized void close() {
    if (started.get()) {
      getWorkStream.shutdown();
      getDataStream.shutdown();
      workCommitter.stop();
      commitWorkStream.shutdown();
    }
  }

  @Override
  public void setBudget(long items, long bytes) {
    GetWorkBudget adjustment = GetWorkBudget.builder().setItems(items).setBytes(bytes).build();
    getWorkBudget.set(adjustment);
    if (started.get()) {
      getWorkStream.setBudget(adjustment);
    }
  }

  long getAndResetThrottleTime() {
    return streamingEngineThrottleTimers.getAndResetThrottleTime();
  }

  long getCurrentActiveCommitBytes() {
    return workCommitter.currentActiveCommitBytes();
  }
}
