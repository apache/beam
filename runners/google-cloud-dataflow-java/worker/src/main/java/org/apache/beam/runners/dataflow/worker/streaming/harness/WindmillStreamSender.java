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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Suppliers;

/**
 * Owns and maintains a set of streams used to communicate with a specific Windmill worker.
 * Underlying streams are "cached" in a threadsafe manner so that once {@link Supplier#get} is
 * called, a stream that is already started is returned.
 *
 * <p>Holds references to {@link
 * Supplier<org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream>} because
 * initializing the streams automatically start them, and we want to do so lazily here once the
 * {@link GetWorkBudget} is set.
 *
 * <p>Once started, the underlying streams are "alive" until they are manually closed via {@link
 * #closeAllStreams()}.
 *
 * <p>If closed, it means that the backend endpoint is no longer in the worker set. Once closed,
 * these instances are not reused.
 *
 * @implNote Does not manage streams for fetching {@link
 *     org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalData} for side inputs.
 */
@Internal
@ThreadSafe
final class WindmillStreamSender implements GetWorkBudgetSpender {
  private final AtomicBoolean started;
  private final AtomicReference<GetWorkBudget> getWorkBudget;
  private final Supplier<GetWorkStream> getWorkStream;
  private final Supplier<GetDataStream> getDataStream;
  private final Supplier<CommitWorkStream> commitWorkStream;
  private final Supplier<WorkCommitter> workCommitter;
  private final StreamingEngineThrottleTimers streamingEngineThrottleTimers;

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

    // All streams are memoized/cached since they are expensive to create and some implementations
    // perform side effects on construction (i.e. sending initial requests to the stream server to
    // initiate the streaming RPC connection). Stream instances connect/reconnect internally, so we
    // can reuse the same instance through the entire lifecycle of WindmillStreamSender.
    this.getDataStream =
        Suppliers.memoize(
            () ->
                streamingEngineStreamFactory.createGetDataStream(
                    connection.stub(), streamingEngineThrottleTimers.getDataThrottleTimer()));
    this.commitWorkStream =
        Suppliers.memoize(
            () ->
                streamingEngineStreamFactory.createCommitWorkStream(
                    connection.stub(), streamingEngineThrottleTimers.commitWorkThrottleTimer()));
    this.workCommitter =
        Suppliers.memoize(() -> workCommitterFactory.apply(commitWorkStream.get()));
    this.getWorkStream =
        Suppliers.memoize(
            () ->
                streamingEngineStreamFactory.createDirectGetWorkStream(
                    connection,
                    withRequestBudget(getWorkRequest, getWorkBudget.get()),
                    streamingEngineThrottleTimers.getWorkThrottleTimer(),
                    () -> FixedStreamHeartbeatSender.create(getDataStream.get()),
                    () -> getDataClientFactory.apply(getDataStream.get()),
                    workCommitter,
                    workItemScheduler));
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

  @SuppressWarnings("ReturnValueIgnored")
  void startStreams() {
    getWorkStream.get();
    getDataStream.get();
    commitWorkStream.get();
    workCommitter.get().start();
    // *stream.get() is all memoized in a threadsafe manner.
    started.set(true);
  }

  void closeAllStreams() {
    // Supplier<Stream>.get() starts the stream which is an expensive operation as it initiates the
    // streaming RPCs by possibly making calls over the network. Do not close the streams unless
    // they have already been started.
    if (started.get()) {
      getWorkStream.get().shutdown();
      getDataStream.get().shutdown();
      commitWorkStream.get().shutdown();
      workCommitter.get().stop();
    }
  }

  @Override
  public void adjustBudget(long itemsDelta, long bytesDelta) {
    getWorkBudget.set(getWorkBudget.get().apply(itemsDelta, bytesDelta));
    if (started.get()) {
      getWorkStream.get().adjustBudget(itemsDelta, bytesDelta);
    }
  }

  @Override
  public GetWorkBudget remainingBudget() {
    return started.get() ? getWorkStream.get().remainingBudget() : getWorkBudget.get();
  }

  long getAndResetThrottleTime() {
    return streamingEngineThrottleTimers.getAndResetThrottleTime();
  }

  long getCurrentActiveCommitBytes() {
    return started.get() ? workCommitter.get().currentActiveCommitBytes() : 0;
  }
}
