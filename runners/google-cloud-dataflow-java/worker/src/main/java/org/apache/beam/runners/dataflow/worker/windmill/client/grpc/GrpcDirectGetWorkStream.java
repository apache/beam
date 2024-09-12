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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc;

import java.io.PrintWriter;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetWorkResponseChunk;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem;
import org.apache.beam.runners.dataflow.worker.windmill.client.AbstractWindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.GetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GetWorkResponseChunkAssembler.AssembledWorkItem;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.StreamObserverFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.throttling.ThrottleTimer;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemScheduler;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Suppliers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link GetWorkStream} that passes along a specific {@link
 * org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream} and {@link
 * org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.CommitWorkStream} to the
 * processing context {@link Work.ProcessingContext}. During the work item processing lifecycle,
 * these direct streams are used to facilitate these RPC calls to specific backend workers.
 */
@Internal
public final class GrpcDirectGetWorkStream
    extends AbstractWindmillStream<StreamingGetWorkRequest, StreamingGetWorkResponseChunk>
    implements GetWorkStream {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcDirectGetWorkStream.class);
  private static final StreamingGetWorkRequest HEALTH_CHECK_REQUEST =
      StreamingGetWorkRequest.newBuilder()
          .setRequestExtension(
              Windmill.StreamingGetWorkRequestExtension.newBuilder()
                  .setMaxItems(0)
                  .setMaxBytes(0)
                  .build())
          .build();

  private final AtomicReference<GetWorkBudget> maxGetWorkBudget;
  private final AtomicReference<GetWorkBudget> inFlightBudget;
  private final GetWorkRequest requestHeader;
  private final WorkItemScheduler workItemScheduler;
  private final ThrottleTimer getWorkThrottleTimer;
  private final Supplier<HeartbeatSender> heartbeatSender;
  private final Supplier<WorkCommitter> workCommitter;
  private final Supplier<GetDataClient> getDataClient;
  private final AtomicReference<StreamingGetWorkRequest> lastRequest;

  /**
   * Map of stream IDs to their buffers. Used to aggregate streaming gRPC response chunks as they
   * come in. Once all chunks for a response has been received, the chunk is processed and the
   * buffer is cleared.
   *
   * @implNote Buffers are not persisted across stream restarts.
   */
  private final ConcurrentMap<Long, GetWorkResponseChunkAssembler> workItemAssemblers;

  private GrpcDirectGetWorkStream(
      String backendWorkerToken,
      Function<
              StreamObserver<StreamingGetWorkResponseChunk>,
              StreamObserver<StreamingGetWorkRequest>>
          startGetWorkRpcFn,
      GetWorkRequest requestHeader,
      BackOff backoff,
      StreamObserverFactory streamObserverFactory,
      Set<AbstractWindmillStream<?, ?>> streamRegistry,
      int logEveryNStreamFailures,
      ThrottleTimer getWorkThrottleTimer,
      Supplier<HeartbeatSender> heartbeatSender,
      Supplier<GetDataClient> getDataClient,
      Supplier<WorkCommitter> workCommitter,
      WorkItemScheduler workItemScheduler) {
    super(
        LOG,
        "GetWorkStream",
        startGetWorkRpcFn,
        backoff,
        streamObserverFactory,
        streamRegistry,
        logEveryNStreamFailures,
        backendWorkerToken);
    this.requestHeader = requestHeader;
    this.getWorkThrottleTimer = getWorkThrottleTimer;
    this.workItemScheduler = workItemScheduler;
    this.workItemAssemblers = new ConcurrentHashMap<>();
    this.heartbeatSender = Suppliers.memoize(heartbeatSender::get);
    this.workCommitter = Suppliers.memoize(workCommitter::get);
    this.getDataClient = Suppliers.memoize(getDataClient::get);
    this.maxGetWorkBudget =
        new AtomicReference<>(
            GetWorkBudget.builder()
                .setItems(requestHeader.getMaxItems())
                .setBytes(requestHeader.getMaxBytes())
                .build());
    this.inFlightBudget = new AtomicReference<>(GetWorkBudget.noBudget());
    this.lastRequest = new AtomicReference<>();
  }

  public static GrpcDirectGetWorkStream create(
      String backendWorkerToken,
      Function<
              StreamObserver<StreamingGetWorkResponseChunk>,
              StreamObserver<StreamingGetWorkRequest>>
          startGetWorkRpcFn,
      GetWorkRequest request,
      BackOff backoff,
      StreamObserverFactory streamObserverFactory,
      Set<AbstractWindmillStream<?, ?>> streamRegistry,
      int logEveryNStreamFailures,
      ThrottleTimer getWorkThrottleTimer,
      Supplier<HeartbeatSender> heartbeatSender,
      Supplier<GetDataClient> getDataClient,
      Supplier<WorkCommitter> workCommitter,
      WorkItemScheduler workItemScheduler) {
    GrpcDirectGetWorkStream getWorkStream =
        new GrpcDirectGetWorkStream(
            backendWorkerToken,
            startGetWorkRpcFn,
            request,
            backoff,
            streamObserverFactory,
            streamRegistry,
            logEveryNStreamFailures,
            getWorkThrottleTimer,
            heartbeatSender,
            getDataClient,
            workCommitter,
            workItemScheduler);
    getWorkStream.startStream();
    return getWorkStream;
  }

  private static Watermarks createWatermarks(
      WorkItem workItem, GetWorkResponseChunkAssembler.ComputationMetadata metadata) {
    return Watermarks.builder()
        .setInputDataWatermark(metadata.inputDataWatermark())
        .setOutputDataWatermark(workItem.getOutputDataWatermark())
        .setSynchronizedProcessingTime(metadata.synchronizedProcessingTime())
        .build();
  }

  /**
   * @implNote Do not lock/synchronize here due to this running on grpc serial executor for message
   *     which can deadlock since we send on the stream beneath the synchronization. {@link
   *     AbstractWindmillStream#send(Object)} is synchronized so the sends are already guarded.
   */
  private void sendRequestExtension() {
    GetWorkBudget currentInFlightBudget = inFlightBudget.get();
    GetWorkBudget currentMaxBudget = maxGetWorkBudget.get();

    // If the outstanding items or bytes limit has gotten too low, top both off with a
    // GetWorkExtension. The goal is to keep the limits relatively close to their maximum
    // values without sending too many extension requests.
    if (currentInFlightBudget.items() < currentMaxBudget.items() / 2
        || currentInFlightBudget.bytes() < currentMaxBudget.bytes() / 2) {
      GetWorkBudget extension = currentMaxBudget.subtract(currentInFlightBudget);
      if (extension.items() > 0 || extension.bytes() > 0) {
        inFlightBudget.getAndUpdate(budget -> budget.apply(extension));
        executeSafely(
            () -> {
              StreamingGetWorkRequest request =
                  StreamingGetWorkRequest.newBuilder()
                      .setRequestExtension(
                          Windmill.StreamingGetWorkRequestExtension.newBuilder()
                              .setMaxItems(extension.items())
                              .setMaxBytes(extension.bytes()))
                      .build();
              lastRequest.getAndSet(request);
              send(request);
            });
      }
    }
  }

  @Override
  protected synchronized void onNewStream() {
    workItemAssemblers.clear();
    if (!isShutdown()) {
      GetWorkBudget currentMaxGetWorkBudget = maxGetWorkBudget.get();
      inFlightBudget.getAndSet(currentMaxGetWorkBudget);
      StreamingGetWorkRequest request =
          StreamingGetWorkRequest.newBuilder()
              .setRequest(
                  requestHeader
                      .toBuilder()
                      .setMaxItems(currentMaxGetWorkBudget.items())
                      .setMaxBytes(currentMaxGetWorkBudget.bytes())
                      .build())
              .build();
      lastRequest.getAndSet(request);
      send(request);
    }
  }

  @Override
  protected boolean hasPendingRequests() {
    return false;
  }

  @Override
  public void appendSpecificHtml(PrintWriter writer) {
    // Number of buffers is same as distinct workers that sent work on this stream.
    writer.format(
        "GetWorkStream: %d buffers, in-flight budget: %s; last sent request: %s.",
        workItemAssemblers.size(), inFlightBudget.get(), lastRequest.get());
  }

  @Override
  public void sendHealthCheck() {
    send(HEALTH_CHECK_REQUEST);
  }

  @Override
  protected void onResponse(StreamingGetWorkResponseChunk chunk) {
    getWorkThrottleTimer.stop();
    workItemAssemblers
        .computeIfAbsent(chunk.getStreamId(), unused -> new GetWorkResponseChunkAssembler())
        .append(chunk)
        .ifPresent(this::consumeAssembledWorkItem);
  }

  private void consumeAssembledWorkItem(AssembledWorkItem assembledWorkItem) {
    // Record the fact that there are now fewer outstanding messages and bytes on the stream.
    inFlightBudget.updateAndGet(budget -> budget.subtract(1, assembledWorkItem.bufferedSize()));
    WorkItem workItem = assembledWorkItem.workItem();
    GetWorkResponseChunkAssembler.ComputationMetadata metadata =
        assembledWorkItem.computationMetadata();
    workItemScheduler.scheduleWork(
        workItem,
        createWatermarks(workItem, Preconditions.checkNotNull(metadata)),
        createProcessingContext(Preconditions.checkNotNull(metadata.computationId())),
        assembledWorkItem.latencyAttributions());
    sendRequestExtension();
  }

  private Work.ProcessingContext createProcessingContext(String computationId) {
    return Work.createProcessingContext(
        computationId,
        getDataClient.get(),
        workCommitter.get()::commit,
        heartbeatSender.get(),
        backendWorkerToken());
  }

  @Override
  protected void startThrottleTimer() {
    getWorkThrottleTimer.start();
  }

  @Override
  public void adjustBudget(long itemsDelta, long bytesDelta) {
    maxGetWorkBudget.getAndSet(
        GetWorkBudget.builder().setItems(itemsDelta).setBytes(bytesDelta).build());
    sendRequestExtension();
  }

  @Override
  public GetWorkBudget remainingBudget() {
    return maxGetWorkBudget.get().subtract(inFlightBudget.get());
  }

  @Override
  protected void shutdownInternal() {
    workItemAssemblers.clear();
  }
}
