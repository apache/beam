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

  private final AtomicReference<GetWorkBudget> inFlightBudget;
  private final AtomicReference<GetWorkBudget> nextBudgetAdjustment;
  private final AtomicReference<GetWorkBudget> pendingResponseBudget;
  private final GetWorkRequest request;
  private final WorkItemScheduler workItemScheduler;
  private final ThrottleTimer getWorkThrottleTimer;
  private final Supplier<HeartbeatSender> heartbeatSender;
  private final Supplier<WorkCommitter> workCommitter;
  private final Supplier<GetDataClient> getDataClient;

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
    super(
        LOG,
        "GetWorkStream",
        startGetWorkRpcFn,
        backoff,
        streamObserverFactory,
        streamRegistry,
        logEveryNStreamFailures,
        backendWorkerToken);
    this.request = request;
    this.getWorkThrottleTimer = getWorkThrottleTimer;
    this.workItemScheduler = workItemScheduler;
    this.workItemAssemblers = new ConcurrentHashMap<>();
    this.heartbeatSender = Suppliers.memoize(heartbeatSender::get);
    this.workCommitter = Suppliers.memoize(workCommitter::get);
    this.getDataClient = Suppliers.memoize(getDataClient::get);
    this.nextBudgetAdjustment = new AtomicReference<>(GetWorkBudget.noBudget());
    this.inFlightBudget = new AtomicReference<>(GetWorkBudget.noBudget());
    this.pendingResponseBudget = new AtomicReference<>(GetWorkBudget.noBudget());
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

  private void sendRequestExtension(GetWorkBudget adjustment) {
    StreamingGetWorkRequest extension =
        StreamingGetWorkRequest.newBuilder()
            .setRequestExtension(
                Windmill.StreamingGetWorkRequestExtension.newBuilder()
                    .setMaxItems(adjustment.items())
                    .setMaxBytes(adjustment.bytes()))
            .build();

    executeSafely(
        () -> {
          try {
            send(extension);
          } catch (IllegalStateException e) {
            // Stream was closed.
          }
        });
  }

  @Override
  protected synchronized void onNewStream() {
    workItemAssemblers.clear();
    if (!isShutdown()) {
      // Add the current in-flight budget to the next adjustment. Only positive values are allowed
      // here with negatives defaulting to 0, since GetWorkBudgets cannot be created with negative
      // values. We just sent the budget, reset it.
      GetWorkBudget currentBudgetAdjustment =
          nextBudgetAdjustment.getAndUpdate(ignored -> GetWorkBudget.noBudget());
      GetWorkBudget budgetAdjustment = currentBudgetAdjustment.apply(inFlightBudget.get());

      inFlightBudget.updateAndGet(budget -> budget.apply(currentBudgetAdjustment));

      send(
          StreamingGetWorkRequest.newBuilder()
              .setRequest(
                  request.toBuilder()
                      .setMaxBytes(budgetAdjustment.bytes())
                      .setMaxItems(budgetAdjustment.items()))
              .build());
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
        "GetWorkStream: %d buffers, %s inflight budget allowed.",
        workItemAssemblers.size(), inFlightBudget.get());
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
    pendingResponseBudget.getAndUpdate(budget -> budget.apply(1, workItem.getSerializedSize()));
    try {
      workItemScheduler.scheduleWork(
          workItem,
          createWatermarks(workItem, Preconditions.checkNotNull(metadata)),
          createProcessingContext(Preconditions.checkNotNull(metadata.computationId())),
          assembledWorkItem.latencyAttributions());
    } finally {
      pendingResponseBudget.getAndUpdate(budget -> budget.apply(-1, -workItem.getSerializedSize()));
    }
  }

  private Work.ProcessingContext createProcessingContext(String computationId) {
    return Work.createProcessingContext(
        computationId, getDataClient.get(), workCommitter.get()::commit, heartbeatSender.get());
  }

  @Override
  protected void startThrottleTimer() {
    getWorkThrottleTimer.start();
  }

  @Override
  public void adjustBudget(long itemsDelta, long bytesDelta) {
    GetWorkBudget adjustment =
        nextBudgetAdjustment
            // Get the current value, and reset the nextBudgetAdjustment. This will be set again
            // when adjustBudget is called.
            .getAndUpdate(unused -> GetWorkBudget.noBudget())
            .apply(itemsDelta, bytesDelta);
    sendRequestExtension(adjustment);
  }

  @Override
  public GetWorkBudget remainingBudget() {
    // Snapshot the current budgets.
    GetWorkBudget currentPendingResponseBudget = pendingResponseBudget.get();
    GetWorkBudget currentNextBudgetAdjustment = nextBudgetAdjustment.get();
    GetWorkBudget currentInflightBudget = inFlightBudget.get();

    return currentPendingResponseBudget
        .apply(currentNextBudgetAdjustment)
        .apply(currentInflightBudget);
  }

  @Override
  protected void shutdownInternal() {
    workItemAssemblers.clear();
  }
}
