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
import javax.annotation.concurrent.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetWorkResponseChunk;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem;
import org.apache.beam.runners.dataflow.worker.windmill.client.AbstractWindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamShutdownException;
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
final class GrpcDirectGetWorkStream
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

  private final GetWorkBudgetTracker budgetTracker;
  private final GetWorkRequest requestHeader;
  private final WorkItemScheduler workItemScheduler;
  private final ThrottleTimer getWorkThrottleTimer;
  private final HeartbeatSender heartbeatSender;
  private final WorkCommitter workCommitter;
  private final GetDataClient getDataClient;
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
      HeartbeatSender heartbeatSender,
      GetDataClient getDataClient,
      WorkCommitter workCommitter,
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
    this.heartbeatSender = heartbeatSender;
    this.workCommitter = workCommitter;
    this.getDataClient = getDataClient;
    this.lastRequest = new AtomicReference<>();
    this.budgetTracker =
        new GetWorkBudgetTracker(
            GetWorkBudget.builder()
                .setItems(requestHeader.getMaxItems())
                .setBytes(requestHeader.getMaxBytes())
                .build());
  }

  static GrpcDirectGetWorkStream create(
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
      HeartbeatSender heartbeatSender,
      GetDataClient getDataClient,
      WorkCommitter workCommitter,
      WorkItemScheduler workItemScheduler) {
    return new GrpcDirectGetWorkStream(
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
   *     AbstractWindmillStream#trySend(Object)} is synchronized so the sends are already guarded.
   */
  private void maybeSendRequestExtension(GetWorkBudget extension) {
    if (extension.items() > 0 || extension.bytes() > 0) {
      executeSafely(
          () -> {
            StreamingGetWorkRequest request =
                StreamingGetWorkRequest.newBuilder()
                    .setRequestExtension(
                        Windmill.StreamingGetWorkRequestExtension.newBuilder()
                            .setMaxItems(extension.items())
                            .setMaxBytes(extension.bytes()))
                    .build();
            lastRequest.set(request);
            budgetTracker.recordBudgetRequested(extension);
            try {
              trySend(request);
            } catch (WindmillStreamShutdownException e) {
              // Stream was closed.
            }
          });
    }
  }

  @Override
  protected synchronized void onNewStream() throws WindmillStreamShutdownException {
    workItemAssemblers.clear();
    budgetTracker.reset();
    GetWorkBudget initialGetWorkBudget = budgetTracker.computeBudgetExtension();
    StreamingGetWorkRequest request =
        StreamingGetWorkRequest.newBuilder()
            .setRequest(
                requestHeader
                    .toBuilder()
                    .setMaxItems(initialGetWorkBudget.items())
                    .setMaxBytes(initialGetWorkBudget.bytes())
                    .build())
            .build();
    lastRequest.set(request);
    budgetTracker.recordBudgetRequested(initialGetWorkBudget);
    trySend(request);
  }

  @Override
  protected boolean hasPendingRequests() {
    return false;
  }

  @Override
  public void appendSpecificHtml(PrintWriter writer) {
    // Number of buffers is same as distinct workers that sent work on this stream.
    writer.format(
        "GetWorkStream: %d buffers, " + "last sent request: %s; ",
        workItemAssemblers.size(), lastRequest.get());
    writer.print(budgetTracker.debugString());
  }

  @Override
  public void sendHealthCheck() throws WindmillStreamShutdownException {
    trySend(HEALTH_CHECK_REQUEST);
  }

  @Override
  protected void shutdownInternal() {
    workItemAssemblers.clear();
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
    WorkItem workItem = assembledWorkItem.workItem();
    GetWorkResponseChunkAssembler.ComputationMetadata metadata =
        assembledWorkItem.computationMetadata();
    workItemScheduler.scheduleWork(
        workItem,
        createWatermarks(workItem, metadata),
        createProcessingContext(metadata.computationId()),
        assembledWorkItem.latencyAttributions());
    budgetTracker.recordBudgetReceived(assembledWorkItem.bufferedSize());
    GetWorkBudget extension = budgetTracker.computeBudgetExtension();
    maybeSendRequestExtension(extension);
  }

  private Work.ProcessingContext createProcessingContext(String computationId) {
    return Work.createProcessingContext(
        computationId, getDataClient, workCommitter::commit, heartbeatSender, backendWorkerToken());
  }

  @Override
  protected void startThrottleTimer() {
    getWorkThrottleTimer.start();
  }

  @Override
  public void setBudget(GetWorkBudget newBudget) {
    GetWorkBudget extension = budgetTracker.consumeAndComputeBudgetUpdate(newBudget);
    maybeSendRequestExtension(extension);
  }

  /**
   * Tracks sent, received, max {@link GetWorkBudget} and uses this information to generate request
   * extensions.
   */
  @ThreadSafe
  private static final class GetWorkBudgetTracker {

    @GuardedBy("GetWorkBudgetTracker.this")
    private GetWorkBudget maxGetWorkBudget;

    @GuardedBy("GetWorkBudgetTracker.this")
    private long itemsRequested = 0;

    @GuardedBy("GetWorkBudgetTracker.this")
    private long bytesRequested = 0;

    @GuardedBy("GetWorkBudgetTracker.this")
    private long itemsReceived = 0;

    @GuardedBy("GetWorkBudgetTracker.this")
    private long bytesReceived = 0;

    private GetWorkBudgetTracker(GetWorkBudget maxGetWorkBudget) {
      this.maxGetWorkBudget = maxGetWorkBudget;
    }

    private synchronized void reset() {
      itemsRequested = 0;
      bytesRequested = 0;
      itemsReceived = 0;
      bytesReceived = 0;
    }

    private synchronized String debugString() {
      return String.format(
          "max budget: %s; "
              + "in-flight budget: %s; "
              + "total budget requested: %s; "
              + "total budget received: %s.",
          maxGetWorkBudget, inFlightBudget(), totalRequestedBudget(), totalReceivedBudget());
    }

    /** Consumes the new budget and computes an extension based on the new budget. */
    private synchronized GetWorkBudget consumeAndComputeBudgetUpdate(GetWorkBudget newBudget) {
      maxGetWorkBudget = newBudget;
      return computeBudgetExtension();
    }

    private synchronized void recordBudgetRequested(GetWorkBudget budgetRequested) {
      itemsRequested += budgetRequested.items();
      bytesRequested += budgetRequested.bytes();
    }

    private synchronized void recordBudgetReceived(long returnedBudget) {
      itemsReceived++;
      bytesReceived += returnedBudget;
    }

    /**
     * If the outstanding items or bytes limit has gotten too low, top both off with a
     * GetWorkExtension. The goal is to keep the limits relatively close to their maximum values
     * without sending too many extension requests.
     */
    private synchronized GetWorkBudget computeBudgetExtension() {
      // Expected items and bytes can go negative here, since WorkItems returned might be larger
      // than the initially requested budget.
      long inFlightItems = itemsRequested - itemsReceived;
      long inFlightBytes = bytesRequested - bytesReceived;

      // Don't send negative budget extensions.
      long requestBytes = Math.max(0, maxGetWorkBudget.bytes() - inFlightBytes);
      long requestItems = Math.max(0, maxGetWorkBudget.items() - inFlightItems);

      return (inFlightItems > requestItems / 2 && inFlightBytes > requestBytes / 2)
          ? GetWorkBudget.noBudget()
          : GetWorkBudget.builder().setItems(requestItems).setBytes(requestBytes).build();
    }

    private synchronized GetWorkBudget inFlightBudget() {
      return GetWorkBudget.builder()
          .setItems(itemsRequested - itemsReceived)
          .setBytes(bytesRequested - bytesReceived)
          .build();
    }

    private synchronized GetWorkBudget totalRequestedBudget() {
      return GetWorkBudget.builder().setItems(itemsRequested).setBytes(bytesRequested).build();
    }

    private synchronized GetWorkBudget totalReceivedBudget() {
      return GetWorkBudget.builder().setItems(itemsReceived).setBytes(bytesReceived).build();
    }
  }
}
