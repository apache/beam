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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.PrintWriter;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.annotation.Nullable;
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
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemScheduler;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.StreamObserver;
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
  private final HeartbeatSender heartbeatSender;
  private final WorkCommitter workCommitter;
  private final GetDataClient getDataClient;
  private final AtomicReference<StreamingGetWorkRequest> lastRequest;

  private final boolean requestBatchedGetWorkResponse;

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
      boolean requestBatchedGetWorkResponse,
      HeartbeatSender heartbeatSender,
      GetDataClient getDataClient,
      WorkCommitter workCommitter,
      WorkItemScheduler workItemScheduler,
      Duration halfClosePhysicalStreamAfter,
      ScheduledExecutorService executorService) {
    super(
        LOG,
        startGetWorkRpcFn,
        backoff,
        streamObserverFactory,
        streamRegistry,
        logEveryNStreamFailures,
        backendWorkerToken,
        halfClosePhysicalStreamAfter,
        executorService);
    this.requestHeader = requestHeader;
    this.workItemScheduler = workItemScheduler;
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
    this.requestBatchedGetWorkResponse = requestBatchedGetWorkResponse;
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
      boolean requestBatchedGetWorkResponse,
      HeartbeatSender heartbeatSender,
      GetDataClient getDataClient,
      WorkCommitter workCommitter,
      WorkItemScheduler workItemScheduler,
      Duration halfClosePhysicalStreamAfter,
      ScheduledExecutorService executor) {
    return new GrpcDirectGetWorkStream(
        backendWorkerToken,
        startGetWorkRpcFn,
        request,
        backoff,
        streamObserverFactory,
        streamRegistry,
        logEveryNStreamFailures,
        requestBatchedGetWorkResponse,
        heartbeatSender,
        getDataClient,
        workCommitter,
        workItemScheduler,
        halfClosePhysicalStreamAfter,
        executor);
  }

  private static Watermarks createWatermarks(
      WorkItem workItem, GetWorkResponseChunkAssembler.ComputationMetadata metadata) {
    return Watermarks.builder()
        .setInputDataWatermark(checkNotNull(metadata.inputDataWatermark()))
        .setOutputDataWatermark(workItem.getOutputDataWatermark())
        .setSynchronizedProcessingTime(metadata.synchronizedProcessingTime())
        .build();
  }

  /**
   * @implNote Do not lock/synchronize on the calling thread due to this running on grpc serial
   *     executor for message which can deadlock since we send on the stream beneath the
   *     synchronization. Instead, synchronize on {@code this} inside the async {@link
   *     #executeSafely(Runnable)} task to verify {@code targetStream} is still the active physical
   *     stream before calling {@link AbstractWindmillStream#trySend(Object)}.
   */
  @SuppressWarnings("ReferenceEquality")
  private void maybeSendRequestExtension(
      DirectGetWorkPhysicalStreamHandler targetStream, GetWorkBudget budget) {
    if (budget.items() > 0 || budget.bytes() > 0) {
      executeSafely(
          () -> {
            StreamingGetWorkRequest request =
                StreamingGetWorkRequest.newBuilder()
                    .setRequestExtension(
                        Windmill.StreamingGetWorkRequestExtension.newBuilder()
                            .setMaxItems(budget.items())
                            .setMaxBytes(budget.bytes()))
                    .build();
            synchronized (this) {
              if (currentPhysicalStream != targetStream || clientClosed || isShutdown) {
                // Drop extensions queued before a reset, half-close, or shutdown.
                return;
              }
              lastRequest.set(request);
              try {
                trySend(request);
              } catch (WindmillStreamShutdownException e) {
                // Stream was closed.
              }
            }
          });
    }
  }

  private class DirectGetWorkPhysicalStreamHandler extends PhysicalStreamHandler {
    /**
     * Map of stream IDs to their buffers. Used to aggregate streaming gRPC response chunks as they
     * come in. Once all chunks for a response has been received, the chunk is processed and the
     * buffer is cleared.
     *
     * @implNote Buffers are not persisted across stream restarts.
     */
    final ConcurrentMap<Long, GetWorkResponseChunkAssembler> workItemAssemblers =
        new ConcurrentHashMap<>();

    @Override
    public void onResponse(StreamingGetWorkResponseChunk response) {
      workItemAssemblers
          .computeIfAbsent(response.getStreamId(), unused -> new GetWorkResponseChunkAssembler())
          .append(response)
          .forEach(item -> consumeAssembledWorkItem(item, this));
    }

    @Override
    public boolean hasPendingRequests() {
      return false;
    }

    @Override
    public void onDone(Status status) {
      workItemAssemblers.clear();
      budgetTracker.clearActiveStream(this);
    }

    @Override
    public void appendHtml(PrintWriter writer) {
      // Number of buffers is same as distinct workers that sent work on this stream.
      writer.format("%d buffers", workItemAssemblers.size());
    }
  }

  @Override
  protected PhysicalStreamHandler newResponseHandler() {
    return new DirectGetWorkPhysicalStreamHandler();
  }

  @Override
  protected synchronized void onFlushPending(boolean isNewStream)
      throws WindmillStreamShutdownException {
    if (!isNewStream) {
      return;
    }
    // Reset and record initial budget before sending the header so concurrent updates observe it.
    GetWorkBudget initialGetWorkBudget =
        budgetTracker.resetAndRecordInitialBudget(
            (DirectGetWorkPhysicalStreamHandler) checkNotNull(currentPhysicalStream));
    StreamingGetWorkRequest request =
        StreamingGetWorkRequest.newBuilder()
            .setRequest(
                requestHeader.toBuilder()
                    .setMaxItems(initialGetWorkBudget.items())
                    .setMaxBytes(initialGetWorkBudget.bytes())
                    .build())
            .setSupportsMultipleWorkItemsInChunk(requestBatchedGetWorkResponse)
            .build();
    lastRequest.set(request);
    trySend(request);
  }

  @Override
  public void appendSpecificHtml(PrintWriter writer) {
    writer.format("GetWorkStream: last sent request: %s; ", lastRequest.get());
    writer.print(budgetTracker.debugString());
  }

  @Override
  protected void sendHealthCheck() throws WindmillStreamShutdownException {
    trySend(HEALTH_CHECK_REQUEST);
  }

  private void consumeAssembledWorkItem(
      AssembledWorkItem assembledWorkItem, DirectGetWorkPhysicalStreamHandler responseStream) {
    WorkItem workItem = assembledWorkItem.workItem();
    GetWorkResponseChunkAssembler.ComputationMetadata metadata =
        assembledWorkItem.computationMetadata();
    workItemScheduler.scheduleWork(
        workItem,
        assembledWorkItem.bufferedSize(),
        createWatermarks(workItem, metadata),
        createProcessingContext(metadata.computationId()),
        metadata.drainMode(),
        assembledWorkItem.appliedFinalizeIds(),
        assembledWorkItem.latencyAttributions());
    GetWorkBudget extension =
        budgetTracker.recordReceivedAndComputeExtension(
            assembledWorkItem.bufferedSize(), responseStream);
    maybeSendRequestExtension(responseStream, extension);
  }

  private Work.ProcessingContext createProcessingContext(String computationId) {
    return Work.createProcessingContext(
        computationId, getDataClient, workCommitter::commit, heartbeatSender, backendWorkerToken());
  }

  @Override
  public void setBudget(GetWorkBudget newBudget) {
    @Nullable BudgetExtension extension = budgetTracker.consumeAndComputeBudgetUpdate(newBudget);
    if (extension != null) {
      maybeSendRequestExtension(extension.targetStream, extension.budget);
    }
  }

  /** Extension budget paired with the physical stream under which it was computed. */
  private static final class BudgetExtension {
    private final DirectGetWorkPhysicalStreamHandler targetStream;
    private final GetWorkBudget budget;

    private BudgetExtension(DirectGetWorkPhysicalStreamHandler targetStream, GetWorkBudget budget) {
      this.targetStream = targetStream;
      this.budget = budget;
    }
  }

  /**
   * Tracks sent, received, max {@link GetWorkBudget} and uses this information to generate request
   * extensions.
   */
  @ThreadSafe
  private static final class GetWorkBudgetTracker {

    @GuardedBy("GetWorkBudgetTracker.this")
    private @Nullable DirectGetWorkPhysicalStreamHandler activeStream = null;

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

    private synchronized String debugString() {
      return String.format(
          "max budget: %s; "
              + "in-flight budget: %s; "
              + "total budget requested: %s; "
              + "total budget received: %s.",
          maxGetWorkBudget, inFlightBudget(), totalRequestedBudget(), totalReceivedBudget());
    }

    private synchronized GetWorkBudget resetAndRecordInitialBudget(
        DirectGetWorkPhysicalStreamHandler newStream) {
      activeStream = newStream;
      itemsRequested = 0;
      bytesRequested = 0;
      itemsReceived = 0;
      bytesReceived = 0;
      return computeAndRecordBudgetExtension();
    }

    @SuppressWarnings("ReferenceEquality")
    private synchronized void clearActiveStream(DirectGetWorkPhysicalStreamHandler stream) {
      if (activeStream == stream) {
        activeStream = null;
      }
    }

    /** Consumes the new budget and computes an extension based on the new budget. */
    private synchronized @Nullable BudgetExtension consumeAndComputeBudgetUpdate(
        GetWorkBudget newBudget) {
      maxGetWorkBudget = newBudget;
      DirectGetWorkPhysicalStreamHandler stream = activeStream;
      // Stream has not opened yet (or previous stream ended); onFlushPending sends the header.
      if (stream == null) {
        return null;
      }
      return new BudgetExtension(stream, computeAndRecordBudgetExtension());
    }

    @SuppressWarnings("ReferenceEquality")
    private synchronized GetWorkBudget recordReceivedAndComputeExtension(
        long returnedBudget, DirectGetWorkPhysicalStreamHandler responseStream) {
      // Draining items from a half-closed stream must not count toward the active stream's budget.
      if (responseStream != activeStream) {
        return GetWorkBudget.noBudget();
      }
      recordBudgetReceived(returnedBudget);
      return computeAndRecordBudgetExtension();
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
    private synchronized GetWorkBudget computeAndRecordBudgetExtension() {
      // Expected items and bytes can go negative here, since WorkItems returned might be larger
      // than the initially requested budget.
      long inFlightItems = itemsRequested - itemsReceived;
      long inFlightBytes = bytesRequested - bytesReceived;

      // Don't send negative budget extensions.
      long requestBytes = Math.max(0, maxGetWorkBudget.bytes() - inFlightBytes);
      long requestItems = Math.max(0, maxGetWorkBudget.items() - inFlightItems);

      if (inFlightItems > requestItems / 2 && inFlightBytes > requestBytes / 2) {
        return GetWorkBudget.noBudget();
      }
      GetWorkBudget extension =
          GetWorkBudget.builder().setItems(requestItems).setBytes(requestBytes).build();
      // Record synchronously before returning so queued sends cannot cause duplicate extensions.
      itemsRequested += extension.items();
      bytesRequested += extension.bytes();
      return extension;
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
