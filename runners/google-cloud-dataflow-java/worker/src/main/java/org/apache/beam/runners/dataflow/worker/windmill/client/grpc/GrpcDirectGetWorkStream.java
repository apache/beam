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

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.WindmillTimeUtils;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationWorkItemMetadata;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetWorkResponseChunk;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem;
import org.apache.beam.runners.dataflow.worker.windmill.client.AbstractWindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.StreamObserverFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.throttling.ThrottleTimer;
import org.apache.beam.runners.dataflow.worker.windmill.work.ProcessWorkItemClient;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemProcessor;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Suppliers;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link GetWorkStream} that passes along a specific {@link
 * org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream} and {@link
 * org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.CommitWorkStream} to the
 * processing context {@link ProcessWorkItemClient}. During the work item processing lifecycle,
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
  private final WorkItemProcessor workItemProcessorFn;
  private final ThrottleTimer getWorkThrottleTimer;
  private final Supplier<GetDataStream> getDataStream;
  private final Supplier<CommitWorkStream> commitWorkStream;
  /**
   * Map of stream IDs to their buffers. Used to aggregate streaming gRPC response chunks as they
   * come in. Once all chunks for a response has been received, the chunk is processed and the
   * buffer is cleared.
   */
  private final ConcurrentMap<Long, WorkItemBuffer> workItemBuffers;

  private GrpcDirectGetWorkStream(
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
      Supplier<GetDataStream> getDataStream,
      Supplier<CommitWorkStream> commitWorkStream,
      WorkItemProcessor workItemProcessorFn) {
    super(
        startGetWorkRpcFn, backoff, streamObserverFactory, streamRegistry, logEveryNStreamFailures);
    this.request = request;
    this.getWorkThrottleTimer = getWorkThrottleTimer;
    this.workItemProcessorFn = workItemProcessorFn;
    this.workItemBuffers = new ConcurrentHashMap<>();
    // Use the same GetDataStream and CommitWorkStream instances to process all the work in this
    // stream.
    this.getDataStream = Suppliers.memoize(getDataStream::get);
    this.commitWorkStream = Suppliers.memoize(commitWorkStream::get);
    this.inFlightBudget = new AtomicReference<>(GetWorkBudget.noBudget());
    this.nextBudgetAdjustment = new AtomicReference<>(GetWorkBudget.noBudget());
    this.pendingResponseBudget = new AtomicReference<>(GetWorkBudget.noBudget());
  }

  public static GrpcDirectGetWorkStream create(
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
      Supplier<GetDataStream> getDataStream,
      Supplier<CommitWorkStream> commitWorkStream,
      WorkItemProcessor workItemProcessorFn) {
    GrpcDirectGetWorkStream getWorkStream =
        new GrpcDirectGetWorkStream(
            startGetWorkRpcFn,
            request,
            backoff,
            streamObserverFactory,
            streamRegistry,
            logEveryNStreamFailures,
            getWorkThrottleTimer,
            getDataStream,
            commitWorkStream,
            workItemProcessorFn);
    getWorkStream.startStream();
    return getWorkStream;
  }

  private synchronized GetWorkBudget getThenResetBudgetAdjustment() {
    return nextBudgetAdjustment.getAndUpdate(unused -> GetWorkBudget.noBudget());
  }

  private void sendRequestExtension() {
    // Just sent the request extension, reset the nextBudgetAdjustment. This will be set when
    // adjustBudget is called.
    GetWorkBudget adjustment = getThenResetBudgetAdjustment();
    StreamingGetWorkRequest extension =
        StreamingGetWorkRequest.newBuilder()
            .setRequestExtension(
                Windmill.StreamingGetWorkRequestExtension.newBuilder()
                    .setMaxItems(adjustment.items())
                    .setMaxBytes(adjustment.bytes()))
            .build();

    executor()
        .execute(
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
    workItemBuffers.clear();
    // Add the current in-flight budget to the next adjustment. Only positive values are allowed
    // here
    // with negatives defaulting to 0, since GetWorkBudgets cannot be created with negative values.
    GetWorkBudget budgetAdjustment = nextBudgetAdjustment.get().apply(inFlightBudget.get());
    inFlightBudget.set(budgetAdjustment);
    send(
        StreamingGetWorkRequest.newBuilder()
            .setRequest(
                request
                    .toBuilder()
                    .setMaxBytes(budgetAdjustment.bytes())
                    .setMaxItems(budgetAdjustment.items()))
            .build());

    // We just sent the budget, reset it.
    nextBudgetAdjustment.set(GetWorkBudget.noBudget());
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
        workItemBuffers.size(), inFlightBudget.get());
  }

  @Override
  public void sendHealthCheck() {
    send(HEALTH_CHECK_REQUEST);
  }

  @Override
  protected void onResponse(StreamingGetWorkResponseChunk chunk) {
    getWorkThrottleTimer.stop();
    WorkItemBuffer workItemBuffer =
        workItemBuffers.computeIfAbsent(chunk.getStreamId(), unused -> new WorkItemBuffer());
    workItemBuffer.append(chunk);

    // The entire WorkItem has been received, it is ready to be processed.
    if (chunk.getRemainingBytesForWorkItem() == 0) {
      workItemBuffer.runAndReset();
      // Record the fact that there are now fewer outstanding messages and bytes on the stream.
      inFlightBudget.updateAndGet(budget -> budget.subtract(1, workItemBuffer.bufferedSize()));
    }
  }

  @Override
  protected void startThrottleTimer() {
    getWorkThrottleTimer.start();
  }

  @Override
  public synchronized void adjustBudget(long itemsDelta, long bytesDelta) {
    nextBudgetAdjustment.set(nextBudgetAdjustment.get().apply(itemsDelta, bytesDelta));
    sendRequestExtension();
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

  private synchronized void updatePendingResponseBudget(long itemsDelta, long bytesDelta) {
    pendingResponseBudget.set(pendingResponseBudget.get().apply(itemsDelta, bytesDelta));
  }

  @AutoValue
  abstract static class ComputationMetadata {
    private static ComputationMetadata fromProto(ComputationWorkItemMetadata metadataProto) {
      return new AutoValue_GrpcDirectGetWorkStream_ComputationMetadata(
          metadataProto.getComputationId(),
          WindmillTimeUtils.windmillToHarnessWatermark(metadataProto.getInputDataWatermark()),
          WindmillTimeUtils.windmillToHarnessWatermark(
              metadataProto.getDependentRealtimeInputWatermark()));
    }

    abstract String computationId();

    abstract Instant inputDataWatermark();

    abstract Instant synchronizedProcessingTime();
  }

  private class WorkItemBuffer {
    private final GetWorkTimingInfosTracker workTimingInfosTracker;
    private ByteString data;
    private @Nullable ComputationMetadata metadata;

    private WorkItemBuffer() {
      workTimingInfosTracker = new GetWorkTimingInfosTracker(System::currentTimeMillis);
      data = ByteString.EMPTY;
      this.metadata = null;
    }

    private void append(StreamingGetWorkResponseChunk chunk) {
      if (chunk.hasComputationMetadata()) {
        this.metadata = ComputationMetadata.fromProto(chunk.getComputationMetadata());
      }

      this.data = data.concat(chunk.getSerializedWorkItem());
      workTimingInfosTracker.addTimingInfo(chunk.getPerWorkItemTimingInfosList());
    }

    private long bufferedSize() {
      return data.size();
    }

    private void runAndReset() {
      try {
        WorkItem workItem = WorkItem.parseFrom(data.newInput());
        updatePendingResponseBudget(1, workItem.getSerializedSize());
        Preconditions.checkNotNull(metadata);
        workItemProcessorFn.processWork(
            metadata.computationId(),
            metadata.inputDataWatermark(),
            metadata.synchronizedProcessingTime(),
            ProcessWorkItemClient.create(
                WorkItem.parseFrom(data.newInput()), getDataStream.get(), commitWorkStream.get()),
            // After the work item is successfully queued or dropped by ActiveWorkState, remove it
            // from the pendingResponseBudget.
            queuedWorkItem -> updatePendingResponseBudget(-1, -workItem.getSerializedSize()),
            workTimingInfosTracker.getLatencyAttributions());
      } catch (IOException e) {
        LOG.error("Failed to parse work item from stream: ", e);
      }
      workTimingInfosTracker.reset();
      data = ByteString.EMPTY;
    }
  }
}
