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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.worker.WindmillTimeUtils;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetWorkResponseChunk;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem;
import org.apache.beam.runners.dataflow.worker.windmill.client.AbstractWindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.StreamObserverFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.throttling.ThrottleTimer;
import org.apache.beam.runners.dataflow.worker.windmill.work.ProcessWorkItem;
import org.apache.beam.runners.dataflow.worker.windmill.work.ProcessWorkItemClient;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.stub.StreamObserver;
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

  private final GetWorkRequest request;
  private final ProcessWorkItem processWorkItemFn;
  private final ThrottleTimer getWorkThrottleTimer;
  /**
   * Map of stream IDs to their buffers. Used to aggregate streaming gRPC response chunks as they
   * come in. Once all chunks for a response has been received, the chunk is processed and the
   * buffer is cleared.
   */
  private final ConcurrentMap<Long, WorkItemBuffer> workItemBuffers;

  private final Supplier<GetDataStream> getDataStream;
  private final Supplier<CommitWorkStream> commitWorkStream;

  private final AtomicLong inflightMessages;
  private final AtomicLong inflightBytes;
  private final AtomicReference<GetWorkBudget> nextBudgetAdjustment;
  private final AtomicReference<GetWorkBudget> pendingResponseBudget;

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
      ProcessWorkItem processWorkItemFn) {
    super(
        startGetWorkRpcFn, backoff, streamObserverFactory, streamRegistry, logEveryNStreamFailures);
    this.request = request;
    this.getWorkThrottleTimer = getWorkThrottleTimer;
    this.processWorkItemFn = processWorkItemFn;
    this.workItemBuffers = new ConcurrentHashMap<>();
    // Use the same GetDataStream and CommitWorkStream instances to process all the work in this
    // stream.
    this.getDataStream = Suppliers.memoize(getDataStream::get);
    this.commitWorkStream = Suppliers.memoize(commitWorkStream::get);

    this.inflightMessages = new AtomicLong();
    this.inflightBytes = new AtomicLong();
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
      ProcessWorkItem processWorkItemFn) {
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
            processWorkItemFn);
    getWorkStream.startStream();
    return getWorkStream;
  }

  private void sendRequestExtension() {
    GetWorkBudget adjustment = nextBudgetAdjustment.get();
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

    synchronized (this) {
      // Just sent the request extension, reset the nextBudgetAdjustment. This will be set when
      // adjustBudget is called.
      nextBudgetAdjustment.set(GetWorkBudget.noBudget());
    }
  }

  @Override
  protected synchronized void onNewStream() {
    workItemBuffers.clear();
    long currentInflightItems = inflightMessages.get();
    long currentInflightBytes = inflightBytes.get();
    GetWorkBudget currentNextBudgetAdjustment = nextBudgetAdjustment.get();
    GetWorkBudget.Builder newNextBudgetAdjustmentBuilder = currentNextBudgetAdjustment.toBuilder();
    if (currentInflightItems > 0) {
      newNextBudgetAdjustmentBuilder.setItems(
          currentNextBudgetAdjustment.items() + currentInflightItems);
    }

    if (currentInflightBytes > 0) {
      newNextBudgetAdjustmentBuilder.setBytes(
          currentNextBudgetAdjustment.bytes() + currentInflightBytes);
    }

    GetWorkBudget newNextBudgetAdjustment = newNextBudgetAdjustmentBuilder.build();

    inflightMessages.set(newNextBudgetAdjustment.items());
    inflightBytes.set(newNextBudgetAdjustment.bytes());

    send(
        StreamingGetWorkRequest.newBuilder()
            .setRequest(
                request
                    .toBuilder()
                    .setMaxBytes(currentNextBudgetAdjustment.bytes())
                    .setMaxItems(currentNextBudgetAdjustment.items()))
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
        "GetWorkStream: %d buffers, %d inflight messages allowed, %d inflight bytes allowed",
        workItemBuffers.size(), inflightMessages.intValue(), inflightBytes.intValue());
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
      inflightMessages.decrementAndGet();
      inflightBytes.addAndGet(-workItemBuffer.bufferedSize());
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
    // inflightMessages or inflightBytes will be at minimum 0.
    GetWorkBudget currentInflightBudget =
        GetWorkBudget.builder()
            .setItems(inflightMessages.get())
            .setBytes(inflightBytes.get())
            .build();

    return GetWorkBudget.builder()
        .setItems(
            currentNextBudgetAdjustment.items()
                + currentPendingResponseBudget.items()
                + currentInflightBudget.items())
        .setBytes(
            currentNextBudgetAdjustment.bytes()
                + currentPendingResponseBudget.bytes()
                + currentInflightBudget.bytes())
        .build();
  }

  private synchronized void updatePendingResponseBudget(long itemsDelta, long bytesDelta) {
    pendingResponseBudget.set(pendingResponseBudget.get().apply(itemsDelta, bytesDelta));
  }

  private class WorkItemBuffer {
    private final GetWorkTimingInfosTracker workTimingInfosTracker;
    private String computation;
    private ByteString data;
    private long bufferedSize;
    private Instant inputDataWatermark;
    private Instant synchronizedProcessingTime;

    WorkItemBuffer() {
      workTimingInfosTracker = new GetWorkTimingInfosTracker(System::currentTimeMillis);
      data = ByteString.EMPTY;
      bufferedSize = 0;
      inputDataWatermark = Instant.EPOCH;
      synchronizedProcessingTime = Instant.EPOCH;
    }

    private void setMetadata(Windmill.ComputationWorkItemMetadata metadata) {
      this.computation = metadata.getComputationId();
      this.inputDataWatermark =
          WindmillTimeUtils.windmillToHarnessWatermark(metadata.getInputDataWatermark());
      this.synchronizedProcessingTime =
          WindmillTimeUtils.windmillToHarnessWatermark(
              metadata.getDependentRealtimeInputWatermark());
    }

    private void append(StreamingGetWorkResponseChunk chunk) {
      if (chunk.hasComputationMetadata()) {
        setMetadata(chunk.getComputationMetadata());
      }

      this.data = data.concat(chunk.getSerializedWorkItem());
      this.bufferedSize += chunk.getSerializedWorkItem().size();
      workTimingInfosTracker.addTimingInfo(chunk.getPerWorkItemTimingInfosList());
    }

    private long bufferedSize() {
      return bufferedSize;
    }

    private void runAndReset() {
      try {
        WorkItem workItem = WorkItem.parseFrom(data.newInput());
        updatePendingResponseBudget(1, workItem.getSerializedSize());
        processWorkItemFn.processWork(
            computation,
            inputDataWatermark,
            synchronizedProcessingTime,
            ProcessWorkItemClient.create(
                WorkItem.parseFrom(data.newInput()), getDataStream.get(), commitWorkStream.get()),
            queuedWorkItem -> updatePendingResponseBudget(-1, -workItem.getSerializedSize()),
            workTimingInfosTracker.getLatencyAttributions());
      } catch (IOException e) {
        LOG.error("Failed to parse work item from stream: ", e);
      }
      workTimingInfosTracker.reset();
      data = ByteString.EMPTY;
      bufferedSize = 0;
    }
  }
}
