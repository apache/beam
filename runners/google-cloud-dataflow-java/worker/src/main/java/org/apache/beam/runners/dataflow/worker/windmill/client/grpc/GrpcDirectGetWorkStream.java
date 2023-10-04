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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
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

  private final GetWorkRequest request;
  private final ProcessWorkItem processWorkItemFn;
  private final ThrottleTimer getWorkThrottleTimer;
  private final Map<Long, WorkItemBuffer> buffers;
  private final AtomicLong inflightMessages;
  private final AtomicLong inflightBytes;
  private final Supplier<GetDataStream> getDataStream;
  private final Supplier<CommitWorkStream> commitWorkStream;

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
    this.buffers = new ConcurrentHashMap<>();
    this.inflightMessages = new AtomicLong();
    this.inflightBytes = new AtomicLong();
    // Use the same GetDataStream and CommitWorkStream instances to process all the work in this
    // stream.
    this.getDataStream = Suppliers.memoize(getDataStream::get);
    this.commitWorkStream = Suppliers.memoize(commitWorkStream::get);
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

  private void sendRequestExtension(long moreItems, long moreBytes) {
    final StreamingGetWorkRequest extension =
        StreamingGetWorkRequest.newBuilder()
            .setRequestExtension(
                Windmill.StreamingGetWorkRequestExtension.newBuilder()
                    .setMaxItems(moreItems)
                    .setMaxBytes(moreBytes))
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
    buffers.clear();
    inflightMessages.set(request.getMaxItems());
    inflightBytes.set(request.getMaxBytes());
    send(StreamingGetWorkRequest.newBuilder().setRequest(request).build());
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
        buffers.size(), inflightMessages.intValue(), inflightBytes.intValue());
  }

  @Override
  public void sendHealthCheck() {
    send(
        StreamingGetWorkRequest.newBuilder()
            .setRequestExtension(
                Windmill.StreamingGetWorkRequestExtension.newBuilder()
                    .setMaxItems(0)
                    .setMaxBytes(0)
                    .build())
            .build());
  }

  @Override
  protected void onResponse(StreamingGetWorkResponseChunk chunk) {
    getWorkThrottleTimer.stop();
    WorkItemBuffer buffer =
        buffers.computeIfAbsent(chunk.getStreamId(), unused -> new WorkItemBuffer());
    buffer.append(chunk);

    if (chunk.getRemainingBytesForWorkItem() == 0) {
      long size = buffer.bufferedSize();
      buffer.runAndReset();

      // Record the fact that there are now fewer outstanding messages and bytes on the stream.
      long numInflight = inflightMessages.decrementAndGet();
      long bytesInflight = inflightBytes.addAndGet(-size);

      // If the outstanding items or bytes limit has gotten too low, top both off with a
      // GetWorkExtension.  The goal is to keep the limits relatively close to their maximum
      // values without sending too many extension requests.
      if (numInflight < request.getMaxItems() / 2 || bytesInflight < request.getMaxBytes() / 2) {
        long moreItems = request.getMaxItems() - numInflight;
        long moreBytes = request.getMaxBytes() - bytesInflight;
        inflightMessages.getAndAdd(moreItems);
        inflightBytes.getAndAdd(moreBytes);
        sendRequestExtension(moreItems, moreBytes);
      }
    }
  }

  @Override
  protected void startThrottleTimer() {
    getWorkThrottleTimer.start();
  }

  @Override
  public void adjustBudget(long itemsDelta, long bytesDelta) {
    // no-op
  }

  @Override
  public GetWorkBudget remainingBudget() {
    return GetWorkBudget.builder()
        .setBytes(request.getMaxBytes() - inflightBytes.get())
        .setItems(request.getMaxItems() - inflightMessages.get())
        .build();
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
        processWorkItemFn.processWork(
            computation,
            inputDataWatermark,
            synchronizedProcessingTime,
            ProcessWorkItemClient.create(
                WorkItem.parseFrom(data.newInput()), getDataStream.get(), commitWorkStream.get()),
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
