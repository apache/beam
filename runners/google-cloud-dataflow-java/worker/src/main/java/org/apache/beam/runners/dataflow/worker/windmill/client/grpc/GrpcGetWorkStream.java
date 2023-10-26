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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.WindmillTimeUtils;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.LatencyAttribution;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetWorkRequestExtension;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetWorkResponseChunk;
import org.apache.beam.runners.dataflow.worker.windmill.client.AbstractWindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.StreamObserverFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.throttling.ThrottleTimer;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemReceiver;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.stub.StreamObserver;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GrpcGetWorkStream
    extends AbstractWindmillStream<StreamingGetWorkRequest, StreamingGetWorkResponseChunk>
    implements GetWorkStream {

  private static final Logger LOG = LoggerFactory.getLogger(GrpcGetWorkStream.class);

  private final GetWorkRequest request;
  private final WorkItemReceiver receiver;
  private final ThrottleTimer getWorkThrottleTimer;
  private final Map<Long, GrpcGetWorkStream.WorkItemBuffer> buffers;
  private final AtomicLong inflightMessages;
  private final AtomicLong inflightBytes;

  private GrpcGetWorkStream(
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
      WorkItemReceiver receiver) {
    super(
        startGetWorkRpcFn, backoff, streamObserverFactory, streamRegistry, logEveryNStreamFailures);
    this.request = request;
    this.getWorkThrottleTimer = getWorkThrottleTimer;
    this.receiver = receiver;
    this.buffers = new ConcurrentHashMap<>();
    this.inflightMessages = new AtomicLong();
    this.inflightBytes = new AtomicLong();
  }

  public static GrpcGetWorkStream create(
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
      WorkItemReceiver receiver) {
    GrpcGetWorkStream getWorkStream =
        new GrpcGetWorkStream(
            startGetWorkRpcFn,
            request,
            backoff,
            streamObserverFactory,
            streamRegistry,
            logEveryNStreamFailures,
            getWorkThrottleTimer,
            receiver);
    getWorkStream.startStream();
    return getWorkStream;
  }

  private void sendRequestExtension(long moreItems, long moreBytes) {
    final StreamingGetWorkRequest extension =
        StreamingGetWorkRequest.newBuilder()
            .setRequestExtension(
                StreamingGetWorkRequestExtension.newBuilder()
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
                StreamingGetWorkRequestExtension.newBuilder().setMaxItems(0).setMaxBytes(0).build())
            .build());
  }

  @Override
  protected void onResponse(StreamingGetWorkResponseChunk chunk) {
    getWorkThrottleTimer.stop();

    GrpcGetWorkStream.WorkItemBuffer buffer =
        buffers.computeIfAbsent(
            chunk.getStreamId(), unused -> new GrpcGetWorkStream.WorkItemBuffer());
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
    @Nullable private Instant inputDataWatermark;
    @Nullable private Instant synchronizedProcessingTime;
    private ByteString data;
    private long bufferedSize;

    @SuppressWarnings("initialization.fields.uninitialized")
    WorkItemBuffer() {
      workTimingInfosTracker = new GetWorkTimingInfosTracker(System::currentTimeMillis);
      data = ByteString.EMPTY;
      bufferedSize = 0;
    }

    @SuppressWarnings("NullableProblems")
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
        Windmill.WorkItem workItem = Windmill.WorkItem.parseFrom(data.newInput());
        List<LatencyAttribution> getWorkStreamLatencies =
            workTimingInfosTracker.getLatencyAttributions();
        receiver.receiveWork(
            computation,
            inputDataWatermark,
            synchronizedProcessingTime,
            workItem,
            getWorkStreamLatencies);
      } catch (IOException e) {
        LOG.error("Failed to parse work item from stream: ", e);
      }
      workTimingInfosTracker.reset();
      data = ByteString.EMPTY;
      bufferedSize = 0;
    }
  }
}
