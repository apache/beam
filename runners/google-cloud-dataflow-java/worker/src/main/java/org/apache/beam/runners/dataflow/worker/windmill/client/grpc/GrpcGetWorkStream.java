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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetWorkRequestExtension;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetWorkResponseChunk;
import org.apache.beam.runners.dataflow.worker.windmill.client.AbstractWindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamShutdownException;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GetWorkResponseChunkAssembler.AssembledWorkItem;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.StreamObserverFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.throttling.ThrottleTimer;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemReceiver;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class GrpcGetWorkStream
    extends AbstractWindmillStream<StreamingGetWorkRequest, StreamingGetWorkResponseChunk>
    implements GetWorkStream {

  private static final Logger LOG = LoggerFactory.getLogger(GrpcGetWorkStream.class);
  private static final StreamingGetWorkRequest HEALTH_CHECK =
      StreamingGetWorkRequest.newBuilder()
          .setRequestExtension(
              StreamingGetWorkRequestExtension.newBuilder().setMaxItems(0).setMaxBytes(0).build())
          .build();

  private final GetWorkRequest request;
  private final WorkItemReceiver receiver;
  private final ThrottleTimer getWorkThrottleTimer;
  private final Map<Long, GetWorkResponseChunkAssembler> workItemAssemblers;
  private final AtomicLong inflightMessages;
  private final AtomicLong inflightBytes;
  private final boolean requestBatchedGetWorkResponse;

  private GrpcGetWorkStream(
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
      ThrottleTimer getWorkThrottleTimer,
      WorkItemReceiver receiver) {
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
    this.receiver = receiver;
    this.workItemAssemblers = new ConcurrentHashMap<>();
    this.inflightMessages = new AtomicLong();
    this.inflightBytes = new AtomicLong();
    this.requestBatchedGetWorkResponse = requestBatchedGetWorkResponse;
  }

  public static GrpcGetWorkStream create(
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
      ThrottleTimer getWorkThrottleTimer,
      WorkItemReceiver receiver) {
    return new GrpcGetWorkStream(
        backendWorkerToken,
        startGetWorkRpcFn,
        request,
        backoff,
        streamObserverFactory,
        streamRegistry,
        logEveryNStreamFailures,
        requestBatchedGetWorkResponse,
        getWorkThrottleTimer,
        receiver);
  }

  private void sendRequestExtension(long moreItems, long moreBytes) {
    final StreamingGetWorkRequest extension =
        StreamingGetWorkRequest.newBuilder()
            .setRequestExtension(
                StreamingGetWorkRequestExtension.newBuilder()
                    .setMaxItems(moreItems)
                    .setMaxBytes(moreBytes))
            .build();

    executeSafely(
        () -> {
          try {
            trySend(extension);
          } catch (WindmillStreamShutdownException e) {
            // Stream was closed.
          }
        });
  }

  @Override
  protected synchronized void onNewStream() throws WindmillStreamShutdownException {
    workItemAssemblers.clear();
    inflightMessages.set(request.getMaxItems());
    inflightBytes.set(request.getMaxBytes());
    trySend(
        StreamingGetWorkRequest.newBuilder()
            .setSupportsMultipleWorkItemsInChunk(requestBatchedGetWorkResponse)
            .setRequest(request)
            .build());
  }

  @Override
  protected void shutdownInternal() {}

  @Override
  protected boolean hasPendingRequests() {
    return false;
  }

  @Override
  public void appendSpecificHtml(PrintWriter writer) {
    // Number of buffers is same as distinct workers that sent work on this stream.
    writer.format(
        "GetWorkStream: %d buffers, %d inflight messages allowed, %d inflight bytes allowed",
        workItemAssemblers.size(), inflightMessages.intValue(), inflightBytes.intValue());
  }

  @Override
  public void sendHealthCheck() throws WindmillStreamShutdownException {
    trySend(HEALTH_CHECK);
  }

  @Override
  protected void onResponse(StreamingGetWorkResponseChunk chunk) {
    getWorkThrottleTimer.stop();
    workItemAssemblers
        .computeIfAbsent(chunk.getStreamId(), unused -> new GetWorkResponseChunkAssembler())
        .append(chunk)
        .forEach(this::consumeAssembledWorkItem);
  }

  private void consumeAssembledWorkItem(AssembledWorkItem assembledWorkItem) {
    receiver.receiveWork(
        assembledWorkItem.computationMetadata().computationId(),
        assembledWorkItem.computationMetadata().inputDataWatermark(),
        assembledWorkItem.computationMetadata().synchronizedProcessingTime(),
        assembledWorkItem.workItem(),
        assembledWorkItem.latencyAttributions());

    // Record the fact that there are now fewer outstanding messages and bytes on the stream.
    long numInflight = inflightMessages.decrementAndGet();
    long bytesInflight = inflightBytes.addAndGet(-assembledWorkItem.bufferedSize());

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

  @Override
  protected void startThrottleTimer() {
    getWorkThrottleTimer.start();
  }

  @Override
  public void setBudget(GetWorkBudget newBudget) {
    // no-op
  }
}
