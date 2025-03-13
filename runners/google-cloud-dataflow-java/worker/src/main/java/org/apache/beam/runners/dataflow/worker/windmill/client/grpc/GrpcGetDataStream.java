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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Verify.verify;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationHeartbeatRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationHeartbeatResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalData;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.HeartbeatRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetDataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.client.AbstractWindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamShutdownException;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcGetDataStreamRequests.QueuedBatch;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcGetDataStreamRequests.QueuedRequest;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.StreamObserverFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.throttling.ThrottleTimer;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.StreamObserver;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
final class GrpcGetDataStream
    extends AbstractWindmillStream<StreamingGetDataRequest, StreamingGetDataResponse>
    implements GetDataStream {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcGetDataStream.class);
  private static final StreamingGetDataRequest HEALTH_CHECK_REQUEST =
      StreamingGetDataRequest.newBuilder().build();

  /** @implNote {@link QueuedBatch} objects in the queue are is guarded by {@code this} */
  private final Deque<QueuedBatch> batches;

  private final Map<Long, AppendableInputStream> pending;
  private final AtomicLong idGenerator;
  private final ThrottleTimer getDataThrottleTimer;
  private final JobHeader jobHeader;
  private final int streamingRpcBatchLimit;
  // If true, then active work refreshes will be sent as KeyedGetDataRequests. Otherwise, use the
  // newer ComputationHeartbeatRequests.
  private final boolean sendKeyedGetDataRequests;
  private final Consumer<List<ComputationHeartbeatResponse>> processHeartbeatResponses;

  private GrpcGetDataStream(
      String backendWorkerToken,
      Function<StreamObserver<StreamingGetDataResponse>, StreamObserver<StreamingGetDataRequest>>
          startGetDataRpcFn,
      BackOff backoff,
      StreamObserverFactory streamObserverFactory,
      Set<AbstractWindmillStream<?, ?>> streamRegistry,
      int logEveryNStreamFailures,
      ThrottleTimer getDataThrottleTimer,
      JobHeader jobHeader,
      AtomicLong idGenerator,
      int streamingRpcBatchLimit,
      boolean sendKeyedGetDataRequests,
      Consumer<List<Windmill.ComputationHeartbeatResponse>> processHeartbeatResponses) {
    super(
        LOG,
        "GetDataStream",
        startGetDataRpcFn,
        backoff,
        streamObserverFactory,
        streamRegistry,
        logEveryNStreamFailures,
        backendWorkerToken);
    this.idGenerator = idGenerator;
    this.getDataThrottleTimer = getDataThrottleTimer;
    this.jobHeader = jobHeader;
    this.streamingRpcBatchLimit = streamingRpcBatchLimit;
    this.batches = new ConcurrentLinkedDeque<>();
    this.pending = new ConcurrentHashMap<>();
    this.sendKeyedGetDataRequests = sendKeyedGetDataRequests;
    this.processHeartbeatResponses = processHeartbeatResponses;
  }

  static GrpcGetDataStream create(
      String backendWorkerToken,
      Function<StreamObserver<StreamingGetDataResponse>, StreamObserver<StreamingGetDataRequest>>
          startGetDataRpcFn,
      BackOff backoff,
      StreamObserverFactory streamObserverFactory,
      Set<AbstractWindmillStream<?, ?>> streamRegistry,
      int logEveryNStreamFailures,
      ThrottleTimer getDataThrottleTimer,
      JobHeader jobHeader,
      AtomicLong idGenerator,
      int streamingRpcBatchLimit,
      boolean sendKeyedGetDataRequests,
      Consumer<List<Windmill.ComputationHeartbeatResponse>> processHeartbeatResponses) {
    return new GrpcGetDataStream(
        backendWorkerToken,
        startGetDataRpcFn,
        backoff,
        streamObserverFactory,
        streamRegistry,
        logEveryNStreamFailures,
        getDataThrottleTimer,
        jobHeader,
        idGenerator,
        streamingRpcBatchLimit,
        sendKeyedGetDataRequests,
        processHeartbeatResponses);
  }

  private static WindmillStreamShutdownException shutdownExceptionFor(QueuedBatch batch) {
    return new WindmillStreamShutdownException(
        "Stream was closed when attempting to send " + batch.requestsCount() + " requests.");
  }

  private static WindmillStreamShutdownException shutdownExceptionFor(QueuedRequest request) {
    return new WindmillStreamShutdownException(
        "Cannot send request=[" + request + "] on closed stream.");
  }

  private void sendIgnoringClosed(StreamingGetDataRequest getDataRequest)
      throws WindmillStreamShutdownException {
    trySend(getDataRequest);
  }

  @Override
  protected synchronized void onNewStream() throws WindmillStreamShutdownException {
    trySend(StreamingGetDataRequest.newBuilder().setHeader(jobHeader).build());
    if (clientClosed) {
      // We rely on close only occurring after all methods on the stream have returned.
      // Since the requestKeyedData and requestGlobalData methods are blocking this
      // means there should be no pending requests.
      verify(!hasPendingRequests(), "Pending requests not expected if we've half-closed.");
    } else {
      for (AppendableInputStream responseStream : pending.values()) {
        responseStream.cancel();
      }
    }
  }

  @Override
  protected boolean hasPendingRequests() {
    return !pending.isEmpty() || !batches.isEmpty();
  }

  @Override
  protected void onResponse(StreamingGetDataResponse chunk) {
    checkArgument(chunk.getRequestIdCount() == chunk.getSerializedResponseCount());
    checkArgument(chunk.getRemainingBytesForResponse() == 0 || chunk.getRequestIdCount() == 1);
    getDataThrottleTimer.stop();
    onHeartbeatResponse(chunk.getComputationHeartbeatResponseList());

    for (int i = 0; i < chunk.getRequestIdCount(); ++i) {
      @Nullable AppendableInputStream responseStream = pending.get(chunk.getRequestId(i));
      if (responseStream == null) {
        synchronized (this) {
          // shutdown()/shutdownInternal() cleans up pending, else we expect a pending
          // responseStream for every response.
          verify(isShutdown, "No pending response stream");
        }
        continue;
      }
      responseStream.append(chunk.getSerializedResponse(i).newInput());
      if (chunk.getRemainingBytesForResponse() == 0) {
        responseStream.complete();
      }
    }
  }

  @Override
  protected void startThrottleTimer() {
    getDataThrottleTimer.start();
  }

  private long uniqueId() {
    return idGenerator.incrementAndGet();
  }

  @Override
  public KeyedGetDataResponse requestKeyedData(String computation, KeyedGetDataRequest request)
      throws WindmillStreamShutdownException {
    return issueRequest(
        QueuedRequest.forComputation(uniqueId(), computation, request),
        KeyedGetDataResponse::parseFrom);
  }

  @Override
  public GlobalData requestGlobalData(GlobalDataRequest request)
      throws WindmillStreamShutdownException {
    return issueRequest(QueuedRequest.global(uniqueId(), request), GlobalData::parseFrom);
  }

  @Override
  public void refreshActiveWork(Map<String, Collection<HeartbeatRequest>> heartbeats)
      throws WindmillStreamShutdownException {
    StreamingGetDataRequest.Builder builder = StreamingGetDataRequest.newBuilder();
    if (sendKeyedGetDataRequests) {
      long builderBytes = 0;
      for (Map.Entry<String, Collection<HeartbeatRequest>> entry : heartbeats.entrySet()) {
        for (HeartbeatRequest request : entry.getValue()) {
          // Calculate the bytes with some overhead for proto encoding.
          long bytes = (long) entry.getKey().length() + request.getSerializedSize() + 10;
          if (builderBytes > 0
              && (builderBytes + bytes > AbstractWindmillStream.RPC_STREAM_CHUNK_SIZE
                  || builder.getRequestIdCount() >= streamingRpcBatchLimit)) {
            sendIgnoringClosed(builder.build());
            builderBytes = 0;
            builder.clear();
          }
          builderBytes += bytes;
          builder.addStateRequest(
              ComputationGetDataRequest.newBuilder()
                  .setComputationId(entry.getKey())
                  .addRequests(
                      Windmill.KeyedGetDataRequest.newBuilder()
                          .setShardingKey(request.getShardingKey())
                          .setWorkToken(request.getWorkToken())
                          .setCacheToken(request.getCacheToken())
                          .addAllLatencyAttribution(request.getLatencyAttributionList())
                          .build()));
        }
      }

      if (builderBytes > 0) {
        sendIgnoringClosed(builder.build());
      }
    } else {
      // No translation necessary, but we must still respect `RPC_STREAM_CHUNK_SIZE`.
      long builderBytes = 0;
      for (Map.Entry<String, Collection<HeartbeatRequest>> entry : heartbeats.entrySet()) {
        ComputationHeartbeatRequest.Builder computationHeartbeatBuilder =
            ComputationHeartbeatRequest.newBuilder().setComputationId(entry.getKey());
        for (HeartbeatRequest request : entry.getValue()) {
          long bytes = (long) entry.getKey().length() + request.getSerializedSize() + 10;
          if (builderBytes > 0
              && builderBytes + bytes > AbstractWindmillStream.RPC_STREAM_CHUNK_SIZE) {
            if (computationHeartbeatBuilder.getHeartbeatRequestsCount() > 0) {
              builder.addComputationHeartbeatRequest(computationHeartbeatBuilder.build());
            }
            sendIgnoringClosed(builder.build());
            builderBytes = 0;
            builder.clear();
            computationHeartbeatBuilder.clear().setComputationId(entry.getKey());
          }
          builderBytes += bytes;
          computationHeartbeatBuilder.addHeartbeatRequests(request);
        }
        builder.addComputationHeartbeatRequest(computationHeartbeatBuilder.build());
      }

      if (builderBytes > 0) {
        sendIgnoringClosed(builder.build());
      }
    }
  }

  @Override
  public void onHeartbeatResponse(List<Windmill.ComputationHeartbeatResponse> responses) {
    processHeartbeatResponses.accept(responses);
  }

  @Override
  public void sendHealthCheck() throws WindmillStreamShutdownException {
    if (hasPendingRequests()) {
      trySend(HEALTH_CHECK_REQUEST);
    }
  }

  @Override
  protected synchronized void shutdownInternal() {
    // Stream has been explicitly closed. Drain pending input streams and request batches.
    // Future calls to send RPCs will fail.
    pending.values().forEach(AppendableInputStream::cancel);
    pending.clear();
    batches.forEach(
        batch -> {
          batch.markFinalized();
          batch.notifyFailed();
        });
    batches.clear();
  }

  @Override
  public void appendSpecificHtml(PrintWriter writer) {
    writer.format(
        "GetDataStream: %d queued batches, %d pending requests [", batches.size(), pending.size());
    for (Map.Entry<Long, AppendableInputStream> entry : pending.entrySet()) {
      writer.format("Stream %d ", entry.getKey());
      if (entry.getValue().isCancelled()) {
        writer.append("cancelled ");
      }
      if (entry.getValue().isComplete()) {
        writer.append("complete ");
      }
      int queueSize = entry.getValue().size();
      if (queueSize > 0) {
        writer.format("%d queued responses ", queueSize);
      }
      long blockedMs = entry.getValue().getBlockedStartMs();
      if (blockedMs > 0) {
        writer.format("blocked for %dms", Instant.now().getMillis() - blockedMs);
      }
    }
    writer.append("]");
  }

  private <ResponseT> ResponseT issueRequest(QueuedRequest request, ParseFn<ResponseT> parseFn)
      throws WindmillStreamShutdownException {
    while (true) {
      request.resetResponseStream();
      try {
        queueRequestAndWait(request);
        return parseFn.parse(request.getResponseStream());
      } catch (AppendableInputStream.InvalidInputStreamStateException | CancellationException e) {
        throwIfShutdown(request, e);
        if (!(e instanceof CancellationException)) {
          throw e;
        }
      } catch (IOException e) {
        LOG.error("Parsing GetData response failed: ", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throwIfShutdown(request, e);
        throw new RuntimeException(e);
      } finally {
        pending.remove(request.id());
      }
    }
  }

  private synchronized void throwIfShutdown(QueuedRequest request, Throwable cause)
      throws WindmillStreamShutdownException {
    if (isShutdown) {
      WindmillStreamShutdownException shutdownException = shutdownExceptionFor(request);
      shutdownException.addSuppressed(cause);
      throw shutdownException;
    }
  }

  private void queueRequestAndWait(QueuedRequest request)
      throws InterruptedException, WindmillStreamShutdownException {
    QueuedBatch batch;
    boolean responsibleForSend = false;
    @Nullable QueuedBatch prevBatch = null;
    synchronized (this) {
      if (isShutdown) {
        throw shutdownExceptionFor(request);
      }

      batch = batches.isEmpty() ? null : batches.getLast();
      if (batch == null
          || batch.isFinalized()
          || batch.requestsCount() >= streamingRpcBatchLimit
          || batch.byteSize() + request.byteSize() > AbstractWindmillStream.RPC_STREAM_CHUNK_SIZE) {
        if (batch != null) {
          prevBatch = batch;
        }
        batch = new QueuedBatch();
        batches.addLast(batch);
        responsibleForSend = true;
      }
      batch.addRequest(request);
    }
    if (responsibleForSend) {
      if (prevBatch == null) {
        // If there was not a previous batch wait a little while to improve
        // batching.
        sleeper.sleep(1);
      } else {
        prevBatch.waitForSendOrFailNotification();
      }
      // Finalize the batch so that no additional requests will be added.  Leave the batch in the
      // queue so that a subsequent batch will wait for its completion.
      synchronized (this) {
        if (isShutdown) {
          throw shutdownExceptionFor(batch);
        }

        verify(batch == batches.peekFirst(), "GetDataStream request batch removed before send().");
        batch.markFinalized();
      }
      trySendBatch(batch);
    } else {
      // Wait for this batch to be sent before parsing the response.
      batch.waitForSendOrFailNotification();
    }
  }

  void trySendBatch(QueuedBatch batch) throws WindmillStreamShutdownException {
    try {
      sendBatch(batch);
      synchronized (this) {
        if (isShutdown) {
          throw shutdownExceptionFor(batch);
        }

        verify(
            batch == batches.pollFirst(),
            "Sent GetDataStream request batch removed before send() was complete.");
      }
      // Notify all waiters with requests in this batch as well as the sender
      // of the next batch (if one exists).
      batch.notifySent();
    } catch (Exception e) {
      // Free waiters if the send() failed.
      batch.notifyFailed();
      // Propagate the exception to the calling thread.
      throw e;
    }
  }

  private void sendBatch(QueuedBatch batch) throws WindmillStreamShutdownException {
    if (batch.isEmpty()) {
      return;
    }

    // Synchronization of pending inserts is necessary with send to ensure duplicates are not
    // sent on stream reconnect.
    synchronized (this) {
      if (isShutdown) {
        throw shutdownExceptionFor(batch);
      }

      for (QueuedRequest request : batch.requestsReadOnly()) {
        // Map#put returns null if there was no previous mapping for the key, meaning we have not
        // seen it before.
        verify(
            pending.put(request.id(), request.getResponseStream()) == null,
            "Request already sent.");
      }

      if (!trySend(batch.asGetDataRequest())) {
        // The stream broke before this call went through; onNewStream will retry the fetch.
        LOG.warn("GetData stream broke before call started.");
      }
    }
  }

  @FunctionalInterface
  private interface ParseFn<ResponseT> {
    ResponseT parse(InputStream input) throws IOException;
  }
}
