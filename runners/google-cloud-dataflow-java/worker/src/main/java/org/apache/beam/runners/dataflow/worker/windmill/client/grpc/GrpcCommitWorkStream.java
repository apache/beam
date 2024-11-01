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

import com.google.auto.value.AutoValue;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.CommitStatus;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingCommitRequestChunk;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingCommitResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingCommitWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItemCommitRequest;
import org.apache.beam.runners.dataflow.worker.windmill.client.AbstractWindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.CommitWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.StreamObserverFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.throttling.ThrottleTimer;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.EvictingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class GrpcCommitWorkStream
    extends AbstractWindmillStream<StreamingCommitWorkRequest, StreamingCommitResponse>
    implements CommitWorkStream {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcCommitWorkStream.class);

  private static final long HEARTBEAT_REQUEST_ID = Long.MAX_VALUE;

  private final ConcurrentMap<Long, PendingRequest> pending;
  private final AtomicLong idGenerator;
  private final JobHeader jobHeader;
  private final ThrottleTimer commitWorkThrottleTimer;
  private final int streamingRpcBatchLimit;

  private GrpcCommitWorkStream(
      String backendWorkerToken,
      Function<StreamObserver<StreamingCommitResponse>, StreamObserver<StreamingCommitWorkRequest>>
          startCommitWorkRpcFn,
      BackOff backoff,
      StreamObserverFactory streamObserverFactory,
      Set<AbstractWindmillStream<?, ?>> streamRegistry,
      int logEveryNStreamFailures,
      ThrottleTimer commitWorkThrottleTimer,
      JobHeader jobHeader,
      AtomicLong idGenerator,
      int streamingRpcBatchLimit) {
    super(
        LOG,
        "CommitWorkStream",
        startCommitWorkRpcFn,
        backoff,
        streamObserverFactory,
        streamRegistry,
        logEveryNStreamFailures,
        backendWorkerToken);
    pending = new ConcurrentHashMap<>();
    this.idGenerator = idGenerator;
    this.jobHeader = jobHeader;
    this.commitWorkThrottleTimer = commitWorkThrottleTimer;
    this.streamingRpcBatchLimit = streamingRpcBatchLimit;
  }

  static GrpcCommitWorkStream create(
      String backendWorkerToken,
      Function<StreamObserver<StreamingCommitResponse>, StreamObserver<StreamingCommitWorkRequest>>
          startCommitWorkRpcFn,
      BackOff backoff,
      StreamObserverFactory streamObserverFactory,
      Set<AbstractWindmillStream<?, ?>> streamRegistry,
      int logEveryNStreamFailures,
      ThrottleTimer commitWorkThrottleTimer,
      JobHeader jobHeader,
      AtomicLong idGenerator,
      int streamingRpcBatchLimit) {
    return new GrpcCommitWorkStream(
        backendWorkerToken,
        startCommitWorkRpcFn,
        backoff,
        streamObserverFactory,
        streamRegistry,
        logEveryNStreamFailures,
        commitWorkThrottleTimer,
        jobHeader,
        idGenerator,
        streamingRpcBatchLimit);
  }

  @Override
  public void appendSpecificHtml(PrintWriter writer) {
    writer.format("CommitWorkStream: %d pending", pending.size());
  }

  @Override
  protected synchronized void onNewStream() {
    send(StreamingCommitWorkRequest.newBuilder().setHeader(jobHeader).build());
    try (Batcher resendBatcher = new Batcher()) {
      for (Map.Entry<Long, PendingRequest> entry : pending.entrySet()) {
        if (!resendBatcher.canAccept(entry.getValue().getBytes())) {
          resendBatcher.flush();
        }
        resendBatcher.add(entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * Returns a builder that can be used for sending requests. Each builder is not thread-safe but
   * different builders for the same stream may be used simultaneously.
   */
  @Override
  public CommitWorkStream.RequestBatcher batcher() {
    return new Batcher();
  }

  @Override
  protected boolean hasPendingRequests() {
    return !pending.isEmpty();
  }

  @Override
  public void sendHealthCheck() {
    if (hasPendingRequests()) {
      StreamingCommitWorkRequest.Builder builder = StreamingCommitWorkRequest.newBuilder();
      builder.addCommitChunkBuilder().setRequestId(HEARTBEAT_REQUEST_ID);
      send(builder.build());
    }
  }

  @Override
  protected void onResponse(StreamingCommitResponse response) {
    commitWorkThrottleTimer.stop();

    CommitCompletionException failures = new CommitCompletionException();
    for (int i = 0; i < response.getRequestIdCount(); ++i) {
      long requestId = response.getRequestId(i);
      if (requestId == HEARTBEAT_REQUEST_ID) {
        continue;
      }
      PendingRequest pendingRequest = pending.remove(requestId);
      CommitStatus commitStatus =
          i < response.getStatusCount() ? response.getStatus(i) : CommitStatus.OK;
      if (pendingRequest == null) {
        if (!hasReceivedShutdownSignal()) {
          // Skip responses when the stream is shutdown since they are now invalid.
          LOG.error("Got unknown commit request ID: {}", requestId);
        }
      } else {
        try {
          pendingRequest.completeWithStatus(commitStatus);
        } catch (RuntimeException e) {
          // Catch possible exceptions to ensure that an exception for one commit does not prevent
          // other commits from being processed. Aggregate all the failures to throw after
          // processing the response if they exist.
          LOG.warn("Exception while processing commit response.", e);
          failures.recordError(commitStatus, e);
        }
      }
    }
    if (failures.hasErrors()) {
      throw failures;
    }
  }

  @Override
  protected void shutdownInternal() {
    Iterator<PendingRequest> pendingRequests = pending.values().iterator();
    while (pendingRequests.hasNext()) {
      PendingRequest pendingRequest = pendingRequests.next();
      pendingRequest.abort();
      pendingRequests.remove();
    }
  }

  @Override
  protected void startThrottleTimer() {
    commitWorkThrottleTimer.start();
  }

  private void flushInternal(Map<Long, PendingRequest> requests) {
    if (requests.isEmpty()) {
      return;
    }

    if (requests.size() == 1) {
      Map.Entry<Long, PendingRequest> elem = requests.entrySet().iterator().next();
      if (elem.getValue().request().getSerializedSize()
          > AbstractWindmillStream.RPC_STREAM_CHUNK_SIZE) {
        issueMultiChunkRequest(elem.getKey(), elem.getValue());
      } else {
        issueSingleRequest(elem.getKey(), elem.getValue());
      }
    } else {
      issueBatchedRequest(requests);
    }
  }

  private void issueSingleRequest(long id, PendingRequest pendingRequest) {
    StreamingCommitWorkRequest.Builder requestBuilder = StreamingCommitWorkRequest.newBuilder();
    requestBuilder
        .addCommitChunkBuilder()
        .setComputationId(pendingRequest.computationId())
        .setRequestId(id)
        .setShardingKey(pendingRequest.shardingKey())
        .setSerializedWorkItemCommit(pendingRequest.serializedCommit());
    StreamingCommitWorkRequest chunk = requestBuilder.build();
    synchronized (this) {
      try {
        if (!prepareForSend(id, pendingRequest)) {
          pendingRequest.abort();
          return;
        }
        send(chunk);
      } catch (IllegalStateException e) {
        // Stream was broken, request will be retried when stream is reopened.
      }
    }
  }

  private void issueBatchedRequest(Map<Long, PendingRequest> requests) {
    StreamingCommitWorkRequest.Builder requestBuilder = StreamingCommitWorkRequest.newBuilder();
    String lastComputation = null;
    for (Map.Entry<Long, PendingRequest> entry : requests.entrySet()) {
      PendingRequest request = entry.getValue();
      StreamingCommitRequestChunk.Builder chunkBuilder = requestBuilder.addCommitChunkBuilder();
      if (lastComputation == null || !lastComputation.equals(request.computationId())) {
        chunkBuilder.setComputationId(request.computationId());
        lastComputation = request.computationId();
      }
      chunkBuilder
          .setRequestId(entry.getKey())
          .setShardingKey(request.shardingKey())
          .setSerializedWorkItemCommit(request.serializedCommit());
    }
    StreamingCommitWorkRequest request = requestBuilder.build();
    synchronized (this) {
      if (!prepareForSend(requests)) {
        requests.forEach((ignored, pendingRequest) -> pendingRequest.abort());
        return;
      }
      try {
        send(request);
      } catch (IllegalStateException e) {
        // Stream was broken, request will be retried when stream is reopened.
      }
    }
  }

  private void issueMultiChunkRequest(long id, PendingRequest pendingRequest) {
    checkNotNull(pendingRequest.computationId(), "Cannot commit WorkItem w/o a computationId.");
    ByteString serializedCommit = pendingRequest.serializedCommit();
    synchronized (this) {
      if (!prepareForSend(id, pendingRequest)) {
        pendingRequest.abort();
        return;
      }

      for (int i = 0;
          i < serializedCommit.size();
          i += AbstractWindmillStream.RPC_STREAM_CHUNK_SIZE) {
        int end = i + AbstractWindmillStream.RPC_STREAM_CHUNK_SIZE;
        ByteString chunk = serializedCommit.substring(i, Math.min(end, serializedCommit.size()));
        StreamingCommitRequestChunk.Builder chunkBuilder =
            StreamingCommitRequestChunk.newBuilder()
                .setRequestId(id)
                .setSerializedWorkItemCommit(chunk)
                .setComputationId(pendingRequest.computationId())
                .setShardingKey(pendingRequest.shardingKey());
        int remaining = serializedCommit.size() - end;
        if (remaining > 0) {
          chunkBuilder.setRemainingBytesForWorkItem(remaining);
        }
        StreamingCommitWorkRequest requestChunk =
            StreamingCommitWorkRequest.newBuilder().addCommitChunk(chunkBuilder).build();
        try {
          send(requestChunk);
        } catch (IllegalStateException e) {
          // Stream was broken, request will be retried when stream is reopened.
          break;
        }
      }
    }
  }

  /** Returns true if prepare for send succeeded. */
  private synchronized boolean prepareForSend(long id, PendingRequest request) {
    synchronized (shutdownLock) {
      if (!hasReceivedShutdownSignal()) {
        pending.put(id, request);
        return true;
      }
      return false;
    }
  }

  /** Returns true if prepare for send succeeded. */
  private synchronized boolean prepareForSend(Map<Long, PendingRequest> requests) {
    synchronized (shutdownLock) {
      if (!hasReceivedShutdownSignal()) {
        pending.putAll(requests);
        return true;
      }
      return false;
    }
  }

  @AutoValue
  abstract static class PendingRequest {

    private static PendingRequest create(
        String computationId, WorkItemCommitRequest request, Consumer<CommitStatus> onDone) {
      return new AutoValue_GrpcCommitWorkStream_PendingRequest(computationId, request, onDone);
    }

    abstract String computationId();

    abstract WorkItemCommitRequest request();

    abstract Consumer<CommitStatus> onDone();

    private long getBytes() {
      return (long) request().getSerializedSize() + computationId().length();
    }

    private ByteString serializedCommit() {
      return request().toByteString();
    }

    private void completeWithStatus(CommitStatus commitStatus) {
      onDone().accept(commitStatus);
    }

    private long shardingKey() {
      return request().getShardingKey();
    }

    private void abort() {
      completeWithStatus(CommitStatus.ABORTED);
    }
  }

  private static class CommitCompletionException extends RuntimeException {
    private static final int MAX_PRINTABLE_ERRORS = 10;
    private final Map<Pair<CommitStatus, Class<? extends Throwable>>, Integer> errorCounter;
    private final EvictingQueue<Throwable> detailedErrors;

    private CommitCompletionException() {
      super("Exception while processing commit response.");
      this.errorCounter = new HashMap<>();
      this.detailedErrors = EvictingQueue.create(MAX_PRINTABLE_ERRORS);
    }

    private void recordError(CommitStatus commitStatus, Throwable error) {
      errorCounter.compute(
          Pair.of(commitStatus, error.getClass()),
          (ignored, current) -> current == null ? 1 : current + 1);
      detailedErrors.add(error);
    }

    private boolean hasErrors() {
      return !errorCounter.isEmpty();
    }

    @Override
    public final String getMessage() {
      return "CommitCompletionException{"
          + "errorCounter="
          + errorCounter
          + ", detailedErrors="
          + detailedErrors
          + '}';
    }
  }

  private class Batcher implements CommitWorkStream.RequestBatcher {

    private final Map<Long, PendingRequest> queue;
    private long queuedBytes;

    private Batcher() {
      this.queuedBytes = 0;
      this.queue = new HashMap<>();
    }

    @Override
    public boolean commitWorkItem(
        String computation, WorkItemCommitRequest commitRequest, Consumer<CommitStatus> onDone) {
      if (!canAccept(commitRequest.getSerializedSize() + computation.length())
          || hasReceivedShutdownSignal()) {
        return false;
      }

      PendingRequest request = PendingRequest.create(computation, commitRequest, onDone);
      add(idGenerator.incrementAndGet(), request);
      return true;
    }

    /** Flushes any pending work items to the wire. */
    @Override
    public void flush() {
      try {
        if (!hasReceivedShutdownSignal()) {
          flushInternal(queue);
        } else {
          queue.forEach((ignored, request) -> request.abort());
        }
      } finally {
        queuedBytes = 0;
        queue.clear();
      }
    }

    void add(long id, PendingRequest request) {
      Preconditions.checkState(canAccept(request.getBytes()));
      queuedBytes += request.getBytes();
      queue.put(id, request);
    }

    private boolean canAccept(long requestBytes) {
      return queue.isEmpty()
          || (queue.size() < streamingRpcBatchLimit
              && (requestBytes + queuedBytes) < AbstractWindmillStream.RPC_STREAM_CHUNK_SIZE);
    }
  }
}
