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
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.PrintWriter;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.CommitStatus;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingCommitRequestChunk;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingCommitResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingCommitWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItemCommitRequest;
import org.apache.beam.runners.dataflow.worker.windmill.client.AbstractWindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.CommitWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamShutdownException;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.StreamObserverFactory;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.EvictingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class GrpcCommitWorkStream
    extends AbstractWindmillStream<StreamingCommitWorkRequest, StreamingCommitResponse>
    implements CommitWorkStream {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcCommitWorkStream.class);

  private static final long HEARTBEAT_REQUEST_ID = Long.MAX_VALUE;

  private static class StreamAndRequest {
    StreamAndRequest(@Nullable CommitWorkPhysicalStreamHandler handler, PendingRequest request) {
      this.handler = handler;
      this.request = request;
    }

    final @Nullable CommitWorkPhysicalStreamHandler handler;
    final PendingRequest request;
  }

  private final ConcurrentMap<Long, StreamAndRequest> pending = new ConcurrentHashMap<>();

  private final AtomicLong idGenerator;
  private final JobHeader jobHeader;
  private final int streamingRpcBatchLimit;
  private volatile boolean logMissingResponse = true;
  private final Duration maxRetryDuration;

  private GrpcCommitWorkStream(
      String backendWorkerToken,
      Function<StreamObserver<StreamingCommitResponse>, StreamObserver<StreamingCommitWorkRequest>>
          startCommitWorkRpcFn,
      BackOff backoff,
      StreamObserverFactory streamObserverFactory,
      Set<AbstractWindmillStream<?, ?>> streamRegistry,
      int logEveryNStreamFailures,
      JobHeader jobHeader,
      AtomicLong idGenerator,
      int streamingRpcBatchLimit,
      Duration halfClosePhysicalStreamAfter,
      Duration maxRetryDuration,
      ScheduledExecutorService executor) {
    super(
        LOG,
        startCommitWorkRpcFn,
        backoff,
        streamObserverFactory,
        streamRegistry,
        logEveryNStreamFailures,
        backendWorkerToken,
        halfClosePhysicalStreamAfter,
        executor);
    this.idGenerator = idGenerator;
    this.jobHeader = jobHeader;
    this.streamingRpcBatchLimit = streamingRpcBatchLimit;
    this.maxRetryDuration = maxRetryDuration;
  }

  static GrpcCommitWorkStream create(
      String backendWorkerToken,
      Function<StreamObserver<StreamingCommitResponse>, StreamObserver<StreamingCommitWorkRequest>>
          startCommitWorkRpcFn,
      BackOff backoff,
      StreamObserverFactory streamObserverFactory,
      Set<AbstractWindmillStream<?, ?>> streamRegistry,
      int logEveryNStreamFailures,
      JobHeader jobHeader,
      AtomicLong idGenerator,
      int streamingRpcBatchLimit,
      Duration halfClosePhysicalStreamAfter,
      Duration maxRetryDuration,
      ScheduledExecutorService executor) {
    return new GrpcCommitWorkStream(
        backendWorkerToken,
        startCommitWorkRpcFn,
        backoff,
        streamObserverFactory,
        streamRegistry,
        logEveryNStreamFailures,
        jobHeader,
        idGenerator,
        streamingRpcBatchLimit,
        halfClosePhysicalStreamAfter,
        maxRetryDuration,
        executor);
  }

  @Override
  public void appendSpecificHtml(PrintWriter writer) {
    writer.format("CommitWorkStream: %d pending ", pending.size());
  }

  @Override
  @SuppressWarnings("ReferenceEquality")
  protected synchronized void onFlushPending(boolean isNewStream)
      throws WindmillStreamShutdownException {
    if (isNewStream) {
      trySend(StreamingCommitWorkRequest.newBuilder().setHeader(jobHeader).build());
    }
    // Flush all pending requests that are no longer on active streams.
    try (Batcher resendBatcher = new Batcher()) {
      for (Map.Entry<Long, StreamAndRequest> entry : pending.entrySet()) {
        CommitWorkPhysicalStreamHandler requestHandler = entry.getValue().handler;
        checkState(requestHandler != currentPhysicalStream);
        if (requestHandler != null && closingPhysicalStreams.contains(requestHandler)) {
          LOG.debug(
              "Not resending request that is active on background half-closing physical stream.");
          continue;
        }
        long id = entry.getKey();
        PendingRequest request = entry.getValue().request;
        if (!resendBatcher.canAccept(request.getBytes())) {
          resendBatcher.flush();
        }
        resendBatcher.add(id, request);
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
  protected synchronized void sendHealthCheck() throws WindmillStreamShutdownException {
    if (currentPhysicalStream != null && currentPhysicalStream.hasPendingRequests()) {
      StreamingCommitWorkRequest.Builder builder = StreamingCommitWorkRequest.newBuilder();
      builder.addCommitChunkBuilder().setRequestId(HEARTBEAT_REQUEST_ID);
      trySend(builder.build());
    }
  }

  private class CommitWorkPhysicalStreamHandler extends PhysicalStreamHandler {
    @Override
    @SuppressWarnings("ReferenceEquality")
    public void onResponse(StreamingCommitResponse response) {
      CommitCompletionFailureHandler failureHandler = new CommitCompletionFailureHandler();
      for (int i = 0; i < response.getRequestIdCount(); ++i) {
        long requestId = response.getRequestId(i);
        if (requestId == HEARTBEAT_REQUEST_ID) {
          continue;
        }

        // From windmill.proto: Indices must line up with the request_id field, but trailing OKs may
        // be omitted.
        CommitStatus commitStatus =
            i < response.getStatusCount() ? response.getStatus(i) : CommitStatus.OK;

        @Nullable StreamAndRequest entry = pending.remove(requestId);
        if (entry == null) {
          if (logMissingResponse) {
            LOG.error("Got unknown commit request ID: {}", requestId);
          }
          continue;
        }
        if (entry.handler != this) {
          LOG.error("Got commit request id {} on unexpected stream", requestId);
        }
        PendingRequest pendingRequest = entry.request;
        try {
          pendingRequest.completeWithStatus(commitStatus);
        } catch (RuntimeException e) {
          // Catch possible exceptions to ensure that an exception for one commit does not prevent
          // other commits from being processed. Aggregate all the failures to throw after
          // processing the response if they exist.
          LOG.warn("Exception while processing commit response.", e);
          failureHandler.addError(commitStatus, e);
        }
      }

      failureHandler.throwIfNonEmpty();
    }

    @SuppressWarnings("ReferenceEquality")
    private boolean belongsToThisHandler(StreamAndRequest streamAndRequest) {
      return streamAndRequest.handler == this;
    }

    @Override
    public boolean hasPendingRequests() {
      return pending.entrySet().stream().anyMatch(e -> belongsToThisHandler(e.getValue()));
    }

    @Override
    @SuppressWarnings("ReferenceEquality")
    public void onDone(Status status) {
      if (maxRetryDuration.compareTo(Duration.ZERO) > 0) {
        // Remove the requests that have exceeded the retry time so they are not retried.
        long nowNanos = System.nanoTime();
        long maxRetryDurationNanos = maxRetryDuration.toNanos();
        Iterator<Map.Entry<Long, StreamAndRequest>> iterator = pending.entrySet().iterator();
        int keptRequests = 0, removedRequests = 0;
        while (iterator.hasNext()) {
          StreamAndRequest streamAndRequest = checkNotNull(iterator.next().getValue());
          PendingRequest pendingRequest = streamAndRequest.request;
          if (!belongsToThisHandler(streamAndRequest)
              || nowNanos - pendingRequest.getStartTimeNanos() < maxRetryDurationNanos) {
            ++keptRequests;
            continue;
          }
          ++removedRequests;
          iterator.remove();
          try {
            pendingRequest.completeWithStatus(CommitStatus.ABORTED);
          } catch (RuntimeException e) {
            LOG.warn("Exception while aborting commit due to retry timeout.", e);
          }
        }
        if (removedRequests > 0) {
          LOG.info(
              "Aborting {} commits which have exceeded retry deadline, kept {}. Work will be retried as needed by service.",
              removedRequests,
              keptRequests);
        }
      }
      if (status.isOk() && hasPendingRequests()) {
        LOG.warn("Unexpected requests without responses on drained physical stream, retrying.");
      }
    }

    @Override
    @SuppressWarnings("ReferenceEquality")
    public void appendHtml(PrintWriter writer) {
      writer.format(
          "CommitWorkStream: %d pending",
          pending.entrySet().stream().filter(e -> e.getValue().handler == this).count());
    }
  }

  @Override
  protected PhysicalStreamHandler newResponseHandler() {
    return new CommitWorkPhysicalStreamHandler();
  }

  @Override
  protected synchronized void shutdownInternal() {
    logMissingResponse = false;
    Iterator<StreamAndRequest> pendingRequests = pending.values().iterator();
    while (pendingRequests.hasNext()) {
      PendingRequest pendingRequest = pendingRequests.next().request;
      pendingRequest.abort();
      pendingRequests.remove();
    }
  }

  private void flushInternal(Map<Long, PendingRequest> requests)
      throws WindmillStreamShutdownException {
    if (requests.isEmpty()) {
      return;
    }

    if (requests.size() == 1) {
      Map.Entry<Long, PendingRequest> elem = requests.entrySet().iterator().next();
      if (elem.getValue().getRequest().getSerializedSize()
          > AbstractWindmillStream.RPC_STREAM_CHUNK_SIZE) {
        issueMultiChunkRequest(elem.getKey(), elem.getValue());
      } else {
        issueSingleRequest(elem.getKey(), elem.getValue());
      }
    } else {
      issueBatchedRequest(requests);
    }
  }

  private void issueSingleRequest(long id, PendingRequest pendingRequest)
      throws WindmillStreamShutdownException {
    StreamingCommitWorkRequest.Builder requestBuilder = StreamingCommitWorkRequest.newBuilder();
    requestBuilder
        .addCommitChunkBuilder()
        .setComputationId(pendingRequest.getComputationId())
        .setRequestId(id)
        .setShardingKey(pendingRequest.shardingKey())
        .setSerializedWorkItemCommit(pendingRequest.serializedCommit());
    StreamingCommitWorkRequest chunk = requestBuilder.build();
    synchronized (this) {
      if (isShutdown) {
        pendingRequest.abort();
        return;
      }
      pending.put(
          id,
          new StreamAndRequest(
              (CommitWorkPhysicalStreamHandler) currentPhysicalStream, pendingRequest));
      trySend(chunk);
    }
  }

  private void issueBatchedRequest(Map<Long, PendingRequest> requests)
      throws WindmillStreamShutdownException {
    StreamingCommitWorkRequest.Builder requestBuilder = StreamingCommitWorkRequest.newBuilder();
    String lastComputation = null;
    for (Map.Entry<Long, PendingRequest> entry : requests.entrySet()) {
      PendingRequest request = entry.getValue();
      StreamingCommitRequestChunk.Builder chunkBuilder = requestBuilder.addCommitChunkBuilder();
      if (lastComputation == null || !lastComputation.equals(request.getComputationId())) {
        chunkBuilder.setComputationId(request.getComputationId());
        lastComputation = request.getComputationId();
      }
      chunkBuilder
          .setRequestId(entry.getKey())
          .setShardingKey(request.shardingKey())
          .setSerializedWorkItemCommit(request.serializedCommit());
    }
    StreamingCommitWorkRequest request = requestBuilder.build();
    synchronized (this) {
      if (isShutdown) {
        requests.forEach((ignored, pendingRequest) -> pendingRequest.abort());
        return;
      }
      for (Map.Entry<Long, PendingRequest> entry : requests.entrySet()) {
        pending.put(
            entry.getKey(),
            new StreamAndRequest(
                (CommitWorkPhysicalStreamHandler) currentPhysicalStream, entry.getValue()));
      }
      trySend(request);
    }
  }

  private void issueMultiChunkRequest(long id, PendingRequest pendingRequest)
      throws WindmillStreamShutdownException {
    checkNotNull(pendingRequest.getComputationId(), "Cannot commit WorkItem w/o a computationId.");
    ByteString serializedCommit = pendingRequest.serializedCommit();
    synchronized (this) {
      if (isShutdown) {
        pendingRequest.abort();
        return;
      }

      pending.put(
          id,
          new StreamAndRequest(
              (CommitWorkPhysicalStreamHandler) currentPhysicalStream, pendingRequest));
      for (int i = 0;
          i < serializedCommit.size();
          i += AbstractWindmillStream.RPC_STREAM_CHUNK_SIZE) {
        int end = i + AbstractWindmillStream.RPC_STREAM_CHUNK_SIZE;
        ByteString chunk = serializedCommit.substring(i, Math.min(end, serializedCommit.size()));
        StreamingCommitRequestChunk.Builder chunkBuilder =
            StreamingCommitRequestChunk.newBuilder()
                .setRequestId(id)
                .setSerializedWorkItemCommit(chunk)
                .setComputationId(pendingRequest.getComputationId())
                .setShardingKey(pendingRequest.shardingKey());
        int remaining = serializedCommit.size() - end;
        if (remaining > 0) {
          chunkBuilder.setRemainingBytesForWorkItem(remaining);
        }
        StreamingCommitWorkRequest requestChunk =
            StreamingCommitWorkRequest.newBuilder().addCommitChunk(chunkBuilder).build();

        if (!trySend(requestChunk)) {
          // The stream broke, don't try to send the rest of the chunks here.
          break;
        }
      }
    }
  }

  private static class PendingRequest {
    private final String computationId;
    private final WorkItemCommitRequest request;
    private final Consumer<CommitStatus> onDone;
    private final long startTimeNanos; // System.nanoTime() of when request began.

    private PendingRequest(
        String computationId, WorkItemCommitRequest request, Consumer<CommitStatus> onDone) {
      this.computationId = computationId;
      this.request = request;
      this.onDone = onDone;
      this.startTimeNanos = System.nanoTime();
    }

    String getComputationId() {
      return computationId;
    }

    WorkItemCommitRequest getRequest() {
      return request;
    }

    long getStartTimeNanos() {
      return startTimeNanos;
    }

    private long getBytes() {
      return (long) request.getSerializedSize() + computationId.length();
    }

    private ByteString serializedCommit() {
      return request.toByteString();
    }

    private void completeWithStatus(CommitStatus commitStatus) {
      onDone.accept(commitStatus);
    }

    private long shardingKey() {
      return request.getShardingKey();
    }

    private void abort() {
      completeWithStatus(CommitStatus.ABORTED);
    }
  }

  private static class CommitCompletionException extends RuntimeException {
    private CommitCompletionException(String message) {
      super(message);
    }
  }

  private static class CommitCompletionFailureHandler {
    private static final int MAX_PRINTABLE_ERRORS = 10;
    private final Map<Pair<CommitStatus, Class<? extends Throwable>>, Integer> errorCounter;
    private final EvictingQueue<Throwable> detailedErrors;

    private CommitCompletionFailureHandler() {
      this.errorCounter = new HashMap<>();
      this.detailedErrors = EvictingQueue.create(MAX_PRINTABLE_ERRORS);
    }

    private void addError(CommitStatus commitStatus, Throwable error) {
      errorCounter.compute(
          Pair.of(commitStatus, error.getClass()),
          (ignored, current) -> current == null ? 1 : current + 1);
      detailedErrors.add(error);
    }

    private void throwIfNonEmpty() {
      if (!errorCounter.isEmpty()) {
        String errorMessage =
            String.format(
                "Exception while processing commit response. ErrorCounter: %s; Details: %s",
                errorCounter, detailedErrors);
        throw new CommitCompletionException(errorMessage);
      }
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
      if (!canAccept(commitRequest.getSerializedSize() + computation.length())) {
        return false;
      }

      PendingRequest request = new PendingRequest(computation, commitRequest, onDone);
      add(idGenerator.incrementAndGet(), request);
      return true;
    }

    /** Flushes any pending work items to the wire. */
    @Override
    public void flush() {
      try {
        flushInternal(queue);
      } catch (WindmillStreamShutdownException e) {
        queue.forEach((ignored, request) -> request.abort());
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
