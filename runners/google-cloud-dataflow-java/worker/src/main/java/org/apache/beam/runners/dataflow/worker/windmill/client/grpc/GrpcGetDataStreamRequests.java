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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList.toImmutableList;

import com.google.auto.value.AutoOneOf;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamShutdownException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility data classes for {@link GrpcGetDataStream}. */
final class GrpcGetDataStreamRequests {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcGetDataStreamRequests.class);
  private static final int STREAM_CANCELLED_ERROR_LOG_LIMIT = 3;

  private GrpcGetDataStreamRequests() {}

  private static String debugFormat(long value) {
    return String.format("%016x", value);
  }

  static class QueuedRequest {
    private final long id;
    private final ComputationOrGlobalDataRequest dataRequest;
    private AppendableInputStream responseStream;

    private QueuedRequest(long id, ComputationOrGlobalDataRequest dataRequest) {
      this.id = id;
      this.dataRequest = dataRequest;
      responseStream = new AppendableInputStream();
    }

    static QueuedRequest forComputation(
        long id, String computation, KeyedGetDataRequest keyedGetDataRequest) {
      ComputationGetDataRequest computationGetDataRequest =
          ComputationGetDataRequest.newBuilder()
              .setComputationId(computation)
              .addRequests(keyedGetDataRequest)
              .build();
      return new QueuedRequest(
          id, ComputationOrGlobalDataRequest.computation(computationGetDataRequest));
    }

    static QueuedRequest global(long id, GlobalDataRequest globalDataRequest) {
      return new QueuedRequest(id, ComputationOrGlobalDataRequest.global(globalDataRequest));
    }

    static Comparator<QueuedRequest> globalRequestsFirst() {
      return (QueuedRequest r1, QueuedRequest r2) -> {
        boolean r1gd = r1.dataRequest.isGlobal();
        boolean r2gd = r2.dataRequest.isGlobal();
        return r1gd == r2gd ? 0 : (r1gd ? -1 : 1);
      };
    }

    long id() {
      return id;
    }

    long byteSize() {
      return dataRequest.serializedSize();
    }

    AppendableInputStream getResponseStream() {
      return responseStream;
    }

    void resetResponseStream() {
      this.responseStream = new AppendableInputStream();
    }

    public ComputationOrGlobalDataRequest getDataRequest() {
      return dataRequest;
    }

    void addToStreamingGetDataRequest(Windmill.StreamingGetDataRequest.Builder builder) {
      builder.addRequestId(id);
      if (dataRequest.isForComputation()) {
        builder.addStateRequest(dataRequest.computation());
      } else {
        builder.addGlobalDataRequest(dataRequest.global());
      }
    }

    @Override
    public final String toString() {
      return "QueuedRequest{" + "dataRequest=" + dataRequest + ", id=" + id + '}';
    }
  }

  /**
   * Represents a batch of queued requests. Methods are not thread-safe unless commented otherwise.
   */
  static class QueuedBatch {
    private final List<QueuedRequest> requests = new ArrayList<>();
    private final CountDownLatch sent = new CountDownLatch(1);
    private long byteSize = 0;
    private volatile boolean finalized = false;
    private volatile boolean failed = false;

    /** Returns a read-only view of requests. */
    List<QueuedRequest> requestsReadOnly() {
      return Collections.unmodifiableList(requests);
    }

    /**
     * Converts the batch to a {@link
     * org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetDataRequest}.
     */
    Windmill.StreamingGetDataRequest asGetDataRequest() {
      Windmill.StreamingGetDataRequest.Builder builder =
          Windmill.StreamingGetDataRequest.newBuilder();

      requests.stream()
          // Put all global data requests first because there is only a single repeated field for
          // request ids and the initial ids correspond to global data requests if they are present.
          .sorted(QueuedRequest.globalRequestsFirst())
          .forEach(request -> request.addToStreamingGetDataRequest(builder));

      return builder.build();
    }

    boolean isEmpty() {
      return requests.isEmpty();
    }

    int requestsCount() {
      return requests.size();
    }

    long byteSize() {
      return byteSize;
    }

    boolean isFinalized() {
      return finalized;
    }

    void markFinalized() {
      finalized = true;
    }

    /** Adds a request to the batch. */
    void addRequest(QueuedRequest request) {
      requests.add(request);
      byteSize += request.byteSize();
    }

    /**
     * Let waiting for threads know that the request has been successfully sent.
     *
     * @implNote Thread safe.
     */
    void notifySent() {
      sent.countDown();
    }

    /**
     * Let waiting for threads know that a failure occurred.
     *
     * @implNote Thread safe.
     */
    void notifyFailed() {
      failed = true;
      sent.countDown();
    }

    /**
     * Block until notified of a successful send via {@link #notifySent()} or a non-retryable
     * failure via {@link #notifyFailed()}. On failure, throw an exception for waiters.
     *
     * @implNote Thread safe.
     */
    void waitForSendOrFailNotification() throws InterruptedException {
      sent.await();
      if (failed) {
        ImmutableList<String> cancelledRequests = createStreamCancelledErrorMessages();
        if (!cancelledRequests.isEmpty()) {
          LOG.error("Requests failed for the following batches: {}", cancelledRequests);
          throw new WindmillStreamShutdownException(
              "Requests failed for batch containing "
                  + String.join(", ", cancelledRequests)
                  + " ... requests. This is most likely due to the stream being explicitly closed"
                  + " which happens when the work is marked as invalid on the streaming"
                  + " backend when key ranges shuffle around. This is transient and corresponding"
                  + " work will eventually be retried.");
        }

        throw new WindmillStreamShutdownException("Stream was shutdown while waiting for send.");
      }
    }

    private ImmutableList<String> createStreamCancelledErrorMessages() {
      return requests.stream()
          .flatMap(
              request -> {
                switch (request.getDataRequest().getKind()) {
                  case GLOBAL:
                    return Stream.of("GetSideInput=" + request.getDataRequest().global());
                  case COMPUTATION:
                    return request.getDataRequest().computation().getRequestsList().stream()
                        .map(
                            keyedRequest ->
                                "KeyedGetState=["
                                    + "shardingKey="
                                    + debugFormat(keyedRequest.getShardingKey())
                                    + "cacheToken="
                                    + debugFormat(keyedRequest.getCacheToken())
                                    + "workToken"
                                    + debugFormat(keyedRequest.getWorkToken())
                                    + "]");
                  default:
                    // Will never happen switch is exhaustive.
                    throw new IllegalStateException();
                }
              })
          .limit(STREAM_CANCELLED_ERROR_LOG_LIMIT)
          .collect(toImmutableList());
    }
  }

  @AutoOneOf(ComputationOrGlobalDataRequest.Kind.class)
  abstract static class ComputationOrGlobalDataRequest {
    static ComputationOrGlobalDataRequest computation(
        ComputationGetDataRequest computationGetDataRequest) {
      return AutoOneOf_GrpcGetDataStreamRequests_ComputationOrGlobalDataRequest.computation(
          computationGetDataRequest);
    }

    static ComputationOrGlobalDataRequest global(GlobalDataRequest globalDataRequest) {
      return AutoOneOf_GrpcGetDataStreamRequests_ComputationOrGlobalDataRequest.global(
          globalDataRequest);
    }

    abstract Kind getKind();

    abstract ComputationGetDataRequest computation();

    abstract GlobalDataRequest global();

    boolean isGlobal() {
      return getKind() == Kind.GLOBAL;
    }

    boolean isForComputation() {
      return getKind() == Kind.COMPUTATION;
    }

    long serializedSize() {
      switch (getKind()) {
        case GLOBAL:
          return global().getSerializedSize();
        case COMPUTATION:
          return computation().getSerializedSize();
          // this will never happen since the switch is exhaustive.
        default:
          throw new UnsupportedOperationException("unknown dataRequest type.");
      }
    }

    enum Kind {
      COMPUTATION,
      GLOBAL
    }
  }
}
